import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from batch.journal import parse_event_envelope
from batch.reducer import TransactionReducer
from batch.worker import process_postings, process_transactions, run_loop

ROOT = Path(__file__).resolve().parents[2]


def fixtures():
    auth = json.loads((ROOT / 'streaming/src/test/resources/decision.json').read_text())
    clear = json.loads((ROOT / 'schemas/fixtures/lifecycle_clear.json').read_text())
    clear.update(transaction_id=auth['transaction_id'], run_id=auth['run_id'],
                 parent_event_id=auth['event_id'], amount_minor=auth['amount_minor'],
                 merchant_id=auth['merchant_id'], issuer_bank_id=auth['issuer_bank_id'],
                 acquirer_bank_id=auth['acquirer_bank_id'], authorization_date=auth['authorization_date'],
                 reference_version=auth['reference_version'])
    cb = dict(clear, event_id='chargeback', kind='CHARGEBACK', lifecycle_seq=3,
              parent_event_id=clear['event_id'])
    return auth, clear, cb


class TestWireRegression(unittest.TestCase):
    def test_actual_decision_zero_source_sequence(self):
        auth, _, _ = fixtures()
        event = parse_event_envelope(json.dumps(auth))
        state, applied = TransactionReducer().reduce_transaction(None, [event])
        self.assertEqual(event['source_seq'], 0)
        self.assertEqual(event['logical_seq'], 1)
        self.assertEqual(event['validation_state'], 'VALID')
        self.assertEqual(state['status'], 'AUTHORIZED')
        self.assertEqual(state['authorized_amount'], 5891)
        self.assertEqual(state['issuer_bank_id'], 'bank-3')
        self.assertEqual(state['authorization_date'], '2026-10-08')
        self.assertEqual(state['decision_id'], auth['decision_id'])
        self.assertEqual(json.loads(event['payload_json']), auth)

    def test_actual_lifecycle_normalization(self):
        _, clear, _ = fixtures()
        event = parse_event_envelope(clear)
        self.assertEqual(event['validation_state'], 'VALID')
        self.assertEqual(event['logical_seq'], 2)
        self.assertEqual(event['event_type'], 'CLEAR')
        self.assertEqual(event['payload']['amount'], 5891)
        self.assertEqual(event['parent_event_id'], clear['parent_event_id'])

    def test_wrong_parent_and_changed_identity_do_not_apply(self):
        auth, clear, _ = fixtures()
        for field, value in [('parent_event_id', 'wrong'), ('amount_minor', 4),
                             ('issuer_bank_id', 'other'), ('authorization_date', '2020-01-01'),
                             ('run_id', 'other'), ('currency', 'EUR')]:
            with self.subTest(field=field):
                bad = dict(clear, **{field: value})
                state, accepted = TransactionReducer().reduce_transaction(None, [
                    parse_event_envelope(auth), parse_event_envelope(bad)])
                self.assertEqual(state['status'], 'AUTHORIZED')
                self.assertEqual(len(accepted), 1)

    def test_one_version_for_multiple_transitions(self):
        auth, clear, cb = fixtures()
        reducer = TransactionReducer()
        initial, _ = reducer.reduce_transaction(None, [parse_event_envelope(auth)])
        state, _ = reducer.reduce_transaction(initial, [parse_event_envelope(clear), parse_event_envelope(cb)])
        self.assertEqual(state['state_version'], initial['state_version'] + 1)


class TestFailureRegression(unittest.TestCase):
    def test_once_propagates_failure(self):
        with patch('batch.worker.run_batch', side_effect=RuntimeError('read failed')):
            with self.assertRaisesRegex(RuntimeError, 'read failed'):
                run_loop(MagicMock(), MagicMock(), once=True)

    def test_transaction_read_failure_propagates(self):
        spark = MagicMock()
        spark.read.format.return_value.load.side_effect = RuntimeError('storage failed')
        with patch('batch.worker.is_hudi_table', return_value=True):
            with self.assertRaisesRegex(RuntimeError, 'storage failed'):
                process_transactions(spark, [parse_event_envelope(fixtures()[0])], 'batch')

    def test_posting_read_failure_propagates(self):
        spark = MagicMock()
        spark.read.format.return_value.load.side_effect = RuntimeError('storage failed')
        with patch('batch.worker.is_hudi_table', return_value=True):
            with self.assertRaisesRegex(RuntimeError, 'storage failed'):
                process_postings(spark, {}, {}, 'batch', '2026-10-11')

    def test_storage_probe_failure_propagates(self):
        from batch.hudi_tables import is_hudi_table
        spark = MagicMock()
        spark.sparkContext._jsc.hadoopConfiguration.side_effect = RuntimeError('probe failed')
        with self.assertRaisesRegex(RuntimeError, 'probe failed'):
            is_hudi_table(spark, '/missing')

class TestDurableChains(unittest.TestCase):
    def test_split_batches_rebuild_journal_history_and_resume(self):
        import tempfile
        from batch.session import get_spark_session
        from batch.worker import run_batch
        from batch.journal import ingest_journal
        spark = get_spark_session('durable-chain-regression')
        auth, clear, cb = fixtures()
        # Three independent deliveries: auth+chargeback, lifecycle first, auth first.
        chains = []
        for i in range(3):
            a, c, b = [dict(x, transaction_id=f'tx-{i}', event_id=f'{x["event_id"]}-{i}')
                       for x in (auth, clear, cb)]
            c['parent_event_id'] = a['event_id']
            b['parent_event_id'] = c['event_id']
            chains.append((a, c, b))
        with tempfile.TemporaryDirectory() as tmp:
            paths = {name: f'{tmp}/{name}' for name in ('journal', 'transactions', 'postings')}
            coordinator = MagicMock()
            coordinator.acquire_advisory_lock.return_value = True
            coordinator.get_pending_batch.return_value = None
            coordinator.start_batch.side_effect = ['first', 'second', 'third']
            assignments = {}
            def persist(batch_id, rows):
                stored = assignments.setdefault(batch_id, {})
                for key, value in rows.items():
                    stored.setdefault(key, dict(value))
                return stored
            coordinator.persist_posting_assignments.side_effect = persist
            def raw(records):
                return spark.createDataFrame([(json.dumps(r),) for r in records], ['value'])
            def journal(*args, **kwargs):
                return ingest_journal(*args, **kwargs, table_path=paths['journal'])
            with patch('batch.worker.TABLE_LEDGER_EVENTS', paths['journal']), \
                 patch('batch.worker.TABLE_TRANSACTIONS', paths['transactions']), \
                 patch('batch.worker.TABLE_SETTLEMENT_POSTINGS', paths['postings']), \
                 patch('batch.worker.ingest_journal', side_effect=journal), \
                 patch('batch.worker.read_kafka_range') as read:
                read.return_value = raw([chains[0][0], chains[0][2], chains[1][1], chains[2][0]])
                run_batch(spark, coordinator, start_offsets={'t': {'0': 0}}, end_offsets={'t': {'0': 4}})
                read.return_value = raw([chains[0][1], chains[1][0], chains[2][1]])
                # Crash after transaction commit; resume must derive all accepted events again.
                with patch('batch.worker.upsert_postings', side_effect=RuntimeError('posting crash')):
                    with self.assertRaisesRegex(RuntimeError, 'posting crash'):
                        run_batch(spark, coordinator, start_offsets={'t': {'0': 4}}, end_offsets={'t': {'0': 7}})
                coordinator.get_pending_batch.return_value = dict(batch_id='second', state='TRANSACTIONS_COMMITTED',
                    starting_offsets={'t': {'0': 4}}, ending_offsets={'t': {'0': 7}}, posting_assignments={})
                run_batch(spark, coordinator)
                states = {r.transaction_id: r.asDict() for r in spark.read.format('hudi').load(paths['transactions']).collect()}
                postings = spark.read.format('hudi').load(paths['postings']).collect()
                self.assertEqual(len(postings), 4)
                for i, chain in enumerate(chains):
                    expected, _ = TransactionReducer().reduce_transaction(None, list(map(parse_event_envelope, chain if i == 0 else chain[:2])))
                    self.assertEqual(states[f'tx-{i}']['state_hash'], expected['state_hash'])
                before = [r.asDict() for r in postings]
                version = states['tx-0']['state_version']
                run_batch(spark, coordinator)
                after = [r.asDict() for r in spark.read.format('hudi').load(paths['postings']).collect()]
                self.assertEqual(sorted(before, key=lambda r:r['posting_id']), sorted(after, key=lambda r:r['posting_id']))
                self.assertEqual(spark.read.format('hudi').load(paths['transactions']).filter("transaction_id='tx-0'").collect()[0].state_version, version)
                coordinator.get_pending_batch.return_value = None
                conflict = dict(chains[0][2], event_id='000-late-reversal', kind='REVERSE')
                read.return_value = raw([conflict, dict(chains[0][2], event_id='000-late-chargeback')])
                run_batch(spark, coordinator, start_offsets={'t': {'0': 7}}, end_offsets={'t': {'0': 8}})
                final = spark.read.format('hudi').load(paths['transactions']).filter("transaction_id='tx-0'").collect()[0]
                self.assertEqual(final.status, 'CHARGED_BACK')
                self.assertEqual(final.state_version, version)
                self.assertEqual(spark.read.format('hudi').load(paths['postings']).count(), 4)


class TestAssignmentRegression(unittest.TestCase):
    def test_persisted_assignment_survives_changed_batch_day(self):
        from batch.postings import reconstruct_postings
        auth, clear, _ = fixtures()
        state, accepted = TransactionReducer().reduce_transaction(None, list(map(parse_event_envelope, (auth, clear))))
        p = reconstruct_postings(state, accepted, {}, 'first', '2040-01-01')[0]
        self.assertEqual(p['posting_date'], clear['close_day'])

    def test_coordinator_persists_assignments(self):
        from batch.db import BatchCoordinator
        conn = MagicMock()
        conn.closed = False
        cursor = conn.cursor.return_value.__enter__.return_value
        assignment = {'p': {'posting_date': '2026-10-11', 'batch_id': 'batch'}}
        cursor.fetchone.return_value = (assignment,)
        db = BatchCoordinator()
        db._conn = conn
        self.assertEqual(db.persist_posting_assignments('batch', assignment), assignment)
        self.assertIn('posting_assignments', cursor.execute.call_args[0][0])

    def test_completed_offsets_query_excludes_pending(self):
        from batch.db import BatchCoordinator
        conn = MagicMock(); conn.closed = False
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None
        db = BatchCoordinator(); db._conn = conn
        self.assertEqual(db.get_latest_offsets(), {})
        self.assertIn("WHERE state = 'COMPLETE'", cursor.execute.call_args[0][0])

    def test_competing_terminal_events_only_one_applies(self):
        auth, clear, cb = fixtures()
        reverse = dict(cb, event_id='reverse', kind='REVERSE')
        state, accepted = TransactionReducer().reduce_transaction(None, list(map(parse_event_envelope, (auth, clear, cb, reverse))))
        from batch.postings import reconstruct_postings
        postings = reconstruct_postings(state, accepted, {}, 'batch', '2026-10-11')
        self.assertEqual(len(postings), 2)
        self.assertEqual(sum(p['signed_principal'] for p in postings), 0)

class TestAdditionalSafety(unittest.TestCase):
    def test_journal_rejects_changed_business_hash_for_existing_event(self):
        from batch.journal import ingest_journal
        spark = MagicMock()
        auth = fixtures()[0]
        raw = MagicMock()
        row = MagicMock()
        row.__contains__.return_value = True
        row.__getitem__.return_value = json.dumps(dict(auth, payload_hash='changed'))
        raw.collect.return_value = [row]
        spark.read.format.return_value.load.return_value.select.return_value.collect.return_value = [
            MagicMock(asDict=MagicMock(return_value={'event_id': auth['event_id'], 'payload_hash': auth['payload_hash']}))]
        spark.createDataFrame.return_value.join.return_value.count.return_value = 0
        with patch('batch.hudi_tables.is_hudi_table', return_value=True), patch('batch.hudi_tables.write_hudi_table') as write:
            with self.assertRaisesRegex(RuntimeError, 'conflict'):
                ingest_journal(spark, raw, 'batch', '2026-10-11')
            write.assert_not_called()

    def test_fee_has_no_decimal_precision_limit(self):
        from batch.postings import calculate_interchange_fee
        amount = 10 ** 40 + 33
        self.assertEqual(calculate_interchange_fee(amount), (amount * 150 + 5000) // 10000)

    def test_wire_missing_parent_remains_unapplied(self):
        auth, clear, _ = fixtures()
        del clear['parent_event_id']
        state, applied = TransactionReducer().reduce_transaction(None, list(map(parse_event_envelope, (auth, clear))))
        self.assertEqual(state['status'], 'AUTHORIZED')
        self.assertEqual(len(applied), 1)

class TestWorkerBoundaryRegression(unittest.TestCase):
    def test_default_reads_both_topics_with_actual_partition_vector(self):
        from batch.worker import run_batch
        coordinator = MagicMock()
        coordinator.acquire_advisory_lock.return_value = True
        coordinator.get_pending_batch.return_value = None
        coordinator.get_latest_offsets.return_value = {}
        coordinator.start_batch.return_value = 'batch'
        ends = {'swipeline.decisions.v1': {'7': 4}, 'swipeline.lifecycle.v1': {'2': 1}}
        with patch('batch.worker.get_kafka_end_offsets', return_value=ends) as offsets, \
             patch('batch.worker.read_kafka_range') as read, patch('batch.worker._finish_batch'):
            run_batch(MagicMock(), coordinator)
        self.assertEqual(offsets.call_args[0][2], ['swipeline.decisions.v1', 'swipeline.lifecycle.v1'])
        self.assertEqual(read.call_args[0][3], {t: {p: 0 for p in parts} for t, parts in ends.items()})
        self.assertEqual(read.call_args[0][4], ends)

    def test_failures_never_complete_or_hold_writer_lock(self):
        from batch.worker import run_batch
        for stage in ('ingest_journal', 'reconstruct_transaction_chains', 'process_transactions', 'process_postings'):
            with self.subTest(stage=stage):
                coordinator = MagicMock()
                coordinator.acquire_advisory_lock.return_value = True
                coordinator.get_pending_batch.return_value = None
                with patch('batch.worker.read_kafka_range'), \
                     patch('batch.worker.ingest_journal', return_value='j'), \
                     patch('batch.worker.reconstruct_transaction_chains', return_value=({}, {})), \
                     patch('batch.worker.process_transactions', return_value='t'), \
                     patch('batch.worker.process_postings', return_value='p'), \
                     patch('batch.worker.' + stage, side_effect=RuntimeError('failed read')):
                    with self.assertRaisesRegex(RuntimeError, 'failed read'):
                        run_batch(MagicMock(), coordinator, start_offsets={'t': {'0': 0}}, end_offsets={'t': {'0': 1}})
                coordinator.complete_batch.assert_not_called()
                coordinator.release_advisory_lock.assert_called_once_with(72002)

    def test_competing_terminal_event_is_quarantined(self):
        auth, clear, cb = fixtures()
        reverse = dict(cb, event_id='reverse', kind='REVERSE')
        reducer = TransactionReducer()
        state, accepted = reducer.reduce_transaction(None, list(map(parse_event_envelope, (auth, clear, cb, reverse))))
        self.assertEqual(state['status'], 'CHARGED_BACK')
        self.assertEqual(len(accepted), 3)
        self.assertEqual(reducer.quarantine_count, 1)

class TestRetryMetadata(unittest.TestCase):
    def test_missing_posting_assignment_survives_calendar_advance(self):
        auth, clear, _ = fixtures()
        del clear['close_day']
        state, accepted = TransactionReducer().reduce_transaction(None, list(map(parse_event_envelope, (auth, clear))))
        states, events = {state['transaction_id']: state}, {state['transaction_id']: accepted}
        saved = {}
        coordinator = MagicMock()
        def persist(batch_id, rows):
            for key, row in rows.items():
                saved.setdefault(key, dict(row))
            return saved
        coordinator.persist_posting_assignments.side_effect = persist
        spark = MagicMock()
        with patch('batch.worker.is_hudi_table', return_value=False), \
             patch('batch.worker.upsert_postings', side_effect=[RuntimeError('crash'), 'instant']):
            with self.assertRaisesRegex(RuntimeError, 'crash'):
                process_postings(spark, states, events, 'batch', '2026-10-11', coordinator=coordinator)
            process_postings(spark, states, events, 'batch', '2026-10-12', coordinator=coordinator)
        rows = spark.createDataFrame.call_args[0][0]
        self.assertEqual(rows[0][12], '2026-10-11')
        self.assertEqual(rows[0][14], 'batch')

    def test_main_closes_resources_when_once_fails(self):
        from batch.worker import main
        spark, coordinator = MagicMock(), MagicMock()
        with patch('batch.worker.parse_args', return_value=MagicMock(poll='1s', once=True)), \
             patch('batch.session.get_spark_session', return_value=spark), \
             patch('batch.worker.BatchCoordinator', return_value=coordinator), \
             patch('batch.worker.run_loop', side_effect=RuntimeError('failure')), \
             patch('batch.worker.signal.signal'):
            with self.assertRaisesRegex(RuntimeError, 'failure'):
                main()
        coordinator.close.assert_called_once()
        spark.stop.assert_called_once()

class TestCheckpointInstants(unittest.TestCase):
    def test_identical_state_keeps_verified_commit_instant(self):
        auth = parse_event_envelope(fixtures()[0])
        state, _ = TransactionReducer().reduce_transaction(None, [auth])
        state['_hoodie_commit_time'] = '20261011100000'
        with patch('batch.worker.read_snapshot', return_value={state['transaction_id']: state}):
            self.assertEqual(process_transactions(MagicMock(), [auth], 'retry'), '20261011100000')

    def test_existing_postings_keep_verified_commit_instant(self):
        from batch.postings import reconstruct_postings
        auth, clear, _ = fixtures()
        state, accepted = TransactionReducer().reduce_transaction(None, list(map(parse_event_envelope, (auth, clear))))
        posting = reconstruct_postings(state, accepted, {}, 'first', '2026-10-11')[0]
        posting['_hoodie_commit_time'] = '20261011100001'
        with patch('batch.worker.read_snapshot', return_value={posting['posting_id']: posting}):
            self.assertEqual(process_postings(MagicMock(), {state['transaction_id']: state}, {state['transaction_id']: accepted}, 'retry', '2026-10-12'), '20261011100001')

class CanonicalDecisionIdentityTests(unittest.TestCase):
    def test_flat_final_preserves_scheduler_parent_identity(self):
        from batch.journal import parse_event_envelope
        parsed = parse_event_envelope({'event_id': 'request-id', 'decision_id': 'decision-id', 'decision': 'APPROVE', 'transaction_id': 'tx', 'run_id': 'run', 'source_seq': 0, 'event_time_ms': 1, 'amount_minor': 100, 'payload_hash': 'hash'})
        self.assertEqual(parsed['event_id'], 'request-id')
