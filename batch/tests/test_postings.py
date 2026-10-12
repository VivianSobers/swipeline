import unittest

from batch.postings import calculate_interchange_fee


class TestPostingsFee(unittest.TestCase):
    def test_fee_calculation_boundaries(self):
        # 100 cents * 1.5% = 1.5 cents -> rounds to 2 cents
        self.assertEqual(calculate_interchange_fee(100), 2)
        
        # 10,000 cents * 1.5% = 150.0 cents -> 150 cents
        self.assertEqual(calculate_interchange_fee(10000), 150)

        # 0 cents -> 0
        self.assertEqual(calculate_interchange_fee(0), 0)

        # 33 cents * 150 / 10000 = 0.495 -> 0
        self.assertEqual(calculate_interchange_fee(33), 0)

        # 34 cents * 150 / 10000 = 0.51 -> 1
        self.assertEqual(calculate_interchange_fee(34), 1)

        # Negative amounts (e.g. reversals/chargebacks)
        self.assertEqual(calculate_interchange_fee(-100), -2)
        self.assertEqual(calculate_interchange_fee(-10000), -150)

    def test_derive_posting_id(self):
        from batch.postings import derive_posting_id

        pid1 = derive_posting_id("evt-100")
        pid2 = derive_posting_id("evt-100")
        pid3 = derive_posting_id("evt-200")

        self.assertEqual(len(pid1), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in pid1))
        self.assertEqual(pid1, pid2)
        self.assertNotEqual(pid1, pid3)

    def test_reconstruct_postings(self):
        from batch.postings import reconstruct_postings

        tx = {
            "transaction_id": "tx-100",
            "run_id": "run-test",
            "issuer_bank_id": "bank-iss",
            "acquirer_bank_id": "bank-acq",
            "currency": "USD",
            "authorized_amount": 5000,
            "authorization_date": "2026-10-11",
            "cleared_amount": 5000,
            "status": "CLEARED",
        }
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "logical_seq": 2,
            "payload": {"amount": 5000},
        }

        # 1. CLEAR generates +5000 principal, +75 fee
        postings = reconstruct_postings(tx, [clear_evt], {}, "batch-01", "2026-10-11")
        self.assertEqual(len(postings), 1)
        p = postings[0]
        self.assertEqual(p["signed_principal"], 5000)
        self.assertEqual(p["signed_interchange"], 75)
        self.assertEqual(p["posting_date"], "2026-10-11")
        self.assertEqual(p["original_business_date"], "2026-10-11")

        # 2. Pre-clearing REVERSE generates 0 postings
        tx_pre_rev = dict(tx, status="REVERSED", cleared_amount=0)
        pre_rev_evt = {
            "event_id": "rev-100",
            "event_type": "REVERSE",
            "logical_seq": 2,
            "payload": {"amount": 5000},
        }
        postings_pre_rev = reconstruct_postings(tx_pre_rev, [pre_rev_evt], {}, "batch-01", "2026-10-11")
        self.assertEqual(len(postings_pre_rev), 0)

        # 3. Post-clearing REVERSE generates cancelling posting
        post_rev_evt = {
            "event_id": "rev-200",
            "event_type": "REVERSE",
            "logical_seq": 3,
            "payload": {"amount": 5000},
        }
        postings_post_rev = reconstruct_postings(
            tx, [clear_evt, post_rev_evt], {}, "batch-01", "2026-10-12"
        )
        self.assertEqual(len(postings_post_rev), 2)
        p_rev = [p for p in postings_post_rev if p["event_id"] == "rev-200"][0]
        self.assertEqual(p_rev["signed_principal"], -5000)
        self.assertEqual(p_rev["signed_interchange"], -75)
        self.assertEqual(p_rev["adjustment_reason"], "REVERSAL")

        # 4. Post-clearing CHARGEBACK generates chargeback posting
        cb_evt = {
            "event_id": "cb-200",
            "event_type": "CHARGEBACK",
            "logical_seq": 3,
            "payload": {"amount": 5000},
        }
        postings_cb = reconstruct_postings(
            tx, [clear_evt, cb_evt], {}, "batch-01", "2026-10-12"
        )
        self.assertEqual(len(postings_cb), 2)
        p_cb = [p for p in postings_cb if p["event_id"] == "cb-200"][0]
        self.assertEqual(p_cb["signed_principal"], -5000)
        self.assertEqual(p_cb["signed_interchange"], -75)
        self.assertEqual(p_cb["adjustment_reason"], "CHARGEBACK")

    def test_preserve_existing_posting_metadata(self):
        from batch.postings import reconstruct_postings, derive_posting_id

        tx = {
            "transaction_id": "tx-200",
            "run_id": "run-test",
            "issuer_bank_id": "bank-iss",
            "acquirer_bank_id": "bank-acq",
            "currency": "USD",
            "authorized_amount": 5000,
            "authorization_date": "2026-10-10",
            "cleared_amount": 5000,
            "status": "CLEARED",
        }
        clear_evt = {
            "event_id": "clear-200",
            "event_type": "CLEAR",
            "logical_seq": 2,
            "payload": {"amount": 5000},
        }
        clear_pid = derive_posting_id("clear-200")
        existing_clear = {
            "posting_id": clear_pid,
            "posting_revision": 1,
            "event_id": "clear-200",
            "transaction_id": "tx-200",
            "run_id": "run-test",
            "issuer_bank_id": "bank-iss",
            "acquirer_bank_id": "bank-acq",
            "currency": "USD",
            "signed_principal": 5000,
            "signed_interchange": 75,  # canonical 150 bps fee
            "fee_schedule_version": "150bps-v1",
            "original_business_date": "2026-10-10",
            "posting_date": "2026-10-10",
            "adjustment_reason": None,
            "batch_id": "batch-initial",
        }

        # Keyed by posting_id
        existing_postings = {clear_pid: existing_clear}

        rev_evt = {
            "event_id": "rev-300",
            "event_type": "REVERSE",
            "logical_seq": 3,
            "payload": {"amount": 5000},
        }

        # Re-run reconstruction with a new batch and new close_day
        postings = reconstruct_postings(
            tx, [clear_evt, rev_evt], existing_postings, "batch-second", "2026-10-12"
        )
        self.assertEqual(len(postings), 2)
        p_clear = [p for p in postings if p["event_id"] == "clear-200"][0]
        p_rev = [p for p in postings if p["event_id"] == "rev-300"][0]

        # Existing CLEAR must preserve original batch_id, posting_date, fee schedule, and fee
        self.assertEqual(p_clear["batch_id"], "batch-initial")
        self.assertEqual(p_clear["posting_date"], "2026-10-10")
        self.assertEqual(p_clear["signed_interchange"], 75)
        self.assertEqual(p_clear["fee_schedule_version"], "150bps-v1")

        # Reversal cancels the verified canonical fee (-75)
        self.assertEqual(p_rev["signed_interchange"], -75)
        self.assertEqual(p_rev["batch_id"], "batch-second")
        self.assertEqual(p_rev["posting_date"], "2026-10-12")

    def test_upsert_postings(self):
        import shutil
        import tempfile
        from batch.session import get_spark_session
        from batch.hudi_tables import SCHEMA_SETTLEMENT_POSTINGS
        from batch.postings import upsert_postings

        spark = get_spark_session("test-upsert-postings")
        temp_dir = tempfile.mkdtemp()
        table_path = f"{temp_dir}/settlement_postings"
        try:
            row = (
                "post-100",  # posting_id
                1,  # posting_revision
                "clear-100",  # event_id
                "tx-100",  # transaction_id
                "run-01",  # run_id
                "bank-iss",  # issuer_bank_id
                "bank-acq",  # acquirer_bank_id
                "USD",  # currency
                5000,  # signed_principal
                75,  # signed_interchange
                "150bps-v1",  # fee_schedule_version
                "2026-10-11",  # original_business_date
                "2026-10-11",  # posting_date
                None,  # adjustment_reason
                "batch-01",  # batch_id
            )
            df = spark.createDataFrame([row], schema=SCHEMA_SETTLEMENT_POSTINGS)
            instant = upsert_postings(spark, df, table_path=table_path)
            self.assertIsNotNone(instant)
            self.assertTrue(len(str(instant)) > 0)

            # Read snapshot and verify
            res = spark.read.format("hudi").load(table_path)
            self.assertEqual(res.count(), 1)
            row_out = res.collect()[0]
            self.assertEqual(row_out["posting_id"], "post-100")
            self.assertEqual(row_out["signed_principal"], 5000)
            self.assertEqual(row_out["signed_interchange"], 75)
            self.assertEqual(row_out["posting_date"], "2026-10-11")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()



class ExistingPostingIntegrityTests(unittest.TestCase):
    def fixture(self):
        from batch.postings import reconstruct_postings
        tx = {'transaction_id': 'tx', 'run_id': 'run', 'issuer_bank_id': 'iss', 'acquirer_bank_id': 'acq', 'currency': 'USD', 'authorized_amount': 100, 'cleared_amount': 100, 'authorization_date': '2026-10-11'}
        events = [{'event_id': 'clear', 'logical_seq': 2, 'event_type': 'CLEAR'}, {'event_id': 'reverse', 'logical_seq': 3, 'event_type': 'REVERSE'}]
        postings = reconstruct_postings(tx, events, {}, 'first', '2026-10-11')
        return tx, events, {p['posting_id']: p for p in postings}

    def test_existing_immutable_content_fails_closed(self):
        from batch.postings import reconstruct_postings
        for field, bad in [('signed_principal', 999), ('signed_interchange', 15), ('event_id', 'other'), ('currency', 'EUR'), ('fee_schedule_version', 'wrong'), ('adjustment_reason', 'wrong')]:
            for index in (0, 1):
                with self.subTest(field=field, index=index):
                    tx, events, existing = self.fixture()
                    list(existing.values())[index][field] = bad
                    with self.assertRaisesRegex(RuntimeError, 'posting'):
                        reconstruct_postings(tx, events, existing, 'retry', '2026-10-12')

    def test_correct_retry_preserves_dates_and_batch_assignments(self):
        from batch.postings import reconstruct_postings
        tx, events, existing = self.fixture()
        retry = reconstruct_postings(tx, events, existing, 'retry', '2026-10-12')
        self.assertEqual(retry, list(existing.values()))
        self.assertEqual(sum(p['signed_principal'] for p in retry), 0)
        self.assertEqual(sum(p['signed_interchange'] for p in retry), 0)

    def test_unexpected_existing_posting_fails_closed(self):
        from batch.postings import reconstruct_postings
        tx, events, existing = self.fixture()
        wrong = dict(next(iter(existing.values())), posting_id='unexpected', event_id='unexpected')
        existing['unexpected'] = wrong
        with self.assertRaisesRegex(RuntimeError, 'posting'):
            reconstruct_postings(tx, events, existing, 'retry', '2026-10-12')
