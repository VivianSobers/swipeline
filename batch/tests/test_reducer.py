import unittest

from batch.reducer import TransactionReducer


class TestTransactionReducer(unittest.TestCase):
    def setUp(self):
        self.reducer = TransactionReducer()
        self.base_auth = {
            "schema_version": "1.0.0",
            "event_id": "dec-100",
            "event_type": "AUTH_DECISION",
            "run_id": "run-test",
            "transaction_id": "tx-100",
            "source_seq": 1,
            "logical_seq": 1,
            "event_time_ms": 1770000000000,
            "produced_at_ms": 1770000000000,
            "payload_hash": "hash-auth",
            "payload": {
                "card_id": "card-1",
                "merchant_id": "merch-1",
                "mcc": "5411",
                "issuer_bank_id": "bank-iss",
                "acquirer_bank_id": "bank-acq",
                "currency": "USD",
                "amount": 5000,
                "decision": "APPROVE",
                "decision_source": "ENGINE",
                "authorization_date": "2026-10-11",
            },
        }

    def test_auth_approve(self):
        state, applied = self.reducer.reduce_transaction(None, [self.base_auth])
        self.assertIsNotNone(state)
        self.assertEqual(state["status"], "AUTHORIZED")
        self.assertEqual(state["latest_applied_sequence"], 1)
        self.assertEqual(state["authorized_amount"], 5000)
        self.assertEqual(state["cleared_amount"], 0)
        self.assertEqual(len(applied), 1)

    def test_auth_decline(self):
        auth_decline = dict(self.base_auth)
        auth_decline["payload"] = dict(self.base_auth["payload"])
        auth_decline["payload"]["decision"] = "DECLINE"
        state, applied = self.reducer.reduce_transaction(None, [auth_decline])
        self.assertIsNotNone(state)
        self.assertEqual(state["status"], "DECLINED")
        self.assertEqual(state["latest_applied_sequence"], 1)

    def test_clearing(self):
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "run_id": "run-test",
            "transaction_id": "tx-100",
            "source_seq": 2,
            "logical_seq": 2,
            "event_time_ms": 1770000005000,
            "produced_at_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }
        state, applied = self.reducer.reduce_transaction(None, [self.base_auth, clear_evt])
        self.assertEqual(state["status"], "CLEARED")
        self.assertEqual(state["latest_applied_sequence"], 2)
        self.assertEqual(state["cleared_amount"], 5000)
        self.assertEqual(len(applied), 2)

    def test_pre_clearing_reversal(self):
        reverse_evt = {
            "event_id": "rev-100",
            "event_type": "REVERSE",
            "run_id": "run-test",
            "transaction_id": "tx-100",
            "source_seq": 2,
            "logical_seq": 2,
            "event_time_ms": 1770000002000,
            "produced_at_ms": 1770000002000,
            "payload_hash": "hash-rev",
            "payload": {"amount": 5000},
        }
        state, applied = self.reducer.reduce_transaction(None, [self.base_auth, reverse_evt])
        self.assertEqual(state["status"], "REVERSED")
        self.assertEqual(state["latest_applied_sequence"], 2)
        self.assertEqual(state["reversed_amount"], 5000)
        self.assertEqual(state["cleared_amount"], 0)
        self.assertEqual(len(applied), 2)

    def test_post_clearing_reversal(self):
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "run_id": "run-test",
            "transaction_id": "tx-100",
            "source_seq": 2,
            "logical_seq": 2,
            "event_time_ms": 1770000005000,
            "produced_at_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }
        reverse_evt = {
            "event_id": "rev-100",
            "event_type": "REVERSE",
            "run_id": "run-test",
            "transaction_id": "tx-100",
            "source_seq": 3,
            "logical_seq": 3,
            "event_time_ms": 1770000010000,
            "produced_at_ms": 1770000010000,
            "payload_hash": "hash-rev",
            "payload": {"amount": 5000},
        }
        state, applied = self.reducer.reduce_transaction(None, [self.base_auth, clear_evt, reverse_evt])
        self.assertEqual(state["status"], "REVERSED")
        self.assertEqual(state["latest_applied_sequence"], 3)
        self.assertEqual(state["cleared_amount"], 5000)
        self.assertEqual(state["reversed_amount"], 5000)
        self.assertEqual(len(applied), 3)

    def test_post_clearing_chargeback(self):
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "run_id": "run-test",
            "transaction_id": "tx-100",
            "source_seq": 2,
            "logical_seq": 2,
            "event_time_ms": 1770000005000,
            "produced_at_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }
        cb_evt = {
            "event_id": "cb-100",
            "event_type": "CHARGEBACK",
            "run_id": "run-test",
            "transaction_id": "tx-100",
            "source_seq": 3,
            "logical_seq": 3,
            "event_time_ms": 1770000010000,
            "produced_at_ms": 1770000010000,
            "payload_hash": "hash-cb",
            "payload": {"amount": 5000},
        }
        state, applied = self.reducer.reduce_transaction(None, [self.base_auth, clear_evt, cb_evt])
        self.assertEqual(state["status"], "CHARGED_BACK")
        self.assertEqual(state["latest_applied_sequence"], 3)
        self.assertEqual(state["cleared_amount"], 5000)
        self.assertEqual(state["charged_back_amount"], 5000)
        self.assertEqual(len(applied), 3)

    def test_quarantine_clear_after_pre_clearing_reverse(self):
        rev_evt = {
            "event_id": "rev-100",
            "event_type": "REVERSE",
            "logical_seq": 2,
            "event_time_ms": 1770000002000,
            "payload_hash": "hash-rev",
            "payload": {"amount": 5000},
        }
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "logical_seq": 3,
            "event_time_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }
        state, applied = self.reducer.reduce_transaction(None, [self.base_auth, rev_evt, clear_evt])
        self.assertEqual(state["status"], "REVERSED")
        self.assertEqual(state["latest_applied_sequence"], 2)
        self.assertEqual(len(applied), 2)
        self.assertEqual(self.reducer.quarantine_count, 1)

    def test_quarantine_clear_on_declined(self):
        auth_decline = dict(self.base_auth)
        auth_decline["payload"] = dict(self.base_auth["payload"])
        auth_decline["payload"]["decision"] = "DECLINE"
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "logical_seq": 2,
            "event_time_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }
        state, applied = self.reducer.reduce_transaction(None, [auth_decline, clear_evt])
        self.assertEqual(state["status"], "DECLINED")
        self.assertEqual(state["latest_applied_sequence"], 1)
        self.assertEqual(len(applied), 1)
        self.assertEqual(self.reducer.quarantine_count, 1)

    def test_quarantine_chargeback_on_reversed(self):
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "logical_seq": 2,
            "event_time_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }
        rev_evt = {
            "event_id": "rev-100",
            "event_type": "REVERSE",
            "logical_seq": 3,
            "event_time_ms": 1770000007000,
            "payload_hash": "hash-rev",
            "payload": {"amount": 5000},
        }
        cb_evt = {
            "event_id": "cb-100",
            "event_type": "CHARGEBACK",
            "logical_seq": 4,
            "event_time_ms": 1770000010000,
            "payload_hash": "hash-cb",
            "payload": {"amount": 5000},
        }
        state, applied = self.reducer.reduce_transaction(None, [self.base_auth, clear_evt, rev_evt, cb_evt])
        self.assertEqual(state["status"], "REVERSED")
        self.assertEqual(state["latest_applied_sequence"], 3)
        self.assertEqual(len(applied), 3)
        self.assertEqual(self.reducer.quarantine_count, 1)

    def test_out_of_order_chargeback_resolution(self):
        cb_evt = {
            "event_id": "cb-100",
            "event_type": "CHARGEBACK",
            "logical_seq": 3,
            "event_time_ms": 1770000010000,
            "payload_hash": "hash-cb",
            "payload": {"amount": 5000},
        }
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "logical_seq": 2,
            "event_time_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }

        # Step 1: Chargeback arrives before clearing (seq 3 with seq 2 missing)
        state_batch1, applied1 = self.reducer.reduce_transaction(None, [self.base_auth, cb_evt])
        self.assertEqual(state_batch1["status"], "AUTHORIZED")
        self.assertEqual(state_batch1["latest_applied_sequence"], 1)
        self.assertEqual(len(applied1), 1)
        self.assertEqual(self.reducer.unapplied_count, 1)

        # Step 2: Clearing arrives in next batch. Full sequence is re-evaluated.
        state_batch2, applied2 = self.reducer.reduce_transaction(state_batch1, [clear_evt, cb_evt])
        self.assertEqual(state_batch2["status"], "CHARGED_BACK")
        self.assertEqual(state_batch2["latest_applied_sequence"], 3)
        self.assertEqual(state_batch2["cleared_amount"], 5000)
        self.assertEqual(state_batch2["charged_back_amount"], 5000)
        self.assertEqual(len(applied2), 2)

    def test_event_before_auth(self):
        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "logical_seq": 2,
            "event_time_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }
        # Clear arrives before auth
        state, applied = self.reducer.reduce_transaction(None, [clear_evt])
        self.assertIsNone(state)
        self.assertEqual(len(applied), 0)
        self.assertEqual(self.reducer.unapplied_count, 1)

    def test_assign_state_version_on_change(self):
        from batch.reducer import compute_state_hash, is_state_changed

        clear_evt = {
            "event_id": "clear-100",
            "event_type": "CLEAR",
            "logical_seq": 2,
            "event_time_ms": 1770000005000,
            "payload_hash": "hash-clear",
            "payload": {"amount": 5000},
        }

        # Initial auth reduction -> version 1
        state1, _ = self.reducer.reduce_transaction(None, [self.base_auth])
        self.assertEqual(state1["state_version"], 1)
        h1 = state1["state_hash"]
        self.assertIsNotNone(h1)
        self.assertTrue(is_state_changed(None, state1))

        # Re-evaluating identical events preserves version and skips change
        state1_re, _ = self.reducer.reduce_transaction(state1, [self.base_auth])
        self.assertEqual(state1_re["state_version"], 1)
        self.assertEqual(state1_re["state_hash"], h1)
        self.assertFalse(is_state_changed(state1, state1_re))

        # State transition increments version by 1
        state2, _ = self.reducer.reduce_transaction(state1, [clear_evt])
        self.assertEqual(state2["state_version"], 2)
        h2 = state2["state_hash"]
        self.assertNotEqual(h1, h2)
        self.assertTrue(is_state_changed(state1, state2))

        # Re-evaluating clear again does not increment version
        state2_re, _ = self.reducer.reduce_transaction(state2, [clear_evt])
        self.assertEqual(state2_re["state_version"], 2)
        self.assertEqual(state2_re["state_hash"], h2)
        self.assertFalse(is_state_changed(state2, state2_re))

    def test_upsert_transactions(self):
        import shutil
        import tempfile
        from batch.session import get_spark_session
        from batch.hudi_tables import SCHEMA_TRANSACTIONS
        from batch.reducer import upsert_transactions

        spark = get_spark_session("test-upsert-tx")
        temp_dir = tempfile.mkdtemp()
        table_path = f"{temp_dir}/transactions"
        try:
            state, _ = self.reducer.reduce_transaction(None, [self.base_auth])
            # Prepare row matching SCHEMA_TRANSACTIONS
            row = (
                state["transaction_id"],
                state["run_id"],
                state["card_id"],
                state["merchant_id"],
                state["mcc"],
                state["issuer_bank_id"],
                state["acquirer_bank_id"],
                state["currency"],
                state["authorized_amount"],
                state["decision_id"],
                state["decision"],
                state["decision_source"],
                state["decision_reason"],
                state["decision_flags"],
                state["rule_version"],
                state["reference_version"],
                state["status"],
                state["latest_applied_sequence"],
                state["cleared_amount"],
                state["reversed_amount"],
                state["charged_back_amount"],
                state["original_event_time_ms"],
                state["latest_lifecycle_time_ms"],
                state["state_hash"],
                state["state_version"],
                state["authorization_date"],
            )
            df = spark.createDataFrame([row], schema=SCHEMA_TRANSACTIONS)
            instant = upsert_transactions(spark, df, table_path=table_path)
            self.assertIsNotNone(instant)
            self.assertTrue(len(str(instant)) > 0)

            # Read snapshot and verify
            res = spark.read.format("hudi").load(table_path)
            self.assertEqual(res.count(), 1)
            row_out = res.collect()[0]
            self.assertEqual(row_out["transaction_id"], "tx-100")
            self.assertEqual(row_out["status"], "AUTHORIZED")
            self.assertEqual(row_out["state_version"], 1)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
