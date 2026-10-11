import json
import unittest

from batch.journal import parse_event_envelope


class TestJournalEnvelope(unittest.TestCase):
    def test_parse_valid_lifecycle_event(self):
        raw = {
            "schema_version": "1.0.0",
            "event_id": "evt-001",
            "event_type": "CLEAR",
            "run_id": "run-demo",
            "transaction_id": "tx-100",
            "source_seq": 42,
            "event_time_ms": 1770000000000,
            "produced_at_ms": 1770000000500,
            "payload_hash": "hash12345",
            "payload": {
                "amount": 5000,
                "currency": "USD",
            },
        }
        res = parse_event_envelope(json.dumps(raw))
        self.assertEqual(res["validation_state"], "VALID")
        self.assertEqual(res["event_id"], "evt-001")
        self.assertEqual(res["event_type"], "CLEAR")
        self.assertEqual(res["transaction_id"], "tx-100")
        self.assertEqual(res["run_id"], "run-demo")
        self.assertEqual(res["source_seq"], 42)
        self.assertEqual(res["payload_hash"], "hash12345")

    def test_parse_malformed_json(self):
        res = parse_event_envelope("not a json payload")
        self.assertEqual(res["validation_state"], "INVALID")
        self.assertIsNotNone(res["event_id"])

    def test_parse_missing_required_fields(self):
        raw = {
            "schema_version": "1.0.0",
            # missing event_id
            "event_type": "CLEAR",
            "transaction_id": "tx-100",
        }
        res = parse_event_envelope(raw)
        self.assertEqual(res["validation_state"], "INVALID")

    def test_ingest_journal(self):
        import shutil
        import tempfile
        from batch.session import get_spark_session
        from batch.journal import ingest_journal
        from batch.hudi_tables import SCHEMA_LEDGER_EVENTS

        spark = get_spark_session("test-journal-ingest")
        temp_dir = tempfile.mkdtemp()
        table_path = f"{temp_dir}/ledger_events"
        try:
            records = [
                {
                    "schema_version": "1.0.0",
                    "event_id": f"evt-{i}",
                    "event_type": "CLEAR",
                    "run_id": "run-test",
                    "transaction_id": f"tx-{i}",
                    "source_seq": i,
                    "event_time_ms": 1770000000000 + i,
                    "produced_at_ms": 1770000000000 + i,
                    "payload_hash": f"hash-{i}",
                    "payload": {"amount": 1000 * i},
                }
                for i in range(1, 4)
            ]
            raw_json_rows = [(json.dumps(r),) for r in records]
            raw_df = spark.createDataFrame(raw_json_rows, ["value"])

            instant = ingest_journal(
                spark,
                raw_df,
                batch_id="batch-001",
                ingest_date="2026-10-11",
                table_path=table_path,
            )
            self.assertIsNotNone(instant)
            self.assertTrue(len(str(instant)) > 0)

            # Read back and verify count and fields
            journal_df = spark.read.format("hudi").load(table_path)
            self.assertEqual(journal_df.count(), 3)
            
            row = journal_df.filter("event_id = 'evt-1'").collect()[0]
            self.assertEqual(row["event_revision"], 1)
            self.assertEqual(row["batch_id"], "batch-001")
            self.assertEqual(row["ingest_date"], "2026-10-11")
            self.assertEqual(row["validation_state"], "VALID")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_journal_idempotent_append(self):
        import shutil
        import tempfile
        from batch.session import get_spark_session
        from batch.journal import ingest_journal

        spark = get_spark_session("test-journal-idempotent")
        temp_dir = tempfile.mkdtemp()
        table_path = f"{temp_dir}/ledger_events"
        try:
            records = [
                {
                    "schema_version": "1.0.0",
                    "event_id": f"dup-evt-{i}",
                    "event_type": "CLEAR",
                    "run_id": "run-test",
                    "transaction_id": f"tx-dup-{i}",
                    "source_seq": i,
                    "event_time_ms": 1770000000000 + i,
                    "produced_at_ms": 1770000000000 + i,
                    "payload_hash": f"hash-{i}",
                    "payload": {"amount": 500 * i},
                }
                for i in range(1, 21)
            ]
            raw_json_rows = [(json.dumps(r),) for r in records]
            raw_df = spark.createDataFrame(raw_json_rows, ["value"])

            # Ingest first batch
            inst1 = ingest_journal(
                spark,
                raw_df,
                batch_id="batch-001",
                ingest_date="2026-10-11",
                table_path=table_path,
            )
            df1 = spark.read.format("hudi").load(table_path)
            self.assertEqual(df1.count(), 20)

            # Re-ingest the exact same batch of 20 events
            inst2 = ingest_journal(
                spark,
                raw_df,
                batch_id="batch-002",
                ingest_date="2026-10-11",
                table_path=table_path,
            )
            df2 = spark.read.format("hudi").load(table_path)
            # Row count must remain 20, zero duplicates written
            self.assertEqual(df2.count(), 20)
            self.assertIsNotNone(inst2)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
