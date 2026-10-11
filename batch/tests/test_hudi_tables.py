import unittest
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructType,
)

from batch.hudi_tables import (
    KEY_LEDGER_EVENTS,
    KEY_SETTLEMENT_POSTINGS,
    KEY_TRANSACTIONS,
    PARTITION_LEDGER_EVENTS,
    PARTITION_SETTLEMENT_POSTINGS,
    PARTITION_TRANSACTIONS,
    PRECOMBINE_LEDGER_EVENTS,
    PRECOMBINE_SETTLEMENT_POSTINGS,
    PRECOMBINE_TRANSACTIONS,
    SCHEMA_LEDGER_EVENTS,
    SCHEMA_SETTLEMENT_POSTINGS,
    SCHEMA_TRANSACTIONS,
    TABLE_LEDGER_EVENTS,
    TABLE_SETTLEMENT_POSTINGS,
    TABLE_TRANSACTIONS,
)


class TestHudiTableSchemas(unittest.TestCase):
    def test_table_constants(self):
        self.assertEqual(TABLE_TRANSACTIONS, "s3a://swipeline/ledger/transactions")
        self.assertEqual(TABLE_LEDGER_EVENTS, "s3a://swipeline/ledger/ledger_events")
        self.assertEqual(TABLE_SETTLEMENT_POSTINGS, "s3a://swipeline/ledger/settlement_postings")

        self.assertEqual(KEY_TRANSACTIONS, "transaction_id")
        self.assertEqual(PRECOMBINE_TRANSACTIONS, "state_version")
        self.assertEqual(PARTITION_TRANSACTIONS, "authorization_date")

        self.assertEqual(KEY_LEDGER_EVENTS, "event_id")
        self.assertEqual(PRECOMBINE_LEDGER_EVENTS, "event_revision")
        self.assertEqual(PARTITION_LEDGER_EVENTS, "ingest_date")

        self.assertEqual(KEY_SETTLEMENT_POSTINGS, "posting_id")
        self.assertEqual(PRECOMBINE_SETTLEMENT_POSTINGS, "posting_revision")
        self.assertEqual(PARTITION_SETTLEMENT_POSTINGS, "posting_date")

    def test_schema_transactions(self):
        self.assertIsInstance(SCHEMA_TRANSACTIONS, StructType)
        field_names = {f.name: f.dataType for f in SCHEMA_TRANSACTIONS.fields}
        
        required = [
            ("transaction_id", StringType),
            ("run_id", StringType),
            ("card_id", StringType),
            ("merchant_id", StringType),
            ("mcc", StringType),
            ("issuer_bank_id", StringType),
            ("acquirer_bank_id", StringType),
            ("currency", StringType),
            ("authorized_amount", LongType),
            ("decision_id", StringType),
            ("decision", StringType),
            ("decision_source", StringType),
            ("status", StringType),
            ("latest_applied_sequence", LongType),
            ("cleared_amount", LongType),
            ("reversed_amount", LongType),
            ("charged_back_amount", LongType),
            ("original_event_time_ms", LongType),
            ("latest_lifecycle_time_ms", LongType),
            ("state_hash", StringType),
            ("state_version", LongType),
            ("authorization_date", StringType),
        ]
        for name, dtype in required:
            self.assertIn(name, field_names, f"missing field {name} in SCHEMA_TRANSACTIONS")
            self.assertIsInstance(field_names[name], dtype, f"wrong type for {name}")

    def test_schema_ledger_events(self):
        self.assertIsInstance(SCHEMA_LEDGER_EVENTS, StructType)
        field_names = {f.name: f.dataType for f in SCHEMA_LEDGER_EVENTS.fields}
        
        required = [
            ("event_id", StringType),
            ("event_revision", LongType),
            ("event_type", StringType),
            ("transaction_id", StringType),
            ("run_id", StringType),
            ("logical_seq", LongType),
            ("payload_json", StringType),
            ("payload_hash", StringType),
            ("validation_state", StringType),
            ("original_event_time_ms", LongType),
            ("first_ingest_time_ms", LongType),
            ("batch_id", StringType),
            ("ingest_date", StringType),
        ]
        for name, dtype in required:
            self.assertIn(name, field_names, f"missing field {name} in SCHEMA_LEDGER_EVENTS")
            self.assertIsInstance(field_names[name], dtype, f"wrong type for {name}")

    def test_schema_settlement_postings(self):
        self.assertIsInstance(SCHEMA_SETTLEMENT_POSTINGS, StructType)
        field_names = {f.name: f.dataType for f in SCHEMA_SETTLEMENT_POSTINGS.fields}
        
        required = [
            ("posting_id", StringType),
            ("posting_revision", LongType),
            ("event_id", StringType),
            ("transaction_id", StringType),
            ("run_id", StringType),
            ("issuer_bank_id", StringType),
            ("acquirer_bank_id", StringType),
            ("currency", StringType),
            ("signed_principal", LongType),
            ("signed_interchange", LongType),
            ("fee_schedule_version", StringType),
            ("original_business_date", StringType),
            ("posting_date", StringType),
            ("batch_id", StringType),
        ]
        for name, dtype in required:
            self.assertIn(name, field_names, f"missing field {name} in SCHEMA_SETTLEMENT_POSTINGS")
            self.assertIsInstance(field_names[name], dtype, f"wrong type for {name}")

    def test_init_empty_tables(self):
        import shutil
        import tempfile
        from batch.session import get_spark_session
        from batch.hudi_tables import init_tables

        spark = get_spark_session("test-init-tables")
        temp_dir = tempfile.mkdtemp()
        try:
            paths = {
                "transactions": f"{temp_dir}/transactions",
                "ledger_events": f"{temp_dir}/ledger_events",
                "settlement_postings": f"{temp_dir}/settlement_postings",
            }
            init_tables(spark, table_paths=paths)

            # Assert all three tables are queryable
            for name, path in paths.items():
                df = spark.read.format("hudi").load(path)
                self.assertEqual(df.count(), 0)
                df.createOrReplaceTempView(name)
                res = spark.sql(f"SELECT COUNT(*) as cnt FROM {name}").collect()
                self.assertEqual(res[0]["cnt"], 0)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_hudi_cow_write_and_snapshot_read(self):
        import shutil
        import tempfile
        from batch.session import get_spark_session
        from batch.hudi_tables import write_hudi_table, SCHEMA_TRANSACTIONS

        spark = get_spark_session("test-cow-write-snapshot")
        temp_dir = tempfile.mkdtemp()
        table_path = f"{temp_dir}/transactions"
        try:
            sample_data = [
                (
                    "tx-1001",
                    "run-demo",
                    "card-abc",
                    "merch-xyz",
                    "5411",
                    "bank-issuer-1",
                    "bank-acquirer-1",
                    "USD",
                    5000,
                    "dec-1001",
                    "APPROVE",
                    "ENGINE",
                    None,
                    [],
                    "v1",
                    "v1",
                    "AUTHORIZED",
                    1,
                    0,
                    0,
                    0,
                    1770000000000,
                    1770000000000,
                    "statehash001",
                    1,
                    "2026-10-11",
                )
            ]
            df = spark.createDataFrame(sample_data, schema=SCHEMA_TRANSACTIONS)
            write_hudi_table(df, "transactions", table_path, operation="upsert")

            # Snapshot query
            snapshot_df = (
                spark.read.format("hudi")
                .option("hoodie.datasource.query.type", "snapshot")
                .load(table_path)
            )
            self.assertEqual(snapshot_df.count(), 1)
            row = snapshot_df.filter("transaction_id = 'tx-1001'").collect()[0]
            self.assertEqual(row["authorized_amount"], 5000)
            self.assertEqual(row["status"], "AUTHORIZED")
            self.assertEqual(row["state_version"], 1)

            # Upsert updated record with state_version=2 and status=CLEARED
            updated_data = [
                (
                    "tx-1001",
                    "run-demo",
                    "card-abc",
                    "merch-xyz",
                    "5411",
                    "bank-issuer-1",
                    "bank-acquirer-1",
                    "USD",
                    5000,
                    "dec-1001",
                    "APPROVE",
                    "ENGINE",
                    None,
                    [],
                    "v1",
                    "v1",
                    "CLEARED",
                    2,
                    5000,
                    0,
                    0,
                    1770000000000,
                    1770000100000,
                    "statehash002",
                    2,
                    "2026-10-11",
                )
            ]
            df2 = spark.createDataFrame(updated_data, schema=SCHEMA_TRANSACTIONS)
            write_hudi_table(df2, "transactions", table_path, operation="upsert")

            updated_snapshot = (
                spark.read.format("hudi")
                .option("hoodie.datasource.query.type", "snapshot")
                .load(table_path)
            )
            self.assertEqual(updated_snapshot.count(), 1)
            row2 = updated_snapshot.filter("transaction_id = 'tx-1001'").collect()[0]
            self.assertEqual(row2["status"], "CLEARED")
            self.assertEqual(row2["state_version"], 2)
            self.assertEqual(row2["cleared_amount"], 5000)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
