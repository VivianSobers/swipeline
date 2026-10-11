from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Table S3A paths
TABLE_TRANSACTIONS = "s3a://swipeline/ledger/transactions"
TABLE_LEDGER_EVENTS = "s3a://swipeline/ledger/ledger_events"
TABLE_SETTLEMENT_POSTINGS = "s3a://swipeline/ledger/settlement_postings"

# Hudi record key, precombine key, and partition field names
KEY_TRANSACTIONS = "transaction_id"
PRECOMBINE_TRANSACTIONS = "state_version"
PARTITION_TRANSACTIONS = "authorization_date"

KEY_LEDGER_EVENTS = "event_id"
PRECOMBINE_LEDGER_EVENTS = "event_revision"
PARTITION_LEDGER_EVENTS = "ingest_date"

KEY_SETTLEMENT_POSTINGS = "posting_id"
PRECOMBINE_SETTLEMENT_POSTINGS = "posting_revision"
PARTITION_SETTLEMENT_POSTINGS = "posting_date"

# Schemas for Hudi COW tables
SCHEMA_TRANSACTIONS = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("card_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("mcc", StringType(), False),
        StructField("issuer_bank_id", StringType(), False),
        StructField("acquirer_bank_id", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("authorized_amount", LongType(), False),
        StructField("decision_id", StringType(), False),
        StructField("decision", StringType(), False),
        StructField("decision_source", StringType(), False),
        StructField("decision_reason", StringType(), True),
        StructField("decision_flags", ArrayType(StringType()), True),
        StructField("rule_version", StringType(), True),
        StructField("reference_version", StringType(), True),
        StructField("status", StringType(), False),
        StructField("latest_applied_sequence", LongType(), False),
        StructField("cleared_amount", LongType(), False),
        StructField("reversed_amount", LongType(), False),
        StructField("charged_back_amount", LongType(), False),
        StructField("original_event_time_ms", LongType(), False),
        StructField("latest_lifecycle_time_ms", LongType(), False),
        StructField("state_hash", StringType(), False),
        StructField("state_version", LongType(), False),
        StructField("authorization_date", StringType(), False),
    ]
)

SCHEMA_LEDGER_EVENTS = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_revision", LongType(), False),
        StructField("event_type", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("logical_seq", LongType(), False),
        StructField("payload_json", StringType(), False),
        StructField("payload_hash", StringType(), False),
        StructField("validation_state", StringType(), False),
        StructField("original_event_time_ms", LongType(), False),
        StructField("first_ingest_time_ms", LongType(), False),
        StructField("batch_id", StringType(), False),
        StructField("ingest_date", StringType(), False),
    ]
)

SCHEMA_SETTLEMENT_POSTINGS = StructType(
    [
        StructField("posting_id", StringType(), False),
        StructField("posting_revision", LongType(), False),
        StructField("event_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("issuer_bank_id", StringType(), False),
        StructField("acquirer_bank_id", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("signed_principal", LongType(), False),
        StructField("signed_interchange", LongType(), False),
        StructField("fee_schedule_version", StringType(), False),
        StructField("original_business_date", StringType(), False),
        StructField("posting_date", StringType(), False),
        StructField("adjustment_reason", StringType(), True),
        StructField("batch_id", StringType(), False),
    ]
)

HUDI_OPTIONS_TRANSACTIONS = {
    "hoodie.table.name": "transactions",
    "hoodie.datasource.write.recordkey.field": KEY_TRANSACTIONS,
    "hoodie.datasource.write.precombine.field": PRECOMBINE_TRANSACTIONS,
    "hoodie.datasource.write.partitionpath.field": PARTITION_TRANSACTIONS,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
}

HUDI_OPTIONS_LEDGER_EVENTS = {
    "hoodie.table.name": "ledger_events",
    "hoodie.datasource.write.recordkey.field": KEY_LEDGER_EVENTS,
    "hoodie.datasource.write.precombine.field": PRECOMBINE_LEDGER_EVENTS,
    "hoodie.datasource.write.partitionpath.field": PARTITION_LEDGER_EVENTS,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
}

HUDI_OPTIONS_SETTLEMENT_POSTINGS = {
    "hoodie.table.name": "settlement_postings",
    "hoodie.datasource.write.recordkey.field": KEY_SETTLEMENT_POSTINGS,
    "hoodie.datasource.write.precombine.field": PRECOMBINE_SETTLEMENT_POSTINGS,
    "hoodie.datasource.write.partitionpath.field": PARTITION_SETTLEMENT_POSTINGS,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
}


def is_hudi_table(spark, path: str) -> bool:
    """Checks whether a valid Hudi table metadata directory exists at path."""
    try:
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        hpath = sc._gateway.jvm.org.apache.hadoop.fs.Path(path + "/.hoodie")
        fs = hpath.getFileSystem(conf)
        return bool(fs.exists(hpath))
    except Exception:
        return False


def init_tables(
    spark,
    table_paths: dict[str, str] | None = None,
) -> None:
    """Initializes empty queryable Hudi COW tables at declared paths if they do not exist."""
    if table_paths is None:
        table_paths = {
            "transactions": TABLE_TRANSACTIONS,
            "ledger_events": TABLE_LEDGER_EVENTS,
            "settlement_postings": TABLE_SETTLEMENT_POSTINGS,
        }

    definitions = [
        ("transactions", SCHEMA_TRANSACTIONS, HUDI_OPTIONS_TRANSACTIONS),
        ("ledger_events", SCHEMA_LEDGER_EVENTS, HUDI_OPTIONS_LEDGER_EVENTS),
        ("settlement_postings", SCHEMA_SETTLEMENT_POSTINGS, HUDI_OPTIONS_SETTLEMENT_POSTINGS),
    ]

    for name, schema, options in definitions:
        path = table_paths.get(name)
        if not path:
            continue
        if not is_hudi_table(spark, path):
            empty_df = spark.createDataFrame([], schema=schema)
            (
                empty_df.write.format("hudi")
                .options(**options)
                .mode("append")
                .save(path)
            )


def write_hudi_table(
    df,
    table_name: str,
    path: str | None = None,
    operation: str = "upsert",
) -> None:
    """Writes a DataFrame to the specified Hudi COW table using declared table options."""
    options_map = {
        "transactions": (HUDI_OPTIONS_TRANSACTIONS, TABLE_TRANSACTIONS),
        "ledger_events": (HUDI_OPTIONS_LEDGER_EVENTS, TABLE_LEDGER_EVENTS),
        "settlement_postings": (
            HUDI_OPTIONS_SETTLEMENT_POSTINGS,
            TABLE_SETTLEMENT_POSTINGS,
        ),
    }

    if table_name not in options_map:
        raise ValueError(f"Unknown Hudi table name: {table_name}")

    base_options, default_path = options_map[table_name]
    target_path = path or default_path

    options = dict(base_options)
    options["hoodie.datasource.write.operation"] = operation

    (
        df.write.format("hudi")
        .options(**options)
        .mode("append")
        .save(target_path)
    )
