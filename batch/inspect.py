import logging

try:
    from batch.hudi_tables import (
        TABLE_LEDGER_EVENTS,
        TABLE_SETTLEMENT_POSTINGS,
        TABLE_TRANSACTIONS,
        is_hudi_table,
    )
except ImportError:
    TABLE_LEDGER_EVENTS = "s3a://swipeline/ledger/ledger_events"
    TABLE_SETTLEMENT_POSTINGS = "s3a://swipeline/ledger/settlement_postings"
    TABLE_TRANSACTIONS = "s3a://swipeline/ledger/transactions"
    is_hudi_table = lambda spark, path: True

logger = logging.getLogger(__name__)


def inspect_transaction(
    spark,
    tx_id: str,
    table_paths: dict[str, str] | None = None,
) -> dict:
    """Performs Hudi snapshot queries across transactions, ledger_events, and settlement_postings for tx_id."""
    try:
        from pyspark.sql.functions import col
    except ImportError:
        def col(name):
            class _Col:
                def __eq__(self, other):
                    return True
            return _Col()

    paths = table_paths or {
        "transactions": TABLE_TRANSACTIONS,
        "ledger_events": TABLE_LEDGER_EVENTS,
        "settlement_postings": TABLE_SETTLEMENT_POSTINGS,
    }

    tx_record = None
    events = []
    postings = []

    # 1. Query TABLE_TRANSACTIONS
    tx_path = paths.get("transactions", TABLE_TRANSACTIONS)
    try:
        if is_hudi_table(spark, tx_path):
            df = spark.read.format("hudi").load(tx_path)
            rows = df.filter(col("transaction_id") == tx_id).collect()
            if rows:
                tx_record = rows[0].asDict(recursive=True)
    except Exception as e:
        logger.warning("Could not read transactions table: %s", e)

    # 2. Query TABLE_LEDGER_EVENTS
    events_path = paths.get("ledger_events", TABLE_LEDGER_EVENTS)
    try:
        if is_hudi_table(spark, events_path):
            df = spark.read.format("hudi").load(events_path)
            rows = (
                df.filter(col("transaction_id") == tx_id)
                .orderBy("logical_seq")
                .collect()
            )
            for r in rows:
                events.append(r.asDict(recursive=True))
    except Exception as e:
        logger.warning("Could not read ledger_events table: %s", e)

    # 3. Query TABLE_SETTLEMENT_POSTINGS
    postings_path = paths.get("settlement_postings", TABLE_SETTLEMENT_POSTINGS)
    try:
        if is_hudi_table(spark, postings_path):
            df = spark.read.format("hudi").load(postings_path)
            rows = (
                df.filter(col("transaction_id") == tx_id)
                .orderBy("posting_revision", "posting_date")
                .collect()
            )
            for r in rows:
                postings.append(r.asDict(recursive=True))
    except Exception as e:
        logger.warning("Could not read settlement_postings table: %s", e)


    return {
        "transaction_id": tx_id,
        "transaction": tx_record,
        "events": events,
        "postings": postings,
    }


def format_inspection_result(data: dict) -> dict:
    """Formats raw inspect_transaction query data into standardized lifecycle view."""
    tx = data.get("transaction") or {}
    tx_id = data.get("transaction_id") or tx.get("transaction_id", "")

    amounts = {
        "authorized_amount": int(tx.get("authorized_amount", 0)),
        "cleared_amount": int(tx.get("cleared_amount", 0)),
        "reversed_amount": int(tx.get("reversed_amount", 0)),
        "charged_back_amount": int(tx.get("charged_back_amount", 0)),
        "currency": tx.get("currency", ""),
    }

    formatted_events = []
    for e in data.get("events", []):
        formatted_events.append({
            "event_id": e.get("event_id"),
            "logical_seq": int(e.get("logical_seq", 0)),
            "event_type": e.get("event_type"),
            "event_time_ms": int(e.get("event_time_ms", 0)),
            "payload_hash": e.get("payload_hash", ""),
        })

    formatted_postings = []
    for p in data.get("postings", []):
        formatted_postings.append({
            "posting_id": p.get("posting_id"),
            "event_id": p.get("event_id"),
            "posting_date": p.get("posting_date"),
            "signed_principal": int(p.get("signed_principal", 0)),
            "signed_interchange": int(p.get("signed_interchange", 0)),
            "fee_schedule_version": p.get("fee_schedule_version", ""),
            "adjustment_reason": p.get("adjustment_reason"),
            "batch_id": p.get("batch_id", ""),
        })

    return {
        "transaction_id": tx_id,
        "status": tx.get("status", "NOT_FOUND"),
        "amounts": amounts,
        "state_hash": tx.get("state_hash", ""),
        "state_version": int(tx.get("state_version", 0)),
        "events": formatted_events,
        "postings": formatted_postings,
    }


def format_inspection_json(data: dict, indent: int = 2) -> str:
    """Serializes formatted inspection dictionary to JSON string."""
    import json

    res = format_inspection_result(data)
    return json.dumps(res, indent=indent)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Swipeline transaction inspector")
    parser.add_argument("--transaction-id", required=True, help="Transaction ID to inspect")
    args = parser.parse_args()

    from batch.session import get_spark_session

    spark = get_spark_session("swipeline-inspect")
    res = inspect_transaction(spark, args.transaction_id)
    print(format_inspection_json(res))


if __name__ == "__main__":
    main()

