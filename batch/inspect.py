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
    paths = table_paths or {
        "transactions": TABLE_TRANSACTIONS,
        "ledger_events": TABLE_LEDGER_EVENTS,
        "settlement_postings": TABLE_SETTLEMENT_POSTINGS,
    }

    result = {"transaction_id": tx_id, "transaction": None, "events": [], "postings": []}
    for name in ("transactions", "ledger_events", "settlement_postings"):
        path = paths[name]
        if not is_hudi_table(spark, path):
            continue
        frame = spark.read.format("hudi").load(path)
        # Column expressions bind literals; transaction IDs never become SQL source.
        frame = frame.filter(frame["transaction_id"] == tx_id)
        if name == "ledger_events":
            frame = frame.orderBy("logical_seq", "event_id")
        elif name == "settlement_postings":
            frame = frame.orderBy("posting_revision", "posting_date", "posting_id")
        rows = [row.asDict(recursive=True) for row in frame.collect()]
        if name == "transactions":
            if len(rows) > 1:
                raise RuntimeError("Duplicate transaction snapshot")
            result["transaction"] = rows[0] if rows else None
        else:
            result["events" if name == "ledger_events" else "postings"] = rows
    return result


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
            "event_time_ms": int(e.get("original_event_time_ms", e.get("event_time_ms", 0))),
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

    import os
    from urllib.parse import urlencode
    from urllib.request import urlopen

    endpoint = os.environ.get("SWIPELINE_INSPECT_URL", "http://127.0.0.1:8080")
    with urlopen(endpoint.rstrip("/") + "/inspect?" + urlencode({"transaction_id": args.transaction_id}), timeout=120) as response:
        print(response.read().decode("utf-8"))


if __name__ == "__main__":
    main()

