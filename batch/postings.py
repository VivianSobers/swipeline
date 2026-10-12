import hashlib
import json


def calculate_interchange_fee(amount_minor: int) -> int:
    """Calculates 150 bps interchange fee using exact integer arithmetic and round-half-up."""
    sign = -1 if amount_minor < 0 else 1
    return sign * ((abs(amount_minor) * 150 + 5000) // 10000)


def derive_posting_id(event_id: str) -> str:
    """Computes deterministic 64-character hex posting ID as SHA-256 of [event_id, 'posting']."""
    raw = json.dumps([event_id, "posting"])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def validate_existing_posting(expected: dict, existing: dict) -> None:
    """Verify business content before reusing immutable publication metadata."""
    for key, value in expected.items():
        if key in ("posting_date", "batch_id"):
            continue
        if existing.get(key) != value:
            raise RuntimeError(f"Conflicting persisted posting {expected['posting_id']}: {key}")


def reconstruct_postings(
    tx: dict,
    events: list[dict],
    existing_postings: dict,
    batch_id: str,
    close_day: str,
) -> list[dict]:
    """Derive immutable amounts from the accepted chain and verify persisted rows."""
    postings = []
    has_cleared = tx.get("cleared_amount", 0) > 0
    amount = int(tx.get("authorized_amount", 0))
    fee = calculate_interchange_fee(amount)
    for evt in sorted(events, key=lambda e: e.get("logical_seq", 0)):
        kind, seq, event_id = evt.get("event_type"), evt.get("logical_seq", 0), evt.get("event_id")
        if not event_id:
            continue
        if kind == "CLEAR" and seq == 2:
            has_cleared, sign, reason = True, 1, None
        elif kind in ("REVERSE", "CHARGEBACK") and has_cleared and seq == 3:
            sign, reason = -1, "REVERSAL" if kind == "REVERSE" else "CHARGEBACK"
        else:
            continue
        payload = evt.get("payload") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        expected = {
            "posting_id": derive_posting_id(event_id), "posting_revision": 1,
            "event_id": event_id, "transaction_id": tx["transaction_id"],
            "run_id": tx["run_id"], "issuer_bank_id": tx["issuer_bank_id"],
            "acquirer_bank_id": tx["acquirer_bank_id"], "currency": tx["currency"],
            "signed_principal": sign * amount, "signed_interchange": sign * fee,
            "fee_schedule_version": "150bps-v1",
            "original_business_date": tx.get("authorization_date", close_day),
            "posting_date": payload.get("close_day") or close_day,
            "adjustment_reason": reason, "batch_id": batch_id,
        }
        existing = existing_postings.get(expected["posting_id"]) or existing_postings.get(event_id)
        if existing:
            validate_existing_posting(expected, existing)
            postings.append(dict(existing))
        else:
            postings.append(expected)
    expected_ids = {p["posting_id"] for p in postings}
    if any(p.get("posting_id") not in expected_ids for p in existing_postings.values()):
        raise RuntimeError("Unexpected persisted posting outside accepted chain")
    return postings


def upsert_postings(
    spark,
    postings_df,
    table_path: str | None = None,
) -> str:
    """Inserts or upserts financial postings into TABLE_SETTLEMENT_POSTINGS partitioned by posting_date."""
    from batch.hudi_tables import TABLE_SETTLEMENT_POSTINGS, write_hudi_table

    target_path = table_path or TABLE_SETTLEMENT_POSTINGS
    write_hudi_table(
        postings_df,
        "settlement_postings",
        path=target_path,
        operation="insert",
    )
    res_df = spark.read.format("hudi").load(target_path)
    latest_instant = res_df.agg({"_hoodie_commit_time": "max"}).collect()[0][0]
    return str(latest_instant)

