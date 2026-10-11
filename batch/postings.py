import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP


def calculate_interchange_fee(amount_minor: int) -> int:
    """Calculates 150 bps interchange fee using exact integer arithmetic and round-half-up."""
    if amount_minor == 0:
        return 0
    # Formula: round_half_up(amount_minor * 150 / 10000)
    d = (Decimal(amount_minor) * Decimal(150)) / Decimal(10000)
    return int(d.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def derive_posting_id(event_id: str) -> str:
    """Computes deterministic 64-character hex posting ID as SHA-256 of [event_id, 'posting']."""
    raw = json.dumps([event_id, "posting"])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def reconstruct_postings(
    tx: dict,
    events: list[dict],
    existing_postings: dict,
    batch_id: str,
    close_day: str,
) -> list[dict]:
    """Reconstructs financial postings for a transaction from its applied lifecycle events."""
    postings = []
    has_cleared = (
        tx.get("cleared_amount", 0) > 0
        or tx.get("status") in ("CLEARED", "REVERSED", "CHARGED_BACK")
    )
    clear_fee = 0

    # Pre-populate clear_fee from existing postings if already cleared
    for p in existing_postings.values():
        if p.get("signed_principal", 0) > 0:
            has_cleared = True
            clear_fee = abs(p.get("signed_interchange", 0))
            break

    sorted_events = sorted(events, key=lambda e: e.get("logical_seq", 0))

    for evt in sorted_events:
        event_type = evt.get("event_type")
        seq = evt.get("logical_seq", 0)
        event_id = evt.get("event_id")
        if not event_id:
            continue

        payload = evt.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        elif not isinstance(payload, dict):
            payload = {}

        amount = payload.get("amount") or tx.get("authorized_amount", 0)
        pid = derive_posting_id(event_id)

        existing = existing_postings.get(pid) or existing_postings.get(event_id)

        if event_type == "CLEAR" and seq == 2:
            has_cleared = True
            if existing:
                fee = abs(
                    existing.get(
                        "signed_interchange", calculate_interchange_fee(amount)
                    )
                )
                clear_fee = fee
                postings.append(dict(existing))
            else:
                fee = calculate_interchange_fee(amount)
                clear_fee = fee

                postings.append(
                    {
                        "posting_id": pid,
                        "posting_revision": 1,
                        "event_id": event_id,
                        "transaction_id": tx["transaction_id"],
                        "run_id": tx["run_id"],
                        "issuer_bank_id": tx["issuer_bank_id"],
                        "acquirer_bank_id": tx["acquirer_bank_id"],
                        "currency": tx["currency"],
                        "signed_principal": int(amount),
                        "signed_interchange": int(fee),
                        "fee_schedule_version": "150bps-v1",
                        "original_business_date": tx.get(
                            "authorization_date", close_day
                        ),
                        "posting_date": close_day,
                        "adjustment_reason": None,
                        "batch_id": batch_id,
                    }
                )

        elif event_type == "REVERSE":
            if not has_cleared and seq == 2:
                # Pre-clearing reversal generates zero financial postings
                continue
            elif has_cleared and seq == 3:
                fee = clear_fee or calculate_interchange_fee(amount)
                if existing:
                    postings.append(dict(existing))
                else:
                    postings.append(
                        {
                            "posting_id": pid,
                            "posting_revision": 1,
                            "event_id": event_id,
                            "transaction_id": tx["transaction_id"],
                            "run_id": tx["run_id"],
                            "issuer_bank_id": tx["issuer_bank_id"],
                            "acquirer_bank_id": tx["acquirer_bank_id"],
                            "currency": tx["currency"],
                            "signed_principal": -int(amount),
                            "signed_interchange": -int(fee),
                            "fee_schedule_version": "150bps-v1",
                            "original_business_date": tx.get(
                                "authorization_date", close_day
                            ),
                            "posting_date": close_day,
                            "adjustment_reason": "REVERSAL",
                            "batch_id": batch_id,
                        }
                    )

        elif event_type == "CHARGEBACK":
            if has_cleared and seq == 3:
                fee = clear_fee or calculate_interchange_fee(amount)
                if existing:
                    postings.append(dict(existing))
                else:
                    postings.append(
                        {
                            "posting_id": pid,
                            "posting_revision": 1,
                            "event_id": event_id,
                            "transaction_id": tx["transaction_id"],
                            "run_id": tx["run_id"],
                            "issuer_bank_id": tx["issuer_bank_id"],
                            "acquirer_bank_id": tx["acquirer_bank_id"],
                            "currency": tx["currency"],
                            "signed_principal": -int(amount),
                            "signed_interchange": -int(fee),
                            "fee_schedule_version": "150bps-v1",
                            "original_business_date": tx.get(
                                "authorization_date", close_day
                            ),
                            "posting_date": close_day,
                            "adjustment_reason": "CHARGEBACK",
                            "batch_id": batch_id,
                        }
                    )

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

