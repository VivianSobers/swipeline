import json
import time
import uuid


def parse_event_envelope(record: str | bytes | dict) -> dict:
    """Parses and validates an incoming Kafka event record into an envelope dictionary."""
    if isinstance(record, (str, bytes)):
        try:
            data = json.loads(record)
        except Exception:
            return {
                "schema_version": "unknown",
                "event_id": str(uuid.uuid4()),
                "event_type": "UNKNOWN",
                "run_id": "unknown",
                "transaction_id": "unknown",
                "logical_seq": 0,
                "event_time_ms": int(time.time() * 1000),
                "produced_at_ms": int(time.time() * 1000),
                "payload_hash": "",
                "payload_json": str(record),
                "validation_state": "INVALID",
            }
    elif isinstance(record, dict):
        data = record
    else:
        return {
            "schema_version": "unknown",
            "event_id": str(uuid.uuid4()),
            "event_type": "UNKNOWN",
            "run_id": "unknown",
            "transaction_id": "unknown",
            "logical_seq": 0,
            "event_time_ms": int(time.time() * 1000),
            "produced_at_ms": int(time.time() * 1000),
            "payload_hash": "",
            "payload_json": str(record),
            "validation_state": "INVALID",
        }

    if not isinstance(data, dict):
        return parse_event_envelope(None)

    schema_version = str(data.get("schema_version") or "1.0.0")
    event_id = data.get("event_id") or data.get("decision_id")
    event_type = data.get("event_type") or data.get("kind") or ("AUTH_DECISION" if "decision" in data else None)
    run_id = data.get("run_id")
    transaction_id = data.get("transaction_id")
    source_seq = data.get("source_seq") if "source_seq" in data else data.get("sequence")
    event_time_ms = data.get("event_time_ms") or data.get("timestamp_ms")
    produced_at_ms = data.get("produced_at_ms") or data.get("admitted_at_ms") or event_time_ms
    payload_hash = data.get("payload_hash")

    payload = dict(data.get("payload") or {}) if isinstance(data.get("payload"), dict) else dict(data)
    aliases = {"amount_minor": "amount", "card_token": "card_id", "reason": "decision_reason", "fraud_flags": "decision_flags"}
    for source, target in aliases.items():
        if source in payload:
            payload[target] = payload[source]
    logical_seq = 1 if event_type == "AUTH_DECISION" else data.get("lifecycle_seq", data.get("logical_seq", source_seq))

    # Validation criteria
    is_valid = (
        bool(event_id and isinstance(event_id, str))
        and bool(run_id and isinstance(run_id, str))
        and bool(transaction_id and isinstance(transaction_id, str))
        and bool(event_type and isinstance(event_type, str))
        and (source_seq is not None and isinstance(source_seq, int) and not isinstance(source_seq, bool))
        and bool(event_time_ms and isinstance(event_time_ms, int) and not isinstance(event_time_ms, bool))
        and bool(payload_hash and isinstance(payload_hash, str))
    )

    is_valid = is_valid and isinstance(logical_seq, int) and not isinstance(logical_seq, bool)
    validation_state = "VALID" if is_valid else "INVALID"

    return {
        "schema_version": schema_version,
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": str(event_type or "UNKNOWN"),
        "run_id": str(run_id or "unknown"),
        "transaction_id": str(transaction_id or "unknown"),
        "source_seq": source_seq if isinstance(source_seq, int) else 0,
        "logical_seq": logical_seq if isinstance(logical_seq, int) else 0,
        "payload": payload,
        "parent_event_id": data.get("parent_event_id", payload.get("parent_event_id")),
        "event_time_ms": int(event_time_ms or 0),
        "produced_at_ms": int(produced_at_ms or 0),
        "payload_hash": str(payload_hash or ""),
        "payload_json": json.dumps(data) if not isinstance(data, str) else data,
        "validation_state": validation_state,
    }


def ingest_journal(
    spark,
    raw_df,
    batch_id: str,
    ingest_date: str,
    table_path: str | None = None,
) -> str:
    """Ingests raw Kafka records into TABLE_LEDGER_EVENTS, deduplicating against existing keys."""
    from batch.hudi_tables import (
        SCHEMA_LEDGER_EVENTS,
        TABLE_LEDGER_EVENTS,
        is_hudi_table,
        write_hudi_table,
    )

    target_path = table_path or TABLE_LEDGER_EVENTS
    now_ms = int(time.time() * 1000)

    parsed_rows = []
    incoming_hashes = {}
    for row in raw_df.collect():
        val = row["value"] if "value" in row else row.asDict()
        envelope = parse_event_envelope(val)
        if envelope["event_id"] in incoming_hashes and incoming_hashes[envelope["event_id"]] != envelope["payload_hash"]:
            raise RuntimeError("Journal event ID/hash conflict")
        if envelope["event_id"] in incoming_hashes:
            continue
        incoming_hashes[envelope["event_id"]] = envelope["payload_hash"]
        parsed_rows.append(
            (
                envelope["event_id"],
                1,  # event_revision: fixed at 1
                envelope["event_type"],
                envelope["transaction_id"],
                envelope["run_id"],
                envelope["logical_seq"],
                envelope["payload_json"],
                envelope["payload_hash"],
                envelope["validation_state"],
                envelope["event_time_ms"],
                now_ms,  # first_ingest_time_ms
                batch_id,
                ingest_date,
            )
        )

    if not parsed_rows:
        if is_hudi_table(spark, target_path):
            existing_df = spark.read.format("hudi").load(target_path)
            if existing_df.count() > 0:
                return str(
                    existing_df.agg({"_hoodie_commit_time": "max"}).collect()[0][0]
                )
        return str(now_ms)

    incoming_df = spark.createDataFrame(parsed_rows, schema=SCHEMA_LEDGER_EVENTS)

    if is_hudi_table(spark, target_path):
        existing = spark.read.format("hudi").load(target_path)
        for row in existing.select("event_id", "payload_hash").collect():
            prior = row.asDict()
            if prior["event_id"] in incoming_hashes and incoming_hashes[prior["event_id"]] != prior["payload_hash"]:
                raise RuntimeError("Journal event ID/hash conflict")
        existing_keys = existing.select("event_id")
        to_insert = incoming_df.join(existing_keys, on="event_id", how="left_anti")
    else:
        to_insert = incoming_df

    if to_insert.count() > 0:
        write_hudi_table(
            to_insert, "ledger_events", path=target_path, operation="upsert"
        )

    committed_df = spark.read.format("hudi").load(target_path)
    latest_instant = committed_df.agg({"_hoodie_commit_time": "max"}).collect()[0][0]
    return str(latest_instant)
