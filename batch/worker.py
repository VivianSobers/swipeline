import argparse
import datetime
import logging
import os
import signal
import time
import threading

from batch.db import BatchCoordinator

try:
    from batch.hudi_tables import (
        KEY_SETTLEMENT_POSTINGS,
        KEY_TRANSACTIONS,
        SCHEMA_SETTLEMENT_POSTINGS,
        SCHEMA_TRANSACTIONS,
        TABLE_LEDGER_EVENTS,
        TABLE_SETTLEMENT_POSTINGS,
        TABLE_TRANSACTIONS,
        is_hudi_table,
    )
    from batch.journal import ingest_journal, parse_event_envelope
    from batch.kafka_consumer import read_kafka_range
    from batch.postings import reconstruct_postings, upsert_postings, validate_existing_posting, derive_posting_id
    from batch.reducer import TransactionReducer, upsert_transactions
except ImportError:
    # Allow imports without pyspark installed in lightweight environments
    (
        KEY_SETTLEMENT_POSTINGS,
        KEY_TRANSACTIONS,
        SCHEMA_SETTLEMENT_POSTINGS,
        SCHEMA_TRANSACTIONS,
        TABLE_LEDGER_EVENTS,
        TABLE_SETTLEMENT_POSTINGS,
        TABLE_TRANSACTIONS,
        is_hudi_table,
    ) = (None,) * 8
    (
        ingest_journal,
        parse_event_envelope,
        read_kafka_range,
        reconstruct_postings,
        upsert_postings,
        TransactionReducer,
        upsert_transactions,
    ) = (None,) * 7


logger = logging.getLogger(__name__)


def get_kafka_end_offsets(spark, brokers: str, topics: list[str]) -> dict:
    """Fetches current end offsets for given topics from Kafka using KafkaConsumer via Spark JVM."""
    try:
        jvm = spark.sparkContext._gateway.jvm
        props = jvm.java.util.Properties()
        props.put("bootstrap.servers", brokers)
        props.put("isolation.level", "read_committed")
        props.put(
            "key.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        )
        props.put(
            "value.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        )
        consumer = jvm.org.apache.kafka.clients.consumer.KafkaConsumer(props)
        try:
            tps = jvm.java.util.ArrayList()
            for t in topics:
                part_infos = consumer.partitionsFor(t)
                if part_infos is None or part_infos.isEmpty():
                    raise RuntimeError(f"No Kafka partitions found for {t}")
                if part_infos is not None:
                    for pi in part_infos:
                        tps.add(
                            jvm.org.apache.kafka.common.TopicPartition(
                                pi.topic(), pi.partition()
                            )
                        )
            end_map = consumer.endOffsets(tps)
            res = {}
            for entry in end_map.entrySet():
                tp = entry.getKey()
                offset = entry.getValue()
                topic = tp.topic()
                part = str(tp.partition())
                if topic not in res:
                    res[topic] = {}
                res[topic][part] = int(offset)
            return res
        finally:
            consumer.close()
    except Exception as e:
        raise RuntimeError("Failed to query Kafka offsets") from e


def process_transactions(
    spark,
    events: list[dict],
    batch_id: str,
    table_path: str | None = None,
    tx_states: dict | None = None,
) -> str:
    """Reduces events by transaction and upserts resulting states to TABLE_TRANSACTIONS."""
    if not events:
        return ""

    tx_table = table_path or TABLE_TRANSACTIONS
    existing_states = read_snapshot(spark, tx_table, {e.get("transaction_id") for e in events}, "transaction_id")
    reducer = TransactionReducer()
    events_by_tx = {}
    for evt in events:
        tid = evt.get("transaction_id")
        if tid:
            events_by_tx.setdefault(tid, []).append(evt)

    updated_states = []
    for tid, tx_events in events_by_tx.items():
        curr_state = existing_states.get(tid)
        new_state = tx_states.get(tid) if tx_states is not None else reducer.reduce_transaction(curr_state, tx_events)[0]
        if new_state and (not curr_state or new_state["state_hash"] != curr_state.get("state_hash")):
            updated_states.append(new_state)

    if not updated_states:
        return max((str(row.get("_hoodie_commit_time", "")) for row in existing_states.values()), default="")

    rows = []
    for s in updated_states:
        row = (
            s.get("transaction_id"),
            s.get("run_id"),
            s.get("card_id"),
            s.get("merchant_id"),
            s.get("mcc"),
            s.get("issuer_bank_id"),
            s.get("acquirer_bank_id"),
            s.get("currency"),
            int(s.get("authorized_amount", 0)),
            s.get("decision_id"),
            s.get("decision"),
            s.get("decision_source"),
            s.get("decision_reason"),
            s.get("decision_flags"),
            s.get("rule_version"),
            s.get("reference_version"),
            s.get("status"),
            int(s.get("latest_applied_sequence", 0)),
            int(s.get("cleared_amount", 0)),
            int(s.get("reversed_amount", 0)),
            int(s.get("charged_back_amount", 0)),
            int(s.get("original_event_time_ms", 0)),
            int(s.get("latest_lifecycle_time_ms", 0)),
            s.get("state_hash"),
            int(s.get("state_version", 1)),
            s.get("authorization_date"),
        )
        rows.append(row)

    tx_df = spark.createDataFrame(rows, schema=SCHEMA_TRANSACTIONS)
    return upsert_transactions(spark, tx_df, table_path=tx_table)


def process_postings(
    spark,
    tx_states: dict,
    events_by_tx: dict,
    batch_id: str,
    close_day: str,
    table_path: str | None = None,
    coordinator: BatchCoordinator | None = None,
) -> str:
    """Reconstructs financial postings for cleared/reversed/chargeback transactions and inserts into Hudi."""
    postings_table = table_path or TABLE_SETTLEMENT_POSTINGS
    existing_postings = {}

    expected_ids = {derive_posting_id(e["event_id"]) for chain in events_by_tx.values() for e in chain
                    if e.get("event_type") in ("CLEAR", "REVERSE", "CHARGEBACK")}
    existing_postings = read_snapshot(spark, postings_table, set(tx_states), "posting_id", record_ids=expected_ids)

    all_postings = []
    for tid, tx in tx_states.items():
        evts = events_by_tx.get(tid, [])
        tx_existing = {
            pid: p for pid, p in existing_postings.items() if p.get("transaction_id") == tid
        }
        reconstructed = reconstruct_postings(tx, evts, tx_existing, batch_id, close_day)
        all_postings.extend(reconstructed)

    actual_ids = {p["posting_id"] for p in all_postings}
    if tx_states and any(pid not in actual_ids for pid in existing_postings):
        raise RuntimeError("Unexpected persisted posting outside accepted chain")

    # Check globally as well: a corrupt transaction_id must not evade the
    # per-transaction subset and then get skipped solely by deterministic ID.
    for posting in all_postings:
        if posting["posting_id"] in existing_postings:
            validate_existing_posting(posting, existing_postings[posting["posting_id"]])

    # Exclude postings that already existed prior to this run
    new_postings = [p for p in all_postings if p["posting_id"] not in existing_postings]

    if not new_postings:
        return max((str(row.get("_hoodie_commit_time", "")) for row in existing_postings.values()), default="")

    if coordinator is not None:
        assignments = coordinator.persist_posting_assignments(batch_id, {p["posting_id"]: p for p in new_postings})
        for posting in new_postings:
            saved = assignments[posting["posting_id"]]
            immutable = set(posting) - {"posting_date", "batch_id"}
            if any(saved.get(k) != posting[k] for k in immutable):
                raise RuntimeError("Conflicting persisted posting assignment")
            posting.update(saved)
    rows = []
    for p in new_postings:
        row = (
            p["posting_id"],
            int(p.get("posting_revision", 1)),
            p["event_id"],
            p["transaction_id"],
            p["run_id"],
            p["issuer_bank_id"],
            p["acquirer_bank_id"],
            p["currency"],
            int(p["signed_principal"]),
            int(p["signed_interchange"]),
            p.get("fee_schedule_version", "150bps-v1"),
            p["original_business_date"],
            p["posting_date"],
            p.get("adjustment_reason"),
            p.get("batch_id", batch_id),
        )
        rows.append(row)

    postings_df = spark.createDataFrame(rows, schema=SCHEMA_SETTLEMENT_POSTINGS)
    return upsert_postings(spark, postings_df, table_path=postings_table)


def read_snapshot(spark, path, tx_ids, key, record_ids=None):
    """Read a verified snapshot; an inaccessible table is never treated as absent."""
    if not is_hudi_table(spark, path):
        return {}
    from pyspark.sql.functions import col
    frame = spark.read.format("hudi").load(path)
    if tx_ids:
        predicate = col("transaction_id").isin(list(tx_ids))
        if record_ids:
            predicate = predicate | col(key).isin(list(record_ids))
        frame = frame.filter(predicate)
    rows = [row.asDict(recursive=True) for row in frame.collect()]
    result = {}
    for row in rows:
        if row[key] in result:
            raise RuntimeError(f"Duplicate snapshot key {row[key]}")
        result[row[key]] = row
    return result


def reconstruct_transaction_chains(spark, events):
    """Rebuild affected transactions from durable valid journal rows."""
    tids = {e["transaction_id"] for e in events if e.get("validation_state") == "VALID"}
    if not tids:
        return {}, {}
    history = read_snapshot(spark, TABLE_LEDGER_EVENTS, tids, "event_id")
    existing = read_snapshot(spark, TABLE_TRANSACTIONS, tids, "transaction_id")
    postings = read_snapshot(spark, TABLE_SETTLEMENT_POSTINGS, tids, "posting_id")
    committed_event_ids = {p["event_id"] for p in postings.values()}
    grouped = {}
    for row in history.values():
        if row["validation_state"] != "VALID":
            continue
        event = parse_event_envelope(row["payload_json"])
        event["_history_order"] = (row["first_ingest_time_ms"], row["event_id"])
        grouped.setdefault(event["transaction_id"], []).append(event)
    states, accepted = {}, {}
    for tid, chain in grouped.items():
        previous = existing.get(tid)
        # A later competing event cannot replace an already committed transition.
        if previous:
            for event in chain:
                seq, kind = event["logical_seq"], event["event_type"]
                expected = "CLEAR" if previous["cleared_amount"] else "REVERSE"
                if seq == 3:
                    expected = "CHARGEBACK" if previous["status"] == "CHARGED_BACK" else "REVERSE"
                if seq <= previous["latest_applied_sequence"] and kind == expected:
                    event["_history_order"] = (-2 if event["event_id"] in committed_event_ids else -1, event["_history_order"])
        state, applied = TransactionReducer().reduce_transaction(None, chain)
        if state and previous:
            immutable = ("run_id", "card_id", "merchant_id", "mcc", "issuer_bank_id", "acquirer_bank_id", "currency", "authorized_amount", "decision_id", "decision", "authorization_date", "original_event_time_ms")
            if any(state[k] != previous[k] for k in immutable):
                raise RuntimeError(f"Authorization identity changed for {tid}")
            if state["latest_applied_sequence"] < previous["latest_applied_sequence"]:
                raise RuntimeError(f"Committed chain missing for {tid}")
            state["state_version"] = previous["state_version"] + int(state["state_hash"] != previous["state_hash"])
        if state:
            states[tid], accepted[tid] = state, applied
    return states, accepted


def _parse_records(raw_df):
    return [parse_event_envelope(row.value) for row in raw_df.collect()]


def _finish_batch(spark, coordinator, batch_id, state, raw_df, close_day):
    events = _parse_records(raw_df)
    if state == "STARTED":
        instant = ingest_journal(spark, raw_df, batch_id, ingest_date=close_day)
        coordinator.update_instant(batch_id, "journal", instant)
        state = "JOURNAL_COMMITTED"
    if state in ("JOURNAL_COMMITTED", "TRANSACTIONS_COMMITTED"):
        states, accepted = reconstruct_transaction_chains(spark, events)
        if state == "JOURNAL_COMMITTED":
            instant = process_transactions(spark, [e for chain in accepted.values() for e in chain], batch_id, tx_states=states)
            coordinator.update_instant(batch_id, "transaction", instant)
        instant = process_postings(spark, states, accepted, batch_id, close_day, coordinator=coordinator)
        coordinator.update_instant(batch_id, "posting", instant)
        state = "POSTINGS_COMMITTED"
    if state != "POSTINGS_COMMITTED":
        raise RuntimeError(f"Unsupported batch state {state}")
    coordinator.complete_batch(batch_id)


def run_batch(spark, coordinator: BatchCoordinator, brokers=None, topic=None,
              close_day=None, start_offsets=None, end_offsets=None) -> bool:
    """Execute one cycle under the shared ingestion lock."""
    brokers = brokers or os.environ.get("KAFKA_BROKERS", "kafka:9092")
    topic = topic or os.environ.get("KAFKA_TOPIC", "swipeline.decisions.v1,swipeline.lifecycle.v1")
    close_day = close_day or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    if not coordinator.acquire_advisory_lock(72002):
        return False
    try:
        pending = coordinator.get_pending_batch()
        if pending:
            resume_pending_batch(spark, coordinator, pending["batch_id"], brokers, topic, close_day)
            return True
        if end_offsets is None:
            end_offsets = get_kafka_end_offsets(spark, brokers, topic.split(","))
        if not end_offsets:
            raise RuntimeError("Kafka end offset vector is empty")
        if start_offsets is None:
            start_offsets = coordinator.get_latest_offsets()
        for t, partitions in start_offsets.items():
            if t not in end_offsets or not set(partitions).issubset(end_offsets[t]):
                raise RuntimeError("Kafka partition disappeared from end offset vector")
        start_offsets = {t: {p: start_offsets.get(t, {}).get(p, 0) for p in parts} for t, parts in end_offsets.items()}
        for t, parts in end_offsets.items():
            if any(start_offsets[t][p] < 0 or end < start_offsets[t][p] for p, end in parts.items()):
                raise RuntimeError("Invalid Kafka offset range")
        if start_offsets == end_offsets:
            return False
        batch_id = coordinator.start_batch(start_offsets, end_offsets)
        raw_df = read_kafka_range(spark, brokers, ",".join(end_offsets), start_offsets, end_offsets)
        _finish_batch(spark, coordinator, batch_id, "STARTED", raw_df, close_day)
        return True
    finally:
        coordinator.release_advisory_lock(72002)


def resume_pending_batch(spark, coordinator: BatchCoordinator, batch_id: str,
                         brokers=None, topic=None, close_day=None) -> None:
    """Resume the stable offset vector using the same durable reconstruction."""
    brokers = brokers or os.environ.get("KAFKA_BROKERS", "kafka:9092")
    close_day = close_day or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    pending = coordinator.get_pending_batch()
    if not pending or pending["batch_id"] != batch_id:
        return
    if pending["state"] == "POSTINGS_COMMITTED":
        coordinator.complete_batch(batch_id)
        return
    raw_df = read_kafka_range(spark, brokers, ",".join(pending["ending_offsets"]), pending["starting_offsets"], pending["ending_offsets"])
    _finish_batch(spark, coordinator, batch_id, pending["state"], raw_df, close_day)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Swipeline batch worker")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument(
        "--poll",
        type=str,
        default="60s",
        help="Poll interval (e.g. 60s, 30s, 10)",
    )
    return parser.parse_args(argv)


def parse_interval_seconds(val: str) -> int:
    val = val.lower().strip()
    if val.endswith("s"):
        return int(val[:-1])
    return int(val)


def run_loop(
    spark,
    coordinator: BatchCoordinator,
    once: bool = False,
    interval_secs: int = 60,
    stop_event: list[bool] | None = None,
    max_loops: int | None = None,
):
    """Executes the batch worker polling loop with graceful shutdown."""
    loops = 0
    while True:
        if stop_event and stop_event[0]:
            break

        try:
            run_batch(spark, coordinator)
        except Exception as e:
            logger.exception("Error running batch: %s", e)
            if once:
                raise

        loops += 1
        if once or (max_loops is not None and loops >= max_loops):
            break

        for _ in range(interval_secs):
            if stop_event and stop_event[0]:
                break
            time.sleep(1)


def main():
    args = parse_args()
    interval_secs = parse_interval_seconds(args.poll)

    from batch.session import get_spark_session

    spark = get_spark_session("swipeline-batch-worker")
    coordinator = BatchCoordinator()

    stop_event = [False]

    def handle_signal(sig, frame):
        logger.info("Received signal %s; shutting down gracefully...", sig)
        stop_event[0] = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    server = None
    server_thread = None
    if not args.once:
        from batch.http import create_server
        server = create_server(spark, BatchCoordinator)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

    try:
        run_loop(spark, coordinator, once=args.once, interval_secs=interval_secs, stop_event=stop_event)
    finally:
        if server:
            server.shutdown()
            server.server_close()
            server_thread.join()
        try:
            coordinator.close()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()

