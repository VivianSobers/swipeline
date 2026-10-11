import argparse
import datetime
import json
import logging
import os
import signal
import sys
import time

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
    from batch.postings import reconstruct_postings, upsert_postings
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
        logger.warning("Failed to query Kafka offsets via JVM: %s", e)
        return {}


def process_transactions(
    spark,
    events: list[dict],
    batch_id: str,
    table_path: str | None = None,
) -> str:
    """Reduces events by transaction and upserts resulting states to TABLE_TRANSACTIONS."""
    if not events:
        return ""

    tx_table = table_path or TABLE_TRANSACTIONS
    existing_states = {}

    # Load existing states from Hudi if table exists
    try:
        if is_hudi_table(spark, tx_table):
            existing_df = spark.read.format("hudi").load(tx_table)
            tx_ids = {e.get("transaction_id") for e in events if e.get("transaction_id")}
            if tx_ids:
                from pyspark.sql.functions import col
                matched = existing_df.filter(col("transaction_id").isin(list(tx_ids))).collect()
                for row in matched:
                    d = row.asDict(recursive=True)
                    existing_states[d["transaction_id"]] = d
    except Exception as e:
        logger.warning("Could not read existing transactions from Hudi: %s", e)

    reducer = TransactionReducer()
    events_by_tx = {}
    for evt in events:
        tid = evt.get("transaction_id")
        if tid:
            events_by_tx.setdefault(tid, []).append(evt)

    updated_states = []
    for tid, tx_events in events_by_tx.items():
        curr_state = existing_states.get(tid)
        new_state, _ = reducer.reduce_transaction(curr_state, tx_events)
        if new_state:
            updated_states.append(new_state)

    if not updated_states:
        return ""

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
) -> str:
    """Reconstructs financial postings for cleared/reversed/chargeback transactions and inserts into Hudi."""
    postings_table = table_path or TABLE_SETTLEMENT_POSTINGS
    existing_postings = {}

    try:
        if is_hudi_table(spark, postings_table):
            from pyspark.sql.functions import col
            p_df = spark.read.format("hudi").load(postings_table)
            tids = list(tx_states.keys())
            if tids:
                matched = p_df.filter(col("transaction_id").isin(tids)).collect()
                for row in matched:
                    d = row.asDict(recursive=True)
                    existing_postings[d["posting_id"]] = d
    except Exception as e:
        logger.warning("Could not read existing postings from Hudi: %s", e)

    all_postings = []
    for tid, tx in tx_states.items():
        evts = events_by_tx.get(tid, [])
        tx_existing = {
            pid: p for pid, p in existing_postings.items() if p.get("transaction_id") == tid
        }
        reconstructed = reconstruct_postings(tx, evts, tx_existing, batch_id, close_day)
        all_postings.extend(reconstructed)

    # Exclude postings that already existed prior to this run
    new_postings = [p for p in all_postings if p["posting_id"] not in existing_postings]

    if not new_postings:
        return ""

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


def run_batch(
    spark,
    coordinator: BatchCoordinator,
    brokers: str | None = None,
    topic: str | None = None,
    close_day: str | None = None,
    start_offsets: dict | None = None,
    end_offsets: dict | None = None,
) -> bool:
    """Executes a single serialized batch ingestion cycle across journal, transactions, and postings."""
    brokers = brokers or os.environ.get("KAFKA_BROKERS", "kafka:9092")
    topic = topic or os.environ.get("KAFKA_TOPIC", "swipeline.lifecycle.v1")
    close_day = close_day or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

    # Acquire advisory lock 72002
    if not coordinator.acquire_advisory_lock(72002):
        logger.info("Advisory lock 72002 busy; skipping batch run")
        return False

    try:
        # Check for uncompleted batch recovery first
        pending = coordinator.get_pending_batch()
        if pending:
            logger.info("Resuming uncompleted pending batch: %s", pending["batch_id"])
            resume_pending_batch(
                spark,
                coordinator,
                pending["batch_id"],
                brokers=brokers,
                topic=topic,
                close_day=close_day,
            )
            return True

        if start_offsets is None:
            start_offsets = coordinator.get_latest_offsets()
            if not start_offsets:
                start_offsets = {topic: {"0": 0, "1": 0, "2": 0, "3": 0}}

        if end_offsets is None:
            end_offsets = get_kafka_end_offsets(spark, brokers, [topic])

        if not end_offsets or start_offsets == end_offsets:
            logger.info("No new Kafka offsets to ingest")
            return False

        batch_id = coordinator.start_batch(start_offsets, end_offsets)
        raw_df = read_kafka_range(spark, brokers, topic, start_offsets, end_offsets)

        # 1. Journal ingestion
        j_instant = ingest_journal(spark, raw_df, batch_id, ingest_date=close_day)
        coordinator.update_instant(batch_id, "journal", j_instant)

        # Parse envelopes for reduction and posting reconstruction
        parsed_events = []
        try:
            for row in raw_df.collect():
                val = row.value.decode("utf-8") if isinstance(row.value, bytes) else str(row.value)
                parsed_events.append(parse_event_envelope(val))
        except Exception as e:
            logger.warning("Error parsing envelopes from Kafka dataframe: %s", e)

        # 2. Transaction reduction & upsert
        t_instant = process_transactions(spark, parsed_events, batch_id)
        coordinator.update_instant(batch_id, "transaction", t_instant)

        # 3. Financial postings reconstruction & write
        events_by_tx = {}
        for evt in parsed_events:
            tid = evt.get("transaction_id")
            if tid:
                events_by_tx.setdefault(tid, []).append(evt)

        # Build map of current states for posting reconstruction
        reducer = TransactionReducer()
        tx_states = {}
        for tid, evts in events_by_tx.items():
            state, _ = reducer.reduce_transaction(None, evts)
            if state:
                tx_states[tid] = state

        p_instant = process_postings(
            spark, tx_states, events_by_tx, batch_id, close_day
        )
        coordinator.update_instant(batch_id, "posting", p_instant)

        # 4. Mark batch complete (offsets advance only now)
        coordinator.complete_batch(batch_id)
        return True


    finally:
        coordinator.release_advisory_lock(72002)


def resume_pending_batch(
    spark,
    coordinator: BatchCoordinator,
    batch_id: str,
    brokers: str | None = None,
    topic: str | None = None,
    close_day: str | None = None,
) -> None:
    """Resumes an uncompleted batch from its persisted state checkpoints."""
    brokers = brokers or os.environ.get("KAFKA_BROKERS", "kafka:9092")
    topic = topic or os.environ.get("KAFKA_TOPIC", "swipeline.lifecycle.v1")
    close_day = close_day or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

    pending = coordinator.get_pending_batch()
    if not pending or pending["batch_id"] != batch_id:
        return

    start_offsets = pending["starting_offsets"]
    end_offsets = pending["ending_offsets"]
    state = pending["state"]

    raw_df = read_kafka_range(spark, brokers, topic, start_offsets, end_offsets)
    parsed_events = []
    try:
        for row in raw_df.collect():
            val = row.value.decode("utf-8") if isinstance(row.value, bytes) else str(row.value)
            parsed_events.append(parse_event_envelope(val))
    except Exception as e:
        logger.warning("Error parsing envelopes in resume_pending_batch: %s", e)

    events_by_tx = {}
    for evt in parsed_events:
        tid = evt.get("transaction_id")
        if tid:
            events_by_tx.setdefault(tid, []).append(evt)

    reducer = TransactionReducer()
    tx_states = {}
    for tid, evts in events_by_tx.items():
        st, _ = reducer.reduce_transaction(None, evts)
        if st:
            tx_states[tid] = st

    if state == "STARTED":
        j_instant = ingest_journal(spark, raw_df, batch_id, ingest_date=close_day)
        coordinator.update_instant(batch_id, "journal", j_instant)
        state = "JOURNAL_COMMITTED"




    if state == "JOURNAL_COMMITTED":
        t_instant = process_transactions(spark, parsed_events, batch_id)
        coordinator.update_instant(batch_id, "transaction", t_instant)
        state = "TRANSACTIONS_COMMITTED"

    if state == "TRANSACTIONS_COMMITTED":
        p_instant = process_postings(
            spark, tx_states, events_by_tx, batch_id, close_day
        )
        coordinator.update_instant(batch_id, "posting", p_instant)
        state = "POSTINGS_COMMITTED"

    coordinator.complete_batch(batch_id)


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
            logger.error("Error running batch: %s", e)

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

    run_loop(
        spark,
        coordinator,
        once=args.once,
        interval_secs=interval_secs,
        stop_event=stop_event,
    )


if __name__ == "__main__":
    main()

