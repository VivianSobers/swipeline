import json
from pyspark.sql import DataFrame, SparkSession


def read_kafka_range(
    spark: SparkSession,
    brokers: str,
    topic: str,
    start_offsets: dict,
    end_offsets: dict,
) -> DataFrame:
    """Reads a bounded range of Kafka messages between explicit startingOffsets and endingOffsets JSON vectors."""
    start_json = (
        json.dumps(start_offsets)
        if isinstance(start_offsets, dict)
        else str(start_offsets)
    )
    end_json = (
        json.dumps(end_offsets)
        if isinstance(end_offsets, dict)
        else str(end_offsets)
    )

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", start_json)
        .option("endingOffsets", end_json)
        .option("failOnDataLoss", "true")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )
    return df
