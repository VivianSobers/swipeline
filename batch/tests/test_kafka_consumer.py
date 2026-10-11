import json
import unittest
from unittest.mock import MagicMock, patch

from batch.kafka_consumer import read_kafka_range


class TestKafkaConsumer(unittest.TestCase):
    def test_read_kafka_range_options(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_df = MagicMock()
        mock_reader.load.return_value = mock_df

        brokers = "kafka:9092"
        topic = "swipeline.lifecycle.v1"
        start_offsets = {topic: {"0": 100, "1": 150}}
        end_offsets = {topic: {"0": 200, "1": 250}}

        df = read_kafka_range(
            mock_spark,
            brokers=brokers,
            topic=topic,
            start_offsets=start_offsets,
            end_offsets=end_offsets,
        )
        self.assertEqual(df, mock_df)

        mock_spark.read.format.assert_called_once_with("kafka")

        # Verify options were configured
        option_calls = {
            call[0][0]: call[0][1] for call in mock_reader.option.call_args_list
        }
        self.assertEqual(option_calls.get("kafka.bootstrap.servers"), brokers)
        self.assertEqual(option_calls.get("subscribe"), topic)
        self.assertEqual(option_calls.get("failOnDataLoss"), "true")
        self.assertEqual(option_calls.get("kafka.isolation.level"), "read_committed")
        
        self.assertEqual(json.loads(option_calls.get("startingOffsets")), start_offsets)
        self.assertEqual(json.loads(option_calls.get("endingOffsets")), end_offsets)

    def test_real_spark_reader_passes_read_committed_consumer_property(self):
        from batch.session import get_spark_session
        from pyspark.sql.readwriter import DataFrameReader

        spark = get_spark_session("test-kafka-reader-isolation")
        topics = "swipeline.decisions.v1,swipeline.lifecycle.v1"
        starts = {topic: {"0": 0} for topic in topics.split(",")}
        ends = {topic: {"0": 2} for topic in topics.split(",")}
        configured = {}

        def capture_load(reader):
            # Inspect the real JVM reader options before connecting to Kafka.
            entries = reader._jreader.extraOptions().iterator()
            while entries.hasNext():
                entry = entries.next()
                configured[entry._1()] = entry._2()
            return "bounded-dataframe"

        with patch.object(DataFrameReader, "load", autospec=True, side_effect=capture_load):
            frame = read_kafka_range(spark, "kafka:9092", topics, starts, ends)

        self.assertEqual(frame, "bounded-dataframe")
        # Spark's Kafka source strips precisely the kafka. prefix.
        consumer_properties = {
            key[len("kafka."):]: value
            for key, value in configured.items() if key.startswith("kafka.")
        }
        self.assertEqual(consumer_properties.get("isolation.level"), "read_committed")
        self.assertNotIn("consumer.isolation.level", consumer_properties)
        self.assertEqual(json.loads(configured["startingoffsets"]), starts)
        self.assertEqual(json.loads(configured["endingoffsets"]), ends)


if __name__ == "__main__":
    unittest.main()
