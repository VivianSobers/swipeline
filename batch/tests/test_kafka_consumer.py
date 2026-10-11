import json
import unittest
from unittest.mock import MagicMock

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
        self.assertEqual(option_calls.get("kafka.consumer.isolation.level"), "read_committed")
        
        self.assertEqual(json.loads(option_calls.get("startingOffsets")), start_offsets)
        self.assertEqual(json.loads(option_calls.get("endingOffsets")), end_offsets)


if __name__ == "__main__":
    unittest.main()
