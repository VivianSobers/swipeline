import unittest
from unittest.mock import MagicMock, patch

from batch.inspect import inspect_transaction


class TestInspectTransaction(unittest.TestCase):
    def test_inspect_transaction_extraction(self):
        spark = MagicMock()
        tx_id = "tx-sample-123"

        # Mock table queries
        mock_tx_row = MagicMock()
        mock_tx_row.asDict.return_value = {
            "transaction_id": tx_id,
            "status": "CLEARED",
            "authorized_amount": 5000,
            "state_version": 2,
        }

        mock_event_row = MagicMock()
        mock_event_row.asDict.return_value = {
            "event_id": "evt-1",
            "logical_seq": 1,
            "event_type": "AUTH_DECISION",
        }

        mock_posting_row = MagicMock()
        mock_posting_row.asDict.return_value = {
            "posting_id": "post-1",
            "signed_principal": 5000,
            "signed_interchange": 75,
        }

        def mock_load(path):
            mock_df = MagicMock()
            if "transactions" in path:
                mock_df.filter.return_value.collect.return_value = [mock_tx_row]
            elif "ledger_events" in path:
                mock_df.filter.return_value.orderBy.return_value.collect.return_value = [mock_event_row]
            elif "settlement_postings" in path:
                mock_df.filter.return_value.orderBy.return_value.collect.return_value = [mock_posting_row]
            return mock_df

        spark.read.format.return_value.load.side_effect = mock_load

        with patch("batch.inspect.is_hudi_table", return_value=True):
            res = inspect_transaction(spark, tx_id)
            self.assertEqual(res["transaction_id"], tx_id)
            self.assertIsNotNone(res["transaction"])
            self.assertEqual(res["transaction"]["status"], "CLEARED")
            self.assertEqual(len(res["events"]), 1)
            self.assertEqual(res["events"][0]["event_type"], "AUTH_DECISION")
            self.assertEqual(len(res["postings"]), 1)
            self.assertEqual(res["postings"][0]["signed_interchange"], 75)

    def test_format_inspection_output(self):
        import json
        from batch.inspect import format_inspection_result, format_inspection_json

        raw_data = {
            "transaction_id": "tx-format-100",
            "transaction": {
                "transaction_id": "tx-format-100",
                "status": "CLEARED",
                "authorized_amount": 10000,
                "cleared_amount": 10000,
                "reversed_amount": 0,
                "charged_back_amount": 0,
                "currency": "USD",
                "state_hash": "hash-abc",
                "state_version": 2,
            },
            "events": [
                {
                    "event_id": "evt-1",
                    "logical_seq": 1,
                    "event_type": "AUTH_DECISION",
                    "event_time_ms": 1770000000000,
                    "payload_hash": "h1",
                },
                {
                    "event_id": "evt-2",
                    "logical_seq": 2,
                    "event_type": "CLEAR",
                    "event_time_ms": 1770000005000,
                    "payload_hash": "h2",
                },
            ],
            "postings": [
                {
                    "posting_id": "post-1",
                    "event_id": "evt-2",
                    "posting_date": "2026-10-11",
                    "signed_principal": 10000,
                    "signed_interchange": 150,
                    "fee_schedule_version": "150bps-v1",
                    "adjustment_reason": None,
                    "batch_id": "batch-1",
                }
            ],
        }

        formatted = format_inspection_result(raw_data)
        self.assertEqual(formatted["transaction_id"], "tx-format-100")
        self.assertEqual(formatted["status"], "CLEARED")
        self.assertEqual(formatted["amounts"]["authorized_amount"], 10000)
        self.assertEqual(formatted["amounts"]["cleared_amount"], 10000)
        self.assertEqual(formatted["state_hash"], "hash-abc")
        self.assertEqual(formatted["state_version"], 2)
        self.assertEqual(len(formatted["events"]), 2)
        self.assertEqual(len(formatted["postings"]), 1)
        self.assertEqual(formatted["postings"][0]["signed_interchange"], 150)

        # JSON serialization
        json_str = format_inspection_json(raw_data)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["status"], "CLEARED")
        self.assertEqual(parsed["amounts"]["cleared_amount"], 10000)

    def test_inspect_transaction_filters_by_column_value(self):
        tx_id = "tx' OR '1'='1"
        spark = MagicMock()
        df = spark.read.format.return_value.load.return_value
        df.filter.return_value.collect.return_value = []
        df.filter.return_value.orderBy.return_value.collect.return_value = []
        col_expr = MagicMock(name="col_expr")
        df.__getitem__.return_value.__eq__.return_value = col_expr

        with patch("batch.inspect.is_hudi_table", return_value=True):
            inspect_transaction(spark, tx_id)

        self.assertEqual(df.filter.call_count, 3)
        for call in df.filter.call_args_list:
            self.assertNotIsInstance(call.args[0], str)
            self.assertIs(call.args[0], col_expr)
        df.__getitem__.assert_called_with("transaction_id")
        df.__getitem__.return_value.__eq__.assert_called_with(tx_id)


if __name__ == "__main__":
    unittest.main()


class InspectFailureTests(unittest.TestCase):
    def test_table_read_failure_is_not_not_found(self):
        spark = MagicMock()
        spark.read.format.return_value.load.side_effect = RuntimeError('offline')
        with patch('batch.inspect.is_hudi_table', return_value=True):
            with self.assertRaisesRegex(RuntimeError, 'offline'):
                inspect_transaction(spark, "x' OR 1=1")

    def test_missing_table_returns_not_found(self):
        with patch('batch.inspect.is_hudi_table', return_value=False):
            self.assertIsNone(inspect_transaction(MagicMock(), 'missing')['transaction'])

    def test_identifier_is_column_literal(self):
        spark = MagicMock()
        frame = spark.read.format.return_value.load.return_value
        frame.filter.return_value.collect.return_value = []
        frame.filter.return_value.orderBy.return_value.collect.return_value = []
        with patch('batch.inspect.is_hudi_table', return_value=True):
            inspect_transaction(spark, "x' OR 1=1")
        frame.__getitem__.assert_called_with('transaction_id')
        frame.__getitem__.return_value.__eq__.assert_called_with("x' OR 1=1")

class InspectEventTimestampTests(unittest.TestCase):
    def test_persisted_journal_time_maps_to_public_field(self):
        from batch.inspect import format_inspection_result
        result = format_inspection_result({'events': [{'event_id': 'e', 'original_event_time_ms': 1791417600000}]})
        self.assertEqual(result['events'][0]['event_time_ms'], 1791417600000)
