import unittest
from unittest.mock import MagicMock, patch
import json

from batch.db import BatchCoordinator, BATCH_WRITER_LOCK_KEY


class TestBatchCoordinator(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor

    def test_dsn_from_environment(self):
        with patch.dict("os.environ", {"POSTGRES_DSN": "postgres://env:5432/gateway"}, clear=True):
            self.assertEqual(BatchCoordinator().dsn, "postgres://env:5432/gateway")

    def test_missing_dsn_is_rejected(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(ValueError):
                BatchCoordinator()

    @patch("batch.db.psycopg2.connect")
    def test_start_batch(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        
        starting_offsets = {"swipeline.lifecycle.v1": {"0": 100, "1": 150}}
        ending_offsets = {"swipeline.lifecycle.v1": {"0": 200, "1": 250}}
        
        batch_id = coordinator.start_batch(starting_offsets, ending_offsets)
        self.assertIsNotNone(batch_id)
        self.assertTrue(len(batch_id) > 0)
        
        self.mock_cursor.execute.assert_called_once()
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("INSERT INTO ingest_batches", query)
        self.assertEqual(params[0], batch_id)
        self.assertEqual(json.loads(params[1]), starting_offsets)
        self.assertEqual(json.loads(params[2]), ending_offsets)
        self.assertEqual(params[3], "STARTED")

    @patch("batch.db.psycopg2.connect")
    def test_update_instant(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        
        batch_id = "test-batch-001"
        coordinator.update_instant(batch_id, "JOURNAL_COMMITTED", "20261011120000")
        
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("UPDATE ingest_batches", query)
        self.assertIn("journal_instant", query)
        self.assertEqual(params[0], "20261011120000")
        self.assertEqual(params[1], "JOURNAL_COMMITTED")
        self.assertEqual(params[2], batch_id)

        coordinator.update_instant(batch_id, "TRANSACTIONS_COMMITTED", "20261011120100")
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("transaction_instant", query)
        self.assertEqual(params[0], "20261011120100")
        self.assertEqual(params[1], "TRANSACTIONS_COMMITTED")
        self.assertEqual(params[2], batch_id)

        coordinator.update_instant(batch_id, "POSTINGS_COMMITTED", "20261011120200")
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("posting_instant", query)
        self.assertEqual(params[0], "20261011120200")
        self.assertEqual(params[1], "POSTINGS_COMMITTED")
        self.assertEqual(params[2], batch_id)

    @patch("batch.db.psycopg2.connect")
    def test_complete_batch(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        
        batch_id = "test-batch-001"
        coordinator.complete_batch(batch_id)
        
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("UPDATE ingest_batches", query)
        self.assertIn("COMPLETE", query)
        self.assertEqual(params[0], batch_id)

    @patch("batch.db.psycopg2.connect")
    def test_get_latest_offsets_empty(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        self.mock_cursor.fetchone.return_value = None
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        
        offsets = coordinator.get_latest_offsets()
        self.assertEqual(offsets, {})

    @patch("batch.db.psycopg2.connect")
    def test_get_latest_offsets_completed(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        starting = {"swipeline.lifecycle.v1": {"0": 100}}
        ending = {"swipeline.lifecycle.v1": {"0": 200}}
        self.mock_cursor.fetchone.return_value = (json.dumps(starting), json.dumps(ending), "COMPLETE")
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        
        offsets = coordinator.get_latest_offsets()
        self.assertEqual(offsets, ending)

    @patch("batch.db.psycopg2.connect")
    def test_acquire_and_release_advisory_lock(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        self.mock_cursor.fetchone.side_effect = [(True,), (True,)]
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        
        locked = coordinator.acquire_advisory_lock(BATCH_WRITER_LOCK_KEY)
        self.assertTrue(locked)
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("pg_try_advisory_lock", query)
        self.assertEqual(params[0], BATCH_WRITER_LOCK_KEY)
        
        released = coordinator.release_advisory_lock(BATCH_WRITER_LOCK_KEY)
        self.assertTrue(released)
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("pg_advisory_unlock", query)
        self.assertEqual(params[0], BATCH_WRITER_LOCK_KEY)

    @patch("batch.db.psycopg2.connect")
    def test_advisory_lock_already_held(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        self.mock_cursor.fetchone.return_value = (False,)
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        
        locked = coordinator.acquire_advisory_lock(BATCH_WRITER_LOCK_KEY)
        self.assertFalse(locked)

    @patch("batch.db.psycopg2.connect")
    def test_acquire_and_release_uses_new_constant(self, mock_connect):
        from batch.db import BATCH_WRITER_LOCK_KEY
        mock_connect.return_value = self.mock_conn
        self.mock_cursor.fetchone.side_effect = [(True,), (True,)]
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")

        self.assertNotEqual(BATCH_WRITER_LOCK_KEY, 72002)
        self.assertEqual(BATCH_WRITER_LOCK_KEY, 72003)

        locked = coordinator.acquire_advisory_lock()
        self.assertTrue(locked)
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("pg_try_advisory_lock", query)
        self.assertEqual(params[0], 72003)

        released = coordinator.release_advisory_lock()
        self.assertTrue(released)
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("pg_advisory_unlock", query)
        self.assertEqual(params[0], 72003)

    @patch("batch.db.psycopg2.connect")
    def test_resume_pending_batch_offsets(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        starting = {"swipeline.lifecycle.v1": {"0": 100, "1": 150}}
        ending = {"swipeline.lifecycle.v1": {"0": 200, "1": 250}}
        self.mock_cursor.fetchone.return_value = (
            json.dumps(starting),
            json.dumps(ending),
            "TRANSACTIONS_COMMITTED",
        )
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        
        offsets = coordinator.get_latest_offsets()
        # The query selects completed batches only, so pending batches are excluded.
        self.assertIn("WHERE state = 'COMPLETE'", self.mock_cursor.execute.call_args[0][0])
        self.assertEqual(offsets, ending)

    @patch("batch.db.psycopg2.connect")
    def test_get_pending_batch(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        starting = {"swipeline.lifecycle.v1": {"0": 100}}
        ending = {"swipeline.lifecycle.v1": {"0": 200}}
        self.mock_cursor.fetchone.return_value = (
            "batch-123",
            json.dumps(starting),
            json.dumps(ending),
            "20261011120000",
            "20261011120100",
            None,
            "TRANSACTIONS_COMMITTED",
            {},
        )
        coordinator = BatchCoordinator(dsn="postgres://mock:5432/gateway")
        pending = coordinator.get_pending_batch()
        self.assertIsNotNone(pending)
        self.assertEqual(pending["batch_id"], "batch-123")
        self.assertEqual(pending["state"], "TRANSACTIONS_COMMITTED")
        self.assertEqual(pending["transaction_instant"], "20261011120100")
        self.assertIsNone(pending["posting_instant"])


if __name__ == "__main__":
    unittest.main()
