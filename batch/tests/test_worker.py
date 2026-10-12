import unittest
from unittest.mock import MagicMock, patch

from batch.db import BATCH_WRITER_LOCK_KEY
from batch.worker import run_batch


class TestWorkerPipeline(unittest.TestCase):
    def test_run_batch_pipeline_clean(self):
        spark = MagicMock()
        coordinator = MagicMock()

        coordinator.acquire_advisory_lock.return_value = True
        coordinator.get_pending_batch.return_value = None
        coordinator.get_latest_offsets.return_value = {"swipeline.lifecycle.v1": {"0": 0}}
        coordinator.start_batch.return_value = "batch-test-123"

        start_offsets = {"swipeline.lifecycle.v1": {"0": 0}}
        end_offsets = {"swipeline.lifecycle.v1": {"0": 10}}

        with patch("batch.worker.get_kafka_end_offsets", return_value=end_offsets), \
             patch("batch.worker.read_kafka_range") as mock_read_kafka, \
             patch("batch.worker.ingest_journal", return_value="instant-j-1") as mock_journal, \
             patch("batch.worker.process_transactions", return_value="instant-t-1") as mock_tx, \
             patch("batch.worker.process_postings", return_value="instant-p-1") as mock_postings, \
             patch("batch.worker.TransactionReducer"):


            res = run_batch(
                spark,
                coordinator,
                brokers="localhost:9092",
                topic="swipeline.lifecycle.v1",
                close_day="2026-10-11",
                start_offsets=start_offsets,
                end_offsets=end_offsets,
            )

            self.assertTrue(res)
            coordinator.acquire_advisory_lock.assert_called_with(BATCH_WRITER_LOCK_KEY)
            coordinator.start_batch.assert_called_with(start_offsets, end_offsets)
            coordinator.update_instant.assert_any_call("batch-test-123", "journal", "instant-j-1")
            coordinator.update_instant.assert_any_call("batch-test-123", "transaction", "instant-t-1")
            coordinator.update_instant.assert_any_call("batch-test-123", "posting", "instant-p-1")
            coordinator.complete_batch.assert_called_with("batch-test-123")
            coordinator.release_advisory_lock.assert_called_with(BATCH_WRITER_LOCK_KEY)


    def test_advance_offsets_only_on_completion(self):
        spark = MagicMock()
        coordinator = MagicMock()

        coordinator.acquire_advisory_lock.return_value = True
        coordinator.get_pending_batch.return_value = None
        coordinator.get_latest_offsets.return_value = {"swipeline.lifecycle.v1": {"0": 0}}
        coordinator.start_batch.return_value = "batch-fail-456"

        start_offsets = {"swipeline.lifecycle.v1": {"0": 0}}
        end_offsets = {"swipeline.lifecycle.v1": {"0": 10}}

        state_holder = {"state": "STARTED"}
        def fake_update(bid, step, inst):
            if step == "transaction":
                state_holder["state"] = "TRANSACTIONS_COMMITTED"
            elif step == "journal":
                state_holder["state"] = "JOURNAL_COMMITTED"
            elif step == "posting":
                state_holder["state"] = "POSTINGS_COMMITTED"

        def fake_complete(bid):
            state_holder["state"] = "COMPLETE"

        coordinator.update_instant.side_effect = fake_update
        coordinator.complete_batch.side_effect = fake_complete
        coordinator.get_latest_offsets.side_effect = lambda: (
            end_offsets if state_holder["state"] == "COMPLETE" else start_offsets
        )

        with patch("batch.worker.get_kafka_end_offsets", return_value=end_offsets), \
             patch("batch.worker.read_kafka_range"), \
             patch("batch.worker.ingest_journal", return_value="instant-j-1"), \
             patch("batch.worker.process_transactions", return_value="instant-t-1"), \
             patch("batch.worker.process_postings", side_effect=RuntimeError("Simulated posting failure")), \
             patch("batch.worker.TransactionReducer"):

            with self.assertRaises(RuntimeError):
                run_batch(
                    spark,
                    coordinator,
                    brokers="localhost:9092",
                    topic="swipeline.lifecycle.v1",
                    close_day="2026-10-11",
                    start_offsets=start_offsets,
                    end_offsets=end_offsets,
                )

            # Assert journal and transaction checkpoints were updated
            coordinator.update_instant.assert_any_call("batch-fail-456", "journal", "instant-j-1")
            coordinator.update_instant.assert_any_call("batch-fail-456", "transaction", "instant-t-1")
            
            # Assert posting was NEVER updated and batch was NEVER completed
            for call in coordinator.update_instant.call_args_list:
                self.assertNotEqual(call[0][1], "posting")
            coordinator.complete_batch.assert_not_called()
            
            # Assert state remains TRANSACTIONS_COMMITTED and offsets did NOT advance
            self.assertEqual(state_holder["state"], "TRANSACTIONS_COMMITTED")
            self.assertEqual(coordinator.get_latest_offsets(), start_offsets)

            # Lock must still be released
            coordinator.release_advisory_lock.assert_called_with(BATCH_WRITER_LOCK_KEY)

    def test_recover_after_transactions_committed(self):
        spark = MagicMock()
        coordinator = MagicMock()

        batch_id = "batch-recover-789"
        pending_record = {
            "batch_id": batch_id,
            "starting_offsets": {"swipeline.lifecycle.v1": {"0": 10}},
            "ending_offsets": {"swipeline.lifecycle.v1": {"0": 20}},
            "journal_instant": "instant-j-old",
            "transaction_instant": "instant-t-old",
            "posting_instant": None,
            "state": "TRANSACTIONS_COMMITTED",
        }
        coordinator.get_pending_batch.return_value = pending_record

        with patch("batch.worker.read_kafka_range"), \
             patch("batch.worker.ingest_journal") as mock_journal, \
             patch("batch.worker.process_transactions") as mock_tx, \
             patch("batch.worker.process_postings", return_value="instant-p-new") as mock_postings, \
             patch("batch.worker.TransactionReducer"):

            from batch.worker import resume_pending_batch
            resume_pending_batch(spark, coordinator, batch_id)

            # Assert journal and transactions were NOT re-written
            mock_journal.assert_not_called()
            mock_tx.assert_not_called()

            # Assert postings were processed and committed
            mock_postings.assert_called_once()
            coordinator.update_instant.assert_called_once_with(batch_id, "posting", "instant-p-new")
            coordinator.complete_batch.assert_called_once_with(batch_id)

    def test_cli_argument_parsing(self):
        from batch.worker import parse_args, parse_interval_seconds

        args_once = parse_args(["--once"])
        self.assertTrue(args_once.once)

        args_poll = parse_args(["--poll", "45s"])
        self.assertEqual(args_poll.poll, "45s")
        self.assertEqual(parse_interval_seconds(args_poll.poll), 45)

        args_poll_int = parse_args(["--poll", "15"])
        self.assertEqual(parse_interval_seconds(args_poll_int.poll), 15)

    def test_run_loop_once(self):
        from batch.worker import run_loop

        spark = MagicMock()
        coordinator = MagicMock()
        with patch("batch.worker.run_batch") as mock_run:
            run_loop(spark, coordinator, once=True)
            mock_run.assert_called_once_with(spark, coordinator)

    def test_run_loop_stop_event(self):
        from batch.worker import run_loop

        spark = MagicMock()
        coordinator = MagicMock()
        stop_event = [True]
        with patch("batch.worker.run_batch") as mock_run:
            run_loop(spark, coordinator, once=False, stop_event=stop_event)
            mock_run.assert_not_called()


if __name__ == "__main__":
    unittest.main()




