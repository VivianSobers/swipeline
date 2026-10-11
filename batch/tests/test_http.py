import json
import threading
import unittest
from unittest.mock import MagicMock, patch
from urllib.request import urlopen
from urllib.error import HTTPError

from batch.http import create_server


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.coordinator = MagicMock()
        self.coordinator.acquire_advisory_lock.return_value = True
        self.coordinator.get_pending_batch.return_value = None
        self.server = create_server(object(), lambda: self.coordinator, ('127.0.0.1', 0))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:%d' % self.server.server_port

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_inspect_contract_and_independent_lock(self):
        with patch('batch.http.inspect_transaction', return_value={'transaction_id': "x' OR 1=1", 'transaction': {'status': 'CLEARED'}, 'events': [], 'postings': []}) as query:
            with urlopen(self.url + '/inspect?transaction_id=x%27%20OR%201%3D1') as response:
                data = json.load(response)
            self.assertEqual(data['status'], 'CLEARED')
            query.assert_called_once_with(self.server.spark, "x' OR 1=1")
        self.coordinator.acquire_advisory_lock.assert_called_once_with(72002)
        self.coordinator.release_advisory_lock.assert_called_once_with(72002)
        self.coordinator.close.assert_called_once()

    def test_busy_pending_and_storage_failure_are_errors(self):
        for failure in ('busy', 'pending', 'storage'):
            with self.subTest(failure=failure):
                self.coordinator.acquire_advisory_lock.return_value = failure != 'busy'
                self.coordinator.get_pending_batch.return_value = {'state': 'STARTED'} if failure == 'pending' else None
                with patch('batch.http.inspect_transaction', side_effect=RuntimeError('storage failed')):
                    with self.assertRaises(HTTPError) as caught:
                        urlopen(self.url + '/inspect?transaction_id=tx')
                    self.assertEqual(caught.exception.code, 503 if failure != 'storage' else 500)
                    caught.exception.close()

    def test_missing_id_and_unknown_route(self):
        for path, status in [('/inspect', 400), ('/unknown', 404)]:
            with self.assertRaises(HTTPError) as caught:
                urlopen(self.url + path)
            self.assertEqual(caught.exception.code, status)
            caught.exception.close()

    def test_health_checks_database(self):
        self.coordinator._get_connection.side_effect = RuntimeError('offline')
        with self.assertRaises(HTTPError) as caught:
            urlopen(self.url + '/health')
        self.assertEqual(caught.exception.code, 503)
        caught.exception.close()

    def test_batch_request_propagates_failure(self):
        from urllib.request import Request
        with patch('batch.worker.run_batch', side_effect=RuntimeError('ingestion failed')):
            with self.assertRaises(HTTPError) as caught:
                urlopen(Request(self.url + '/batch', method='POST'))
            self.assertEqual(caught.exception.code, 500)
            caught.exception.close()

    def test_batch_uses_existing_spark(self):
        from urllib.request import Request
        with patch('batch.worker.run_batch', return_value=True) as run:
            with urlopen(Request(self.url + '/batch', method='POST')) as response:
                self.assertEqual(json.load(response), {'processed': True})
            run.assert_called_once_with(self.server.spark, self.coordinator)

    def test_batch_busy_is_retryable(self):
        from urllib.request import Request
        self.coordinator.acquire_advisory_lock.return_value = False
        with self.assertRaises(HTTPError) as caught:
            urlopen(Request(self.url + '/batch', method='POST'))
        self.assertEqual(caught.exception.code, 503)
        caught.exception.close()
