"""Internal inspection service sharing the worker's initialized Spark session."""
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from batch.db import BATCH_WRITER_LOCK_KEY
from batch.inspect import inspect_transaction, format_inspection_result

logger = logging.getLogger(__name__)


def create_server(spark, coordinator_factory, address=('0.0.0.0', 8080)):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            logger.debug(*args)

        def reply(self, status, data):
            body = json.dumps(data).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if urlsplit(self.path).path != '/batch':
                self.reply(404, {'error': 'unknown route'})
                return
            coordinator = coordinator_factory()
            locked = False
            response = None
            try:
                locked = coordinator.acquire_advisory_lock(BATCH_WRITER_LOCK_KEY)
                if not locked:
                    response = (503, {'error': 'batch writer busy'})
                else:
                    from batch.worker import run_batch
                    # run_batch's session lock is reentrant; its release retains
                    # this outer acquisition until the HTTP operation finishes.
                    changed = run_batch(spark, coordinator)
                    response = (200, {'processed': changed})
            except Exception:
                logger.exception('Batch request failed')
                response = (500, {'error': 'batch failed'})
            finally:
                try:
                    if locked:
                        coordinator.release_advisory_lock(BATCH_WRITER_LOCK_KEY)
                finally:
                    coordinator.close()
            if response is not None:
                self.reply(*response)

        def do_GET(self):
            url = urlsplit(self.path)
            if url.path not in ('/health', '/inspect'):
                self.reply(404, {'error': 'unknown route'})
                return
            values = parse_qs(url.query).get('transaction_id', [])
            if url.path == '/inspect' and (len(values) != 1 or not values[0]):
                self.reply(400, {'error': 'transaction_id is required'})
                return
            # A separate DB session is essential: advisory locks are reentrant on
            # the writer's connection and would not exclude another HTTP thread.
            coordinator = coordinator_factory()
            locked = False
            response = None
            try:
                if url.path == '/health':
                    coordinator._get_connection()
                    response = (200, {'status': 'ready'})
                else:
                    locked = coordinator.acquire_advisory_lock(BATCH_WRITER_LOCK_KEY)
                    if not locked or coordinator.get_pending_batch():
                        response = (503, {'error': 'batch publication in progress'})
                    else:
                        result = inspect_transaction(spark, values[0])
                        response = (200, format_inspection_result(result))
            except Exception:
                logger.exception('Inspection request failed')
                response = (503 if url.path == '/health' else 500, {'error': 'inspection unavailable'})
            finally:
                try:
                    if locked:
                        coordinator.release_advisory_lock(BATCH_WRITER_LOCK_KEY)
                finally:
                    coordinator.close()
            if response is not None:
                self.reply(*response)

    server = ThreadingHTTPServer(address, Handler)
    server.spark = spark
    return server
