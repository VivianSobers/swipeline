import json
import os
import uuid

try:
    import psycopg2
except ImportError:
    psycopg2 = None


class BatchCoordinator:
    """Coordinates batch ingestion progress and state transitions in PostgreSQL."""

    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL")
        if not self.dsn:
            raise ValueError("POSTGRES_DSN or DATABASE_URL must be set")
        self._conn = None

    def _get_connection(self):
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is required but not installed")
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = True
        return self._conn

    def close(self):
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def start_batch(
        self,
        starting_offsets: dict,
        ending_offsets: dict,
        batch_id: str | None = None,
    ) -> str:
        """Starts an ingestion batch and persists starting and ending offsets."""
        if not batch_id:
            batch_id = str(uuid.uuid4())

        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest_batches (batch_id, starting_offsets, ending_offsets, state)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    batch_id,
                    json.dumps(starting_offsets),
                    json.dumps(ending_offsets),
                    "STARTED",
                ),
            )
        return batch_id

    def update_instant(self, batch_id: str, step: str, instant: str) -> None:
        """Updates instant timestamp and state checkpoint for a stage."""
        column_map = {
            "JOURNAL_COMMITTED": ("journal_instant", "JOURNAL_COMMITTED"),
            "journal": ("journal_instant", "JOURNAL_COMMITTED"),
            "journal_instant": ("journal_instant", "JOURNAL_COMMITTED"),
            "TRANSACTIONS_COMMITTED": ("transaction_instant", "TRANSACTIONS_COMMITTED"),
            "transaction": ("transaction_instant", "TRANSACTIONS_COMMITTED"),
            "transaction_instant": ("transaction_instant", "TRANSACTIONS_COMMITTED"),
            "POSTINGS_COMMITTED": ("posting_instant", "POSTINGS_COMMITTED"),
            "posting": ("posting_instant", "POSTINGS_COMMITTED"),
            "posting_instant": ("posting_instant", "POSTINGS_COMMITTED"),
        }

        if step not in column_map:
            raise ValueError(f"Unknown step: {step}")

        col_name, state = column_map[step]
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE ingest_batches
                SET {col_name} = %s, state = %s
                WHERE batch_id = %s
                """,
                (instant, state, batch_id),
            )

    def complete_batch(self, batch_id: str) -> None:
        """Marks the batch as complete."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingest_batches
                SET state = 'COMPLETE', completed_at = clock_timestamp()
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

    def get_latest_offsets(self) -> dict:
        """Returns ending offsets of the latest completed batch, or starting offsets of an uncompleted batch."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT starting_offsets, ending_offsets, state
                FROM ingest_batches
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                return {}

            starting_offsets, ending_offsets, state = row
            if state != "COMPLETE":
                return (
                    starting_offsets
                    if isinstance(starting_offsets, dict)
                    else json.loads(starting_offsets)
                )
            return (
                ending_offsets
                if isinstance(ending_offsets, dict)
                else json.loads(ending_offsets)
            )

    def acquire_advisory_lock(self, key: int = 72002) -> bool:
        """Attempts to acquire PostgreSQL session advisory lock using pg_try_advisory_lock."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
            row = cur.fetchone()
            return bool(row[0]) if row else False

    def release_advisory_lock(self, key: int = 72002) -> bool:
        """Releases PostgreSQL session advisory lock using pg_advisory_unlock."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
            row = cur.fetchone()
            return bool(row[0]) if row else False

    def get_pending_batch(self) -> dict | None:
        """Returns the most recent uncompleted batch record, or None if all batches are complete."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT batch_id, starting_offsets, ending_offsets, journal_instant, transaction_instant, posting_instant, state
                FROM ingest_batches
                WHERE state != 'COMPLETE'
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "batch_id": row[0],
                "starting_offsets": (
                    row[1] if isinstance(row[1], dict) else json.loads(row[1])
                ),
                "ending_offsets": (
                    row[2] if isinstance(row[2], dict) else json.loads(row[2])
                ),
                "journal_instant": row[3],
                "transaction_instant": row[4],
                "posting_instant": row[5],
                "state": row[6],
            }
