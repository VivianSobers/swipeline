CREATE TABLE IF NOT EXISTS ingest_batches(
  batch_id text PRIMARY KEY,
  starting_offsets jsonb NOT NULL,
  ending_offsets jsonb NOT NULL,
  journal_instant text,
  transaction_instant text,
  posting_instant text,
  state text NOT NULL CHECK(state IN ('STARTED', 'JOURNAL_COMMITTED', 'TRANSACTIONS_COMMITTED', 'POSTINGS_COMMITTED', 'COMPLETE')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz
);
