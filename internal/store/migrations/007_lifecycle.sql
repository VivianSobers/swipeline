CREATE TABLE IF NOT EXISTS lifecycle_receipts(
  event_id text PRIMARY KEY,
  transaction_id text NOT NULL REFERENCES authorizations(transaction_id),
  payload_hash text NOT NULL,
  payload_json jsonb NOT NULL,
  seal_epoch bigint NOT NULL,
  close_day date NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_lifecycle_receipts_tx ON lifecycle_receipts(transaction_id);
