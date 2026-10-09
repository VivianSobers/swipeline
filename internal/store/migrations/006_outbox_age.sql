-- Outbox rows record when they were written, so relay lag is observable.
ALTER TABLE outbox ADD COLUMN created_at timestamptz NOT NULL DEFAULT clock_timestamp();
