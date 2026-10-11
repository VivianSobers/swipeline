ALTER TABLE ingest_batches
    ADD COLUMN posting_assignments JSONB NOT NULL DEFAULT '{}'::jsonb;
