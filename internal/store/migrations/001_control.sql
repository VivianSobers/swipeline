CREATE TABLE runs(
  run_id text PRIMARY KEY,
  config_json jsonb NOT NULL,
  config_hash text NOT NULL,
  state text NOT NULL DEFAULT 'OPEN',
  seal_epoch bigint NOT NULL DEFAULT 0,
  accepted_count bigint NOT NULL DEFAULT 0,
  lifecycle_count bigint NOT NULL DEFAULT 0);
CREATE TABLE card_sequences(
  run_id text NOT NULL REFERENCES runs,
  card_token text NOT NULL,
  seq bigint NOT NULL,
  PRIMARY KEY(run_id, card_token));
CREATE TABLE authorizations(
  transaction_id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES runs,
  payload_hash text NOT NULL,
  request_json jsonb NOT NULL,
  received_at bigint NOT NULL,
  scoring_deadline_at bigint NOT NULL,
  response_deadline_at bigint NOT NULL,
  deadline_mode text NOT NULL CHECK(deadline_mode IN ('latency', 'throughput')),
  seal_epoch bigint NOT NULL,
  close_day date NOT NULL,
  accepted_card_seq bigint NOT NULL,
  status text NOT NULL CHECK(status IN ('PENDING', 'FINAL')),
  final_json jsonb,
  CHECK((status = 'FINAL') = (final_json IS NOT NULL)));
CREATE TABLE outbox(
  outbox_id bigserial PRIMARY KEY,
  event_id text UNIQUE NOT NULL,
  topic text NOT NULL,
  key bytea NOT NULL,
  partition integer NOT NULL CHECK(partition BETWEEN 0 AND 3),
  payload jsonb NOT NULL,
  sent_at timestamptz,
  broker_offset bigint);
CREATE INDEX outbox_unsent ON outbox(outbox_id) WHERE sent_at IS NULL;
CREATE INDEX pending_deadlines ON authorizations(scoring_deadline_at) WHERE status = 'PENDING';
