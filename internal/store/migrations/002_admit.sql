-- admit stores one validated event as a PENDING authorization with its
-- trusted receipt and request outbox row, or returns the stored row for a
-- retry with the same payload.
CREATE FUNCTION admit(p jsonb, received bigint, k bytea, part integer)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  a authorizations;
  r runs;
  seq bigint;
  q jsonb;
  mode text;
  score_ms integer;
  response_ms integer;
BEGIN
  SELECT * INTO a FROM authorizations WHERE transaction_id = p->>'transaction_id' FOR UPDATE;
  IF FOUND THEN
    IF a.payload_hash <> p->>'payload_hash' THEN
      RAISE EXCEPTION 'payload conflict' USING ERRCODE = '23505';
    END IF;
    RETURN jsonb_build_object('request', a.request_json, 'final', a.final_json);
  END IF;
  SELECT * INTO r FROM runs WHERE run_id = p->>'run_id' FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'unregistered run' USING ERRCODE = '23503';
  END IF;
  IF r.state <> 'OPEN'
    OR r.config_json->>'business_date' <> p->>'authorization_date'
    OR r.config_json->>'rule_version' <> p->>'rule_version'
    OR r.config_json->>'reference_version' <> p->>'reference_version' THEN
    RAISE EXCEPTION 'run conflict' USING ERRCODE = '23505';
  END IF;
  INSERT INTO card_sequences VALUES(r.run_id, p->>'card_token', 1)
    ON CONFLICT(run_id, card_token) DO UPDATE SET seq = card_sequences.seq + 1
    RETURNING card_sequences.seq INTO seq;
  mode := r.config_json->>'deadline_mode';
  score_ms := CASE mode WHEN 'latency' THEN 35 ELSE 80 END;
  response_ms := CASE mode WHEN 'latency' THEN 50 ELSE 100 END;
  q := p || jsonb_build_object('received_at_ms', received, 'scoring_deadline_at_ms', received + score_ms,
    'response_deadline_at_ms', received + response_ms, 'deadline_mode', mode, 'seal_epoch', r.seal_epoch,
    'close_day', p->>'authorization_date', 'accepted_card_seq', seq);
  INSERT INTO authorizations VALUES(p->>'transaction_id', r.run_id, p->>'payload_hash', q, received,
    received + score_ms, received + response_ms, mode, r.seal_epoch, (p->>'authorization_date')::date, seq,
    'PENDING', NULL);
  INSERT INTO outbox(event_id, topic, key, partition, payload)
    VALUES(p->>'event_id', 'swipeline.auth.requests.v1', k, part, q);
  UPDATE runs SET accepted_count = accepted_count + 1 WHERE run_id = r.run_id;
  RETURN jsonb_build_object('request', q, 'final', NULL);
END $$;
