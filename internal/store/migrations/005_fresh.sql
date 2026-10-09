-- finalize also reports whether this call committed the final, so each
-- outcome is counted once however often it is requested.
CREATE OR REPLACE FUNCTION finalize(id text, p jsonb, decision_id text, part integer)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  a authorizations;
  observed numeric;
  f jsonb;
  source text;
BEGIN
  SELECT * INTO STRICT a FROM authorizations WHERE transaction_id = id FOR UPDATE;
  IF a.status = 'FINAL' THEN
    RETURN jsonb_build_object('request', a.request_json, 'final', a.final_json, 'fresh', false);
  END IF;
  observed := extract(epoch FROM clock_timestamp()) * 1000;
  IF p IS NULL AND observed < a.scoring_deadline_at THEN
    RETURN jsonb_build_object('request', a.request_json, 'final', NULL, 'fresh', false);
  END IF;
  IF p IS NOT NULL AND (p->>'transaction_id' <> id OR p->>'payload_hash' <> a.payload_hash
    OR p->>'rule_version' <> a.request_json->>'rule_version') THEN
    RAISE EXCEPTION 'proposal conflict' USING ERRCODE = '23505';
  END IF;
  source := CASE WHEN p IS NOT NULL AND observed < a.scoring_deadline_at THEN 'FLINK' ELSE 'STAND_IN' END;
  IF source = 'FLINK' THEN
    f := jsonb_build_object('decision', p->'decision', 'reason', p->'reason', 'fraud_flags', p->'fraud_flags',
      'proposal_id', p->'proposal_id', 'score', p->'score', 'velocity_60s', p->'velocity_60s',
      'usual_minor', p->'usual_minor', 'spend_ratio', p->'spend_ratio',
      'merchant_distinct_estimate', p->'merchant_distinct_estimate', 'features_late', p->'features_late');
  ELSE
    f := jsonb_build_object('decision', 'DECLINE', 'reason', 'SCORING_TIMEOUT', 'fraud_flags', '[]'::jsonb,
      'proposal_id', NULL, 'score', NULL, 'velocity_60s', NULL, 'usual_minor', NULL, 'spend_ratio', NULL,
      'merchant_distinct_estimate', NULL, 'features_late', NULL);
  END IF;
  f := a.request_json || f || jsonb_build_object('decision_id', decision_id, 'decision_source', source,
    'finalized_at_ms', floor(observed)::bigint, 'decision_latency_ms', greatest(0, observed - a.received_at));
  UPDATE authorizations SET status = 'FINAL', final_json = f WHERE transaction_id = id;
  INSERT INTO outbox(event_id, topic, key, partition, payload)
    VALUES(decision_id, 'swipeline.decisions.raw.v1', convert_to(id, 'UTF8'), part, f);
  RETURN jsonb_build_object('request', a.request_json, 'final', f, 'fresh', true);
END $$;
