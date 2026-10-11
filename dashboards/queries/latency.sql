SELECT
  CASE
    WHEN "decision_latency_ms" < 5.0 THEN '0-5'
    WHEN "decision_latency_ms" < 10.0 THEN '5-10'
    WHEN "decision_latency_ms" < 20.0 THEN '10-20'
    WHEN "decision_latency_ms" < 35.0 THEN '20-35'
    WHEN "decision_latency_ms" < 50.0 THEN '35-50'
    ELSE '50+'
  END AS "latency_bucket",
  COUNT(*) AS "bucket_count",
  AVG("decision_latency_ms") AS "mean_latency_ms",
  MAX("decision_latency_ms") AS "max_latency_ms"
FROM "swipeline_decisions"
WHERE "run_id" = :run_id
GROUP BY
  CASE
    WHEN "decision_latency_ms" < 5.0 THEN '0-5'
    WHEN "decision_latency_ms" < 10.0 THEN '5-10'
    WHEN "decision_latency_ms" < 20.0 THEN '10-20'
    WHEN "decision_latency_ms" < 35.0 THEN '20-35'
    WHEN "decision_latency_ms" < 50.0 THEN '35-50'
    ELSE '50+'
  END
