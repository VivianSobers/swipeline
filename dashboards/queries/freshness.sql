SELECT
  MAX(__time) AS "max_time",
  (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) - EXTRACT(EPOCH FROM MAX(__time))) AS "ingestion_lag_seconds",
  COUNT(*) AS "total_count",
  CASE
    WHEN COUNT(*) = 0 THEN 0.0
    ELSE CAST(SUM(CASE WHEN "decision_source" = 'STAND_IN' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)
  END AS "standin_share"
FROM "swipeline_decisions"
WHERE "run_id" = :run_id
