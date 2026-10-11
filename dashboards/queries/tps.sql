SELECT
  TIME_FLOOR(__time, 'PT10S') AS "time_bucket",
  COUNT(*) / 10.0 AS "tps"
FROM "swipeline_decisions"
WHERE "run_id" = :run_id
GROUP BY 1
ORDER BY 1
