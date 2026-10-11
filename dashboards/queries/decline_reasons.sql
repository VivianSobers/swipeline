SELECT
  "reason",
  "decision_source",
  COUNT(*) AS "decline_count"
FROM "swipeline_decisions"
WHERE "run_id" = :run_id
  AND "decision" = 'DECLINE'
GROUP BY "reason", "decision_source"
ORDER BY "decline_count" DESC
