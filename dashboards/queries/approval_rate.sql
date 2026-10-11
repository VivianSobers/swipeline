SELECT
  CASE
    WHEN COUNT(*) = 0 THEN NULL
    ELSE CAST(SUM(CASE WHEN "decision" = 'APPROVE' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)
  END AS "approval_rate"
FROM "swipeline_decisions"
WHERE "run_id" = :run_id
