SELECT "mcc", 'CARD_TESTING' AS "flag", COUNT(*) AS "flag_count"
FROM "swipeline_decisions"
WHERE "run_id" = :run_id AND MV_CONTAINS("fraud_flags", 'CARD_TESTING')
GROUP BY "mcc"
UNION ALL
SELECT "mcc", 'CARD_VELOCITY' AS "flag", COUNT(*) AS "flag_count"
FROM "swipeline_decisions"
WHERE "run_id" = :run_id AND MV_CONTAINS("fraud_flags", 'CARD_VELOCITY')
GROUP BY "mcc"
UNION ALL
SELECT "mcc", 'SPEND_ANOMALY' AS "flag", COUNT(*) AS "flag_count"
FROM "swipeline_decisions"
WHERE "run_id" = :run_id AND MV_CONTAINS("fraud_flags", 'SPEND_ANOMALY')
GROUP BY "mcc"
