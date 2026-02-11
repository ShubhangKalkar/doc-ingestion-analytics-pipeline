SELECT
    COUNT(*) AS stale_count,
    ROUND(
        100.0 * COUNT(*) /
        (SELECT COUNT(*) FROM CANDIDATE_SK_DOCS_MASTER),
        2
    ) AS stale_percentage
FROM CANDIDATE_SK_DOCS_MASTER
WHERE lastmod IS NOT NULL
  AND date(lastmod) < date('now', '-180 days');
