SELECT
    substr(lastmod, 1, 7) AS month,
    COUNT(*) AS document_count
FROM CANDIDATE_SK_DOCS_MASTER
WHERE lastmod IS NOT NULL
  AND date(lastmod) >= date('now', '-12 months')
GROUP BY month
ORDER BY month ASC;