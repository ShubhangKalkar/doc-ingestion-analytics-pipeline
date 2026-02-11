SELECT
    COUNT(*) AS documents_modified_last_7_days
FROM CANDIDATE_SK_DOCS_MASTER
WHERE lastmod IS NOT NULL
  AND date(lastmod) >= date('now', '-7 days');
