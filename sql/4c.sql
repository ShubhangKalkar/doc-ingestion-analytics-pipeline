SELECT
    source AS source_sitemap,
    COUNT(*) AS total_attempts,
    SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) AS success_count,
    ROUND(
        100.0 * SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS success_rate_pct
FROM (
    SELECT
        dc.url,
        dc.status_code,
        TRIM(value) AS source
    FROM CANDIDATE_SK_DOCUMENT_CONTENT dc
    JOIN CANDIDATE_SK_DOCS_MASTER dm
      ON dc.url = dm.url,
         json_each(
           '["' || REPLACE(dm.sources, ',', '","') || '"]'
         )
)
GROUP BY source
ORDER BY success_rate_pct DESC, source ASC;
