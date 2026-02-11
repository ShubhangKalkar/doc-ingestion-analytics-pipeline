SELECT
    source AS source_sitemap,
    COUNT(*) AS document_count
FROM (
    SELECT
        url,
        TRIM(value) AS source
    FROM CANDIDATE_SK_DOCS_MASTER,
         json_each(
           '["' || REPLACE(sources, ',', '","') || '"]'
         )
)
GROUP BY source
ORDER BY document_count DESC, source ASC;