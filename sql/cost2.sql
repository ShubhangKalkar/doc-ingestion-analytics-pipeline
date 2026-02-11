SELECT
    source,
    COUNT(DISTINCT dm.url) AS unique_url_count,
    AVG(LENGTH(dc.content)) AS mean_content_length
FROM CANDIDATE_SK_DOCS_MASTER dm
JOIN CANDIDATE_SK_DOCUMENT_CONTENT dc
  ON dm.url = dc.url,
     json_each(
       '["' || REPLACE(dm.sources, ',', '","') || '"]'
     ) AS s(source)
GROUP BY source;
