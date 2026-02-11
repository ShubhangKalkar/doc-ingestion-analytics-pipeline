-- Pre-flattened table
CREATE TABLE IF NOT EXISTS DOC_SOURCE_CONTENT_STATS AS
SELECT
    dm.url,
    TRIM(value) AS source,
    LENGTH(dc.content) AS content_length
FROM CANDIDATE_SK_DOCS_MASTER dm
JOIN CANDIDATE_SK_DOCUMENT_CONTENT dc
  ON dm.url = dc.url,
     json_each(
       '["' || REPLACE(dm.sources, ',', '","') || '"]'
     );

-- Fast aggregation
SELECT
    source,
    COUNT(DISTINCT url) AS unique_url_count,
    AVG(content_length) AS mean_content_length
FROM DOC_SOURCE_CONTENT_STATS
GROUP BY source;
