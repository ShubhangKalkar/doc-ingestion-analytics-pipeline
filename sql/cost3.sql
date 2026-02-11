SELECT
    content_hash,
    COUNT(DISTINCT url) AS url_count
FROM CANDIDATE_SK_DOCUMENT_CONTENT
WHERE content_hash IS NOT NULL
GROUP BY content_hash
HAVING COUNT(DISTINCT url) > 1
ORDER BY url_count DESC;
