-- Precomputed hash index
CREATE TABLE IF NOT EXISTS CONTENT_HASH_INDEX AS
SELECT
    content_hash,
    url
FROM CANDIDATE_SK_DOCUMENT_CONTENT
WHERE content_hash IS NOT NULL;

-- Fast duplicate detection
SELECT
    content_hash,
    COUNT(DISTINCT url) AS url_count
FROM CONTENT_HASH_INDEX
GROUP BY content_hash
HAVING COUNT(DISTINCT url) > 1
ORDER BY url_count DESC;
