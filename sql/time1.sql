-- Precomputed helper table updated during ingestion
CREATE TABLE IF NOT EXISTS RECENTLY_MODIFIED_DOCS (
    url TEXT PRIMARY KEY,
    lastmod TEXT
);

-- Fast query
SELECT
    COUNT(*) AS documents_modified_last_7_days
FROM RECENTLY_MODIFIED_DOCS;
