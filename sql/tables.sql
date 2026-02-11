-- Raw sitemap discoveries
CREATE TABLE IF NOT EXISTS CANDIDATE_SK_SITEMAP_STAGING (
    url TEXT NOT NULL,
    source_sitemap TEXT NOT NULL,
    lastmod TEXT,
    discovered_at TEXT NOT NULL
);

-- Canonical document registry
CREATE TABLE IF NOT EXISTS CANDIDATE_SK_DOCS_MASTER (
    url TEXT PRIMARY KEY,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    lastmod TEXT,
    sources TEXT
);

CREATE TABLE IF NOT EXISTS CANDIDATE_SK_DOCUMENT_CONTENT (
    url TEXT PRIMARY KEY,
    content TEXT,
    content_hash TEXT,
    fetched_at TEXT,
    status_code INTEGER,
    error TEXT
);
