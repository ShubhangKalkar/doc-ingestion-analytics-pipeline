INSERT INTO CANDIDATE_SK_DOCS_MASTER (
    url,
    first_seen_at,
    last_seen_at,
    lastmod,
    sources
)
SELECT
    url,
    MIN(discovered_at) AS first_seen_at,
    MAX(discovered_at) AS last_seen_at,
    MAX(lastmod) AS lastmod,
    GROUP_CONCAT(DISTINCT source_sitemap) AS sources
FROM CANDIDATE_SK_SITEMAP_STAGING
GROUP BY url
ON CONFLICT(url) DO UPDATE SET
    last_seen_at = excluded.last_seen_at,
    lastmod = excluded.lastmod,
    sources = excluded.sources;
