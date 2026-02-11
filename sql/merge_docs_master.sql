MERGE INTO CANDIDATE_SK_DOCS_MASTER AS target
USING (
    SELECT
        url,
        MIN(discovered_at) AS first_seen_at,
        MAX(discovered_at) AS last_seen_at,
        MAX(lastmod) AS lastmod,
        ARRAY_AGG(DISTINCT source_sitemap) AS sources
    FROM CANDIDATE_SK_SITEMAP_STAGING
    GROUP BY url
) AS src
ON target.url = src.url

WHEN MATCHED THEN UPDATE SET
    last_seen_at = src.last_seen_at,
    lastmod = src.lastmod,
    sources = ARRAY_DISTINCT(
        ARRAY_CAT(target.sources, src.sources)
    )

WHEN NOT MATCHED THEN INSERT (
    url,
    first_seen_at,
    last_seen_at,
    lastmod,
    sources
) VALUES (
    src.url,
    src.first_seen_at,
    src.last_seen_at,
    src.lastmod,
    src.sources
);
