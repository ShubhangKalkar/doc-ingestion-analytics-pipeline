import sqlite3

conn = sqlite3.connect("local.db")
cur = conn.cursor()

queries = {
    "4a": """SELECT
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
            ORDER BY document_count DESC, source ASC;""",
    
    "4b": """SELECT
                substr(lastmod, 1, 7) AS month,
                COUNT(*) AS document_count
            FROM CANDIDATE_SK_DOCS_MASTER
            WHERE lastmod IS NOT NULL
                AND date(lastmod) >= date('now', '-12 months')
            GROUP BY month
            ORDER BY month ASC;""",

    "4c": """SELECT
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
            ORDER BY success_rate_pct DESC, source ASC;""",
    
    "4d": """SELECT
                '/' || substr(
                    path,
                    1,
                    instr(path, '/') - 1
                ) AS path_segment,
                COUNT(*) AS frequency
            FROM (
                SELECT
                    substr(url, length('https://docs.snowflake.com/') + 1) AS path
                FROM CANDIDATE_SK_DOCS_MASTER
                WHERE url LIKE 'https://docs.snowflake.com/%'
            )
            WHERE instr(path, '/') > 0
            GROUP BY path_segment
            ORDER BY frequency DESC, path_segment ASC
            LIMIT 10;
            """,

    
    "4e": """SELECT
                COUNT(*) AS stale_count,
                ROUND(
                    100.0 * COUNT(*) /
                    (SELECT COUNT(*) FROM CANDIDATE_SK_DOCS_MASTER),
                    2
                ) AS stale_percentage
            FROM CANDIDATE_SK_DOCS_MASTER
            WHERE lastmod IS NOT NULL
                AND date(lastmod) < date('now', '-180 days');""",
}

for k, q in queries.items():
    print(f"\n--- Result {k} ---")
    rows = cur.execute(q).fetchall()
    for r in rows[:10]:  # limit printing
        print(r)

conn.close()
