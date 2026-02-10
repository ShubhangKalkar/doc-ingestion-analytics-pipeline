import sqlite3
from src.sitemap.extractor import crawl_sitemaps
from src.consolidation.docs_master import consolidate_docs_master
from src.ingestion.content_fetcher import fetch_and_store_content

DB_PATH = "local.db"

ROOT_SITEMAPS = [
    "https://docs.snowflake.com/en/sitemap.xml",
    "https://other-docs.snowflake.com/en/sitemap.xml"
]

def create_tables(conn):
    with open("sql/tables.sql", "r") as f:
        conn.executescript(f.read())

def insert_staging_rows(conn, rows):
    query = """
        INSERT INTO CANDIDATE_SK_SITEMAP_STAGING
        (url, source_sitemap, lastmod, discovered_at)
        VALUES (?, ?, ?, ?)
    """
    conn.executemany(
        query,
        [
            (
                r["url"],
                r["source_sitemap"],
                r["lastmod"],
                r["discovered_at"].isoformat()
            )
            for r in rows
        ]
    )
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    rows = crawl_sitemaps(ROOT_SITEMAPS)
    insert_staging_rows(conn, rows)

    # Task 2 execution
    consolidate_docs_master(conn)

    staging_count = conn.execute(
        "SELECT COUNT(*) FROM CANDIDATE_SK_SITEMAP_STAGING"
    ).fetchone()[0]

    master_count = conn.execute(
        "SELECT COUNT(*) FROM CANDIDATE_SK_DOCS_MASTER"
    ).fetchone()[0]

    print(f"SITEMAP_STAGING row count: {staging_count}")
    print(f"DOCS_MASTER row count: {master_count}")

    urls = conn.execute(
    "SELECT url FROM CANDIDATE_SK_DOCS_MASTER LIMIT 50"
    ).fetchall()

    for (url,) in urls:
        fetch_and_store_content(conn, url)

    content_count = conn.execute(
        "SELECT COUNT(*) FROM CANDIDATE_SK_DOCUMENT_CONTENT"
    ).fetchone()[0]

    print(f"DOCUMENT_CONTENT row count: {content_count}")

    conn.close()

if __name__ == "__main__":
    main()
