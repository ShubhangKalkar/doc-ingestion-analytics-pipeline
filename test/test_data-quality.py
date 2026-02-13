import sqlite3
import pytest

def test_docs_master_url_not_null():
    conn = sqlite3.connect(":memory:")

    conn.execute("""
        CREATE TABLE CANDIDATE_SK_DOCS_MASTER (
            url TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            lastmod TEXT,
            sources TEXT,
            PRIMARY KEY (url)
        )
    """)

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("""
            INSERT INTO CANDIDATE_SK_DOCS_MASTER
            (url, first_seen_at, last_seen_at)
            VALUES (NULL, '2024-01-01', '2024-01-02')
        """)
