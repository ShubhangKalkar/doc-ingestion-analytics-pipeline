from unittest.mock import patch
from src.ingestion.content_fetcher import fetch_and_store_content
import sqlite3

def test_throttling_called():
    conn = sqlite3.connect(":memory:")

    conn.execute("""
        CREATE TABLE CANDIDATE_SK_DOCUMENT_CONTENT (
            url TEXT PRIMARY KEY,
            content TEXT,
            content_hash TEXT,
            fetched_at TEXT,
            status_code INTEGER,
            error TEXT
        )
    """)

    with patch("time.sleep") as mock_sleep, \
         patch("requests.get") as mock_get:

        mock_get.return_value.status_code = 200
        mock_get.return_value.text = "test content"

        fetch_and_store_content(conn, "https://example.com")

        mock_sleep.assert_called()
