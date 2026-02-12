import requests
import hashlib
import time
from datetime import datetime

REQUEST_TIMEOUT = 10
THROTTLE_SECONDS = 0.5
MAX_RETRIES = 3


def hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fetch_and_store_content(conn, url):
    cur = conn.cursor()

    existing = cur.execute(
        "SELECT content_hash FROM CANDIDATE_SK_DOCUMENT_CONTENT WHERE url = ?",
        (url,)
    ).fetchone()

    try:
        for attempt in range(MAX_RETRIES):
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            status = response.status_code

            if status != 200:
                raise Exception(f"HTTP {status}")

            content = response.text
            content_hash = hash_content(content)

            # Change detection
            if existing and existing[0] == content_hash:
                return  # unchanged, skip

            cur.execute("""
                INSERT INTO CANDIDATE_SK_DOCUMENT_CONTENT
                (url, content, content_hash, fetched_at, status_code, error)
                VALUES (?, ?, ?, ?, ?, NULL)
                ON CONFLICT(url) DO UPDATE SET
                    content = excluded.content,
                    content_hash = excluded.content_hash,
                    fetched_at = excluded.fetched_at,
                    status_code = excluded.status_code,
                    error = NULL
            """, (
                url,
                content,
                content_hash,
                datetime.utcnow().isoformat(),
                status
            ))

            conn.commit()
            time.sleep(THROTTLE_SECONDS)
            return

    except Exception as e:
        cur.execute("""
            INSERT INTO CANDIDATE_SK_DOCUMENT_CONTENT
            (url, content, content_hash, fetched_at, status_code, error)
            VALUES (?, NULL, NULL, ?, NULL, ?)
            ON CONFLICT(url) DO UPDATE SET
                error = excluded.error,
                fetched_at = excluded.fetched_at
        """, (
            url,
            datetime.utcnow().isoformat(),
            str(e)
        ))
        conn.commit()
