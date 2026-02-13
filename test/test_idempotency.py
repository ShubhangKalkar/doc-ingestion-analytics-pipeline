from datetime import datetime
from collections import defaultdict


def simulate_merge(staging_rows, existing_master=None):
    """
    Simulates DOCS_MASTER MERGE behavior in-memory.
    """
    if existing_master is None:
        existing_master = {}

    aggregated = defaultdict(lambda: {
        "first_seen_at": None,
        "last_seen_at": None,
        "lastmod": None,
        "sources": set()
    })

    for row in staging_rows:
        url = row["url"]
        aggregated[url]["sources"].add(row["source_sitemap"])

        ts = row["discovered_at"]
        aggregated[url]["first_seen_at"] = (
            ts if aggregated[url]["first_seen_at"] is None
            else min(aggregated[url]["first_seen_at"], ts)
        )
        aggregated[url]["last_seen_at"] = (
            ts if aggregated[url]["last_seen_at"] is None
            else max(aggregated[url]["last_seen_at"], ts)
        )

    for url, data in aggregated.items():
        if url not in existing_master:
            existing_master[url] = data
        else:
            existing_master[url]["last_seen_at"] = data["last_seen_at"]
            existing_master[url]["sources"].update(data["sources"])

    return existing_master


def test_docs_master_idempotency():
    now = datetime.utcnow()

    staging_rows = [
        {
            "url": "https://example.com/doc1",
            "source_sitemap": "sitemap_a.xml",
            "discovered_at": now
        },
        {
            "url": "https://example.com/doc1",
            "source_sitemap": "sitemap_b.xml",
            "discovered_at": now
        }
    ]

    # First run
    master_after_first = simulate_merge(staging_rows)

    # Second run (same input)
    master_after_second = simulate_merge(staging_rows, master_after_first)

    assert len(master_after_second) == 1
    assert "https://example.com/doc1" in master_after_second
    assert master_after_second["https://example.com/doc1"]["sources"] == {
        "sitemap_a.xml",
        "sitemap_b.xml"
    }
