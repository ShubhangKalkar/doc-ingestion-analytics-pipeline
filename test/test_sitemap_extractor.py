from src.sitemap.extractor import crawl_sitemaps


def test_extractor_returns_rows():
    fake_sitemap = [
        "https://docs.snowflake.com/en/sitemap.xml"
    ]

    rows = crawl_sitemaps(fake_sitemap)

    assert isinstance(rows, list)
    assert len(rows) > 0
    assert "url" in rows[0]
