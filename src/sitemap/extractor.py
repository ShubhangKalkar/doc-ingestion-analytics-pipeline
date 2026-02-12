import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import deque

REQUEST_TIMEOUT = 10


def fetch_xml(url: str) -> str:
    """
    Fetch sitemap XML content from a URL.
    """
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def parse_sitemap(xml_content: str):
    """
    Parse XML and determine whether it is a sitemapindex or urlset.
    Returns (root_tag, parsed_tree)
    """
    root = ET.fromstring(xml_content)
    return root.tag, root


def crawl_sitemaps(root_sitemaps: list[str]):
    """
    Crawl sitemap indexes and urlsets recursively.
    Returns rows suitable for insertion into SITEMAP_STAGING.
    """
    queue = deque(root_sitemaps)
    visited = set()
    discovered_rows = []

    while queue:
        sitemap_url = queue.popleft()

        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)

        try:
            xml_content = fetch_xml(sitemap_url)
            root_tag, root = parse_sitemap(xml_content)
        except Exception:
            # Skip malformed or unreachable sitemaps
            continue

        # Handle sitemap index
        if root_tag.endswith("sitemapindex"):
            for sitemap in root.findall(".//{*}sitemap"):
                loc = sitemap.find("{*}loc")
                if loc is not None and loc.text:
                    queue.append(loc.text.strip())

        # Handle urlset
        elif root_tag.endswith("urlset"):
            for url_node in root.findall(".//{*}url"):
                loc = url_node.find("{*}loc")
                lastmod_node = url_node.find("{*}lastmod")

                if loc is None or not loc.text:
                    continue

                discovered_rows.append({
                    "url": loc.text.strip(),
                    "source_sitemap": sitemap_url,
                    "lastmod": (
                        lastmod_node.text.strip()
                        if lastmod_node is not None
                        else None
                    ),
                    "discovered_at": datetime.utcnow()
                })

    return discovered_rows
