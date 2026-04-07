"""Text extraction utilities for HTML pages and documents."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    import trafilatura
except ImportError:
    trafilatura = None

try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None

logger = logging.getLogger(__name__)


def extract_text_from_html(html: str, url: str = "") -> str:
    """Extract clean text from HTML content.

    Tries trafilatura first (best quality), falls back to BeautifulSoup.
    """
    if trafilatura:
        text = trafilatura.extract(html, url=url, include_comments=False)
        if text and len(text.strip()) > 50:
            return text.strip()

    # Fallback: BeautifulSoup
    soup = BeautifulSoup(html, "lxml")

    # Remove script and style elements
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)
    # Collapse multiple newlines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_links_from_html(
    html: str,
    base_url: str,
    css_selector: str = "a",
    item_selector: str = "",
    title_selector: str = "",
    link_selector: str = "",
) -> list[dict]:
    """Extract structured items (title + link) from an HTML page.

    Returns a list of dicts with 'title', 'url', and optionally 'date'.
    """
    soup = BeautifulSoup(html, "lxml")
    items = []

    # Try the specific container first
    container = soup
    if css_selector and css_selector not in ("body", "main"):
        found = soup.select_one(css_selector)
        if found:
            container = found

    # Get all matching items
    if item_selector:
        # Try multiple selectors separated by comma
        elements = container.select(item_selector)
    else:
        elements = container.find_all("a", href=True)

    seen_urls = set()
    for elem in elements:
        title = ""
        url = ""

        # Extract title
        if title_selector == "self":
            title = elem.get_text(strip=True)
        elif title_selector:
            for sel in title_selector.split(","):
                sel = sel.strip()
                title_elem = elem.select_one(sel)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            if not title:
                title = elem.get_text(strip=True)
        else:
            title = elem.get_text(strip=True)

        # Extract URL
        if link_selector == "self" or not link_selector:
            url = elem.get("href", "")
        else:
            for sel in link_selector.split(","):
                sel = sel.strip()
                if sel == "self":
                    url = elem.get("href", "")
                    break
                link_elem = elem.select_one(sel)
                if link_elem:
                    url = link_elem.get("href", "")
                    break

        if not url or not title:
            continue

        # Skip anchors, javascript, and common non-content links
        if url.startswith(("#", "javascript:", "mailto:")):
            continue
        if len(title) < 4:
            continue

        # Make URL absolute
        url = urljoin(base_url, url)

        if url in seen_urls:
            continue
        seen_urls.add(url)

        items.append({"title": title.strip(), "url": url})

    return items


def truncate_text(text: str, max_chars: int = 3000) -> str:
    """Truncate text to a maximum number of characters, breaking at word boundary."""
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_space = truncated.rfind(" ")
    if last_space > max_chars * 0.8:
        truncated = truncated[:last_space]
    return truncated + "..."


# --- Publish date extraction ---

_META_DATE_ATTRS = [
    ("property", "article:published_time"),
    ("property", "og:publish_date"),
    ("property", "og:published_time"),
    ("name", "pubdate"),
    ("name", "publishdate"),
    ("name", "date"),
    ("name", "DC.date.issued"),
    ("itemprop", "datePublished"),
    ("itemprop", "dateCreated"),
]


def _parse_date(date_str: str) -> datetime | None:
    """Try to parse a date string into a timezone-aware datetime."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()

    # dateutil (best coverage)
    if dateutil_parser:
        try:
            dt = dateutil_parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, OverflowError):
            pass

    # Manual fallback for common ISO formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%Y年%m月%d日",
    ):
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def extract_publish_date(html: str, url: str = "") -> datetime | None:
    """Extract the publish date from HTML content.

    Strategy order:
    1. trafilatura JSON metadata
    2. <meta> tags (article:published_time, og:publish_date, etc.)
    3. <time datetime> elements
    4. JSON-LD schema.org (datePublished)
    5. dateutil fallback on visible date-like strings
    """
    # 1. trafilatura metadata
    if trafilatura:
        try:
            meta = trafilatura.extract(html, url=url, output_format="json", include_comments=False)
            if meta:
                data = json.loads(meta) if isinstance(meta, str) else meta
                for key in ("date", "date_published", "date_created"):
                    val = data.get(key)
                    if val:
                        dt = _parse_date(str(val))
                        if dt:
                            return dt
        except Exception:
            pass

    soup = BeautifulSoup(html, "lxml")

    # 2. <meta> tags
    for attr_name, attr_value in _META_DATE_ATTRS:
        tag = soup.find("meta", attrs={attr_name: attr_value})
        if tag:
            content = tag.get("content", "")
            dt = _parse_date(content)
            if dt:
                return dt

    # 3. <time datetime> elements
    for time_tag in soup.find_all("time", attrs={"datetime": True}):
        dt = _parse_date(time_tag["datetime"])
        if dt:
            return dt

    # 4. JSON-LD schema.org
    for script_tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            ld = json.loads(script_tag.string or "")
            # Handle both single objects and arrays
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if not isinstance(item, dict):
                    continue
                for key in ("datePublished", "dateCreated", "dateModified"):
                    val = item.get(key)
                    if val:
                        dt = _parse_date(str(val))
                        if dt:
                            return dt
        except (json.JSONDecodeError, TypeError):
            pass

    return None
