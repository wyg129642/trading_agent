"""API-based monitors for SEC EDGAR, Federal Register, SSE, SZSE."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from src.models import NewsItem
from src.monitors.base import BaseMonitor

logger = logging.getLogger(__name__)


class SECEdgarMonitor(BaseMonitor):
    """Monitor SEC EDGAR full-text search API for AI-related filings."""

    SEARCH_QUERIES = [
        '"artificial intelligence"',
        '"machine learning"',
        '"GPU" AND ("data center" OR "AI")',
        '"large language model"',
        '"generative AI"',
    ]

    async def fetch_items(self) -> list[NewsItem]:
        items = []
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Search for AI-related filings
        for query in self.SEARCH_QUERIES[:2]:  # Limit queries per cycle
            try:
                url = "https://efts.sec.gov/LATEST/search-index"
                params = {
                    "q": query,
                    "dateRange": "custom",
                    "startdt": yesterday,
                    "enddt": today,
                    "forms": "8-K,10-K,10-Q,6-K",
                }
                headers = {
                    "User-Agent": "TradingAgent admin@tradingagent.com",
                    "Accept": "application/json",
                }
                raw = await self._get(url, params=params, headers=headers)
                data = json.loads(raw)

                hits = data.get("hits", {}).get("hits", [])
                for hit in hits[:10]:
                    source = hit.get("_source", {})
                    title = source.get("display_names", [""])[0] if source.get("display_names") else ""
                    form_type = source.get("form_type", "")
                    filing_date = source.get("file_date", "")
                    entity = source.get("entity_name", "")

                    if entity:
                        title = f"[{form_type}] {entity}: {title}" if title else f"[{form_type}] {entity}"

                    file_url = ""
                    if source.get("file_num"):
                        file_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum={source['file_num']}&type=&dateb=&owner=include&count=10"

                    if not title:
                        continue

                    item = NewsItem(
                        source_name=self.name,
                        title=title,
                        url=file_url or self.url,
                        content=f"Form: {form_type}, Entity: {entity}, Filed: {filing_date}",
                        language="en",
                        market="us",
                        metadata={"form_type": form_type, "entity": entity},
                    )
                    items.append(item)
            except Exception as e:
                logger.debug("[SEC EDGAR] Query '%s' failed: %s", query, e)

        return items


class FederalRegisterMonitor(BaseMonitor):
    """Monitor US Federal Register for AI-related executive orders and rules."""

    async def fetch_items(self) -> list[NewsItem]:
        items = []

        params = {
            "conditions[term]": "artificial intelligence OR semiconductor OR AI chip",
            "per_page": "20",
            "order": "newest",
        }
        headers = {"Accept": "application/json"}

        raw = await self._get(self.url, params=params, headers=headers)
        data = json.loads(raw)

        for doc in data.get("results", [])[:15]:
            title = doc.get("title", "")
            url = doc.get("html_url", "")
            doc_type = doc.get("type", "")
            pub_date = doc.get("publication_date", "")
            abstract = doc.get("abstract", "") or ""

            if not title:
                continue

            published = None
            if pub_date:
                try:
                    published = datetime.strptime(pub_date, "%Y-%m-%d")
                except ValueError:
                    pass

            # Skip items older than our last-seen timestamp
            if published and self._last_seen_dt and published <= self._last_seen_dt:
                continue

            item = NewsItem(
                source_name=self.name,
                title=f"[{doc_type}] {title}",
                url=url,
                content=abstract[:2000],
                published_at=published,
                language="en",
                market="us",
                metadata={"doc_type": doc_type, "document_number": doc.get("document_number", "")},
            )
            items.append(item)

        return items


class _CninfoMonitorBase(BaseMonitor):
    """Base class for SSE/SZSE monitors using cninfo.com.cn API.

    The old SSE (query.sse.com.cn) and SZSE (www.szse.cn/api/disc) endpoints
    are unreliable or deprecated.  cninfo.com.cn (巨潮资讯网) is the official
    disclosure platform for both exchanges and provides a stable API.
    """

    # Subclasses override with their tracked stock codes
    TRACKED_STOCKS: list[str] = []
    # cninfo column filter: "szse" for SZSE, empty string for SSE/all
    CNINFO_COLUMN: str = ""
    EXCHANGE_TAG: str = "SSE"

    _CNINFO_SEARCH_URL = "http://www.cninfo.com.cn/new/information/topSearch/query"
    _CNINFO_ANN_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"

    # Cache: stock_code -> orgId  (populated once on first call)
    _org_id_cache: dict[str, str] = {}

    async def _lookup_org_id(self, stock_code: str) -> str:
        """Look up cninfo orgId for a stock code (cached)."""
        if stock_code in self._org_id_cache:
            return self._org_id_cache[stock_code]

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "http://www.cninfo.com.cn/",
            }
            payload = f"keyWord={stock_code}&maxSecNum=10&maxListNum=5"
            raw = await self._post(
                self._CNINFO_SEARCH_URL, data=payload, headers=headers,
            )
            data = json.loads(raw)
            if data and isinstance(data, list):
                org_id = data[0].get("orgId", "")
                self._org_id_cache[stock_code] = org_id
                return org_id
        except Exception as e:
            logger.debug("[cninfo] orgId lookup failed for %s: %s", stock_code, e)
        return ""

    async def fetch_items(self) -> list[NewsItem]:
        items = []

        for stock_code in self.TRACKED_STOCKS:
            try:
                org_id = await self._lookup_org_id(stock_code)
                stock_param = f"{stock_code},{org_id}" if org_id else stock_code

                payload = (
                    f"stock={stock_param}&tabName=fulltext"
                    f"&pageSize=10&pageNum=1"
                    f"&column={self.CNINFO_COLUMN}"
                    f"&isHLtitle=true"
                )
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer": "http://www.cninfo.com.cn/",
                    "Accept": "application/json",
                }
                raw = await self._post(
                    self._CNINFO_ANN_URL, data=payload, headers=headers,
                )
                data = json.loads(raw)

                announcements = data.get("announcements") or []
                for ann in announcements[:5]:
                    title = ann.get("announcementTitle", "")
                    adj_url = ann.get("adjunctUrl", "")
                    ann_time = ann.get("announcementTime", 0)

                    if not title:
                        continue

                    doc_url = ""
                    if adj_url:
                        doc_url = f"http://static.cninfo.com.cn/{adj_url}"

                    published = None
                    if isinstance(ann_time, (int, float)) and ann_time > 0:
                        try:
                            published = datetime.fromtimestamp(ann_time / 1000)
                        except (ValueError, OSError):
                            pass

                    # Skip items older than our last-seen timestamp
                    if published and self._last_seen_dt and published <= self._last_seen_dt:
                        continue

                    item = NewsItem(
                        source_name=self.name,
                        title=f"[{stock_code}] {title}",
                        url=doc_url or self.url,
                        published_at=published,
                        language="zh",
                        market="china",
                        metadata={"stock_code": stock_code, "exchange": self.EXCHANGE_TAG},
                    )
                    items.append(item)
            except Exception as e:
                logger.debug("[%s] Stock %s failed: %s", self.EXCHANGE_TAG, stock_code, e)

        return items


class SSEMonitor(_CninfoMonitorBase):
    """Monitor Shanghai Stock Exchange announcements via cninfo.com.cn."""

    TRACKED_STOCKS = ["688256", "688041", "603019", "688111", "601360", "601138"]
    CNINFO_COLUMN = ""  # empty = all exchanges (works for SSE)
    EXCHANGE_TAG = "SSE"


class SZSEMonitor(_CninfoMonitorBase):
    """Monitor Shenzhen Stock Exchange announcements via cninfo.com.cn."""

    TRACKED_STOCKS = ["002230", "300418", "000977", "300308", "000938", "002261", "300033"]
    CNINFO_COLUMN = "szse"
    EXCHANGE_TAG = "SZSE"


# Factory: create the right monitor based on api_type
from src.monitors.hotnews_monitor import HotNewsMonitor

API_MONITOR_MAP = {
    "sec_edgar": SECEdgarMonitor,
    "federal_register": FederalRegisterMonitor,
    "sse": SSEMonitor,
    "szse": SZSEMonitor,
    "hotnews": HotNewsMonitor,
}
