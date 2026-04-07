"""Web search tools: Baidu Search API + Tavily Search API + Jina Search API.

Baidu Search API:
  - Uses Qianfan AI Search endpoint
  - Returns structured references with content, dates, relevance scores
  - Primary search engine for Chinese content
  - Query language: Chinese

Tavily Search API:
  - High-quality web search with content extraction
  - Returns structured results with content, scores, dates
  - Query language: English

Jina Search API:
  - Web search via s.jina.ai with content extraction
  - Also provides r.jina.ai for reading full page content
  - Query language: English
  - A/B testing with Tavily — both run in parallel

DuckDuckGo:
  - Fallback when both Tavily and Jina are unavailable
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any
from urllib.parse import quote, urlparse

import httpx

logger = logging.getLogger(__name__)

BAIDU_API_URL = "https://qianfan.baidubce.com/v2/ai_search/web_search"
TAVILY_API_URL = "https://api.tavily.com/search"
JINA_SEARCH_URL = "https://s.jina.ai/"
JINA_READER_URL = "https://r.jina.ai/"


async def baidu_search(
    query: str,
    api_key: str,
    max_results: int = 10,
    recency: str = "year",
) -> list[dict[str, Any]]:
    """Search via Baidu Qianfan AI Search API.

    Args:
        query: Search query text (Chinese).
        api_key: Baidu Qianfan AppBuilder API key.
        max_results: Maximum number of web results (up to 50).
        recency: Time filter — "week", "month", "semiyear", "year".

    Returns:
        List of dicts: {title, url, content, date, score, source, website}.
    """
    headers = {
        "X-Appbuilder-Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "messages": [{"content": query, "role": "user"}],
        "search_source": "baidu_search_v2",
        "resource_type_filter": [{"type": "web", "top_k": min(max_results, 50)}],
        "search_recency_filter": recency,
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(BAIDU_API_URL, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        if "code" in data and data.get("code") != 0:
            logger.warning("Baidu search API error: %s", data.get("message", "unknown"))
            return []

        references = data.get("references", [])
        results = []
        for ref in references:
            if ref.get("type") != "web":
                continue
            results.append({
                "title": ref.get("title", ""),
                "url": ref.get("url", ""),
                "content": ref.get("content", "")[:2000],
                "date": ref.get("date", ""),
                "score": ref.get("rerank_score", 0),
                "authority": ref.get("authority_score", 0),
                "source": "baidu",
                "website": ref.get("website") or ref.get("web_anchor", ""),
            })
        logger.info("[BaiduSearch] query='%s' -> %d results", query[:50], len(results))
        return results

    except httpx.TimeoutException:
        logger.warning("Baidu search timeout for: %s", query[:50])
        return []
    except Exception as e:
        logger.error("Baidu search failed for '%s': %s", query[:50], e)
        return []


async def tavily_search(
    query: str,
    api_key: str,
    max_results: int = 10,
    search_depth: str = "basic",
    topic: str = "general",
    days: int | None = None,
) -> list[dict[str, Any]]:
    """Search via Tavily Search API.

    Args:
        query: Search query text (English).
        api_key: Tavily API key.
        max_results: Maximum number of results (up to 20).
        search_depth: "basic" (fast) or "advanced" (thorough).
        topic: "general" or "news" for news-focused results.
        days: If set, limit results to the last N days.

    Returns:
        List of dicts: {title, url, content, date, score, source, website}.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload: dict[str, Any] = {
        "query": query,
        "max_results": min(max_results, 20),
        "search_depth": search_depth,
        "topic": topic,
        "include_answer": False,
    }
    if days:
        payload["days"] = days

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(TAVILY_API_URL, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        tavily_results = data.get("results", [])
        results = []
        for item in tavily_results:
            parsed_url = urlparse(item.get("url", ""))
            website = parsed_url.netloc.replace("www.", "")

            # Extract date if available
            date_str = item.get("published_date", "") or ""
            if date_str and len(date_str) >= 10:
                date_str = date_str[:10]

            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "content": (item.get("content", "") or "")[:2000],
                "date": date_str,
                "score": item.get("score", 0),
                "authority": 0,
                "source": "tavily",
                "website": website,
            })
        logger.info("[TavilySearch] query='%s' -> %d results", query[:50], len(results))
        return results

    except httpx.TimeoutException:
        logger.warning("Tavily search timeout for: %s", query[:50])
        return []
    except Exception as e:
        logger.error("Tavily search failed for '%s': %s", query[:50], e)
        return []


async def jina_search(
    query: str,
    api_key: str,
    max_results: int = 10,
) -> list[dict[str, Any]]:
    """Search via Jina Search API (s.jina.ai).

    Args:
        query: Search query text.
        api_key: Jina API key.
        max_results: Maximum number of results.

    Returns:
        List of dicts: {title, url, content, date, score, source, website}.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "X-Retain-Images": "none",
    }

    # Jina Search API: GET https://s.jina.ai/{query}
    url = JINA_SEARCH_URL + quote(query, safe="")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        if data.get("code") != 200:
            logger.warning(
                "Jina search API error: code=%s, status=%s",
                data.get("code"), data.get("status"),
            )
            return []

        jina_results = data.get("data", [])
        results = []
        for item in jina_results[:max_results]:
            item_url = item.get("url", "")
            parsed_url = urlparse(item_url)
            website = parsed_url.netloc.replace("www.", "")

            # Jina returns full content; truncate for consistency
            content = item.get("content", "") or item.get("description", "") or ""

            results.append({
                "title": item.get("title", ""),
                "url": item_url,
                "content": content[:2000],
                "date": "",  # Jina doesn't return dates in search results
                "score": 0,
                "authority": 0,
                "source": "jina",
                "website": website,
            })
        logger.info("[JinaSearch] query='%s' -> %d results", query[:50], len(results))
        return results

    except httpx.TimeoutException:
        logger.warning("Jina search timeout for: %s", query[:50])
        return []
    except Exception as e:
        logger.error("Jina search failed for '%s': %s", query[:50], e)
        return []


async def jina_read_url(
    url: str,
    api_key: str,
    max_chars: int = 8000,
) -> str | None:
    """Extract clean content from a URL via Jina Reader API (r.jina.ai).

    Args:
        url: The URL to read.
        api_key: Jina API key.
        max_chars: Maximum characters to return.

    Returns:
        Extracted text content, or None on failure.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "X-Retain-Images": "none",
        "X-No-Cache": "true",
    }

    reader_url = JINA_READER_URL + url

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(reader_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        if data.get("code") != 200:
            logger.warning("Jina reader error for %s: %s", url[:60], data.get("status"))
            return None

        content = data.get("data", {}).get("content", "")
        if content:
            logger.info("[JinaReader] Extracted %d chars from: %s", len(content), url[:60])
            return content[:max_chars]
        return None

    except httpx.TimeoutException:
        logger.warning("Jina reader timeout for: %s", url[:60])
        return None
    except Exception as e:
        logger.debug("Jina reader failed for '%s': %s", url[:60], e)
        return None


async def duckduckgo_search(
    query: str,
    max_results: int = 8,
) -> list[dict[str, Any]]:
    """Fallback search via DuckDuckGo (when Tavily+Jina both unavailable).

    Uses both text search and news search for better coverage,
    then deduplicates and merges results.

    Returns:
        List of dicts: {title, url, content, date, score, source, website}.
    """
    try:
        from duckduckgo_search import DDGS

        def _search():
            all_results = []
            with DDGS() as ddgs:
                # Text search for general results
                text_results = list(ddgs.text(query, max_results=max_results))
                for r in text_results:
                    parsed_url = urlparse(r.get("href", ""))
                    website = parsed_url.netloc.replace("www.", "")
                    all_results.append({
                        "title": r.get("title", ""),
                        "url": r.get("href", ""),
                        "content": r.get("body", ""),
                        "date": "",
                        "score": 0,
                        "authority": 0,
                        "source": "duckduckgo",
                        "website": website,
                    })

                # News search for more recent/relevant news results
                try:
                    news_results = list(ddgs.news(query, max_results=min(max_results, 5)))
                    for r in news_results:
                        parsed_url = urlparse(r.get("url", ""))
                        website = parsed_url.netloc.replace("www.", "")
                        all_results.append({
                            "title": r.get("title", ""),
                            "url": r.get("url", ""),
                            "content": r.get("body", ""),
                            "date": r.get("date", ""),
                            "score": 0,
                            "authority": 0,
                            "source": "duckduckgo",
                            "website": website or r.get("source", ""),
                        })
                except Exception as e:
                    logger.debug("DuckDuckGo news search failed (non-critical): %s", e)

            return all_results

        results = await asyncio.to_thread(_search)

        # Deduplicate by URL
        seen_urls: set[str] = set()
        deduped = []
        for r in results:
            url = r.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                deduped.append(r)

        logger.info("[DuckDuckGo] query='%s' -> %d results", query[:50], len(deduped))
        return deduped

    except ImportError:
        logger.warning("duckduckgo-search not installed. DDG search unavailable.")
        return []
    except Exception as e:
        logger.error("DuckDuckGo search failed for '%s': %s", query[:50], e)
        return []


async def multi_search(
    query: str,
    baidu_api_key: str = "",
    tavily_api_key: str = "",
    jina_api_key: str = "",
    use_english_search: bool = False,
    max_results: int = 10,
) -> list[dict[str, Any]]:
    """Search using multiple engines in parallel, deduplicate results.

    Args:
        query: Search query.
        baidu_api_key: Baidu API key (required for Baidu search).
        tavily_api_key: Tavily API key.
        jina_api_key: Jina API key.
        use_english_search: Whether to also use Tavily/Jina/DuckDuckGo.
        max_results: Max results per engine.

    Returns:
        Deduplicated list of search results from all engines.
    """
    tasks = []
    if baidu_api_key:
        tasks.append(baidu_search(query, baidu_api_key, max_results=max_results))
    if use_english_search:
        if tavily_api_key:
            tasks.append(tavily_search(query, tavily_api_key, max_results=max_results))
        if jina_api_key:
            tasks.append(jina_search(query, jina_api_key, max_results=max_results))
        if not tavily_api_key and not jina_api_key:
            tasks.append(duckduckgo_search(query, max_results=min(max_results, 5)))

    if not tasks:
        logger.warning("No search engines available for query: %s", query[:50])
        return []

    results_lists = await asyncio.gather(*tasks, return_exceptions=True)

    all_results = []
    for r in results_lists:
        if isinstance(r, list):
            all_results.extend(r)
        elif isinstance(r, Exception):
            logger.warning("Search engine error: %s", r)

    # Deduplicate by URL
    seen_urls: set[str] = set()
    deduped = []
    for item in all_results:
        url = item.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduped.append(item)

    return deduped


async def parallel_search(
    baidu_queries: list[str],
    google_queries: list[str],
    baidu_api_key: str = "",
    tavily_api_key: str = "",
    jina_api_key: str = "",
    google_api_key: str = "",
    google_cx: str = "",
    max_results: int = 10,
) -> dict[str, list[dict[str, Any]]]:
    """Run Baidu (Chinese) + Tavily + Jina (English) searches in parallel.

    This is the primary search function for the deep research pipeline.
    For English queries, both Tavily and Jina run simultaneously for A/B comparison.
    Results are tagged with their source engine for later analysis.

    Args:
        baidu_queries: Chinese search queries for Baidu.
        google_queries: English search queries (name kept for compatibility).
        baidu_api_key: Baidu API key.
        tavily_api_key: Tavily API key.
        jina_api_key: Jina API key.
        google_api_key: Deprecated, ignored.
        google_cx: Deprecated, ignored.
        max_results: Max results per query per engine.

    Returns:
        Dict mapping query -> list of search results, for all queries.
    """
    output: dict[str, list[dict]] = {}

    async def _baidu_one(q: str, delay: float = 0.0) -> tuple[str, list[dict]]:
        if delay > 0:
            await asyncio.sleep(delay)
        if not baidu_api_key:
            return q, []
        results = await baidu_search(q, baidu_api_key, max_results=max_results)
        return q, results

    async def _english_one(q: str, delay: float = 0.0) -> tuple[str, list[dict]]:
        """Run Tavily + Jina in parallel for each English query (A/B comparison)."""
        if delay > 0:
            await asyncio.sleep(delay)

        search_tasks = []

        # Tavily: try general first, supplement with news if few results
        if tavily_api_key:
            async def _tavily_combined():
                results = await tavily_search(
                    q, tavily_api_key, max_results=max_results, topic="general",
                )
                # If few results, supplement with news topic
                if len(results) < 3:
                    news_results = await tavily_search(
                        q, tavily_api_key, max_results=max_results, topic="news",
                    )
                    seen_urls = {r["url"] for r in results}
                    for nr in news_results:
                        if nr["url"] not in seen_urls:
                            results.append(nr)
                            seen_urls.add(nr["url"])
                return results

            search_tasks.append(_tavily_combined())

        # Jina: run in parallel with Tavily
        if jina_api_key:
            search_tasks.append(jina_search(q, jina_api_key, max_results=max_results))

        if search_tasks:
            engine_results = await asyncio.gather(*search_tasks, return_exceptions=True)
            # Merge results from all engines, deduplicate by URL
            all_results = []
            seen_urls: set[str] = set()
            for r in engine_results:
                if isinstance(r, list):
                    for item in r:
                        url = item.get("url", "")
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            all_results.append(item)
                elif isinstance(r, Exception):
                    logger.warning("[EnglishSearch] Engine error for '%s': %s", q[:50], r)

            if all_results:
                return q, all_results

        # Fallback to DuckDuckGo if no results from Tavily/Jina
        if tavily_api_key or jina_api_key:
            logger.warning(
                "[Search] Tavily+Jina returned no results for '%s', falling back to DuckDuckGo.",
                q[:50],
            )
        else:
            logger.info("[Search] No English search API configured, using DuckDuckGo for '%s'", q[:50])
        results = await duckduckgo_search(q, max_results=8)
        return q, results

    # Build all tasks — stagger Baidu by 0.5s to avoid 429s
    tasks = []
    for i, q in enumerate(baidu_queries):
        tasks.append(_baidu_one(q, delay=i * 0.5))
    for i, q in enumerate(google_queries):
        tasks.append(_english_one(q, delay=i * 0.3))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, tuple):
            query, res = r
            if query in output:
                # Merge and deduplicate
                existing_urls = {item["url"] for item in output[query]}
                for item in res:
                    if item["url"] not in existing_urls:
                        output[query].append(item)
                        existing_urls.add(item["url"])
            else:
                output[query] = res
        elif isinstance(r, Exception):
            logger.warning("Parallel search error: %s", r)

    return output


# Legacy interfaces for backward compatibility

async def batch_search(
    queries: list[str],
    baidu_api_key: str = "",
    use_google: bool = False,
    max_results: int = 10,
) -> dict[str, list[dict[str, Any]]]:
    """Legacy batch search -- kept for backward compatibility.

    For new code, use parallel_search() instead.
    """
    output: dict[str, list[dict]] = {}

    async def _search_one(q: str, delay: float = 0.0) -> tuple[str, list[dict]]:
        if delay > 0:
            await asyncio.sleep(delay)
        results = await multi_search(q, baidu_api_key, use_english_search=use_google, max_results=max_results)
        return q, results

    tasks = [_search_one(q, delay=i * 0.5) for i, q in enumerate(queries)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, tuple):
            query, res = r
            output[query] = res
        elif isinstance(r, Exception):
            logger.warning("Batch search error: %s", r)

    return output


def format_search_results(results: list[dict], max_per_result: int = 500) -> str:
    """Format search results into a readable string for LLM consumption."""
    if not results:
        return "No search results found."

    lines = []
    for i, r in enumerate(results, 1):
        title = r.get("title", "N/A")
        url = r.get("url", "")
        content = r.get("content", "")[:max_per_result]
        date = r.get("date", "")
        source = r.get("source", "")
        website = r.get("website", "")

        line = f"[{i}] {title}"
        if website:
            line += f" -- {website}"
        if date:
            line += f" ({date})"
        line += f"\n    {content}"
        if url:
            line += f"\n    URL: {url}"
        line += f"\n    [via {source}]"
        lines.append(line)

    return "\n\n".join(lines)


# Legacy interface for backward compatibility (tool executor)
async def web_search(query: str, max_results: int = 5) -> str:
    """Search the web via DuckDuckGo and return formatted results.

    Legacy interface maintained for tool executor compatibility.
    """
    results = await duckduckgo_search(query, max_results=max_results)
    if not results:
        return f"No search results found for: {query}"

    formatted = []
    for i, r in enumerate(results, 1):
        formatted.append(f"[{i}] {r['title']}\n    {r['content']}\n    URL: {r['url']}")
    return "\n\n".join(formatted)
