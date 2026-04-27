"""Web search tool definitions and execution for LLM function calling.

Provides OpenAI-compatible tool definitions for web_search and read_webpage,
a system prompt for search behavior guidance, and tool execution with
smart engine routing and citation tracking.

Engine routing:
  - Chinese stock / A-share queries → Tavily + Jina (Chinese + English keywords)
  - General / international queries  → Tavily + Jina (English; Chinese fallback if no en query)

Baidu is intentionally disabled (2026-04-27); only Tavily and Jina are used.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any
from urllib.parse import urlparse

from backend.app.config import get_settings

logger = logging.getLogger(__name__)

# ── OpenAI-compatible tool definitions ──────────────────────────

WEB_SEARCH_TOOLS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": (
                "搜索互联网获取实时信息。当你需要查询最新股价、近期新闻、实时市场数据、"
                "最近发生的事件、或任何你训练数据截止后可能变化的信息时使用此工具。"
                "你应当自行生成优化的搜索关键词，而非直接使用用户的原始问题。"
                "可以多次调用此工具，使用不同关键词获取更全面的信息。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query_cn": {
                        "type": "string",
                        "description": (
                            "中文搜索关键词（必填）。用于Tavily和Jina搜索（中文输入）。"
                            "应当简洁、精准，针对搜索引擎优化。"
                        ),
                    },
                    "query_en": {
                        "type": "string",
                        "description": (
                            "英文搜索关键词（强烈建议填写）。用于Tavily和Jina搜索。"
                            "对于A股/中国市场话题，提供英文翻译可获得国际视角的中国市场分析。"
                            "对于国际话题，英文关键词是Tavily和Jina搜索的主要输入。"
                        ),
                    },
                    "search_type": {
                        "type": "string",
                        "enum": ["general", "news", "financial"],
                        "description": (
                            "搜索类型。'general'=通用网页搜索，'news'=近期新闻，"
                            "'financial'=金融财经数据。默认'general'。"
                        ),
                    },
                    "recency": {
                        "type": "string",
                        "enum": ["day", "week", "month", "year"],
                        "description": (
                            "时间过滤。'day'=当日新闻，'week'=近一周，"
                            "'month'=近一月，'year'=近一年。默认'month'。"
                        ),
                    },
                },
                "required": ["query_cn"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_webpage",
            "description": (
                "阅读并提取指定网页的完整内容。"
                "在web_search之后，当你需要某个搜索结果的更详细信息时使用。"
                "也可用于用户提供的URL。返回清洁的文本内容。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "要阅读的网页完整URL。",
                    },
                },
                "required": ["url"],
            },
        },
    },
]

# ── System prompt for search behavior ──────────────────────────

WEB_SEARCH_SYSTEM_PROMPT = """## 联网搜索与网页阅读能力

你拥有两件工具：`web_search`（多引擎搜索）和 `read_webpage`（抓取指定URL的正文全文）。两者配合使用，才能做出扎实的研究。

### web_search — 搜索关键词规则
1. **何时搜索**：问题涉及实时数据、最新新闻、近期事件、你不确定的事实、或用户要求搜索时，主动调用
2. **查询优化**：不要直接用用户原文搜索。将问题转化为简洁、精准的关键词（3-8个词，含股票名+核心议题）
3. **query_cn 必填**；**query_en 强烈建议**。国际视角、外资研报、海外对标公司都依赖英文搜索
4. **多轮细化**：第一轮广撒网抓主题，第二轮用结果中出现的新线索（子业务名、客户名、财务数字）做精准搜索
5. **一次调用的上限**：每轮最多发起 3 条 web_search（query 应当互补，不要近义词反复搜）

### read_webpage — 必须用的场景（深度研究题目更是必做）
**仅看 web_search 返回的 600 字摘要是不够的。** 以下情况必须用 `read_webpage` 抓取完整网页：
1. 发现**上市公司公告/定期报告原文链接**（URL 包含 cninfo、sse、szse、finance.sina / eastmoney 的公告详情页、investor relations PDF）→ 抓全文
2. 发现**券商研报/深度报告的 PDF / html 全文**（pdf.dfcfw.com、券商官网、同花顺研报页）→ 抓全文
3. 摘要里出现**关键数字/客户名/产能数字的线索**但没给具体数值 → 读原文确认
4. 需要**核心业务拆分、产能规划、招标细节**等摘要难以覆盖的内容 → 读纪要或路演页原文
5. 用户提供了具体 URL → 直接 read_webpage

### 来源权威性分级（强制采纳此分级判断每条信息）
- **🟢 高权威（可直接采用，无需特别说明）**：
  - 上市公司官方披露：cninfo / sse / szse / hkex / sec.gov 公告 PDF、IR 网站、年报/季报/招股书
  - 监管机构与官方统计：人民银行、证监会、交易所、统计局、Fed、ECB、BLS、Bloomberg/Reuters/WSJ/FT 主站
  - 头部券商深度研报：中金、中信、华泰、国君、招商、海通、广发、Goldman、Morgan Stanley、JPM、UBS 等
  - 权威财经媒体：财新、第一财经、证券时报、上海证券报、财联社官方、东方财富网研报频道
  - 内部知识库（kb_search / user_kb_*）：纪要、路演、调研、官方研报库
- **🟡 中等权威（可参考，但需结合多源交叉验证）**：
  - 主流财经门户原创栏目（新浪财经/网易财经/腾讯财经/搜狐财经的署名记者稿）
  - 行业垂直媒体：钛媒体、36氪、虎嗅、雪球大V署名长文、Seeking Alpha 编辑稿
  - 知名分析师个人专栏（有可核实身份）
- **🔴 低权威（默认不采用；如确无更好来源被迫使用，必须显式标注置信度）**：
  - **以下站点已在系统层面硬过滤，搜索结果中不会再出现**：百家号 / 搜狐 (sohu.com) / 网易 (163.com) / 知乎 (zhihu.com) / 雪球 (xueqiu.com) / 东方财富股吧 (guba.eastmoney.com) / 百度贴吧·知道·文库
  - 仍可能出现需谨慎判断的来源：头条号、企鹅号、凤凰网、腾讯网自媒体频道、来源不明的"小道消息"/"内部人士"/SEO 内容农场/无作者署名的转载稿
  - 维基百科作为唯一来源（可作为背景但不可作为关键事实唯一支撑）

### 采纳规则（硬约束）
1. **优先级**：高权威 > 中等权威 > 低权威。同一事实有高权威来源时，**忽略**低权威来源，不要并列引用。
2. **关键数字（财务、产能、市占率、估值）**：必须来自高权威来源（官方披露 / 头部券商研报 / 内部 KB），否则禁止引用具体数字。
3. **被迫使用低/中权威来源时**：在该事实后**显式注明置信度**，格式如下（使用括号 + 中文）：
   - `（置信度：低，来源为自媒体/未经核实的转载，建议交叉验证）`
   - `（置信度：中，来源为财经门户原创但缺少官方披露佐证）`
   不要在每条事实后机械追加；仅当该条信息确实来自中/低权威源时才标注。
4. **来源冲突**：高权威与低权威矛盾 → 采信高权威，并简短说明 `（注：自媒体XX口径不同，已忽略）`。多家高权威之间矛盾 → 并列呈现并指出分歧。
5. **完全没有可靠来源**：直接告知用户"暂无权威披露"，**不要用低权威来源拼凑结论**，更不要编造。

### read_webpage — 选择策略
- 优先抓取上面"高权威"清单中的 URL；中等权威按需抓 1-2 篇做交叉验证；低权威 URL 不要抓（即便 web_search 返回了也跳过）
- 一次研究中通常挑 2-4 个最硬核的 URL 深读，不要贪多。**每次 read_webpage 后先判断内容是否已足够，再决定是否再读**

### 引用与信息不足
- **统一引用系统**：web_search / read_webpage / kb_search / user_kb_* 返回的每条结果前都带 `[N]` 引用编号，全局唯一。
- 回答中必须用行内 `[N]` 标注来源（例如 "数据中心电源收入同比增长 80% [5]"），系统会自动渲染为可点击链接。
- **不要**在回答末尾手动罗列 "来源引用" 列表——UI 会自动展示。
- 若搜索+阅读后仍无法回答，如实告知用户缺失的数据，**绝不允许使用低权威来源凑数或编造**。"""

WEB_SEARCH_FORCE_PROMPT = "\n\n用户要求联网搜索。请务必使用web_search工具搜索相关信息后再回答。"

# ── Chinese stock query detection ──────────────────────────────

_CN_STOCK_CODE_RE = re.compile(r'(?:^|\D)([036]\d{5})(?:\D|$)')
_CN_FINANCIAL_KEYWORDS = frozenset([
    'A股', '沪深', '上证', '深证', '创业板', '科创板', '港股', '北交所',
    '涨停', '跌停', '涨幅', '跌幅', '大盘', '指数',
    '基金', '券商', '研报', '公告', '财报', '年报', '季报', '中报',
    '营收', '净利润', '市盈率', '市净率', '市值', '股价', '估值',
    '板块', '概念股', '龙头', '利好', '利空',
    '融资', '融券', '主力', '游资', '机构', '北向资金', '南向资金',
    '分红', '配股', '增发', '回购', '减持', '增持',
])


def _is_chinese_stock_query(query_cn: str, query_en: str | None = None) -> bool:
    """Detect if query is about Chinese stocks / A-shares."""
    if _CN_STOCK_CODE_RE.search(query_cn):
        return True
    return any(kw in query_cn for kw in _CN_FINANCIAL_KEYWORDS)


# ── Search result cache ────────────────────────────────────────

_search_cache: dict[str, tuple[float, list[dict]]] = {}
_CACHE_TTL = 300.0  # 5 minutes


def _cache_key(query: str, search_type: str, recency: str) -> str:
    return f"{query}|{search_type}|{recency}"


def _get_cached(key: str) -> list[dict] | None:
    entry = _search_cache.get(key)
    if entry and (time.monotonic() - entry[0]) < _CACHE_TTL:
        logger.info("[SearchCache] HIT: %s", key[:60])
        try:
            from backend.app.services.chat_debug import chat_trace, get_current_trace_id
            chat_trace(trace_id=get_current_trace_id()).log_search_cache_hit(key, len(entry[1]))
        except Exception:
            pass
        return entry[1]
    if entry:
        del _search_cache[key]
    return None


def _set_cache(key: str, results: list[dict]):
    # Evict old entries if cache grows too large
    if len(_search_cache) > 100:
        cutoff = time.monotonic() - _CACHE_TTL
        expired = [k for k, (t, _) in _search_cache.items() if t < cutoff]
        for k in expired:
            del _search_cache[k]
    _search_cache[key] = (time.monotonic(), results)


# ── Result re-ranking ──────────────────────────────────────────

# Low-authority hosts that survive the source-side filter (e.g. surfaced by
# Tavily/Jina under a different netloc) get heavily downranked here so they
# only appear if no better source exists. Hard-blocked aggregators
# (baijiahao / sohu / 163 / zhihu / xueqiu / guba.eastmoney …) live in
# `src/tools/web_search.py::_LOW_TRUST_HOSTS` and never reach this stage.
_DOWNRANK_HOSTS: frozenset[str] = frozenset({
    "ifeng.com",          # 凤凰网，资讯质量参差
    "qq.com",             # 腾讯网（含腾讯新闻自媒体频道）
    "toutiao.com",        # 头条号
    "kuaibao.qq.com",     # 腾讯快报（自媒体）
})


def _host_of(url: str, website: str = "") -> str:
    if url:
        try:
            host = urlparse(url).netloc.lower().lstrip(".")
            if host.startswith("www."):
                host = host[4:]
            if host:
                return host
        except Exception:
            pass
    return (website or "").lower().lstrip(".")


def _is_downrank_host(url: str, website: str = "") -> bool:
    host = _host_of(url, website)
    if not host:
        return False
    # Match exact host or any parent domain in the blocklist.
    parts = host.split(".")
    for i in range(len(parts) - 1):
        if ".".join(parts[i:]) in _DOWNRANK_HOSTS:
            return True
    return False


def _rerank_results(results: list[dict], max_results: int = 8) -> list[dict]:
    # Engines (Tavily/Jina/...) already return results in their own ranked order;
    # the previous score/authority/has_date sort mixed unnormalized signals
    # (Jina/DDG always reported score=0) and effectively randomized ties.
    # We now preserve arrival order and only enforce two hard rules: drop empty
    # content, and push downrank-list hosts to the back so a real news/research
    # source always wins a tie against a low-trust aggregator.
    results = [r for r in results if r.get("content", "").strip()]
    trusted, downranked = [], []
    for r in results:
        if _is_downrank_host(r.get("url", ""), r.get("website", "")):
            downranked.append(r)
        else:
            trusted.append(r)
    return (trusted + downranked)[:max_results]


# ── Citation tracking ──────────────────────────────────────────

class CitationTracker:
    """Track research sources across multiple tool calls for consistent citation numbering.

    One global index [1]..[N] is shared across web search, AlphaPai, and Jinmen results
    so the LLM can cite any source with matching [N] markers. Each source dict carries
    a `source_type` field ('web' | 'alphapai' | 'jinmen') so the frontend can render
    appropriately (web sources have URLs; alphapai/jinmen may not).
    """

    def __init__(self):
        self._sources: list[dict] = []
        self._key_to_index: dict[str, int] = {}

    def _next_index(self) -> int:
        return len(self._sources) + 1

    def _register(self, key: str, entry: dict) -> int:
        """Register a new source under the given dedup key, returning its index."""
        if key in self._key_to_index:
            return self._key_to_index[key]
        idx = self._next_index()
        entry["index"] = idx
        self._key_to_index[key] = idx
        self._sources.append(entry)
        return idx

    def add_results(self, results: list[dict]) -> list[dict]:
        """Add web search results and return them with `citation_index` attached.

        Deduplicates by URL — the same URL across searches keeps its index
        and its content is upgraded if a later hit returns richer text.
        """
        indexed = []
        for r in results:
            url = r.get("url", "")
            key = f"web:{url}" if url else f"web:title:{r.get('title','')}"
            if key in self._key_to_index:
                idx = self._key_to_index[key]
                existing = self._sources[idx - 1]
                # Prefer the richer content when we see the same URL again
                if len(r.get("content", "")) > len(existing.get("content", "")):
                    existing["content"] = r["content"]
            else:
                idx = self._register(key, {
                    "title": r.get("title", ""),
                    "url": url,
                    "website": r.get("website", ""),
                    "date": r.get("date", ""),
                    "source_type": "web",
                })
            indexed.append({**r, "citation_index": idx})
        return indexed

    def add_alphapai_items(self, items: list[dict]) -> list[dict]:
        """Register AlphaPai recall items and return them with `citation_index`.

        Each item may have: id, type (comment/report/roadShow/ann), title, institution,
        time, industry, contextInfo. Dedup key prefers numeric id; falls back to
        type+title+time so reruns don't create duplicates.
        """
        indexed = []
        for it in items:
            item_id = it.get("id")
            doc_type = it.get("type", "")
            title = it.get("title", "") or _extract_title_from_context(it.get("contextInfo", ""))
            date = it.get("time", "") or _extract_date_from_context(it.get("contextInfo", ""))
            institution = it.get("institution", "") or _extract_institution_from_context(it.get("contextInfo", ""))

            if item_id:
                key = f"alphapai:{item_id}"
            else:
                key = f"alphapai:{doc_type}:{title}:{date}"

            if key in self._key_to_index:
                idx = self._key_to_index[key]
            else:
                # Label mapping for display
                type_label = {
                    "comment": "券商点评", "report": "券商研报",
                    "roadShow": "路演纪要", "ann": "公司公告",
                    "qa": "问答", "roadShow_ir": "投关纪要",
                    "roadShow_us": "美股纪要", "foreign_report": "外资研报",
                }.get(doc_type, doc_type or "投研数据")
                idx = self._register(key, {
                    "title": title,
                    "url": "",
                    "website": institution or "Alpha派",
                    "date": (date or "")[:10],
                    "source_type": "alphapai",
                    "doc_type": type_label,
                })
            indexed.append({**it, "citation_index": idx})
        return indexed

    def add_jinmen_items(self, items: list[dict], tool_name: str = "") -> list[dict]:
        """Register Jinmen MCP items and return them with `citation_index`.

        Dedup by URL when available, else by title+institution+date.
        """
        indexed = []
        # Map jinmen tool name → human readable doc type
        tool_label = {
            "jinmen_search": "进门综合",
            "jinmen_analyst_comments": "分析师点评",
            "jinmen_roadshow": "路演纪要",
            "jinmen_announcements": "公司公告",
            "jinmen_foreign_reports": "外资研报",
        }.get(tool_name, "进门财经")

        for it in items:
            url = it.get("url", "") or ""
            title = it.get("title", "")
            institution = it.get("institution", "") or it.get("institutionName", "")
            date = it.get("date", "") or ""
            if url:
                key = f"jinmen:url:{url}"
            else:
                key = f"jinmen:{title}:{institution}:{date}"

            if key in self._key_to_index:
                idx = self._key_to_index[key]
            else:
                idx = self._register(key, {
                    "title": title or f"{institution} {tool_label}",
                    "url": url,
                    "website": institution or "进门财经",
                    "date": date[:10] if date else "",
                    "source_type": "jinmen",
                    "doc_type": tool_label,
                })
            indexed.append({**it, "citation_index": idx})
        return indexed

    def add_kb_items(self, items: list[dict]) -> list[dict]:
        """Register KB (local Mongo) hits and return them with `citation_index`.

        Dedup key prefers chunk_id (Phase B vector path) and falls back to
        doc_id (Phase A filter path). If we see the same chunk again it keeps
        its index; if we see a different chunk from the same doc it gets a
        fresh index (so the frontend can jump to the specific paragraph).

        Each entry carries, in addition to the legacy fields:
          chunk_id       — stable sha256 prefix identifying the chunk
          chunk_index    — nth child chunk within the doc (0-based)
          char_start     — offset into the source text (for future paragraph jump)
          char_end       — exclusive end offset
          collection     — the Mongo collection the chunk belongs to
          snippet_text   — ≤ 240-char preview for frontend hover

        Unknown keys are permitted and preserved — the frontend ignores them
        today but will consume chunk_id / char_start / char_end when the
        paragraph-jump UI lands.
        """
        indexed: list[dict] = []
        for it in items:
            chunk_id = it.get("chunk_id") or ""
            doc_id = it.get("doc_id") or ""
            title = it.get("title") or ""
            date = it.get("date") or ""
            inst = it.get("institution") or ""
            url = it.get("url") or ""
            doc_type_cn = it.get("doc_type_cn") or it.get("doc_type") or ""

            # Dedup preference: chunk_id > doc_id > title+inst+date.
            if chunk_id:
                key = f"kb:chunk:{chunk_id}"
            elif doc_id:
                key = f"kb:doc:{doc_id}"
            else:
                key = f"kb:{title}:{inst}:{date}"

            if key in self._key_to_index:
                idx = self._key_to_index[key]
            else:
                entry: dict = {
                    "title": title,
                    "url": url,
                    "website": inst or (it.get("source") or "KB"),
                    "date": date[:10] if date else "",
                    "source_type": "kb",
                    "doc_type": doc_type_cn,
                    "doc_id": doc_id,
                }
                # Future-proof: carry chunk-level metadata when provided.
                # Keys are optional; frontend's Source interface ignores unknown.
                for opt_key in ("chunk_id", "chunk_index", "char_start",
                                "char_end", "collection", "snippet_text"):
                    if opt_key in it and it[opt_key] is not None:
                        entry[opt_key] = it[opt_key]
                idx = self._register(key, entry)
            indexed.append({**it, "citation_index": idx})
        return indexed

    @property
    def sources(self) -> list[dict]:
        return list(self._sources)


# ── Helpers for parsing AlphaPai's contextInfo string ──────────

def _extract_title_from_context(ctx: str) -> str:
    """Extract '标题:XXX' from AlphaPai contextInfo string."""
    if not ctx:
        return ""
    m = re.search(r"标题[:：]\s*([^,，\n]+)", ctx)
    return m.group(1).strip() if m else ""


def _extract_date_from_context(ctx: str) -> str:
    """Extract '发布时间为: 2025-xx-xx' from AlphaPai contextInfo string."""
    if not ctx:
        return ""
    m = re.search(r"发布时间为[:：]\s*(\S+)", ctx)
    return m.group(1).strip() if m else ""


def _extract_institution_from_context(ctx: str) -> str:
    """Extract '机构:XXX' from AlphaPai contextInfo string."""
    if not ctx:
        return ""
    m = re.search(r"机构[:：]\s*([^,，\n]+)", ctx)
    return m.group(1).strip() if m else ""


# ── Tool execution ─────────────────────────────────────────────

async def execute_tool(
    name: str,
    arguments: dict,
    citation_tracker: CitationTracker | None = None,
) -> tuple[str, list[dict]]:
    """Execute a web search tool call.

    Returns:
        (result_text, sources_list) where sources_list is for citation metadata.
    """
    import logging as _logging
    _dbg = _logging.getLogger("chat_debug")
    from backend.app.services.chat_debug import get_current_trace_id
    _tid = get_current_trace_id()
    _dbg.info("trace=%s | WEB_SEARCH_TOOL_ENTRY | name=%s args=%s", _tid, name, str(arguments)[:500])

    _t0 = time.time()
    if name == "web_search":
        result = await _execute_web_search(arguments, citation_tracker)
    elif name == "read_webpage":
        result = await _execute_read_webpage(arguments, citation_tracker)
    else:
        result = (f"Unknown tool: {name}", [])

    _elapsed = int((time.time() - _t0) * 1000)
    _dbg.info(
        "trace=%s | WEB_SEARCH_TOOL_EXIT | name=%s elapsed=%dms result_len=%d sources=%d",
        _tid, name, _elapsed, len(result[0]), len(result[1]),
    )
    return result


async def _execute_web_search(
    arguments: dict,
    tracker: CitationTracker | None = None,
) -> tuple[str, list[dict]]:
    """Execute web_search with smart engine routing."""
    from src.tools.web_search import (
        tavily_search, jina_search, duckduckgo_search,
        format_search_results,
    )

    settings = get_settings()
    tavily_key = settings.tavily_api_key
    jina_key = settings.jina_api_key

    query_cn = arguments.get("query_cn", "")
    query_en = arguments.get("query_en", "")
    search_type = arguments.get("search_type", "general")
    recency = arguments.get("recency", "month")

    if not query_cn:
        return "Error: query_cn is required.", []

    # Map recency to engine-specific params
    recency_map_tavily_days = {
        "day": 1, "week": 7, "month": 30, "year": 365,
    }
    tavily_days = recency_map_tavily_days.get(recency)
    tavily_topic = "news" if search_type == "news" else "general"

    # Debug trace reference
    from backend.app.services.chat_debug import chat_trace, get_current_trace_id
    _trace = chat_trace(trace_id=get_current_trace_id())

    is_cn_stock = _is_chinese_stock_query(query_cn, query_en)
    _trace.log_search_keywords(
        round_num=0,
        query_cn=query_cn, query_en=query_en,
        search_type=search_type, recency=recency,
        is_cn_stock=is_cn_stock,
    )

    # Check cache
    cache_key = _cache_key(query_cn + "|" + query_en, search_type, recency)
    cached = _get_cached(cache_key)
    if cached is not None:
        if tracker:
            indexed = tracker.add_results(cached)
            _trace.log_search_top_results(round_num=0, results=indexed)
            return _format_indexed_results(indexed), tracker.sources
        _trace.log_search_top_results(round_num=0, results=cached)
        return format_search_results(cached, max_per_result=600), []

    # Build search tasks based on routing logic
    # Baidu is disabled — only Tavily + Jina are used. Keyword strategy:
    #   Chinese stock: Tavily(CN+EN) + Jina(CN+EN)
    #   Other:         Tavily(EN)    + Jina(EN)
    tasks: list[tuple[str, Any]] = []

    if is_cn_stock:
        # Chinese stock/market: Tavily and Jina get BOTH Chinese + English keywords
        if tavily_key:
            tasks.append(("tavily_cn", tavily_search(
                query_cn, tavily_key, max_results=8,
                topic=tavily_topic, days=tavily_days,
            )))
            if query_en:
                tasks.append(("tavily_en", tavily_search(
                    query_en, tavily_key, max_results=8,
                    topic=tavily_topic, days=tavily_days,
                )))
        if jina_key:
            tasks.append(("jina_cn", jina_search(query_cn, jina_key, max_results=8)))
            if query_en:
                tasks.append(("jina_en", jina_search(query_en, jina_key, max_results=8)))
    else:
        # Other / international: Tavily and Jina get English keywords only
        if tavily_key:
            if query_en:
                tasks.append(("tavily_en", tavily_search(
                    query_en, tavily_key, max_results=8,
                    topic=tavily_topic, days=tavily_days,
                )))
            else:
                # Fallback: use Chinese query if no English provided
                tasks.append(("tavily_cn", tavily_search(
                    query_cn, tavily_key, max_results=8,
                    topic=tavily_topic, days=tavily_days,
                )))
        if jina_key:
            if query_en:
                tasks.append(("jina_en", jina_search(query_en, jina_key, max_results=8)))
            else:
                tasks.append(("jina_cn", jina_search(query_cn, jina_key, max_results=8)))

    # Fallback to DuckDuckGo if no APIs available
    if not tasks:
        tasks.append(("ddg", duckduckgo_search(query_cn, max_results=8)))
        if query_en:
            tasks.append(("ddg_en", duckduckgo_search(query_en, max_results=5)))

    # Execute all searches in parallel
    import logging as _logging
    _dbg = _logging.getLogger("chat_debug")
    _tid = get_current_trace_id()

    task_names = [t[0] for t in tasks]
    task_coros = [t[1] for t in tasks]
    # Build per-engine query map so we can log each engine's actual query
    engine_query_map: dict[str, str] = {}
    for name_tag in task_names:
        if name_tag.endswith("_en"):
            engine_query_map[name_tag] = query_en or query_cn
        else:
            engine_query_map[name_tag] = query_cn

    _dbg.info(
        "trace=%s | WEB_SEARCH_ENGINES | engines=%s query_cn='%s' query_en='%s' "
        "type=%s recency=%s cn_stock=%s",
        _tid, task_names, query_cn[:80], (query_en or "")[:80],
        search_type, recency, is_cn_stock,
    )
    _search_start = time.time()
    raw_results = await asyncio.gather(*task_coros, return_exceptions=True)
    _search_elapsed = int((time.time() - _search_start) * 1000)

    # Merge and deduplicate + log per-engine URL detail
    all_results: list[dict] = []
    seen_urls: set[str] = set()
    engine_stats = {}
    for name_tag, result in zip(task_names, raw_results):
        engine_query = engine_query_map.get(name_tag, "")
        if isinstance(result, Exception):
            logger.warning("[WebSearchTool] %s failed: %s", name_tag, result)
            engine_stats[name_tag] = f"FAILED: {str(result)[:100]}"
            _trace.log_search_engine_call(
                engine=name_tag, query=engine_query, api_url="-",
                status="FAILED", latency_ms=_search_elapsed,
                result_count=0, error=str(result),
            )
            continue
        result_count = len(result) if isinstance(result, list) else 0
        engine_stats[name_tag] = f"OK: {result_count} results"
        _trace.log_search_engine_call(
            engine=name_tag, query=engine_query, api_url="-",
            status="OK", latency_ms=_search_elapsed,
            result_count=result_count,
        )
        if isinstance(result, list):
            _trace.log_search_urls_returned(
                engine=name_tag, query=engine_query, items=result,
            )
            for item in result:
                url = item.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_results.append(item)

    _dbg.info(
        "trace=%s | WEB_SEARCH_RESULTS | elapsed=%dms total_deduped=%d engine_stats=%s",
        _tid, _search_elapsed, len(all_results), engine_stats,
    )

    # Re-rank and limit
    all_results = _rerank_results(all_results, max_results=10)

    # Cache results
    _set_cache(cache_key, all_results)

    if not all_results:
        _trace.log_search_top_results(round_num=0, results=[])
        return "未找到相关搜索结果。", []

    # Track citations
    if tracker:
        indexed = tracker.add_results(all_results)
        _trace.log_search_top_results(round_num=0, results=indexed)
        text = _format_indexed_results(indexed)
        logger.info(
            "[WebSearchTool] query_cn='%s' query_en='%s' cn_stock=%s -> %d results",
            query_cn[:40], (query_en or "")[:40], is_cn_stock, len(indexed),
        )
        return text, tracker.sources

    _trace.log_search_top_results(round_num=0, results=all_results)
    text = format_search_results(all_results, max_per_result=600)
    logger.info(
        "[WebSearchTool] query_cn='%s' query_en='%s' cn_stock=%s -> %d results",
        query_cn[:40], (query_en or "")[:40], is_cn_stock, len(all_results),
    )
    return text, []


async def _execute_read_webpage(
    arguments: dict,
    tracker: CitationTracker | None = None,
) -> tuple[str, list[dict]]:
    """Execute read_webpage via Jina Reader API."""
    from src.tools.web_search import jina_read_url
    from backend.app.services.chat_debug import chat_trace, get_current_trace_id

    settings = get_settings()
    jina_key = settings.jina_api_key
    _trace = chat_trace(trace_id=get_current_trace_id())

    url = arguments.get("url", "")
    if not url:
        _trace.log_webpage_read(url="-", status="ERROR", latency_ms=0,
                                content_len=0, error="url is required")
        return "Error: url is required.", []

    if not jina_key:
        _trace.log_webpage_read(url=url, status="NO_KEY", latency_ms=0,
                                content_len=0, error="jina key not configured")
        return "阅读网页功能需要Jina API密钥，当前未配置。", []

    _t0 = time.monotonic()
    content = await jina_read_url(url, jina_key, max_chars=10000)
    _elapsed_ms = int((time.monotonic() - _t0) * 1000)

    if not content:
        _trace.log_webpage_read(
            url=url, status="EMPTY", latency_ms=_elapsed_ms,
            content_len=0, error="jina reader returned empty",
        )
        return f"无法读取该网页内容: {url}", []

    _trace.log_webpage_read(
        url=url, status="OK", latency_ms=_elapsed_ms,
        content_len=len(content), content_preview=content[:500],
    )

    # Track as a citation source
    parsed = urlparse(url)
    website = parsed.netloc.replace("www.", "")
    source = {
        "title": f"Page from {website}",
        "url": url,
        "website": website,
        "date": "",
        "content": content[:200],
    }

    sources = []
    if tracker:
        tracker.add_results([{**source, "score": 0, "authority": 0, "source": "jina_reader"}])
        sources = tracker.sources

    header = f"**网页内容** ({website}):\n\n"
    return header + content, sources


# ── Formatting helpers ─────────────────────────────────────────

def _format_indexed_results(results: list[dict], max_per_result: int = 600) -> str:
    """Format search results with citation indices [N]."""
    if not results:
        return "No search results found."

    lines = []
    for r in results:
        idx = r.get("citation_index", 0)
        title = r.get("title", "N/A")
        url = r.get("url", "")
        content = r.get("content", "")[:max_per_result]
        date = r.get("date", "")
        website = r.get("website", "")

        line = f"[{idx}] {title}"
        if website:
            line += f" -- {website}"
        if date:
            line += f" ({date})"
        line += f"\n    {content}"
        if url:
            line += f"\n    URL: {url}"
        lines.append(line)

    return "\n\n".join(lines)
