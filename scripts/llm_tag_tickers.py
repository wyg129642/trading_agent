"""LLM-NER ticker tagger — fallback打标 for `_canonical_tickers: []` 文档.

`enrich_tickers.py` 已经把"上游结构化字段 + 标题正则"两层规则跑到极限,
全库还有 ~86万条文档(46.6%)是 `_canonical_tickers: []`。其中真正"行业 /
宏观 / 策略"类文档**就是没个股**(占大头,~55%);剩下一部分是英文研报/纪要里
公司名作为主题但没带括号代码 —— 这部分必须靠 LLM 命名实体识别(NER)+
内置 ticker 知识来打标。

本脚本就是干这件事:

  1. 扫 `_canonical_tickers: []` 的目标集合(可指定 `--source` / `--collection`)
  2. 拼 title + 正文片段 → LLM `chat.completions`(强制 JSON 输出)
  3. LLM 返回 `{"tickers": ["AAPL.US", "0700.HK", ...]}`
  4. 走 `ticker_normalizer.normalize_with_unmatched()` 标准化(白名单卡死,
     LLM 编造的 market 后缀会被丢进 `_unmatched_raw` 而不会污染 canonical)
  5. 写回 Mongo + 累计 token / 美元开销;触达预算或文档上限即停

设计原则:

- **跟 `enrich_tickers.py` 严格分工** —— 它做"无成本的规则路径",本脚本做
  "按量付费的 LLM 路径"。两边都用 `normalize_with_unmatched`,后台审计字段
  `_canonical_extract_source` 一眼区分:
      * `<source>`            ← 结构化字段命中
      * `<source>_title`      ← 标题正则兜底
      * `<source>_llm:<model>` ← 本脚本
- **预算先行,样本估算** —— 跑前先抽样 N 条(默认 20)估算 in/out 平均
  tokens,投影到目标文档总数,弹出 `(预估 tokens / 美元 / 时长)` 表;
  `--yes` 跳过确认。
- **预算硬停** —— `--max-cost-usd` / `--max-docs` 任一触达即 `await _flush()`
  + `sys.exit(0)`,不会偷偷超标。
- **幂等** —— 已被 LLM 打过标的文档(`_canonical_extract_source` 含 `_llm:`)
  默认 skip,除非 `--force-relabel`。
- **失败退避** —— LLM 单条失败不写回,下次再扫;500/502/503/429 指数退避两次。

使用文档 + 模型菜单 + token 预估见
``crawl/ticker_untagged_snapshot_2026_04_25.md`` 末尾的 "LLM 自动打标脚本" 一节。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import httpx  # noqa: E402
from motor.motor_asyncio import AsyncIOMotorClient  # noqa: E402
from openai import AsyncOpenAI  # noqa: E402
from pymongo import UpdateOne  # noqa: E402

from backend.app.config import get_settings  # noqa: E402
from backend.app.services.ticker_normalizer import (  # noqa: E402
    normalize_with_unmatched,
    reload_aliases,
)


# ---------------------------------------------------------------------------
# Model catalog
# ---------------------------------------------------------------------------
# Per 1M tokens, USD. Numbers are 2026-04 list prices for the OpenAI-compatible
# endpoints we already have keys for. The "speed_qps" hint caps concurrency to
# stay under each provider's rate limit; we rarely need more than 5–10 in flight.
@dataclass(frozen=True)
class ModelSpec:
    key: str                  # CLI label
    provider: str             # "openrouter" | "openai" | "bailian"
    model_id: str             # actual model id sent to the API
    in_usd_per_mtok: float
    out_usd_per_mtok: float
    speed_qps: int            # safe concurrency
    note: str

MODELS: dict[str, ModelSpec] = {
    "claude-sonnet": ModelSpec(
        key="claude-sonnet",
        provider="openrouter",
        model_id="anthropic/claude-sonnet-4-6",
        in_usd_per_mtok=3.00,
        out_usd_per_mtok=15.00,
        speed_qps=5,
        note="Anthropic 中端旗舰,英文研报 NER 召回最稳;预算够才用",
    ),
    "claude-haiku": ModelSpec(
        key="claude-haiku",
        provider="openrouter",
        model_id="anthropic/claude-haiku-4-5",
        in_usd_per_mtok=1.00,
        out_usd_per_mtok=5.00,
        speed_qps=8,
        note="性价比甜点;NER 召回略低于 Sonnet,但便宜 3x",
    ),
    "gpt-5-mini": ModelSpec(
        key="gpt-5-mini",
        provider="openai",
        model_id="gpt-5.4-mini",
        in_usd_per_mtok=0.40,
        out_usd_per_mtok=1.60,
        speed_qps=10,
        note="OpenAI 廉价档,英文实体识别强,需 HTTP 代理 (Clash 7890)",
    ),
    "gpt-5": ModelSpec(
        key="gpt-5",
        provider="openai",
        model_id="gpt-5.4",
        in_usd_per_mtok=2.50,
        out_usd_per_mtok=10.00,
        speed_qps=8,
        note="OpenAI 旗舰;NER 准确度对标 Sonnet,需代理",
    ),
    "gemini-flash": ModelSpec(
        key="gemini-flash",
        provider="openrouter",  # 走 OpenRouter 免代理
        model_id="google/gemini-3.1-flash",
        in_usd_per_mtok=0.30,
        out_usd_per_mtok=2.50,
        speed_qps=10,
        note="超低价 + 中英双语都好;首选海量集合(jinmen.oversea_reports)",
    ),
    "gemini-pro": ModelSpec(
        key="gemini-pro",
        provider="openrouter",
        model_id="google/gemini-3.1-pro-preview",
        in_usd_per_mtok=1.25,
        out_usd_per_mtok=10.00,
        speed_qps=6,
        note="Gemini 旗舰;复杂中英混合纪要场景比 Flash 稳",
    ),
    "deepseek-v3": ModelSpec(
        key="deepseek-v3",
        provider="openrouter",
        model_id="deepseek/deepseek-chat-v3.2",
        in_usd_per_mtok=0.27,
        out_usd_per_mtok=1.10,
        speed_qps=8,
        note="国产 + 中英都行;结构化输出格式遵循度好,价格次于 Qwen",
    ),
    "qwen-plus": ModelSpec(
        key="qwen-plus",
        provider="bailian",  # llm_enrichment_* 已配置 (DashScope OpenAI 兼容)
        model_id="qwen-plus",
        in_usd_per_mtok=0.11,    # ¥0.0008/1k ≈ $0.11/M
        out_usd_per_mtok=0.27,   # ¥0.002/1k ≈ $0.27/M
        speed_qps=8,
        note="最便宜 + 中文报告强;勿处理英文研报(召回明显低于其他)",
    ),
}


# ---------------------------------------------------------------------------
# Source/collection map (与 enrich_tickers.SOURCES 对齐 + 加正文字段)
# ---------------------------------------------------------------------------
# Body fields we ask Mongo to project + try in order; first non-empty string
# wins. Title fields are projected separately and concatenated as "Title: ..."
# in the LLM user prompt.
@dataclass(frozen=True)
class CollSpec:
    db_attr: str       # backend.app.config.Settings attribute that holds DB name
    uri_attr: str      # corresponding URI attribute
    title_fields: tuple[str, ...] = ("title", "title_cn", "title_en")
    body_fields: tuple[str, ...] = (
        "summary_md",
        "content_md",
        "transcript_md",
        "insight_md",
        "oversea_content_md",
        "chief_opinion_md",
        "article_md",
        "body_md",
        "subtitle",
        "truncated_body_text",
        "summary",
        "content",
    )

SOURCES: dict[str, dict[str, CollSpec]] = {
    "alphapai": {
        "roadshows": CollSpec("alphapai_mongo_db", "alphapai_mongo_uri"),
        "reports": CollSpec("alphapai_mongo_db", "alphapai_mongo_uri"),
        "comments": CollSpec("alphapai_mongo_db", "alphapai_mongo_uri"),
        # wechat_articles 永久禁用打标(信噪比过低 + 已 disable 在 monitor)
    },
    "jinmen": {
        "meetings": CollSpec("jinmen_mongo_db", "jinmen_mongo_uri"),
        "reports": CollSpec("jinmen_mongo_db", "jinmen_mongo_uri"),
        "oversea_reports": CollSpec("jinmen_mongo_db", "jinmen_mongo_uri"),
    },
    "meritco": {
        "forum": CollSpec("meritco_mongo_db", "meritco_mongo_uri"),
        "research": CollSpec("meritco_mongo_db", "meritco_mongo_uri"),
    },
    "thirdbridge": {
        "interviews": CollSpec("thirdbridge_mongo_db", "thirdbridge_mongo_uri"),
    },
    "funda": {
        "posts": CollSpec("funda_mongo_db", "funda_mongo_uri"),
        "sentiments": CollSpec("funda_mongo_db", "funda_mongo_uri"),
    },
    "acecamp": {
        "articles": CollSpec("acecamp_mongo_db", "acecamp_mongo_uri"),
    },
    "alphaengine": {
        "summaries": CollSpec("alphaengine_mongo_db", "alphaengine_mongo_uri"),
        "china_reports": CollSpec("alphaengine_mongo_db", "alphaengine_mongo_uri"),
        "foreign_reports": CollSpec("alphaengine_mongo_db", "alphaengine_mongo_uri"),
        "news_items": CollSpec("alphaengine_mongo_db", "alphaengine_mongo_uri"),
    },
    "gangtise": {
        "summaries": CollSpec("gangtise_mongo_db", "gangtise_mongo_uri"),
        "researches": CollSpec("gangtise_mongo_db", "gangtise_mongo_uri"),
        "chief_opinions": CollSpec("gangtise_mongo_db", "gangtise_mongo_uri"),
    },
    "semianalysis": {
        "semianalysis_posts": CollSpec("semianalysis_mongo_db", "semianalysis_mongo_uri"),
    },
}


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a ticker NER tagger. Read the document title + body excerpt and output the individual public stocks that the document is *about* (the analysis subject), as canonical tickers.

OUTPUT (JSON only, nothing else):
{"tickers": ["CODE.MARKET", ...]}

Canonical MARKET codes (use these 2-letter codes only):
US=NASDAQ/NYSE/AMEX  HK=HongKong  SH=Shanghai  SZ=Shenzhen  BJ=Beijing
JP=Tokyo  KS=Korea  TW=Taiwan  AU=ASX  CA=Toronto  GB=LSE
DE=Xetra  FR=Paris  CH=SIX  NL=Amsterdam  SE=Stockholm  NO=Oslo  IT=Milan
AT=Vienna  NZ=NZX  HE=Helsinki  IN=NSE/BSE  BR=B3  ES=BME  DK=Copenhagen
SG=SGX  TH=SET  MY=Bursa  ID=IDX  PH=PSE  VN=HOSE  TR=BIST
MX=BMV  AR=Buenos  CL=Santiago  PE=Lima  CO=Bogota
SA=Tadawul  AE=ADX  EG=EGX  ZA=JSE  QA=QSE  IL=TASE
HU=BET  CZ=PSE  PL=GPW  BE=Brussels  PT=Lisbon  IE=Dublin  GR=ATHEX  RU=MOEX

CODE format rules:
- A-share: 6 digits + .SH / .SZ / .BJ        (e.g. 600519.SH, 300750.SZ)
- HK: pad to 5 digits + .HK                    (e.g. 00700.HK, 09988.HK)
- US: ticker symbol + .US                       (e.g. AAPL.US, NVDA.US, BRK.B.US)
- JP: 4-digit local code + .JP                  (e.g. 7203.JP for Toyota)
- KS: 6-digit code + .KS                        (e.g. 005930.KS for Samsung)
- Others: native ticker + market code           (e.g. NESN.CH, ASML.NL)

Strict rules — follow them to avoid garbage tags:
1. Tag ONLY the document's primary analysis subject(s). Casual mentions ("compared to AAPL...") DO NOT count.
2. If the document is macro / industry / strategy / weekly with no single-stock subject, return {"tickers": []}.
3. If the title gives a Chinese or English company name with no code, use your knowledge — but ONLY when you are confident which listing exchange. If multiply-listed, prefer the primary listing for that company's main market.
4. Return at most 5 tickers, in importance order.
5. NEVER invent or guess CODE.MARKET pairs you are not sure of. Output empty list when uncertain.
6. Output exactly the JSON object — no prose, no markdown fences."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_BODY_CHAR_CAP = 1500       # ≈ 500 tokens for mixed CN/EN
_TITLE_CHAR_CAP = 240       # titles rarely exceed this
_LLM_RETRY_STATUS = {429, 500, 502, 503, 504}
_LLM_MAX_RETRIES = 2


def _strip_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _pick_text(doc: dict, fields: tuple[str, ...], cap: int) -> str:
    for f in fields:
        v = doc.get(f)
        if isinstance(v, str) and v.strip():
            return _strip_html(v)[:cap]
    # Try nested list_item.field as fallback (alphapai/gangtise nest titles there)
    li = doc.get("list_item")
    if isinstance(li, dict):
        for f in fields:
            v = li.get(f)
            if isinstance(v, str) and v.strip():
                return _strip_html(v)[:cap]
    return ""


def _build_user_prompt(doc: dict, spec: CollSpec) -> str:
    title = _pick_text(doc, spec.title_fields, _TITLE_CHAR_CAP)
    body = _pick_text(doc, spec.body_fields, _BODY_CHAR_CAP)
    return f"Title: {title or '(empty)'}\n\nBody: {body or '(empty)'}"


def _parse_json_response(text: str) -> list[str]:
    """LLM → list of canonical-format strings. Tolerate fences & extra prose."""
    text = text.strip()
    # Strip ```json fences if any
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    # Find first {...}
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return []
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return []
    raw = obj.get("tickers") if isinstance(obj, dict) else None
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            out.append(item.strip().upper())
        elif isinstance(item, dict):
            code = item.get("code") or item.get("symbol") or item.get("ticker")
            mkt = item.get("market") or item.get("exchange")
            if isinstance(code, str) and isinstance(mkt, str):
                out.append(f"{code.strip()}.{mkt.strip()}".upper())
    return out


# ---------------------------------------------------------------------------
# LLM client
# ---------------------------------------------------------------------------
@dataclass
class Budget:
    max_docs: int | None
    max_cost_usd: float | None
    docs_done: int = 0
    docs_tagged: int = 0
    in_tokens: int = 0
    out_tokens: int = 0
    cost_usd: float = 0.0
    failures: int = 0

    def add_call(self, in_tok: int, out_tok: int, in_price: float, out_price: float) -> None:
        self.in_tokens += in_tok
        self.out_tokens += out_tok
        self.cost_usd += in_tok / 1_000_000 * in_price + out_tok / 1_000_000 * out_price

    def exhausted(self) -> bool:
        if self.max_docs is not None and self.docs_done >= self.max_docs:
            return True
        if self.max_cost_usd is not None and self.cost_usd >= self.max_cost_usd:
            return True
        return False


def _build_client(spec: ModelSpec) -> AsyncOpenAI:
    settings = get_settings()
    if spec.provider == "bailian":
        return AsyncOpenAI(
            api_key=settings.llm_enrichment_api_key,
            base_url=settings.llm_enrichment_base_url,
            timeout=60.0,
            http_client=httpx.AsyncClient(trust_env=False, timeout=60.0),
        )
    if spec.provider == "openrouter":
        return AsyncOpenAI(
            api_key=settings.openrouter_api_key,
            base_url="https://openrouter.ai/api/v1",
            timeout=60.0,
            # OpenRouter is reachable without proxy in CN
            http_client=httpx.AsyncClient(trust_env=False, timeout=60.0),
            default_headers={
                "HTTP-Referer": "https://trading-intelligence.com",
                "X-Title": "Trading Intelligence Ticker Tagger",
            },
        )
    if spec.provider == "openai":
        # OpenAI needs Clash proxy in CN
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        return AsyncOpenAI(
            api_key=settings.openai_api_key,
            base_url="https://api.openai.com/v1",
            timeout=60.0,
            http_client=httpx.AsyncClient(proxy=proxy, timeout=60.0),
        )
    raise ValueError(f"unknown provider: {spec.provider}")


async def _call_llm(
    client: AsyncOpenAI,
    spec: ModelSpec,
    user_prompt: str,
) -> tuple[list[str], int, int]:
    """Single LLM call with retry. Returns (tickers, in_tokens, out_tokens)."""
    last_exc: Exception | None = None
    for attempt in range(_LLM_MAX_RETRIES + 1):
        try:
            resp = await client.chat.completions.create(
                model=spec.model_id,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                max_tokens=200,
                response_format={"type": "json_object"},
            )
            text = resp.choices[0].message.content or ""
            in_tok = (resp.usage.prompt_tokens if resp.usage else 0) or 0
            out_tok = (resp.usage.completion_tokens if resp.usage else 0) or 0
            return _parse_json_response(text), in_tok, out_tok
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            msg = str(exc)
            retryable = any(str(c) in msg for c in _LLM_RETRY_STATUS)
            if attempt < _LLM_MAX_RETRIES and retryable:
                await asyncio.sleep(2.0 * (attempt + 1))
                continue
            break
    raise RuntimeError(f"LLM call failed after retries: {last_exc}")


# ---------------------------------------------------------------------------
# Sample / estimate
# ---------------------------------------------------------------------------
async def _estimate(
    client: AsyncOpenAI,
    spec: ModelSpec,
    samples: list[tuple[str, dict, str, str, str]],   # (db, doc, source, coll, prompt)
    target_total: int,
) -> dict[str, Any]:
    """Run a small sample and project token / cost to the full target."""
    if not samples:
        return {"sample_n": 0}
    in_toks: list[int] = []
    out_toks: list[int] = []
    sample_tickers: list[tuple[str, list[str]]] = []
    sem = asyncio.Semaphore(min(spec.speed_qps, 5))

    async def one(item):
        _, _, _, _, prompt = item
        async with sem:
            try:
                tickers, i, o = await _call_llm(client, spec, prompt)
                return tickers, i, o
            except Exception as exc:  # noqa: BLE001
                return [f"ERR:{exc}"], 0, 0

    results = await asyncio.gather(*(one(it) for it in samples))
    for (db, doc, source, coll, _prompt), (tickers, i, o) in zip(samples, results):
        in_toks.append(i)
        out_toks.append(o)
        title = _pick_text(doc, SOURCES[source][coll].title_fields, 80)
        sample_tickers.append((title, tickers))

    avg_in = sum(in_toks) / max(len(in_toks), 1)
    avg_out = sum(out_toks) / max(len(out_toks), 1)
    proj_in = avg_in * target_total
    proj_out = avg_out * target_total
    proj_cost = proj_in / 1_000_000 * spec.in_usd_per_mtok + proj_out / 1_000_000 * spec.out_usd_per_mtok
    proj_minutes = target_total / max(spec.speed_qps, 1) / 60

    return {
        "sample_n": len(samples),
        "avg_in_tokens": avg_in,
        "avg_out_tokens": avg_out,
        "proj_in_tokens": proj_in,
        "proj_out_tokens": proj_out,
        "proj_cost_usd": proj_cost,
        "proj_minutes": proj_minutes,
        "sample_tickers": sample_tickers,
    }


# ---------------------------------------------------------------------------
# Process one collection
# ---------------------------------------------------------------------------
async def process_collection(
    client_mongo: AsyncIOMotorClient,
    db_name: str,
    coll_name: str,
    source: str,
    spec: CollSpec,
    llm_client: AsyncOpenAI,
    model_spec: ModelSpec,
    budget: Budget,
    *,
    dry_run: bool,
    force_relabel: bool,
    max_docs: int | None,
) -> tuple[int, int]:
    """Returns (scanned, written)."""
    coll = client_mongo[db_name][coll_name]

    query: dict = {"_canonical_tickers": []}
    if not force_relabel:
        # Skip docs we already LLM-tagged (provenance audit)
        query["$or"] = [
            {"_canonical_extract_source": {"$exists": False}},
            {"_canonical_extract_source": {"$not": {"$regex": "_llm:"}}},
        ]

    projection = {f: 1 for f in (
        *spec.title_fields,
        *spec.body_fields,
        "list_item",
    )}
    cursor = coll.find(query, projection=projection)
    if max_docs is not None:
        cursor = cursor.limit(max_docs)

    sem = asyncio.Semaphore(model_spec.speed_qps)
    pending: list[UpdateOne] = []
    scanned = 0
    written = 0
    BATCH = 100

    async def _flush() -> int:
        nonlocal written
        if not pending:
            return 0
        if not dry_run:
            r = await coll.bulk_write(pending, ordered=False)
            written += (r.matched_count or 0) + (r.upserted_count or 0)
        n = len(pending)
        pending.clear()
        return n

    async def process_one(doc: dict):
        async with sem:
            if budget.exhausted():
                return
            user_prompt = _build_user_prompt(doc, spec)
            try:
                tickers_raw, in_tok, out_tok = await _call_llm(
                    llm_client, model_spec, user_prompt
                )
            except Exception:
                budget.failures += 1
                return
            budget.add_call(in_tok, out_tok, model_spec.in_usd_per_mtok, model_spec.out_usd_per_mtok)
            budget.docs_done += 1

            matched, unmatched = normalize_with_unmatched(tickers_raw)
            if matched:
                budget.docs_tagged += 1
            now = datetime.now(timezone.utc)
            pending.append(UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {
                    "_canonical_tickers": matched,
                    "_canonical_tickers_at": now,
                    "_unmatched_raw": unmatched,
                    "_canonical_extract_source": f"{source}_llm:{model_spec.key}",
                }},
            ))

    tasks: list[asyncio.Task] = []
    async for doc in cursor:
        if budget.exhausted():
            break
        scanned += 1
        tasks.append(asyncio.create_task(process_one(doc)))
        if len(tasks) >= BATCH:
            await asyncio.gather(*tasks)
            tasks.clear()
            await _flush()
            print(
                f"  [{source}.{coll_name}] scanned={scanned} tagged={budget.docs_tagged} "
                f"in={budget.in_tokens:,} out={budget.out_tokens:,} cost=${budget.cost_usd:.4f}",
                flush=True,
            )
    if tasks:
        await asyncio.gather(*tasks)
    await _flush()
    return scanned, written


# ---------------------------------------------------------------------------
# Sample harvester
# ---------------------------------------------------------------------------
async def _gather_samples(
    targets: list[tuple[str, str, str, str]],   # (source, coll, db_name, uri)
    n: int,
    clients: dict[str, AsyncIOMotorClient],
) -> list[tuple[str, dict, str, str, str]]:
    """Pick up to n random docs across the targets to estimate cost."""
    pool: list[tuple[str, dict, str, str, str]] = []
    per = max(n // max(len(targets), 1), 1)
    for source, coll_name, db_name, _uri in targets:
        spec = SOURCES[source][coll_name]
        coll = clients[_uri][db_name][coll_name]
        cursor = coll.aggregate([
            {"$match": {"_canonical_tickers": []}},
            {"$sample": {"size": per}},
        ])
        async for doc in cursor:
            prompt = _build_user_prompt(doc, spec)
            pool.append((db_name, doc, source, coll_name, prompt))
    random.shuffle(pool)
    return pool[:n]


async def _count_targets(
    targets: list[tuple[str, str, str, str]],
    clients: dict[str, AsyncIOMotorClient],
    force_relabel: bool,
) -> int:
    total = 0
    for source, coll_name, db_name, uri in targets:
        coll = clients[uri][db_name][coll_name]
        q: dict = {"_canonical_tickers": []}
        if not force_relabel:
            q["$or"] = [
                {"_canonical_extract_source": {"$exists": False}},
                {"_canonical_extract_source": {"$not": {"$regex": "_llm:"}}},
            ]
        total += await coll.count_documents(q)
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _interactive_pick_model() -> ModelSpec:
    print("\n=== 选择 LLM 模型 ===")
    print(f"{'#':<3} {'key':<14} {'in $/M':>8} {'out $/M':>8}  说明")
    keys = list(MODELS.keys())
    # Default = cheapest by blended cost (in×0.95 + out×0.05 — typical ratio for this task)
    cheapest_key = min(keys, key=lambda k: MODELS[k].in_usd_per_mtok * 0.95 + MODELS[k].out_usd_per_mtok * 0.05)
    cheapest_idx = keys.index(cheapest_key) + 1
    for i, k in enumerate(keys, 1):
        s = MODELS[k]
        marker = " ← 默认(最便宜)" if k == cheapest_key else ""
        print(f"{i:<3} {k:<14} {s.in_usd_per_mtok:>8.2f} {s.out_usd_per_mtok:>8.2f}  {s.note}{marker}")
    while True:
        ans = input(f"\n输入序号或 key (默认 {cheapest_idx}={cheapest_key}): ").strip() or str(cheapest_idx)
        if ans.isdigit() and 1 <= int(ans) <= len(keys):
            return MODELS[keys[int(ans) - 1]]
        if ans in MODELS:
            return MODELS[ans]
        print("  无效输入,重试")


def _interactive_pick_max_docs() -> int | None:
    while True:
        ans = input(
            "\n=== 文档上限 (max-docs) ===\n"
            "  smoke=50  小样=500  中量=5000  跑完一个集合的常用值=20000\n"
            "  输入数字或回车=不限: "
        ).strip()
        if not ans:
            return None
        if ans.isdigit():
            return int(ans)
        print("  请输入正整数或回车")


def _interactive_pick_budget() -> float | None:
    while True:
        ans = input(
            "\n=== 美元预算 (max-cost-usd) ===\n"
            "  smoke=$1  small=$5  full-run-budget=$50  unlimited=回车\n"
            "  USD: "
        ).strip()
        if not ans:
            return None
        try:
            return float(ans)
        except ValueError:
            print("  请输入数字或回车")


def _resolve_targets(args: argparse.Namespace) -> list[tuple[str, str, str, str]]:
    """Returns list of (source, collection, db_name, uri) tuples."""
    settings = get_settings()
    out: list[tuple[str, str, str, str]] = []
    if args.collection:
        # comma-separated `source.collection`
        for tok in args.collection.split(","):
            tok = tok.strip()
            if "." not in tok:
                print(f"ERR: --collection expects 'source.coll', got '{tok}'")
                sys.exit(2)
            source, coll = tok.split(".", 1)
            if source not in SOURCES or coll not in SOURCES[source]:
                print(f"ERR: unknown source.collection: {tok}")
                sys.exit(2)
            spec = SOURCES[source][coll]
            out.append((source, coll, getattr(settings, spec.db_attr), getattr(settings, spec.uri_attr)))
        return out

    if args.source and args.source != "all":
        if args.source not in SOURCES:
            print(f"ERR: unknown --source '{args.source}'")
            sys.exit(2)
        sources = {args.source: SOURCES[args.source]}
    else:
        sources = SOURCES

    for source, colls in sources.items():
        for coll, spec in colls.items():
            out.append((source, coll, getattr(settings, spec.db_attr), getattr(settings, spec.uri_attr)))
    return out


async def _main(args: argparse.Namespace) -> int:
    if args.reload_aliases:
        reload_aliases()

    # ---- Pick model
    if args.model:
        if args.model not in MODELS:
            print(f"ERR: unknown --model '{args.model}'. Choices: {list(MODELS.keys())}")
            return 2
        model_spec = MODELS[args.model]
    else:
        model_spec = _interactive_pick_model()

    # ---- Pick budget
    max_docs = args.max_docs if args.max_docs is not None else _interactive_pick_max_docs()
    max_cost = args.max_cost_usd if args.max_cost_usd is not None else _interactive_pick_budget()
    budget = Budget(max_docs=max_docs, max_cost_usd=max_cost)

    # ---- Resolve targets + open Mongo clients
    targets = _resolve_targets(args)
    clients_by_uri: dict[str, AsyncIOMotorClient] = {}
    for _s, _c, _db, uri in targets:
        if uri not in clients_by_uri:
            clients_by_uri[uri] = AsyncIOMotorClient(uri, tz_aware=True)

    # ---- LLM client
    llm_client = _build_client(model_spec)

    # ---- Estimate
    target_total = await _count_targets(targets, clients_by_uri, args.force_relabel)
    print(f"\nTarget docs (`_canonical_tickers: []` & not yet LLM-tagged): {target_total:,}")
    if max_docs is not None:
        target_total = min(target_total, max_docs)
        print(f"Capped by --max-docs to: {target_total:,}")

    if not args.skip_estimate and target_total > 0:
        print(f"\nSampling {args.sample_size} docs for token estimation...")
        samples = await _gather_samples(targets, args.sample_size, clients_by_uri)
        est = await _estimate(llm_client, model_spec, samples, target_total)
        if est.get("sample_n"):
            print(f"\n=== 抽样估算 (n={est['sample_n']}, 模型={model_spec.key}) ===")
            print(f"  avg in tokens / doc:  {est['avg_in_tokens']:.0f}")
            print(f"  avg out tokens / doc: {est['avg_out_tokens']:.0f}")
            print(f"  → 预估总 input tokens:  {est['proj_in_tokens']:,.0f}")
            print(f"  → 预估总 output tokens: {est['proj_out_tokens']:,.0f}")
            print(f"  → 预估总成本:          ${est['proj_cost_usd']:,.4f}")
            print(f"  → 预估耗时:            {est['proj_minutes']:.1f} 分钟 (单线 qps={model_spec.speed_qps})")
            print(f"\n  抽样命中预览(前 5 条):")
            for title, tickers in est["sample_tickers"][:5]:
                print(f"    {title[:70]} → {tickers}")

        if max_cost is not None and est.get("proj_cost_usd", 0) > max_cost:
            print(
                f"\n⚠️  预估成本 ${est['proj_cost_usd']:.2f} 超过 --max-cost-usd ${max_cost:.2f},"
                f" 实际跑会在花完预算后停下"
            )

        if not args.yes:
            ans = input("\n继续? [y/N]: ").strip().lower()
            if ans not in ("y", "yes"):
                print("已取消")
                for c in clients_by_uri.values():
                    c.close()
                return 0

    # ---- Run
    print(f"\n=== 开始打标 (model={model_spec.key}, dry_run={args.dry_run}) ===")
    t0 = time.time()
    grand_scanned = 0
    grand_written = 0
    for source, coll_name, db_name, uri in targets:
        if budget.exhausted():
            print("Budget exhausted — stopping.")
            break
        spec = SOURCES[source][coll_name]
        scanned, written = await process_collection(
            clients_by_uri[uri],
            db_name,
            coll_name,
            source,
            spec,
            llm_client,
            model_spec,
            budget,
            dry_run=args.dry_run,
            force_relabel=args.force_relabel,
            max_docs=max_docs - budget.docs_done if max_docs is not None else None,
        )
        grand_scanned += scanned
        grand_written += written

    elapsed = time.time() - t0
    print("\n=== 打标完成 ===")
    print(f"  scanned:       {grand_scanned:,}")
    print(f"  llm calls:     {budget.docs_done:,}  (failures: {budget.failures})")
    print(f"  newly tagged:  {budget.docs_tagged:,}  (hit-rate: "
          f"{100*budget.docs_tagged/max(budget.docs_done,1):.1f}%)")
    print(f"  written:       {grand_written:,}")
    print(f"  in tokens:     {budget.in_tokens:,}")
    print(f"  out tokens:    {budget.out_tokens:,}")
    print(f"  cost:          ${budget.cost_usd:.4f}")
    print(f"  elapsed:       {elapsed/60:.1f} min")

    # Write run log to logs/llm_tag/<timestamp>.json
    log_dir = _ROOT / "logs" / "llm_tag"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"{ts}_{model_spec.key}.json"
    log_path.write_text(json.dumps({
        "ts": ts,
        "model": model_spec.key,
        "model_id": model_spec.model_id,
        "targets": [{"source": s, "coll": c} for s, c, _, _ in targets],
        "scanned": grand_scanned,
        "llm_calls": budget.docs_done,
        "tagged": budget.docs_tagged,
        "in_tokens": budget.in_tokens,
        "out_tokens": budget.out_tokens,
        "cost_usd": budget.cost_usd,
        "failures": budget.failures,
        "elapsed_sec": elapsed,
        "max_docs": max_docs,
        "max_cost_usd": max_cost,
        "dry_run": args.dry_run,
    }, indent=2, ensure_ascii=False))
    print(f"\nRun log: {log_path}")

    for c in clients_by_uri.values():
        c.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="LLM-NER ticker tagger for `_canonical_tickers: []` docs."
    )
    ap.add_argument(
        "--model",
        choices=list(MODELS.keys()),
        help="Model key (omit → interactive menu).",
    )
    ap.add_argument(
        "--source",
        default=None,
        help=f"Data source: all / {' / '.join(SOURCES.keys())}",
    )
    ap.add_argument(
        "--collection",
        help="Comma-separated 'source.coll' (overrides --source). "
             "e.g. --collection alphapai.reports,gangtise.researches",
    )
    ap.add_argument("--max-docs", type=int, help="Hard cap on # of LLM-processed docs")
    ap.add_argument(
        "--max-cost-usd",
        type=float,
        help="Hard cap on USD spend (estimated from API usage)",
    )
    ap.add_argument(
        "--sample-size",
        type=int,
        default=20,
        help="N docs to sample for token estimation (default 20). 0 = skip estimate.",
    )
    ap.add_argument(
        "--skip-estimate",
        action="store_true",
        help="Skip the upfront sampling phase (just run).",
    )
    ap.add_argument(
        "--force-relabel",
        action="store_true",
        help="Re-tag docs already touched by a previous LLM run "
             "(default: skip docs whose _canonical_extract_source already contains '_llm:').",
    )
    ap.add_argument(
        "--reload-aliases",
        action="store_true",
        help="Force reload aliases.json before normalizing LLM output",
    )
    ap.add_argument("--dry-run", action="store_true", help="Run LLM but don't write Mongo")
    ap.add_argument("--yes", action="store_true", help="Skip the post-estimate confirmation prompt")
    args = ap.parse_args()

    return asyncio.run(_main(args))


if __name__ == "__main__":
    sys.exit(main())
