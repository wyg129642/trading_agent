"""EN→ZH translator for Funda sentiment fields.

Translates `ai_summary` and a handful of short metadata strings (sector,
industry, company) using Qwen via the DashScope OpenAI-compatible endpoint
(`llm_enrichment_*` settings). The translation is cached back into the same
Mongo doc as `<field>_zh` plus a `<field>_zh_src_hash` so we re-translate only
when the source text actually changes.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from functools import lru_cache
from typing import Any

import httpx
from motor.motor_asyncio import AsyncIOMotorCollection
from openai import AsyncOpenAI
from pymongo import UpdateOne

from backend.app.config import get_settings

logger = logging.getLogger(__name__)

TRANSLATE_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("ai_summary", "ai_summary_zh", "summary"),
    ("sector", "sector_zh", "phrase"),
    ("industry", "industry_zh", "phrase"),
    ("company", "company_zh", "phrase"),
)


def _looks_chinese(text: str) -> bool:
    if not text:
        return True
    ascii_letters = sum(1 for c in text if c.isascii() and c.isalpha())
    return ascii_letters < len(text) * 0.30


def _src_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _hash_key(dest_field: str) -> str:
    return f"{dest_field}_src_hash"


def _strip_wrapping_quotes(s: str) -> str:
    if len(s) >= 2 and s[0] in "\"'“”‘’" and s[-1] in "\"'“”‘’":
        return s[1:-1].strip()
    return s


class SentimentTranslator:
    def __init__(self, *, max_concurrency: int = 8) -> None:
        s = get_settings()
        self._enabled = bool(s.llm_enrichment_api_key)
        if not self._enabled:
            self._client: AsyncOpenAI | None = None
            self._model = ""
        else:
            self._client = AsyncOpenAI(
                api_key=s.llm_enrichment_api_key,
                base_url=s.llm_enrichment_base_url,
                timeout=30.0,
                http_client=httpx.AsyncClient(trust_env=False, timeout=30.0),
            )
            self._model = s.llm_enrichment_model
        self._sem = asyncio.Semaphore(max_concurrency)

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def translate(self, text: str, *, kind: str = "summary") -> str:
        text = (text or "").strip()
        if not text or not self._enabled or _looks_chinese(text):
            return text
        if kind == "phrase":
            system = (
                "你是金融术语翻译。把英文短语翻译成简体中文，保留股票代码和数字。"
                "只输出中文译文，不要引号、解释、英文原文或其它前缀。"
            )
            max_tokens = 60
        else:
            system = (
                "你是专业的金融翻译。把英文社交媒体情绪摘要翻译为简体中文，"
                "保留股票代码、数字、百分比和换行；不要添加解释或英文原文。"
                "只输出中文译文。"
            )
            max_tokens = 800
        async with self._sem:
            try:
                resp = await self._client.chat.completions.create(  # type: ignore[union-attr]
                    model=self._model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": text},
                    ],
                    max_tokens=max_tokens,
                    temperature=0.0,
                )
                out = (resp.choices[0].message.content or "").strip()
                out = _strip_wrapping_quotes(out)
                return out or text
            except Exception:
                logger.exception("[funda-translate] failed (kind=%s)", kind)
                return text


@lru_cache(maxsize=1)
def get_translator() -> SentimentTranslator:
    return SentimentTranslator()


async def translate_docs_in_place(
    coll: AsyncIOMotorCollection | None,
    docs: list[dict[str, Any]],
    *,
    persist: bool = True,
) -> None:
    """Translate untranslated EN fields in `docs` in-place; persist to Mongo.

    Each entry of `docs` is a raw Mongo doc (mutated). Re-uses cached
    translations whose source-text hash still matches; only the deltas hit the
    LLM. When `persist` is True and `coll` is provided, the new translations
    are bulk-written back so subsequent reads are instant.
    """
    tr = get_translator()
    if not tr.enabled or not docs:
        return

    work: list[tuple[dict[str, Any], str, str, str]] = []
    for d in docs:
        for src_field, dest_field, kind in TRANSLATE_FIELDS:
            src = (d.get(src_field) or "").strip()
            if not src:
                continue
            cached = (d.get(dest_field) or "").strip()
            if cached and d.get(_hash_key(dest_field)) == _src_hash(src):
                continue
            if _looks_chinese(src):
                d[dest_field] = src
                d[_hash_key(dest_field)] = _src_hash(src)
                continue
            work.append((d, dest_field, kind, src))
    if not work:
        return

    async def _one(item: tuple[dict[str, Any], str, str, str]) -> None:
        doc, dest_field, kind, src = item
        translated = await tr.translate(src, kind=kind)
        doc[dest_field] = translated
        doc[_hash_key(dest_field)] = _src_hash(src)

    await asyncio.gather(*[_one(w) for w in work], return_exceptions=True)

    if persist and coll is not None:
        ops: list[UpdateOne] = []
        seen_ids: set[Any] = set()
        for d in docs:
            _id = d.get("_id")
            if _id is None or _id in seen_ids:
                continue
            seen_ids.add(_id)
            updates: dict[str, Any] = {}
            for _src_field, dest_field, _kind in TRANSLATE_FIELDS:
                if dest_field in d:
                    updates[dest_field] = d[dest_field]
                    updates[_hash_key(dest_field)] = d[_hash_key(dest_field)]
            if updates:
                ops.append(UpdateOne({"_id": _id}, {"$set": updates}))
        if ops:
            try:
                await coll.bulk_write(ops, ordered=False)
            except Exception:
                logger.exception("[funda-translate] bulk persist failed")
