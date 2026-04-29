"""Shared LLM-based EN→ZH translator for research/commentary/news bodies.

Extracted from ``scripts/translate_portfolio_research.py`` so the news
translator lifespan worker and the one-shot Mongo backfill script can share
one implementation. Behaviour is unchanged: AsyncOpenAI against DashScope's
OpenAI-compatible endpoint, paragraph-aware chunking, internal semaphore.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass

import httpx
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

CJK_RE = re.compile(r"[一-鿿]")


def looks_foreign(text: str, *, min_signal: int = 100) -> bool:
    """True if the leading 2 KB of ``text`` is dominated by ASCII letters and
    has very little CJK — used as the "translate this" gate.

    ``min_signal`` is the minimum (cjk + ascii_letter) sum we require before
    judging — body fields use 100, titles use 20 (a single short headline can
    still be decisive).

    2026-04-29: jinmen.oversea_reports stores titles like
    "杰富瑞 - 2026年美国临床肿瘤学会年会标题：Ideaya Biosciences, Karyopharm
    Therapeutics, Black Diamond Therapeutics, ..." — already Chinese, but
    full of US ticker / company-name English. The 3:1 ASCII heuristic
    flagged it as foreign; the LLM then "translated" the Chinese title back
    to English. To prevent that, treat any text with ≥5 CJK characters as
    Chinese-with-embedded-English-names (not foreign), regardless of the
    ASCII tail of company lists.
    """
    if not text:
        return False
    sample = text[:2000]
    cjk = len(CJK_RE.findall(sample))
    ascii_letters = sum(1 for c in sample if c.isascii() and c.isalpha())
    if (cjk + ascii_letters) < min_signal:
        return False
    if cjk >= 5:
        return False
    return cjk * 3 < ascii_letters


def src_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _strip_wrapping_quotes(s: str) -> str:
    if len(s) >= 2 and s[0] in "\"'“”‘’" and s[-1] in "\"'“”‘’":
        return s[1:-1].strip()
    return s


@dataclass
class TranslatorConfig:
    api_key: str
    base_url: str
    model: str
    chunk_chars: int = 6000
    max_concurrency: int = 6


class LongTranslator:
    """Async EN→ZH translator with paragraph-aware chunking.

    The system prompt is tuned for sell-side research / minutes / news bodies
    (preserves markdown, tickers, numbers, English proper nouns). Chunks are
    fired concurrently with a semaphore — caller can also batch many docs in
    parallel; the semaphore bounds real API concurrency across both axes.
    """

    def __init__(self, cfg: TranslatorConfig) -> None:
        self._cfg = cfg
        self._client = AsyncOpenAI(
            api_key=cfg.api_key,
            base_url=cfg.base_url,
            timeout=120.0,
            http_client=httpx.AsyncClient(trust_env=False, timeout=120.0),
        )
        self._sem = asyncio.Semaphore(cfg.max_concurrency)
        self.in_tokens = 0
        self.out_tokens = 0
        self.calls = 0

    async def _one_call(self, text: str) -> str:
        system = (
            "你是专业的金融翻译。把外文研报/会议纪要/访谈/新闻翻译为简体中文，"
            "严格保留 markdown 排版（标题、列表、加粗、表格、换行）、股票代码、"
            "数字、百分比、货币符号、人名英文原名（首次出现可用括号附中文），"
            "保持段落顺序和长度大致一致。只输出译文，不要引号、解释或前后缀。"
        )
        async with self._sem:
            try:
                resp = await self._client.chat.completions.create(
                    model=self._cfg.model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": text},
                    ],
                    max_tokens=4096,
                    temperature=0.1,
                )
                self.calls += 1
                if resp.usage:
                    self.in_tokens += resp.usage.prompt_tokens or 0
                    self.out_tokens += resp.usage.completion_tokens or 0
                out = (resp.choices[0].message.content or "").strip()
                return _strip_wrapping_quotes(out) or text
            except Exception as e:
                logger.warning("translate call failed: %s", e)
                return ""

    @staticmethod
    def _split_chunks(text: str, max_chars: int) -> list[str]:
        if len(text) <= max_chars:
            return [text]
        paras = text.split("\n\n")
        chunks: list[str] = []
        buf = ""
        for p in paras:
            if len(p) > max_chars:
                if buf:
                    chunks.append(buf)
                    buf = ""
                lines = p.split("\n")
                sub = ""
                for ln in lines:
                    if len(sub) + len(ln) + 1 > max_chars and sub:
                        chunks.append(sub)
                        sub = ln
                    else:
                        sub = (sub + "\n" + ln) if sub else ln
                if sub:
                    chunks.append(sub)
                continue
            if len(buf) + len(p) + 2 > max_chars and buf:
                chunks.append(buf)
                buf = p
            else:
                buf = (buf + "\n\n" + p) if buf else p
        if buf:
            chunks.append(buf)
        out: list[str] = []
        for c in chunks:
            if len(c) <= max_chars:
                out.append(c)
            else:
                for i in range(0, len(c), max_chars):
                    out.append(c[i:i + max_chars])
        return out

    async def translate(self, text: str) -> str:
        text = (text or "").strip()
        if not text:
            return ""
        chunks = self._split_chunks(text, self._cfg.chunk_chars)
        if len(chunks) == 1:
            return await self._one_call(chunks[0])
        results = await asyncio.gather(*[self._one_call(c) for c in chunks])
        if any(r == "" for r in results):
            return ""
        return "\n\n".join(results)
