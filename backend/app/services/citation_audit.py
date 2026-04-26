"""Citation audit — random-sample 5% of model citations and verify their
snippet actually appears in the cited source.

This is the "after-the-fact" anti-hallucination check: even if the LLM
reports a [N] citation with a URL or source_id, we don't trust it until
an independent LLM pass re-reads the source document and confirms the
snippet is present (or at least the claim is supported by the doc).

Writes to ``citation_audit_log`` for historical tracking.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, select, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base, async_session_factory
from backend.app.models.revenue_model import ModelCell
from backend.app.services.step_executors._llm_helper import call_llm_for_json

logger = logging.getLogger(__name__)


# ── ORM model ────────────────────────────────────────────────

class CitationAuditLog(Base):
    """One audit check for one cell-citation pair."""
    __tablename__ = "citation_audit_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    cell_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    cell_path: Mapped[str] = mapped_column(String(500), nullable=False)
    citation_title: Mapped[str] = mapped_column(Text, nullable=False, default="")
    citation_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    claimed_snippet: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # verified | mismatch | unreachable | unverifiable
    verdict: Mapped[str] = mapped_column(String(30), nullable=False)
    verdict_reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    tokens_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_cit_audit_model", "model_id"),
        Index("ix_cit_audit_verdict", "verdict"),
        Index("ix_cit_audit_created_at", "created_at"),
    )


# ── Audit logic ──────────────────────────────────────────────

@dataclass
class AuditResult:
    verdict: str
    reason: str
    details: dict[str, Any]
    tokens: int
    latency_ms: int


async def _verify_one(
    claimed_snippet: str,
    source_title: str,
    source_url: str,
    cell_path: str,
) -> AuditResult:
    """Ask an LLM with web_search / read_webpage to verify the snippet.

    We deliberately do NOT feed the claimed snippet into the LLM's system
    prompt in a way that lets it paraphrase; we ask it to independently
    re-read and judge.
    """
    if not claimed_snippet or not source_url and not source_title:
        return AuditResult(
            verdict="unverifiable",
            reason="Missing source_url/title/snippet",
            details={"claimed_snippet": claimed_snippet[:200]},
            tokens=0,
            latency_ms=0,
        )

    prompt = (
        f"You are an independent fact-checker for a research-modeling "
        f"system. The model asserts the following snippet came from a "
        f"specific source to support a value at `{cell_path}`.\n\n"
        f"CLAIMED SNIPPET (verbatim or paraphrased from source):\n"
        f"<<<\n{claimed_snippet[:600]}\n>>>\n\n"
        f"CLAIMED SOURCE: title='{source_title}' url='{source_url}'\n\n"
        f"Your task: use `read_webpage` on the URL (and/or web_search if "
        f"the URL is unreadable), then decide independently whether the "
        f"source actually contains that claim.\n\n"
        f"Output JSON:\n"
        f"{{\n"
        f"  \"verdict\": \"verified|mismatch|unreachable|unverifiable\",\n"
        f"  \"reason\": \"...\",\n"
        f"  \"evidence_excerpt\": \"... verbatim quote from source, if any ...\"\n"
        f"}}\n\n"
        f"verified = source clearly states the claim; mismatch = source "
        f"exists and contradicts or doesn't mention the claim; unreachable "
        f"= can't fetch the URL; unverifiable = source too vague to judge."
    )

    # Use a lightweight ctx stub — we don't want to pull the full pack in
    class _Ctx:
        pack = None
        dry_run = False
        step_config = {}

    t0 = datetime.now(timezone.utc)
    parsed, _citations, trace = await call_llm_for_json(
        _Ctx(),
        user_prompt=prompt,
        model_id="google/gemini-3.1-pro-preview",
        path_hints=[cell_path],
        tool_set=("web_search", "read_webpage"),
        max_tool_rounds=3,
    )
    elapsed_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
    tokens = sum(t.get("tokens", 0) for t in trace)

    verdict = (parsed or {}).get("verdict", "unverifiable")
    if verdict not in ("verified", "mismatch", "unreachable", "unverifiable"):
        verdict = "unverifiable"
    reason = (parsed or {}).get("reason", "")
    evidence = (parsed or {}).get("evidence_excerpt", "")
    return AuditResult(
        verdict=verdict,
        reason=reason,
        details={
            "evidence_excerpt": (evidence or "")[:500],
            "trace_preview": trace[-1].get("response_preview", "") if trace else "",
        },
        tokens=tokens,
        latency_ms=elapsed_ms,
    )


async def audit_model(
    model_id: uuid.UUID,
    *,
    sample_rate: float = 0.05,
    max_samples: int = 20,
    concurrency: int = 3,
) -> dict[str, Any]:
    """Random-sample citations on this model and verify each."""
    async with async_session_factory() as db:
        q = select(ModelCell).where(
            ModelCell.model_id == model_id,
        )
        cells = list((await db.execute(q)).scalars().all())
        # Build a flat list of (cell, citation) pairs
        pairs: list[tuple[ModelCell, dict[str, Any]]] = []
        for c in cells:
            for cit in (c.citations or []):
                if not isinstance(cit, dict):
                    continue
                pairs.append((c, cit))
        if not pairs:
            return {"sampled": 0, "verified": 0, "mismatch": 0, "unreachable": 0, "unverifiable": 0}

        k = max(1, min(max_samples, int(len(pairs) * sample_rate)))
        sample = random.sample(pairs, k)

        sem = asyncio.Semaphore(concurrency)
        counts: dict[str, int] = {
            "verified": 0, "mismatch": 0, "unreachable": 0, "unverifiable": 0,
        }
        rows: list[CitationAuditLog] = []

        async def _one(pair):
            cell, cit = pair
            async with sem:
                try:
                    res = await _verify_one(
                        claimed_snippet=str(cit.get("snippet") or cit.get("title") or ""),
                        source_title=str(cit.get("title") or ""),
                        source_url=str(cit.get("url") or ""),
                        cell_path=cell.path,
                    )
                except Exception as e:
                    logger.exception("Audit failed for cell %s", cell.path)
                    res = AuditResult(
                        verdict="unverifiable",
                        reason=f"Audit error: {e}",
                        details={}, tokens=0, latency_ms=0,
                    )
                counts[res.verdict] = counts.get(res.verdict, 0) + 1
                rows.append(CitationAuditLog(
                    model_id=model_id,
                    cell_id=cell.id,
                    cell_path=cell.path,
                    citation_title=str(cit.get("title") or "")[:500],
                    citation_url=str(cit.get("url") or "")[:1000],
                    claimed_snippet=str(cit.get("snippet") or "")[:4000],
                    verdict=res.verdict,
                    verdict_reason=res.reason[:2000],
                    details=res.details,
                    tokens_used=res.tokens,
                    latency_ms=res.latency_ms,
                ))

        await asyncio.gather(*[_one(p) for p in sample])
        db.add_all(rows)
        await db.commit()
        return {
            "sampled": k,
            **counts,
            "hallucination_rate": round(
                counts.get("mismatch", 0) / max(k, 1), 4,
            ),
        }


async def hallucination_summary(
    since_days: int = 7, industry: str | None = None,
) -> dict[str, Any]:
    async with async_session_factory() as db:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        q = select(CitationAuditLog).where(CitationAuditLog.created_at >= cutoff)
        rows = list((await db.execute(q)).scalars().all())
        total = len(rows)
        by_verdict: dict[str, int] = {}
        for r in rows:
            by_verdict[r.verdict] = by_verdict.get(r.verdict, 0) + 1
        return {
            "since": cutoff.isoformat(),
            "total_sampled": total,
            "verdicts": by_verdict,
            "hallucination_rate": round(by_verdict.get("mismatch", 0) / max(total, 1), 4),
        }
