"""Weekly feedback consolidator.

Collects all ``UserFeedbackEvent`` rows since the last run (or the past
7 days), groups by industry, and asks an LLM to propose new playbook
lessons. Proposals land in ``pending_lessons`` for admin/boss review.

The scheduler triggers this every Friday 23:00 local time (wired in
lifespan). Can also be invoked ad-hoc via ``/api/playbook/consolidate``.
"""
from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.database import async_session_factory
from backend.app.models.feedback import PendingLesson, UserFeedbackEvent
from backend.app.services.step_executors._llm_helper import (
    call_llm_for_json,
    format_template,
)
from industry_packs import pack_registry

logger = logging.getLogger(__name__)


class _Ctx:
    """Minimal ctx stub to reuse the LLM helper."""
    def __init__(self, pack, dry_run: bool):
        self.pack = pack
        self.dry_run = dry_run
        self.total_tokens = 0
        self.total_latency_ms = 0
        self.model = None

    async def emit(self, *_args, **_kw):  # no-op for the helper
        pass


async def consolidate_feedback(
    since: datetime | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Consume recent UserFeedbackEvent rows → produce PendingLesson rows.

    Returns a summary of proposals.
    """
    cutoff = since or datetime.now(timezone.utc) - timedelta(days=7)
    batch_week = cutoff.date().isoformat()
    async with async_session_factory() as db:
        q = (
            select(UserFeedbackEvent)
            .where(
                UserFeedbackEvent.created_at >= cutoff,
                UserFeedbackEvent.consumed == False,  # noqa
            )
            .order_by(UserFeedbackEvent.created_at)
        )
        events = list((await db.execute(q)).scalars().all())
        if not events:
            return {"processed": 0, "proposals": 0, "note": "no feedback in window"}

        by_industry: dict[str, list[UserFeedbackEvent]] = defaultdict(list)
        for ev in events:
            key = ev.industry or "_global"
            by_industry[key].append(ev)

        total_proposals = 0
        all_ids: list[uuid.UUID] = []
        for industry, evs in by_industry.items():
            pack = pack_registry.get(industry) if industry != "_global" else None
            ctx = _Ctx(pack, dry_run=dry_run)
            # Enrich events with cell context so the consolidator LLM sees
            # not just old/new values but the full semantic picture.
            events_summary = []
            for ev in evs[:80]:
                cell_context = {}
                if ev.cell_id:
                    try:
                        from backend.app.models.revenue_model import ModelCell
                        cc = await db.get(ModelCell, ev.cell_id)
                        if cc:
                            cell_context = {
                                "label": cc.label,
                                "unit": cc.unit,
                                "period": cc.period,
                                "value_type": cc.value_type,
                                "source_type": cc.source_type,
                                "confidence": cc.confidence,
                                "notes": (cc.notes or "")[:200],
                            }
                    except Exception:
                        pass
                events_summary.append({
                    "event_type": ev.event_type,
                    "cell_path": ev.cell_path,
                    "payload": ev.payload,
                    "cell_context": cell_context,
                    "created_at": ev.created_at.isoformat(),
                })
            prompt = format_template(
                "You are the research-team's lesson consolidator. Review the "
                "following user feedback events from the past week for the "
                "`{industry}` industry: {events}. Distill any recurring "
                "pattern (≥ 3 similar corrections) into 1-3 new playbook "
                "lessons. Each lesson should include: \n"
                "  lesson_id (format: L-YYYY-MM-DD-NNN, today's date)\n"
                "  title (under 80 chars)\n"
                "  scenario (when this pattern applies — cell path prefixes, "
                "industry subsection, growth profile, etc.)\n"
                "  observation (what the LLM draft consistently got wrong)\n"
                "  rule (concrete actionable correction for future runs)\n"
                "  applicable_path_patterns (list of fnmatch patterns)\n"
                "Output JSON: {{\"lessons\":[{{...}}]}}. If no pattern reaches "
                "threshold, return {{\"lessons\":[]}}.",
                {"industry": industry, "events": events_summary},
            )
            parsed, _citations, _trace = await call_llm_for_json(
                ctx, user_prompt=prompt, path_hints=None,
            )
            proposals = (parsed or {}).get("lessons", []) or []
            today_suffix = datetime.now(timezone.utc).date().isoformat()
            for idx, p in enumerate(proposals):
                lesson_id = p.get("lesson_id") or f"L-{today_suffix}-{idx + 1:03d}"
                # Idempotency: skip if already present
                q = select(PendingLesson).where(PendingLesson.lesson_id == lesson_id)
                if (await db.execute(q)).scalar_one_or_none():
                    continue
                body = _render_lesson_body(p)
                row = PendingLesson(
                    industry=industry,
                    lesson_id=lesson_id,
                    title=p.get("title", "Untitled lesson"),
                    body=body,
                    scenario=p.get("scenario", ""),
                    observation=p.get("observation", ""),
                    rule=p.get("rule", ""),
                    sources=[{"event_id": str(ev.id)} for ev in evs[:20]],
                    batch_week=batch_week,
                )
                db.add(row)
                total_proposals += 1

            # Mark events as consumed regardless of proposals produced
            all_ids.extend([ev.id for ev in evs])

        if all_ids:
            await db.execute(
                update(UserFeedbackEvent)
                .where(UserFeedbackEvent.id.in_(all_ids))
                .values(consumed=True)
            )
        await db.commit()

        return {
            "processed": len(events),
            "proposals": total_proposals,
            "batch_week": batch_week,
        }


def _render_lesson_body(p: dict[str, Any]) -> str:
    parts = [
        f"## {p.get('lesson_id', 'L-????')} | {p.get('title', 'Untitled')}",
        "",
        f"**场景**: {p.get('scenario', '').strip()}",
        "",
        f"**观察**: {p.get('observation', '').strip()}",
        "",
        f"**新规则**: {p.get('rule', '').strip()}",
    ]
    return "\n".join(parts)
