"""A/B winner distillation — turn ``ab_winner`` feedback events into lessons.

Trigger (cron or manual): every N hours, count (recipe_slug, winning_side)
pairs over the last 30 days. When a side (A or B) wins ≥ threshold times
with a ≥ 2-to-1 margin, emit a PendingLesson distilled from the prompt
diff between the winning and losing recipes.

The distilled lesson captures "for industry X, when the prompt includes
Y pattern, side A-style wording beat side B-style Y% of the time".

This module is pure service logic; the consolidator cron (``main.py``
lifespan) is the scheduler.
"""
from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.database import async_session_factory
from backend.app.models.feedback import PendingLesson, UserFeedbackEvent
from backend.app.models.recipe import Recipe, RecipeRun

logger = logging.getLogger(__name__)


# Minimum number of ab_winner votes on a session-pair before we distill.
MIN_VOTES = 3
# Winner must beat loser by this many votes (absolute margin) to count.
MIN_MARGIN = 2


async def _collect_ab_winner_events(
    db: AsyncSession, since_days: int,
) -> list[UserFeedbackEvent]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    q = (
        select(UserFeedbackEvent)
        .where(
            UserFeedbackEvent.event_type == "ab_winner",
            UserFeedbackEvent.created_at >= cutoff,
        )
    )
    return list((await db.execute(q)).scalars().all())


async def _resolve_runs(
    db: AsyncSession, session_ids: set[str],
) -> dict[str, dict[str, RecipeRun]]:
    """Return session_id → {'A': RecipeRun, 'B': RecipeRun}."""
    if not session_ids:
        return {}
    q = select(RecipeRun).where(RecipeRun.ab_session.in_(session_ids))
    rows = list((await db.execute(q)).scalars().all())
    out: dict[str, dict[str, RecipeRun]] = defaultdict(dict)
    for r in rows:
        if r.ab_session and r.ab_group in ("A", "B"):
            out[r.ab_session][r.ab_group] = r
    return out


def _extract_prompt_diff(winning: dict, losing: dict) -> dict:
    """Return a shape describing how the winner differs from the loser."""
    w_nodes = {n.get("id"): n for n in (winning or {}).get("nodes", []) if isinstance(n, dict)}
    l_nodes = {n.get("id"): n for n in (losing or {}).get("nodes", []) if isinstance(n, dict)}
    changes = []
    for nid in set(w_nodes) & set(l_nodes):
        w_cfg = (w_nodes[nid].get("config") or {})
        l_cfg = (l_nodes[nid].get("config") or {})
        w_prompt = (w_cfg.get("prompt_template") or "")
        l_prompt = (l_cfg.get("prompt_template") or "")
        if w_prompt != l_prompt:
            changes.append({
                "node_id": nid,
                "node_type": w_nodes[nid].get("type"),
                "winning_prompt_preview": w_prompt[:500],
                "losing_prompt_preview": l_prompt[:500],
            })
    return {"changed_nodes": changes}


async def distill_ab_winners(
    since_days: int = 30, *, dry_run: bool = False,
) -> dict[str, Any]:
    """Aggregate ab_winner events, write PendingLessons for clear winners."""
    async with async_session_factory() as db:
        events = await _collect_ab_winner_events(db, since_days=since_days)
        if not events:
            return {"processed": 0, "proposals": 0, "note": "no ab_winner events"}

        # Group votes by ab_session
        votes: dict[str, dict[str, int]] = defaultdict(lambda: {"A": 0, "B": 0})
        industries_by_session: dict[str, str] = {}
        for ev in events:
            payload = ev.payload or {}
            session = str(payload.get("ab_session") or "")
            winner = str(payload.get("winner") or "").upper()
            if not session or winner not in ("A", "B"):
                continue
            votes[session][winner] += 1
            if ev.industry and session not in industries_by_session:
                industries_by_session[session] = ev.industry

        # Resolve RecipeRuns
        runs_by_session = await _resolve_runs(db, set(votes.keys()))

        proposals = 0
        today = datetime.now(timezone.utc).date().isoformat()
        for session, tally in votes.items():
            total = tally["A"] + tally["B"]
            if total < MIN_VOTES:
                continue
            winner = "A" if tally["A"] >= tally["B"] else "B"
            loser = "B" if winner == "A" else "A"
            margin = tally[winner] - tally[loser]
            if margin < MIN_MARGIN:
                continue

            sess_runs = runs_by_session.get(session, {})
            winning_run = sess_runs.get(winner)
            losing_run = sess_runs.get(loser)
            if not winning_run or not losing_run:
                continue

            w_rec = await db.get(Recipe, winning_run.recipe_id)
            l_rec = await db.get(Recipe, losing_run.recipe_id)
            if not w_rec or not l_rec:
                continue
            diff = _extract_prompt_diff(w_rec.graph or {}, l_rec.graph or {})
            if not diff.get("changed_nodes"):
                continue

            industry = industries_by_session.get(session) or w_rec.industry or "_global"
            lesson_id = f"L-{today}-AB-{session[:6]}"
            q = select(PendingLesson).where(PendingLesson.lesson_id == lesson_id)
            if (await db.execute(q)).scalar_one_or_none():
                continue
            body_parts = [
                f"## {lesson_id} | A/B 实验得出的 prompt 偏好",
                "",
                f"**场景**: industry `{industry}`, recipe `{w_rec.slug}` A/B 对比",
                "",
                f"**观察**: 在 {total} 次 A/B 投票中, side {winner} 胜出 "
                f"{tally[winner]} 次 (对方 {tally[loser]} 次), 优势显著。",
                "",
                "**规则**: 保留胜出方 prompt 的以下差异:",
                "",
            ]
            for c in diff["changed_nodes"][:5]:
                body_parts.append(
                    f"- 节点 `{c['node_id']}` ({c['node_type']}): 采用胜出方风格: "
                    f"{(c['winning_prompt_preview'] or '')[:200]}"
                )
            body_parts.append("")
            body_parts.append(
                "applicable_path_patterns: ['segment.*', 'consensus.*', 'peer.*']"
            )
            body_parts.append("")
            body_parts.append(
                f"**支撑**: ab_session={session}, 投票数 A={tally['A']} B={tally['B']}"
            )
            body = "\n".join(body_parts)

            if not dry_run:
                db.add(PendingLesson(
                    industry=industry,
                    lesson_id=lesson_id,
                    title=f"A/B 胜出 prompt 偏好 (session {session[:6]})",
                    body=body,
                    scenario=f"industry={industry}, session={session}",
                    observation=f"side {winner} beat {loser} {tally[winner]}:{tally[loser]}",
                    rule=(
                        f"Prefer winning-side prompt style for nodes: "
                        f"{', '.join(c['node_id'] for c in diff['changed_nodes'][:5])}"
                    ),
                    sources=[{"ab_session": session, "votes": tally}],
                    batch_week=today,
                ))
            proposals += 1
        if not dry_run:
            await db.commit()
        return {
            "processed": len(events),
            "sessions_analysed": len(votes),
            "proposals": proposals,
        }
