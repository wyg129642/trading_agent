"""Recipe Engine — async DAG executor for step-wise revenue modeling.

Concurrency model: the engine runs one ``RecipeRun`` in an asyncio task.
Inside a run, steps execute sequentially in topological order; within a
step, the executor may itself fan out concurrent LLM calls. Cross-run
concurrency is the caller's responsibility (the API handler spawns one
task per run, the DB row is the coordination object).

Resumability: if the service is restarted mid-run, calling
``continue_run(run_id)`` picks up from the last non-completed step.

SSE: the engine publishes events into an ``asyncio.Queue`` per run; the
API endpoint drains the queue and forwards to the SSE connection.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.database import async_session_factory
from backend.app.models.recipe import Recipe, RecipeRun
from backend.app.models.revenue_model import RevenueModel
from backend.app.models.user import User
from backend.app.services.cost_estimation import (
    cost_from_flat_tokens,
    check_user_quota,
)
from backend.app.services.step_executors import STEP_REGISTRY, StepContext, StepEvent
from industry_packs import pack_registry

logger = logging.getLogger(__name__)


# ── Run registry ────────────────────────────────────────────────
# Tracks active runs' event queues for SSE subscribers.

_RUN_QUEUES: dict[uuid.UUID, list[asyncio.Queue[StepEvent]]] = {}


def subscribe(run_id: uuid.UUID) -> asyncio.Queue[StepEvent]:
    q: asyncio.Queue[StepEvent] = asyncio.Queue()
    _RUN_QUEUES.setdefault(run_id, []).append(q)
    return q


def unsubscribe(run_id: uuid.UUID, q: asyncio.Queue[StepEvent]) -> None:
    if run_id in _RUN_QUEUES:
        if q in _RUN_QUEUES[run_id]:
            _RUN_QUEUES[run_id].remove(q)
        if not _RUN_QUEUES[run_id]:
            _RUN_QUEUES.pop(run_id, None)


async def _broadcast(run_id: uuid.UUID, event: StepEvent) -> None:
    for q in list(_RUN_QUEUES.get(run_id, [])):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass


# ── Run orchestration ──────────────────────────────────────────


async def _should_pause_for_cost(
    db: AsyncSession, run: RecipeRun, user: User | None,
) -> tuple[bool, str]:
    """Return (True, reason) if run has exceeded a cost gate."""
    total = float(run.total_cost_usd or 0.0)
    cap = float(run.cost_cap_usd or 0.0)
    if cap > 0 and total >= cap:
        return True, f"run_cap_exceeded:{total:.4f}>={cap:.4f}"
    if user is None:
        return False, ""
    # Defer to user quota
    status = await check_user_quota(db, user, estimated_add_usd=0.0)
    if status.exceeded:
        return True, (
            f"monthly_quota_exceeded:{status.spent_this_month_usd:.2f}"
            f"/{status.monthly_budget_usd:.2f}"
        )
    return False, ""


async def _topo_nodes(recipe: Recipe) -> list[dict[str, Any]]:
    """Return nodes in topological order respecting explicit edges (if any).

    Falls back to file order if edges are absent.
    """
    graph = recipe.graph or {}
    nodes = list(graph.get("nodes", []))
    edges = list(graph.get("edges", []))
    if not edges:
        return nodes
    id_to_node = {n["id"]: n for n in nodes}
    indeg: dict[str, int] = {n["id"]: 0 for n in nodes}
    adj: dict[str, list[str]] = defaultdict(list)
    for e in edges:
        src = e.get("from") or e.get("from_id")
        dst = e.get("to")
        if not src or not dst or src not in id_to_node or dst not in id_to_node:
            continue
        adj[src].append(dst)
        indeg[dst] += 1
    queue: deque[str] = deque(n_id for n_id, d in indeg.items() if d == 0)
    order: list[str] = []
    while queue:
        v = queue.popleft()
        order.append(v)
        for w in adj[v]:
            indeg[w] -= 1
            if indeg[w] == 0:
                queue.append(w)
    # Append any leftover nodes (unconnected) at the end
    for n in nodes:
        if n["id"] not in order:
            order.append(n["id"])
    return [id_to_node[x] for x in order if x in id_to_node]


async def run_recipe(
    run_id: uuid.UUID,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run a recipe end-to-end.

    Designed to be awaited from ``asyncio.create_task`` by the API.
    Broadcasts events via ``_broadcast``; any subscribers (SSE clients)
    receive them via :func:`subscribe`.
    """
    async with async_session_factory() as db:
        run: RecipeRun | None = await db.get(RecipeRun, run_id)
        if not run:
            logger.error("run_recipe: RecipeRun %s not found", run_id)
            return {"error": "run_not_found"}
        recipe: Recipe | None = await db.get(Recipe, run.recipe_id)
        if not recipe:
            run.status = "failed"
            run.error = "recipe_not_found"
            await db.commit()
            return {"error": "recipe_not_found"}
        model: RevenueModel | None = await db.get(RevenueModel, run.model_id)
        if not model:
            run.status = "failed"
            run.error = "model_not_found"
            await db.commit()
            return {"error": "model_not_found"}
        pack = pack_registry.get(model.industry) if model.industry else None

        run.status = "running"
        model.status = "running"
        await db.commit()

        nodes = await _topo_nodes(recipe)

        async def event_sink(evt: StepEvent) -> None:
            await _broadcast(run_id, evt)

        for node in nodes:
            step_type = node.get("type")
            step_id = node.get("id")
            cls = STEP_REGISTRY.get(step_type)
            if cls is None:
                logger.warning("Unknown step type %r; skipping", step_type)
                continue
            run.current_step_id = step_id
            await db.commit()
            await _broadcast(run_id, StepEvent(
                type="step_started",
                step_id=step_id,
                payload={"label": node.get("label"), "type": step_type},
            ))
            ctx = StepContext(
                db=db, model=model, run=run,
                step_config=node.get("config") or {},
                step_id=step_id,
                event_sink=event_sink,
                pack=pack,
                dry_run=dry_run,
            )
            try:
                result = await cls().run(ctx)
            except Exception as e:
                logger.exception("Step %s (%s) failed", step_id, step_type)
                run.status = "failed"
                run.error = f"{step_id}: {e}"
                await db.commit()
                await _broadcast(run_id, StepEvent(
                    type="step_failed",
                    step_id=step_id,
                    payload={"error": str(e)},
                ))
                return {"error": str(e), "step": step_id}

            # Cost accounting — track $ per step and accumulate on the run.
            step_model_id = (
                (node.get("config") or {}).get("model_id")
                or "anthropic/claude-opus-4-7"
            )
            step_cost_usd = cost_from_flat_tokens(step_model_id, ctx.total_tokens)
            run.step_results = {
                **(run.step_results or {}),
                step_id: {
                    "status": "completed",
                    "ended_at": datetime.now(timezone.utc).isoformat(),
                    "output_paths": (result or {}).get("output_paths", []),
                    "tokens": ctx.total_tokens,
                    "cost_usd": step_cost_usd,
                    "model_id": step_model_id,
                    **{k: v for k, v in (result or {}).items() if k != "output_paths"},
                },
            }
            run.total_tokens = (run.total_tokens or 0) + ctx.total_tokens
            run.total_cost_usd = round(float(run.total_cost_usd or 0.0) + step_cost_usd, 6)
            await db.commit()

            # Cost-cap gate: after each step, check against the run's hard cap
            # and the user's monthly quota. Pause the run (status=paused_for_human)
            # so a human can resume or raise the cap.
            try:
                user = None
                if run.started_by:
                    user = await db.get(User, run.started_by)
                paused, reason = await _should_pause_for_cost(db, run, user)
                if paused:
                    run.status = "paused_for_human"
                    run.paused_reason = reason
                    model.status = "paused"
                    await db.commit()
                    await _broadcast(run_id, StepEvent(
                        type="run_paused",
                        step_id=step_id,
                        payload={"reason": reason, "total_cost_usd": run.total_cost_usd},
                    ))
                    logger.warning(
                        "Recipe run %s paused at step %s: %s (spent=$%.4f)",
                        run_id, step_id, reason, run.total_cost_usd,
                    )
                    return {"status": "paused", "reason": reason}
            except Exception:
                logger.exception("cost-gate check failed (non-fatal)")

        # Final evaluation pass (idempotent; margin_cascade already did once)
        from backend.app.services.model_cell_store import evaluate_formulas, update_model_counts
        await evaluate_formulas(db, model.id)

        # Final sanity pass — always run at the end so we catch issues even
        # when the recipe doesn't include VERIFY_AND_ASK. Idempotent: we
        # flush old un-resolved rows for this model on each run.
        try:
            from backend.app.services.model_sanity import check_model
            from backend.app.models.revenue_model import SanityIssue
            from sqlalchemy import delete as sa_delete

            # Clear existing unresolved findings so repeated runs don't pile up
            await db.execute(
                sa_delete(SanityIssue).where(
                    SanityIssue.model_id == model.id,
                    SanityIssue.resolved == False,  # noqa
                )
            )
            issues = await check_model(db, model, pack)
            db.add_all(
                SanityIssue(
                    model_id=model.id,
                    issue_type=i["issue_type"],
                    severity=i["severity"],
                    cell_paths=i["cell_paths"],
                    message=i["message"],
                    suggested_fix=i.get("suggested_fix", ""),
                    details=i.get("details", {}),
                )
                for i in issues
            )
            await _broadcast(run_id, StepEvent(
                type="sanity_check_done",
                step_id="final_sanity",
                payload={"issues": len(issues)},
            ))
        except Exception:
            logger.exception("Final sanity pass failed for run %s", run_id)

        await update_model_counts(db, model.id)

        run.status = "completed"
        run.completed_at = datetime.now(timezone.utc)
        run.current_step_id = None
        model.status = "ready"
        model.last_run_id = run.id
        await db.commit()

        # Project segment cells into the cross-model snapshot table.
        # Best-effort — snapshot failure must not fail the run.
        try:
            from backend.app.services.segment_snapshot_service import refresh_for_model
            snap_result = await refresh_for_model(db, model)
            logger.info("Segment snapshot refreshed: %s", snap_result)
        except Exception:
            logger.exception("refresh_for_model failed (non-fatal)")

        await _broadcast(run_id, StepEvent(
            type="run_completed",
            step_id="run",
            payload={"model_id": str(model.id), "status": "ready"},
        ))
        return {"status": "completed", "model_id": str(model.id)}
