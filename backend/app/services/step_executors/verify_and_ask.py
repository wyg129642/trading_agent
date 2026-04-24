"""Step 7 — VERIFY_AND_ASK.

Runs the three-layer verification on each cell that matches the debate
policy:
  1. Chain-of-Verification (CoVe) with an independent verifier model
  2. Three-way debate (Opus / Gemini / GPT) when configured
  3. Numerical sanity checks (universal, plus pack-declared)
  4. Expert-call-request generation for cells that lacked sources

Policy: ``cfg.debate_policy`` is a list of condition strings (see
``backend/app/services/debate_policy.py``). Absent policy falls back to
``critical_cell_patterns``.
"""
from __future__ import annotations

import fnmatch
import logging
from typing import Any

from sqlalchemy import select

from backend.app.models.revenue_model import ModelCell, SanityIssue
from backend.app.services import model_cell_store as _store
from .base import BaseStepExecutor, StepContext

logger = logging.getLogger(__name__)


class VerifyAndAskStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}
        patterns = cfg.get("critical_cell_patterns") or [
            "segment.*.rev.*",
            "total_revenue.*",
            "ni.*",
            "eps.*",
            "pe.*",
        ]
        policy_rules: list[str] = cfg.get("debate_policy") or []
        diff_threshold = float(cfg.get("diff_threshold_pct", 0.10))
        run_debate = bool(cfg.get("debate_on_critical_cells", False))
        max_debates = int(cfg.get("max_debates_per_run", 15))
        request_expert_calls = bool(cfg.get("request_expert_calls", True))

        q = select(ModelCell).where(ModelCell.model_id == ctx.model.id)
        cells = list((await ctx.db.execute(q)).scalars().all())
        # Fallback pattern-filter only used when no policy rules and no explicit matcher
        match_by_pattern = lambda c: any(fnmatch.fnmatch(c.path, pat) for pat in patterns)
        criticals = [c for c in cells if match_by_pattern(c)]

        # Lazy imports
        from backend.app.services.verification_agent import verify_cell
        from backend.app.services.debate_coordinator import debate_cell
        from backend.app.services.model_sanity import check_model
        from backend.app.services.debate_policy import evaluate_policy

        # Pre-compute YoY for policy (simple: same segment, previous period)
        yoy_by_path: dict[str, float] = {}
        rev_like = [c for c in cells if c.value is not None and
                    (c.path.startswith("segment.") and ".rev." in c.path)]
        by_prefix: dict[str, list[ModelCell]] = {}
        for c in rev_like:
            prefix = ".".join(c.path.split(".")[:-1])
            by_prefix.setdefault(prefix, []).append(c)
        for prefix, group in by_prefix.items():
            group.sort(key=lambda x: x.period or x.path)
            for i in range(1, len(group)):
                prev, curr = group[i - 1], group[i]
                if prev.value and prev.value != 0 and curr.value is not None:
                    yoy_by_path[curr.path] = (curr.value / prev.value) - 1.0

        flagged = 0
        debated = 0
        expert_requests_created = 0
        for c in criticals:
            if c.locked_by_human:
                continue
            if c.value is None and c.value_text is None:
                continue
            try:
                result = await verify_cell(ctx, c, diff_threshold=diff_threshold)
            except Exception:
                logger.exception("verify_cell failed for %s", c.path)
                continue
            if not result.get("consistent", True):
                c.review_status = "flagged"
                c.confidence = result.get("confidence") or "LOW"
                c.confidence_reason = result.get("reason") or "Verifier disagreed"
                c.alternative_values = list(c.alternative_values or []) + [{
                    "value": result.get("verifier_value"),
                    "source": "verifier",
                    "label": result.get("verifier_model", "verifier"),
                    "notes": result.get("verifier_reasoning", ""),
                }]
                flagged += 1
                await ctx.emit("verify_flag", {
                    "cell_path": c.path,
                    "reason": c.confidence_reason,
                    "alternatives": c.alternative_values[-1:],
                })

            # Decide whether to run full debate: policy rules > debate_on_critical_cells
            should_debate = False
            reason = ""
            if policy_rules:
                should_debate, reason = evaluate_policy(
                    policy_rules, cell=c, yoy=yoy_by_path.get(c.path),
                    sample_seed=c.path,
                )
            elif run_debate:
                should_debate, reason = True, "debate_on_critical_cells fallback"

            if should_debate and not ctx.dry_run and debated < max_debates:
                try:
                    await debate_cell(ctx, c)
                    debated += 1
                    await ctx.emit("debate_triggered", {
                        "cell_path": c.path, "reason": reason,
                    })
                except Exception:
                    logger.exception("debate_cell failed for %s", c.path)

        # Optionally create ExpertCallRequest rows for cells with no citations
        if request_expert_calls and not ctx.dry_run:
            from backend.app.models.revenue_model_extras import ExpertCallRequest
            weak = [
                c for c in criticals
                if (c.value is not None
                    and c.source_type in ("inferred", "assumption")
                    and len(c.citations or []) == 0
                    and not c.formula
                    and c.value_type != "text")
            ]
            for c in weak[:10]:  # cap per run
                req = ExpertCallRequest(
                    model_id=ctx.model.id,
                    cell_path=c.path,
                    ticker=ctx.model.ticker,
                    topic=f"{ctx.model.company_name} — {c.label or c.path}",
                    questions=[
                        f"What is the latest industry view on {c.label or c.path}?",
                        f"Can you confirm magnitude of ~{c.value}?",
                        "What are the key drivers (volume vs. price)?",
                    ],
                    rationale=(
                        f"Cell {c.path} has value {c.value} but no external citations "
                        f"(source_type={c.source_type}). We need expert corroboration "
                        "before publishing this model."
                    ),
                )
                ctx.db.add(req)
                expert_requests_created += 1

        # Numerical sanity pass (universal rules + pack rules + declarative)
        issues = await check_model(ctx.db, ctx.model, ctx.pack)
        ctx.db.add_all(
            SanityIssue(
                model_id=ctx.model.id,
                issue_type=i["issue_type"],
                severity=i["severity"],
                cell_paths=i["cell_paths"],
                message=i["message"],
                suggested_fix=i.get("suggested_fix", ""),
                details=i.get("details", {}),
            )
            for i in issues
        )

        await _store.update_model_counts(ctx.db, ctx.model.id)

        await ctx.emit("step_completed", {
            "criticals_checked": len(criticals),
            "flagged": flagged,
            "debated": debated,
            "sanity_issues": len(issues),
            "expert_requests": expert_requests_created,
        })
        return {
            "output_paths": [],
            "flagged": flagged,
            "debated": debated,
            "sanity_issues": len(issues),
            "expert_requests": expert_requests_created,
        }
