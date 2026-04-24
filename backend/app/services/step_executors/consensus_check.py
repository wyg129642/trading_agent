"""Step X — CONSENSUS_CHECK.

Cross-reference the modeled EPS / revenue / net income against the
market-wide analyst consensus. On A-shares this pulls Wind's
ASHARECONSENSUS* tables via ``consensus_forecast.fetch_consensus``. On
US/HK the LLM falls back to ``web_search`` (wrapped as
``external_consensus_lookup``) to estimate sell-side consensus from
public reports.

Emits SanityIssue rows when the model's forecast deviates > 25% from
consensus, so the researcher is forced to explain why.
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select

from backend.app.models.revenue_model import ModelCell, SanityIssue
from backend.app.services import model_cell_store as _store
from ._llm_helper import call_llm_for_json, format_template
from .base import BaseStepExecutor, StepContext

logger = logging.getLogger(__name__)


class ConsensusCheckStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}
        diff_threshold = float(cfg.get("diff_threshold_pct", 0.25))

        # Fetch A-share consensus if ticker is A-share
        consensus_payload = None
        a_share = self._is_a_share(ctx.model.ticker, ctx.model.industry)
        if a_share:
            try:
                from backend.app.services.consensus_forecast import (
                    fetch_consensus,
                    to_windcode,
                )
                plain = ctx.model.ticker.split(".")[0]
                market = cfg.get("market_label") or "主板"
                wc = to_windcode(plain, market)
                if wc:
                    data = await fetch_consensus(plain, market)
                    if data and getattr(data, "windcode", None):
                        consensus_payload = {
                            "source": "wind",
                            "windcode": wc,
                            "analyst_count": data.analyst_count,
                            "target_price": data.target_price,
                            "rating_label": data.rating_label,
                            "yoy_net_profit_pct": data.yoy_net_profit,
                            "fy1": _fy_dict(data.fy1),
                            "fy2": _fy_dict(data.fy2),
                            "fy3": _fy_dict(data.fy3),
                        }
            except Exception:
                logger.exception("Wind consensus fetch failed for %s", ctx.model.ticker)

        # Fall back to LLM + web_search for non-A-share (or A-share with no Wind data)
        if consensus_payload is None:
            prompt = format_template(
                cfg.get(
                    "prompt_template",
                    "Look up the sell-side analyst consensus for {ticker} ({company_name}) "
                    "over periods {periods}. Use web_search to find aggregated analyst "
                    "forecasts (from Bloomberg/Yahoo Finance/Seeking Alpha). "
                    "Output JSON: {{\"fy1\":{{\"revenue\":number,\"eps\":number,\"period\":...}}, "
                    "\"fy2\":{{...}}, \"fy3\":{{...}}, \"analyst_count\":number, "
                    "\"source\":string, \"citations\":[...] }}",
                ),
                {
                    "ticker": ctx.model.ticker,
                    "company_name": ctx.model.company_name,
                    "periods": list(ctx.model.fiscal_periods),
                },
            )
            await ctx.emit("step_progress", {"label": "looking up external consensus via web_search"})
            parsed, citations, trace = await call_llm_for_json(
                ctx, user_prompt=prompt, path_hints=["consensus"],
                tool_set=("web_search", "read_webpage"),
            )
            consensus_payload = {
                "source": "llm_external",
                "payload": parsed or {},
                "citations": citations,
                "trace": trace,
            }

        # Load modeled EPS + total_revenue cells
        q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.in_(
                [f"eps.{p}" for p in ctx.model.fiscal_periods]
                + [f"total_revenue.{p}" for p in ctx.model.fiscal_periods]
                + [f"ni.{p}" for p in ctx.model.fiscal_periods]
            ),
        )
        our = {c.path: c for c in (await ctx.db.execute(q)).scalars().all()}

        trace_row = await _store.record_provenance(
            ctx.db, ctx.model.id,
            cell_path=None, step_id=ctx.step_id,
            steps=[{"step_type": "consensus_check", "consensus": consensus_payload}],
            raw_evidence=[],
        )

        issues_created = 0
        flagged_paths: list[str] = []

        # Write a consensus.* cell family so the researcher can see it inline
        output_paths: list[str] = []
        if consensus_payload.get("source") == "wind":
            for key, fy in [("fy1", consensus_payload["fy1"]),
                            ("fy2", consensus_payload["fy2"]),
                            ("fy3", consensus_payload["fy3"])]:
                year = fy.get("year")
                if not year:
                    continue
                for metric, val, unit in (
                    ("revenue", fy.get("revenue"), "元"),
                    ("eps",     fy.get("eps"),     "元/股"),
                    ("net_profit", fy.get("net_profit"), "元"),
                    ("pe",      fy.get("pe"),      "倍"),
                ):
                    if val is None:
                        continue
                    path = f"consensus.{metric}.{year}"
                    try:
                        val_f = float(val)
                    except (TypeError, ValueError):
                        continue
                    cell = await _store.upsert_cell(
                        ctx.db, ctx.model.id,
                        path=path,
                        label=f"一致预期 {metric} FY{year}",
                        period=f"FY{year}",
                        unit=unit,
                        value=val_f,
                        value_type="currency" if metric in ("revenue", "net_profit", "eps") else "number",
                        source_type="guidance",
                        confidence="HIGH",
                        confidence_reason=(
                            f"Wind 一致预期 (as_of {consensus_payload.get('as_of','n/a')}, "
                            f"{consensus_payload.get('analyst_count', 0)} 位分析师)"
                        ),
                        citations=[{
                            "index": 1,
                            "title": f"Wind 一致预期 {consensus_payload['windcode']}",
                            "website": "Wind (ASHARECONSENSUSROLLINGDATAHIS)",
                            "source_type": "consensus",
                            "doc_type": "一致预期",
                        }],
                        provenance_trace_id=trace_row.id,
                        extra={"dry_run": ctx.dry_run} if ctx.dry_run else None,
                    )
                    output_paths.append(cell.path)

        # Compare modeled values to consensus and emit SanityIssue for big gaps
        def _add_issue(
            path: str, modeled: float, consensus_val: float, metric_label: str
        ):
            if consensus_val == 0:
                return
            diff = (modeled - consensus_val) / abs(consensus_val)
            if abs(diff) <= diff_threshold:
                return
            ctx.db.add(SanityIssue(
                model_id=ctx.model.id,
                issue_type="consensus_divergence",
                severity="warn" if abs(diff) <= 0.5 else "error",
                cell_paths=[path],
                message=(
                    f"{metric_label} 建模值 {modeled:.4g} 与一致预期 {consensus_val:.4g} "
                    f"差 {diff:+.1%}（阈值 {diff_threshold:.0%}）"
                ),
                suggested_fix=(
                    "若偏差有 alpha 逻辑支撑，请在 confidence_reason 中写清来源；"
                    "否则请核对业务部门拆分 / margin / shares 是否合理。"
                ),
                details={
                    "modeled": modeled,
                    "consensus": consensus_val,
                    "diff_pct": diff,
                    "consensus_source": consensus_payload.get("source"),
                },
            ))
            flagged_paths.append(path)

        if consensus_payload.get("source") == "wind":
            # Compare FY1/FY2/FY3
            wind_map = {
                int(consensus_payload["fy1"]["year"]): consensus_payload["fy1"],
                int(consensus_payload["fy2"]["year"]): consensus_payload["fy2"],
                int(consensus_payload["fy3"]["year"]): consensus_payload["fy3"],
            }
            for period in ctx.model.fiscal_periods:
                yr = _period_to_year(period)
                if yr is None:
                    continue
                consensus_fy = wind_map.get(yr)
                if not consensus_fy:
                    continue
                # eps compare
                our_eps = our.get(f"eps.{period}")
                if our_eps and our_eps.value is not None and consensus_fy.get("eps") is not None:
                    _add_issue(f"eps.{period}", our_eps.value, float(consensus_fy["eps"]),
                               f"EPS {period}")
                our_ni = our.get(f"ni.{period}")
                if our_ni and our_ni.value is not None and consensus_fy.get("net_profit") is not None:
                    _add_issue(f"ni.{period}", our_ni.value, float(consensus_fy["net_profit"]) / 1e8,
                               f"Net income {period}")
                our_rev = our.get(f"total_revenue.{period}")
                if our_rev and our_rev.value is not None and consensus_fy.get("revenue") is not None:
                    _add_issue(f"total_revenue.{period}", our_rev.value,
                               float(consensus_fy["revenue"]) / 1e8, f"Revenue {period}")

        if flagged_paths:
            issues_created = len(flagged_paths)

        await _store.update_model_counts(ctx.db, ctx.model.id)
        await ctx.emit("step_completed", {
            "consensus_source": consensus_payload.get("source"),
            "divergences": issues_created,
            "cells_written": len(output_paths),
        })
        return {
            "output_paths": output_paths,
            "divergences": issues_created,
            "consensus_source": consensus_payload.get("source"),
        }

    @staticmethod
    def _is_a_share(ticker: str, industry: str | None) -> bool:
        if not ticker:
            return False
        plain = ticker.split(".")[0]
        if not plain.isdigit():
            return False
        return plain.startswith(("6", "0", "3", "4", "8", "9"))


def _fy_dict(x) -> dict[str, Any]:
    return {
        "year": getattr(x, "year", None),
        "net_profit": getattr(x, "net_profit", None),
        "eps": getattr(x, "eps", None),
        "pe": getattr(x, "pe", None),
        "revenue": getattr(x, "revenue", None),
    }


def _period_to_year(period: str) -> int | None:
    """Convert FY25E / FY25 / 2025 / 25 into 2025."""
    digits = "".join(c for c in period if c.isdigit())
    if not digits:
        return None
    n = int(digits[-2:]) if len(digits) >= 2 else int(digits)
    if n < 50:
        return 2000 + n
    if n < 100:
        return 1900 + n
    return n
