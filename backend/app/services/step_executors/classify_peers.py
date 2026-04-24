"""Step — CLASSIFY_PEERS.

For margin modeling, the industry-pack ``ticker_patterns`` list becomes
the "peer universe". This step asks the LLM to pull each peer's latest
reported operating margin / gross margin / net margin and compute the
peer median + P10/P90 band. The margin_cascade step then uses the band
as a prior — if our forecast is outside the band, we need to justify.

Writes cells under ``peer.<ticker>.*`` so the researcher can see peers
inline and ``peer.median.operating_margin`` as a reference line.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from backend.app.services import model_cell_store as _store
from ._llm_helper import call_llm_for_json, format_template
from .base import BaseStepExecutor, StepContext

logger = logging.getLogger(__name__)


class ClassifyPeersStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}
        peers: list[str] = list(cfg.get("peers") or [])
        if not peers and ctx.pack:
            peers = [
                p for p in (ctx.pack.meta or {}).get("ticker_patterns") or []
                if p != ctx.model.ticker
            ][:5]
        if not peers:
            await ctx.emit("step_completed", {"peers": 0, "reason": "no peers in pack"})
            return {"output_paths": [], "peers": 0}

        prompt = format_template(
            cfg.get(
                "prompt_template",
                "For each of these peer companies {peers}, fetch the most recent full-year "
                "operating margin, gross margin, net margin (use kb_search / web_search / "
                "alphapai_recall). Output JSON: "
                "{{\"peers\":[{{\"ticker\":...,\"operating_margin\":number,\"gross_margin\":number,"
                "\"net_margin\":number,\"as_of\":\"YYYY-Q\",\"source\":string}}],"
                "\"median_operating_margin\":number,\"median_gross_margin\":number,"
                "\"median_net_margin\":number,\"p10_operating_margin\":number,"
                "\"p90_operating_margin\":number,\"justification\":string}}.",
            ),
            {"peers": peers},
        )
        await ctx.emit("step_progress", {"label": "classifying peer margins"})
        parsed, citations, trace = await call_llm_for_json(
            ctx, user_prompt=prompt, path_hints=["peer"],
            tool_set=tuple(cfg.get("tools") or ("kb_search", "alphapai_recall", "web_search")),
        )
        dry = ctx.dry_run or any(t.get("dry_run") for t in trace)
        trace_row = await _store.record_provenance(
            ctx.db, ctx.model.id,
            cell_path="peer", step_id=ctx.step_id,
            steps=trace, raw_evidence=citations,
        )

        peer_rows = (parsed or {}).get("peers") or []
        output_paths: list[str] = []
        margins_op: list[float] = []
        margins_gr: list[float] = []
        margins_nm: list[float] = []
        for pr in peer_rows:
            ticker = pr.get("ticker")
            if not ticker:
                continue
            for metric, unit, collect in (
                ("operating_margin", "%", margins_op),
                ("gross_margin", "%", margins_gr),
                ("net_margin", "%", margins_nm),
            ):
                try:
                    v = float(pr.get(metric))
                except (TypeError, ValueError):
                    continue
                collect.append(v)
                path = f"peer.{_slug(ticker)}.{metric}"
                c = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=path,
                    label=f"{ticker} {metric}",
                    value=v,
                    value_type="percent",
                    unit=unit,
                    source_type="inferred",
                    confidence="MEDIUM" if citations else "LOW",
                    confidence_reason=f"from {pr.get('source') or 'peer research'}",
                    citations=citations[:3],
                    provenance_trace_id=trace_row.id,
                    extra={"as_of": pr.get("as_of"), **({"dry_run": True} if dry else {})},
                )
                output_paths.append(c.path)

        # Write median / p10 / p90 reference cells
        for metric, vs in (
            ("operating_margin", margins_op),
            ("gross_margin", margins_gr),
            ("net_margin", margins_nm),
        ):
            if not vs:
                continue
            med = statistics.median(vs)
            s = sorted(vs)
            p10 = s[max(0, int(len(s) * 0.1) - 1)]
            p90 = s[min(len(s) - 1, int(len(s) * 0.9))]
            for name, val in (("median", med), ("p10", p10), ("p90", p90)):
                path = f"peer.{name}.{metric}"
                c = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=path,
                    label=f"Peer {name} {metric}",
                    value=float(val),
                    value_type="percent",
                    unit="%",
                    source_type="derived",
                    confidence="MEDIUM",
                    provenance_trace_id=trace_row.id,
                    extra={"sample_size": len(vs), **({"dry_run": True} if dry else {})},
                )
                output_paths.append(c.path)

        await ctx.emit("step_completed", {
            "peers_found": len(peer_rows),
            "cells_written": len(output_paths),
        })
        return {"output_paths": output_paths, "peers_found": len(peer_rows)}


def _slug(ticker: str) -> str:
    return ticker.replace(".", "_").replace("-", "_")
