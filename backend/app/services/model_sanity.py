"""Numerical sanity checks — the non-LLM layer of the anti-hallucination stack.

Checks a ``RevenueModel``'s cells against the active ``IndustryPack``'s
``sanity_rules.yaml`` plus a set of universal rules (segment-sum, cycle,
error propagation).

Supports both the legacy shape (rules.margin, rules.yoy, rules.ratios,
rules.structural, rules.absolute) and a new declarative "rules" list
that a pack can ship for custom checks. Each declarative rule has shape:

    - id: unique_id
      severity: info|warn|error
      check: sum_equals | yoy_range | range | ratio | monotonic | absolute_asp
      paths: ["pattern.*.*"]   (optional for some checks)
      params: {...}

Return contract: list of dicts ready to become ``SanityIssue`` rows::

    {
        "issue_type": "sum_mismatch" | "yoy_out_of_range" | ... ,
        "severity": "info" | "warn" | "error",
        "cell_paths": [...],
        "message": "...",
        "suggested_fix": "...",
        "details": {...},
    }
"""
from __future__ import annotations

import fnmatch
import logging
from collections import defaultdict
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.revenue_model import ModelCell, RevenueModel
from backend.app.services.formula_engine import FormulaEngine, parse_dependencies

logger = logging.getLogger(__name__)


async def check_model(
    db: AsyncSession, model: RevenueModel, pack
) -> list[dict[str, Any]]:
    q = select(ModelCell).where(ModelCell.model_id == model.id)
    cells = list((await db.execute(q)).scalars().all())
    issues: list[dict[str, Any]] = []
    rules = (pack.sanity_rules if pack else {}) or {}

    # Legacy-shaped built-ins
    issues.extend(_check_cycles(cells))
    issues.extend(_check_segment_sum(cells, tol=_get_nested(rules, "structural.segment_sum_tolerance", 0.005)))
    issues.extend(_check_margin_ranges(cells, rules.get("margin") or {}))
    issues.extend(_check_yoy_ranges(cells, rules.get("yoy") or {}))
    issues.extend(_check_unknown_dependencies(cells))
    issues.extend(_check_ratios(cells, rules.get("ratios") or {}))
    issues.extend(_check_absolute_asp(cells, rules.get("absolute") or {}))
    issues.extend(_check_no_citations(cells))

    # Declarative rules
    declared = rules.get("rules") or []
    if isinstance(declared, list):
        for rule in declared:
            if not isinstance(rule, dict):
                continue
            try:
                issues.extend(_run_declarative_rule(cells, rule))
            except Exception:
                logger.exception("Declarative sanity rule failed: %s", rule.get("id"))

    return issues


def _get_nested(d: dict[str, Any], path: str, default: Any) -> Any:
    cur: Any = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _check_cycles(cells: list[ModelCell]) -> list[dict[str, Any]]:
    engine = FormulaEngine()
    for c in cells:
        if c.formula:
            engine.set_cell(c.path, formula=c.formula)
        else:
            engine.set_cell(c.path, value=c.value if c.value is not None else c.value_text)
    cycle = engine.find_cycle()
    if cycle:
        return [{
            "issue_type": "cycle",
            "severity": "error",
            "cell_paths": cycle,
            "message": "Formula dependencies form a cycle: " + " -> ".join(cycle),
            "suggested_fix": "Break the cycle by converting one of these cells to a constant.",
            "details": {"cycle": cycle},
        }]
    return []


def _check_segment_sum(
    cells: list[ModelCell], *, tol: float
) -> list[dict[str, Any]]:
    by_period_seg: dict[str, list[tuple[str, ModelCell]]] = defaultdict(list)
    total_by_period: dict[str, ModelCell] = {}
    for c in cells:
        parts = c.path.split(".")
        if parts[0] == "segment" and len(parts) == 4 and parts[2] == "rev":
            period = parts[3]
            by_period_seg[period].append((parts[1], c))
        elif parts[0] == "total_revenue" and len(parts) == 2:
            total_by_period[parts[1]] = c

    issues: list[dict[str, Any]] = []
    for period, total_cell in total_by_period.items():
        if total_cell.value is None:
            continue
        segs = by_period_seg.get(period, [])
        seg_sum = sum(c.value for _, c in segs if c.value is not None)
        if seg_sum == 0:
            continue
        diff = abs(total_cell.value - seg_sum) / max(abs(total_cell.value), 1e-9)
        if diff > tol:
            issues.append({
                "issue_type": "sum_mismatch",
                "severity": "warn",
                "cell_paths": [total_cell.path] + [c.path for _, c in segs],
                "message": (
                    f"total_revenue.{period} ({total_cell.value:.4g}) differs from "
                    f"sum of segments ({seg_sum:.4g}) by {diff:.2%}"
                ),
                "suggested_fix": (
                    "Check that total_revenue's formula sums all segment.*.rev "
                    "paths for this period."
                ),
                "details": {
                    "total": total_cell.value,
                    "sum_of_segments": seg_sum,
                    "tolerance": tol,
                },
            })
    return issues


def _check_margin_ranges(
    cells: list[ModelCell], rules: dict[str, Any]
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for c in cells:
        if c.value is None:
            continue
        parts = c.path.split(".")
        if parts[0] not in ("operating_margin", "net_margin", "gross_margin"):
            continue
        rule_key = parts[0]
        rule = rules.get(rule_key)
        if not rule:
            continue
        rng = rule.get("range") or [None, None]
        lo, hi = (rng + [None, None])[:2]
        if (lo is not None and c.value < lo) or (hi is not None and c.value > hi):
            issues.append({
                "issue_type": "margin_out_of_range",
                "severity": rule.get("severity", "warn"),
                "cell_paths": [c.path],
                "message": f"{c.path} value {c.value} outside range [{lo}, {hi}]",
                "suggested_fix": "Validate against latest management guidance or peer comps.",
                "details": {"value": c.value, "range": [lo, hi]},
            })
    return issues


def _check_yoy_ranges(
    cells: list[ModelCell], rules: dict[str, Any]
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    rev_by_seg: dict[str, list[ModelCell]] = defaultdict(list)
    for c in cells:
        if c.value is None:
            continue
        parts = c.path.split(".")
        if parts[0] == "segment" and len(parts) == 4 and parts[2] == "rev":
            rev_by_seg[parts[1]].append(c)

    rev_rule = rules.get("revenue") or {}
    rng = rev_rule.get("range") or [None, None]
    lo, hi = (rng + [None, None])[:2]
    for seg, xs in rev_by_seg.items():
        xs.sort(key=lambda x: x.period or x.path)
        for i in range(1, len(xs)):
            prev, curr = xs[i - 1], xs[i]
            if prev.value == 0:
                continue
            yoy = curr.value / prev.value - 1
            if (lo is not None and yoy < lo) or (hi is not None and yoy > hi):
                issues.append({
                    "issue_type": "yoy_out_of_range",
                    "severity": rev_rule.get("severity", "warn"),
                    "cell_paths": [prev.path, curr.path],
                    "message": (
                        f"{curr.path} YoY {yoy:.2%} outside "
                        f"[{lo:.0%}, {hi:.0%}] — {prev.value} -> {curr.value}"
                    ),
                    "suggested_fix": (
                        "Confirm growth profile classification (stable vs high_growth) "
                        "and whether this is a one-time event (e.g. new customer win)."
                    ),
                    "details": {"prev": prev.value, "curr": curr.value, "yoy": yoy},
                })
    return issues


def _check_unknown_dependencies(cells: list[ModelCell]) -> list[dict[str, Any]]:
    known = {c.path for c in cells}
    issues: list[dict[str, Any]] = []
    for c in cells:
        if not c.formula:
            continue
        try:
            deps = parse_dependencies(c.formula)
        except Exception:
            continue
        missing = [d for d in deps if d not in known]
        if missing:
            issues.append({
                "issue_type": "unknown_dep",
                "severity": "error",
                "cell_paths": [c.path],
                "message": (
                    f"Formula references unknown cells: {missing[:5]}"
                    + ("..." if len(missing) > 5 else "")
                ),
                "suggested_fix": "Add those cells or fix the formula reference.",
                "details": {"missing_deps": missing},
            })
    return issues


def _check_ratios(cells: list[ModelCell], rules: dict[str, Any]) -> list[dict[str, Any]]:
    """Check pe / price_to_sales etc. within configured ranges."""
    issues: list[dict[str, Any]] = []
    for c in cells:
        if c.value is None:
            continue
        parts = c.path.split(".")
        rule = rules.get(parts[0])
        if not rule:
            continue
        rng = rule.get("range") or [None, None]
        lo, hi = (rng + [None, None])[:2]
        if (lo is not None and c.value < lo) or (hi is not None and c.value > hi):
            issues.append({
                "issue_type": f"{parts[0]}_out_of_range",
                "severity": rule.get("severity", "info"),
                "cell_paths": [c.path],
                "message": f"{c.path}={c.value} outside [{lo}, {hi}]",
                "suggested_fix": "Double-check inputs upstream (ni, shares, market.price).",
                "details": {"value": c.value, "range": [lo, hi]},
            })
    return issues


def _check_absolute_asp(
    cells: list[ModelCell], rules: dict[str, Any]
) -> list[dict[str, Any]]:
    """rules['module_asp_usd']['800g'] = [lo, hi]."""
    issues: list[dict[str, Any]] = []
    module_rules = rules.get("module_asp_usd") or {}
    chip_rules = rules.get("chip_asp_usd") or {}
    for c in cells:
        if c.value is None:
            continue
        parts = c.path.split(".")
        # segment.module_800g.asp.FY26E
        if parts[0] != "segment" or len(parts) < 4 or parts[2] != "asp":
            continue
        seg = parts[1]
        rule_key = None
        if seg.startswith("module_"):
            rule_key = seg.replace("module_", "")
            rng = module_rules.get(rule_key)
        elif seg.startswith("chip_") or seg.startswith("eml_") or seg == "cw_laser":
            rng = chip_rules.get(seg.replace("chip_", "")) or chip_rules.get(seg)
        else:
            continue
        if not rng or not isinstance(rng, list) or len(rng) < 2:
            continue
        lo, hi = rng[0], rng[1]
        if (lo is not None and c.value < lo) or (hi is not None and c.value > hi):
            issues.append({
                "issue_type": "asp_out_of_absolute_range",
                "severity": "warn",
                "cell_paths": [c.path],
                "message": f"{c.path} ASP {c.value} outside industry absolute band [{lo}, {hi}]",
                "suggested_fix": "Sanity-check with expert/industry report; price may be 单位 mismatch.",
                "details": {"value": c.value, "range": [lo, hi]},
            })
    return issues


def _check_no_citations(cells: list[ModelCell]) -> list[dict[str, Any]]:
    """Numeric cells with source_type != derived/assumption but no citations are suspect."""
    issues: list[dict[str, Any]] = []
    for c in cells:
        if c.value is None:
            continue
        if c.source_type in ("derived", "assumption"):
            continue
        if c.formula:
            continue
        if c.citations:
            continue
        # Exclude text-typed like growth_profile, meta, etc.
        if c.value_type == "text":
            continue
        issues.append({
            "issue_type": "no_citation",
            "severity": "warn",
            "cell_paths": [c.path],
            "message": (
                f"{c.path} is source_type='{c.source_type}' but has no citation. "
                f"This value may have been invented by the LLM."
            ),
            "suggested_fix": (
                "Trigger a re-run with real tools (kb_search/alphapai), "
                "or manually add a citation."
            ),
            "details": {"value": c.value, "source_type": c.source_type},
        })
    return issues


# ── Declarative rules engine ─────────────────────────────────

def _run_declarative_rule(
    cells: list[ModelCell], rule: dict[str, Any]
) -> list[dict[str, Any]]:
    check = (rule.get("check") or "").lower()
    severity = rule.get("severity", "warn")
    rule_id = rule.get("id", "unnamed")
    issues: list[dict[str, Any]] = []

    if check == "sum_equals":
        # params.tol pct, paths: [pattern_for_parts, pattern_for_total]
        patterns = rule.get("paths") or []
        if len(patterns) < 2:
            return []
        parts_pat, total_pat = patterns[0], patterns[1]
        tol = float((rule.get("params") or {}).get("tolerance_pct") or 0.01)
        # Group by period suffix
        matched_parts = [c for c in cells if _match_path(c.path, parts_pat) and c.value is not None]
        matched_total = [c for c in cells if _match_path(c.path, total_pat) and c.value is not None]
        group: dict[str, list[ModelCell]] = defaultdict(list)
        for c in matched_parts:
            group[c.period or c.path.split(".")[-1]].append(c)
        totals_by_period = {c.period or c.path.split(".")[-1]: c for c in matched_total}
        for p, total in totals_by_period.items():
            s = sum(c.value for c in group.get(p, []))
            if s == 0 or total.value is None:
                continue
            diff = abs(total.value - s) / max(abs(total.value), 1e-9)
            if diff > tol:
                issues.append({
                    "issue_type": "sum_mismatch",
                    "severity": severity,
                    "cell_paths": [total.path] + [c.path for c in group.get(p, [])],
                    "message": f"[{rule_id}] sum_equals mismatch @ {p}: total={total.value} vs sum={s} ({diff:.2%})",
                    "suggested_fix": "Check formula or values.",
                    "details": {"tolerance_pct": tol, "total": total.value, "sum": s},
                })

    elif check == "range":
        pat = (rule.get("paths") or [None])[0]
        if not pat:
            return []
        params = rule.get("params") or {}
        bounds = params.get("bounds") or [None, None]
        lo, hi = bounds[0], bounds[1]
        for c in cells:
            if c.value is None:
                continue
            if not _match_path(c.path, pat):
                continue
            if (lo is not None and c.value < lo) or (hi is not None and c.value > hi):
                issues.append({
                    "issue_type": "range_violation",
                    "severity": severity,
                    "cell_paths": [c.path],
                    "message": f"[{rule_id}] {c.path}={c.value} outside [{lo}, {hi}]",
                    "suggested_fix": "Validate against source.",
                    "details": {"value": c.value, "bounds": [lo, hi]},
                })

    elif check == "yoy_range":
        pat = (rule.get("paths") or [None])[0]
        if not pat:
            return []
        params = rule.get("params") or {}
        bounds = params.get("bounds") or [None, None]
        lo, hi = bounds[0], bounds[1]
        # Group by cell-prefix excluding final period
        groups: dict[str, list[ModelCell]] = defaultdict(list)
        for c in cells:
            if c.value is None:
                continue
            if not _match_path(c.path, pat):
                continue
            prefix = ".".join(c.path.split(".")[:-1])
            groups[prefix].append(c)
        for prefix, xs in groups.items():
            xs.sort(key=lambda x: x.period or x.path)
            for i in range(1, len(xs)):
                prev, curr = xs[i - 1], xs[i]
                if prev.value == 0:
                    continue
                yoy = curr.value / prev.value - 1
                if (lo is not None and yoy < lo) or (hi is not None and yoy > hi):
                    issues.append({
                        "issue_type": "yoy_range_violation",
                        "severity": severity,
                        "cell_paths": [prev.path, curr.path],
                        "message": (
                            f"[{rule_id}] YoY {yoy:.2%} outside [{lo:.0%},{hi:.0%}] "
                            f"@ {curr.path} ({prev.value} -> {curr.value})"
                        ),
                        "suggested_fix": "Cross-check with guidance / consensus.",
                        "details": {"yoy": yoy, "bounds": [lo, hi]},
                    })

    elif check == "ratio":
        # params.numer_pattern, denom_pattern, bounds
        params = rule.get("params") or {}
        num_pat = params.get("numer_pattern")
        den_pat = params.get("denom_pattern")
        bounds = params.get("bounds") or [None, None]
        lo, hi = bounds[0], bounds[1]
        if not num_pat or not den_pat:
            return []
        # Join by period
        by_period_num: dict[str, ModelCell] = {}
        by_period_den: dict[str, ModelCell] = {}
        for c in cells:
            if c.value is None:
                continue
            per = c.period or c.path.split(".")[-1]
            if _match_path(c.path, num_pat):
                by_period_num[per] = c
            if _match_path(c.path, den_pat):
                by_period_den[per] = c
        for per, n in by_period_num.items():
            d = by_period_den.get(per)
            if not d or d.value in (0, None):
                continue
            r = n.value / d.value
            if (lo is not None and r < lo) or (hi is not None and r > hi):
                issues.append({
                    "issue_type": "ratio_violation",
                    "severity": severity,
                    "cell_paths": [n.path, d.path],
                    "message": (
                        f"[{rule_id}] ratio {n.path}/{d.path}={r:.4g} "
                        f"outside [{lo}, {hi}]"
                    ),
                    "suggested_fix": "Check the underlying values.",
                    "details": {"ratio": r, "bounds": [lo, hi]},
                })

    elif check == "monotonic":
        pat = (rule.get("paths") or [None])[0]
        direction = ((rule.get("params") or {}).get("direction") or "increasing").lower()
        if not pat:
            return []
        groups: dict[str, list[ModelCell]] = defaultdict(list)
        for c in cells:
            if c.value is None:
                continue
            if not _match_path(c.path, pat):
                continue
            prefix = ".".join(c.path.split(".")[:-1])
            groups[prefix].append(c)
        for prefix, xs in groups.items():
            xs.sort(key=lambda x: x.period or x.path)
            vals = [x.value for x in xs]
            bad = False
            for a, b in zip(vals, vals[1:]):
                if direction == "increasing" and b < a:
                    bad = True
                if direction == "decreasing" and b > a:
                    bad = True
            if bad:
                issues.append({
                    "issue_type": "monotonic_violation",
                    "severity": severity,
                    "cell_paths": [c.path for c in xs],
                    "message": f"[{rule_id}] {prefix}.* is not {direction}: {vals}",
                    "suggested_fix": "Consider whether this is an intended bear case.",
                    "details": {"values": vals, "direction": direction},
                })

    return issues


def _match_path(path: str, pattern: str) -> bool:
    """Glob-style path matching using fnmatch (case-sensitive)."""
    return fnmatch.fnmatchcase(path, pattern)
