"""LLM-assisted Industry Pack bootstrap.

Given a list of representative tickers, produce draft pack files:
  * pack.yaml
  * segments_schema.yaml
  * playbook/overview.md
  * playbook/rules.md
  * playbook/lessons.md (empty)
  * playbook/peer_comps.yaml
  * sanity_rules.yaml (starter)
  * recipes/standard_v1.json (reuses the optical_modules DAG topology)

The researcher then refines these in the PackEditor UI (P4.1b).
"""
from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any

import yaml

from backend.app.services.step_executors._llm_helper import call_llm_for_json

logger = logging.getLogger(__name__)


INDUSTRY_PACKS_ROOT = Path(__file__).resolve().parents[3] / "industry_packs"


async def bootstrap_pack(
    slug: str,
    name: str,
    display_name_zh: str,
    tickers: list[str],
    *,
    description: str = "",
    default_periods: list[str] | None = None,
    base_currency: str = "USD",
    overwrite: bool = False,
) -> dict[str, Any]:
    """Create a new industry pack from scratch, LLM-assisted.

    Returns a summary of what was created.
    """
    target = INDUSTRY_PACKS_ROOT / slug
    if target.exists() and not overwrite:
        return {"error": f"Pack {slug} already exists. Use overwrite=True to replace."}
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    (target / "recipes").mkdir(exist_ok=True)
    (target / "playbook").mkdir(exist_ok=True)

    periods = default_periods or ["FY25E", "FY26E", "FY27E"]
    tickers = [t for t in tickers if t]
    if not tickers:
        return {"error": "Need at least 1 ticker to bootstrap"}

    # ── LLM call: extract segments + industry overview ──────
    class _Ctx:
        pack = None
        dry_run = False
        step_config = {}

    prompt = (
        f"Bootstrap an industry pack for '{display_name_zh}' ({name}). "
        f"Representative tickers: {tickers}. "
        f"Use kb_search, alphapai_recall, web_search to pull 10-Ks / annual reports "
        f"for 2-3 of these tickers.\n\n"
        f"Output JSON with fields:\n"
        f"  overview_md: 300-800 字 markdown 行业概述，重点讲 revenue 来源、商业模式、竞争格局、关键 KPI\n"
        f"  rules_md: 200-500 字 markdown 阐述本行业常用的财务习惯 (EBIT vs EBITDA)、季节性、风险点\n"
        f"  segments: [\n"
        f"    {{slug, label_zh, label_en, kind (module|chip|product|service|platform), "
        f"    volume_unit, asp_unit, volume_driver_hint, price_driver_hint, "
        f"    revenue_directly (bool), growth_profile_hint (stable|declining|high_growth|new)}}\n"
        f"  ]\n"
        f"  sanity_rules: {{margin: {{operating_margin: {{range: [lo, hi], severity: warn}}}}, "
        f"    yoy: {{revenue: {{range: [lo, hi], severity: warn}}}}}}\n"
        f"  peer_groups: [{{name, tickers: [..], notes}}]\n\n"
        f"Keep slug snake_case; segments list should be 5-10 items; sanity ranges "
        f"should reflect this industry's historical distribution."
    )
    parsed, _citations, trace = await call_llm_for_json(
        _Ctx(),
        user_prompt=prompt,
        model_id="anthropic/claude-opus-4-7",
        tool_set=("kb_search", "alphapai_recall", "web_search", "read_webpage"),
        max_tool_rounds=5,
    )

    data = parsed or {}
    overview_md = data.get("overview_md") or _default_overview(display_name_zh, tickers)
    rules_md = data.get("rules_md") or _default_rules(display_name_zh)
    segments = data.get("segments") or _default_segments()
    sanity_rules = data.get("sanity_rules") or _default_sanity_rules()
    peer_groups = data.get("peer_groups") or []

    # ── Write pack.yaml ────────────────────────────────────
    pack_meta = {
        "slug": slug,
        "name": name,
        "display_name_zh": display_name_zh,
        "display_name_en": name,
        "description": description or overview_md[:200],
        "ticker_patterns": tickers,
        "default_periods": periods,
        "units": {"currency": base_currency, "revenue": "亿元" if base_currency == "CNY" else "亿美元"},
        "margin_basis": "operating_margin",
        "tax_rate_default": 0.25 if base_currency == "CNY" else 0.15,
        "tags": [slug],
    }
    (target / "pack.yaml").write_text(yaml.safe_dump(pack_meta, allow_unicode=True, sort_keys=False), encoding="utf-8")

    # ── segments_schema.yaml ──────────────────────────────
    seg_schema = {"segments": segments, "margin_rows": _default_margin_rows()}
    (target / "segments_schema.yaml").write_text(
        yaml.safe_dump(seg_schema, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    # ── sanity_rules.yaml ─────────────────────────────────
    (target / "sanity_rules.yaml").write_text(
        yaml.safe_dump(sanity_rules, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    # ── playbook ──────────────────────────────────────────
    (target / "playbook" / "overview.md").write_text(overview_md.strip() + "\n", encoding="utf-8")
    (target / "playbook" / "rules.md").write_text(rules_md.strip() + "\n", encoding="utf-8")
    (target / "playbook" / "lessons.md").write_text(
        f"# Lessons for {display_name_zh}\n\n"
        f"This file collects researcher corrections over time. "
        f"Seeded empty — lessons will be appended by the weekly feedback consolidator.\n",
        encoding="utf-8",
    )
    (target / "playbook" / "peer_comps.yaml").write_text(
        yaml.safe_dump({"peer_groups": peer_groups}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    # ── recipes/standard_v1.json (reuse optical_modules DAG) ──
    default_recipe = {
        "name": f"{display_name_zh} 收入拆分 (标准版)",
        "slug": f"{slug}_standard_v1",
        "industry": slug,
        "version": 1,
        "description": f"Full standard DAG for {display_name_zh} — extracted from optical_modules template.",
        "nodes": _default_recipe_nodes(),
        "edges": _default_recipe_edges(),
    }
    (target / "recipes" / "standard_v1.json").write_text(
        json.dumps(default_recipe, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Invalidate registry cache
    from industry_packs import pack_registry
    pack_registry.reload()

    return {
        "slug": slug,
        "path": str(target),
        "segments_count": len(segments),
        "files_written": [
            "pack.yaml", "segments_schema.yaml", "sanity_rules.yaml",
            "playbook/overview.md", "playbook/rules.md", "playbook/lessons.md",
            "playbook/peer_comps.yaml", "recipes/standard_v1.json",
        ],
        "trace_tokens": sum(t.get("tokens", 0) for t in trace),
    }


def _default_overview(name: str, tickers: list[str]) -> str:
    return f"# {name} 行业概述\n\n代表公司: {', '.join(tickers)}。\n\n(LLM bootstrap 失败 — 请手动编辑此文件。)"


def _default_rules(name: str) -> str:
    return f"# {name} 财务建模规则\n\n- margin_basis: operating_margin\n- 税率默认 15%\n- 每季度更新一次 segment revenue"


def _default_segments() -> list[dict[str, Any]]:
    return [
        {"slug": "product_a", "label_zh": "产品 A", "label_en": "Product A",
         "kind": "product", "volume_unit": "万件", "asp_unit": "元",
         "revenue_directly": True, "growth_profile_hint": "stable"},
        {"slug": "product_b", "label_zh": "产品 B", "label_en": "Product B",
         "kind": "product", "volume_unit": "万件", "asp_unit": "元",
         "revenue_directly": True, "growth_profile_hint": "high_growth"},
    ]


def _default_sanity_rules() -> dict[str, Any]:
    return {
        "margin": {
            "operating_margin": {"range": [-0.10, 0.50], "severity": "warn"},
            "net_margin": {"range": [-0.15, 0.40], "severity": "warn"},
            "gross_margin": {"range": [0.05, 0.70], "severity": "warn"},
        },
        "yoy": {
            "revenue": {"range": [-0.40, 3.00], "severity": "warn"},
        },
        "ratios": {
            "pe": {"range": [2.0, 250.0], "severity": "warn"},
        },
        "structural": {"segment_sum_tolerance": 0.005},
    }


def _default_margin_rows() -> list[dict[str, Any]]:
    return [
        {"slug": "total_revenue", "label": "Total revenue",
         "derivation": "sum of segment revenues", "unit": "亿"},
        {"slug": "operating_margin", "label": "Operating margin",
         "unit": "%", "value_type": "percent", "typical_range": [0.08, 0.40]},
        {"slug": "ebit", "label": "EBIT",
         "formula_template": "=total_revenue * operating_margin", "unit": "亿"},
        {"slug": "ni", "label": "Net income",
         "formula_template": "=ebit * (1 - tax_rate)", "unit": "亿"},
        {"slug": "shares", "label": "Shares outstanding", "unit": "亿股"},
        {"slug": "eps", "label": "EPS",
         "formula_template": "=ni / shares", "unit": "元/股"},
        {"slug": "pe", "label": "PE",
         "formula_template": "=price / eps", "unit": "倍"},
    ]


def _default_recipe_nodes() -> list[dict[str, Any]]:
    return [
        {"id": "step_1", "type": "GATHER_CONTEXT", "label": "读纪要/基本面",
         "config": {"lookback_days": 180,
                    "tools": ["kb_search", "alphapai_recall", "jinmen_search"]}},
        {"id": "step_2", "type": "DECOMPOSE_SEGMENTS", "label": "拆业务部门",
         "config": {"tools": ["kb_search", "alphapai_recall"]}},
        {"id": "step_3", "type": "CLASSIFY_GROWTH_PROFILE", "label": "分类增长曲线",
         "config": {"tools": ["kb_search", "alphapai_recall"]}},
        {"id": "step_4", "type": "EXTRACT_HISTORICAL", "label": "抽历史收入",
         "config": {"years_back": 3, "tools": ["kb_search", "alphapai_recall"]}},
        {"id": "step_4b", "type": "CLASSIFY_PEERS", "label": "对标同行",
         "config": {"tools": ["kb_search", "alphapai_recall", "web_search"]}},
        {"id": "step_5a", "type": "MODEL_VOLUME_PRICE", "label": "量×价建模",
         "config": {"applies_to_profiles": ["high_growth", "new"],
                    "tools": ["alphapai_recall", "jinmen_search", "kb_search", "web_search"]}},
        {"id": "step_5b", "type": "APPLY_GUIDANCE", "label": "套管理层指引",
         "config": {"applies_to_profiles": ["stable", "declining"],
                    "default_growth_rate": 0.03}},
        {"id": "step_5c", "type": "GROWTH_DECOMPOSITION", "label": "量价拆解",
         "config": {"tools": ["alphapai_recall", "jinmen_search", "kb_search"]}},
        {"id": "step_6", "type": "MARGIN_CASCADE", "label": "Margin 级联",
         "config": {"strategy": "guidance_first",
                    "tools": ["kb_search", "alphapai_recall", "consensus_forecast"]}},
        {"id": "step_7a", "type": "MULTI_PATH_CHECK", "label": "多路径交叉",
         "config": {"spread_threshold": 2.0, "tools": ["kb_search", "alphapai_recall", "web_search"]}},
        {"id": "step_7b", "type": "CONSENSUS_CHECK", "label": "一致预期核对",
         "config": {"diff_threshold_pct": 0.25}},
        {"id": "step_8", "type": "VERIFY_AND_ASK", "label": "CoVe + Debate",
         "config": {"debate_policy": [
             "confidence == 'LOW' AND source_type == 'inferred'",
             "abs(yoy) > 0.30",
             "random_sample(0.05)",
         ], "diff_threshold_pct": 0.10}},
    ]


def _default_recipe_edges() -> list[dict[str, str]]:
    return [
        {"from": "step_1", "to": "step_2"},
        {"from": "step_2", "to": "step_3"},
        {"from": "step_3", "to": "step_4"},
        {"from": "step_4", "to": "step_4b"},
        {"from": "step_4b", "to": "step_5a"},
        {"from": "step_4b", "to": "step_5b"},
        {"from": "step_5a", "to": "step_5c"},
        {"from": "step_5b", "to": "step_5c"},
        {"from": "step_5c", "to": "step_6"},
        {"from": "step_6", "to": "step_7a"},
        {"from": "step_7a", "to": "step_7b"},
        {"from": "step_7b", "to": "step_8"},
    ]
