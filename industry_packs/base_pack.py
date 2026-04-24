"""Industry Pack loader and registry.

A pack directory layout::

    industry_packs/<slug>/
        pack.yaml                    # metadata
        segments_schema.yaml         # business-unit skeleton
        sanity_rules.yaml            # numerical sanity bounds
        recipes/*.json               # seed Recipe graphs
        playbook/
            overview.md
            lessons.md
            rules.md
            peer_comps.yaml
        formulas_extra.py            # custom engine functions (optional)

``pack.yaml`` schema::

    slug: optical_modules
    name: 光通信
    display_name_zh: 光通信
    ticker_patterns: ["LITE.US", "INLT.US", ...]
    default_periods: ["FY25E", "FY26E", "FY27E"]
    units:
        currency: USD
        count_default: "万颗"
"""
from __future__ import annotations

import importlib.util
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass
class IndustryPack:
    slug: str
    name: str
    root: Path
    meta: dict[str, Any] = field(default_factory=dict)
    segments_schema: dict[str, Any] = field(default_factory=dict)
    sanity_rules: dict[str, Any] = field(default_factory=dict)
    peer_comps: dict[str, Any] = field(default_factory=dict)
    playbook: dict[str, str] = field(default_factory=dict)
    recipes: dict[str, dict[str, Any]] = field(default_factory=dict)
    extra_functions: dict[str, Any] = field(default_factory=dict)

    # ── accessors ────────────────────────────────────────────

    def overview_md(self) -> str:
        return self.playbook.get("overview.md", "")

    def lessons_md(self) -> str:
        return self.playbook.get("lessons.md", "")

    def rules_md(self) -> str:
        return self.playbook.get("rules.md", "")

    def get_recipe(self, slug: str) -> dict[str, Any] | None:
        return self.recipes.get(slug)

    def list_recipes(self) -> list[str]:
        return list(self.recipes.keys())

    def playbook_snippets(self, cell_path: str, max_chars: int = 1200) -> str:
        """Crude path-prefix recall: return lesson paragraphs that mention
        any segment of ``cell_path``. Good enough until the Milvus
        pipeline for lessons is in place.
        """
        text = self.lessons_md() + "\n\n" + self.rules_md()
        if not text.strip():
            return ""
        parts = [p for p in cell_path.split(".") if p]
        # Split into blocks by blank lines; keep blocks that mention a path part
        blocks = [b for b in text.split("\n\n") if b.strip()]
        hits: list[str] = []
        total = 0
        for b in blocks:
            b_lower = b.lower()
            if any(p.lower() in b_lower for p in parts):
                if total + len(b) > max_chars:
                    break
                hits.append(b)
                total += len(b)
        return "\n\n".join(hits)


class PackRegistry:
    """Runtime registry of available industry packs (hot-reloadable)."""

    def __init__(self, root: Path | None = None) -> None:
        # Allow override via env / DI; default to ``industry_packs/``
        self.root = root or Path(__file__).parent
        self._packs: dict[str, IndustryPack] = {}

    def reload(self) -> None:
        self._packs.clear()
        for child in sorted(self.root.iterdir()):
            if not child.is_dir() or child.name.startswith("_") or child.name.startswith("."):
                continue
            if not (child / "pack.yaml").exists():
                continue
            try:
                pack = self._load_pack(child)
                self._packs[pack.slug] = pack
                logger.info(
                    "Industry pack loaded: %s (%s, %d recipes, %d segments)",
                    pack.slug, pack.name,
                    len(pack.recipes),
                    len(pack.segments_schema or {}),
                )
            except Exception:
                logger.exception("Failed to load industry pack at %s", child)

    def get(self, slug: str) -> IndustryPack | None:
        if not self._packs:
            self.reload()
        return self._packs.get(slug)

    def list(self) -> list[IndustryPack]:
        if not self._packs:
            self.reload()
        return list(self._packs.values())

    def _load_pack(self, path: Path) -> IndustryPack:
        meta = yaml.safe_load((path / "pack.yaml").read_text(encoding="utf-8")) or {}
        slug = meta.get("slug") or path.name
        name = meta.get("name") or slug
        pack = IndustryPack(slug=slug, name=name, root=path, meta=meta)

        # segments schema
        segs = path / "segments_schema.yaml"
        if segs.exists():
            pack.segments_schema = yaml.safe_load(segs.read_text(encoding="utf-8")) or {}

        # sanity rules
        sr = path / "sanity_rules.yaml"
        if sr.exists():
            pack.sanity_rules = yaml.safe_load(sr.read_text(encoding="utf-8")) or {}

        # peer comps
        pc = path / "playbook" / "peer_comps.yaml"
        if pc.exists():
            pack.peer_comps = yaml.safe_load(pc.read_text(encoding="utf-8")) or {}

        # playbook markdown
        playbook_dir = path / "playbook"
        if playbook_dir.exists():
            for md in playbook_dir.glob("*.md"):
                pack.playbook[md.name] = md.read_text(encoding="utf-8")

        # recipes (JSON)
        recipes_dir = path / "recipes"
        if recipes_dir.exists():
            for r in recipes_dir.glob("*.json"):
                try:
                    pack.recipes[r.stem] = json.loads(r.read_text(encoding="utf-8"))
                except Exception:
                    logger.exception("Bad recipe JSON: %s", r)

        # optional formulas_extra.py
        fx = path / "formulas_extra.py"
        if fx.exists():
            try:
                spec = importlib.util.spec_from_file_location(
                    f"industry_packs._{slug}_extras", fx
                )
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    for k in dir(mod):
                        if k.startswith("_"):
                            continue
                        v = getattr(mod, k)
                        if callable(v):
                            pack.extra_functions[k.upper()] = v
            except Exception:
                logger.exception("Failed to load %s", fx)

        return pack


# Module-level singleton the rest of the codebase imports
pack_registry = PackRegistry()
