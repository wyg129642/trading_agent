"""Debate-trigger policy DSL.

A recipe config (``VERIFY_AND_ASK.config.debate_policy``) is a list of
rule strings. A cell triggers debate if ANY rule evaluates True.

Supported condition syntax (parsed with Python's ``ast`` in safe mode):

  * confidence == 'LOW'
  * source_type == 'inferred'
  * yoy > 0.30
  * abs(yoy) > 0.30
  * path matches 'segment.*.rev.*'   (custom matches operator → fnmatch)
  * random_sample(0.05)              (special: 5% sampling)
  * review_status == 'flagged'
  * alt_count > 1

Compound: ``AND`` / ``OR`` / ``NOT`` / parentheses.
"""
from __future__ import annotations

import ast
import fnmatch
import logging
import random
import re
from typing import Any

logger = logging.getLogger(__name__)


_ALLOWED_NAMES = {
    "confidence", "source_type", "path", "review_status",
    "yoy", "value", "alt_count", "is_flagged", "cell",
    "True", "False", "None",
}

_ALLOWED_FNS = {
    "abs": abs, "len": len, "min": min, "max": max,
}


def evaluate_policy(
    rules: list[str],
    *,
    cell: Any,
    yoy: float | None = None,
    sample_seed: str | None = None,
) -> tuple[bool, str]:
    """Return (should_debate, reason).

    ``cell`` is a ModelCell SQLA row; ``yoy`` is the period-over-period
    change (optional); ``sample_seed`` is a stable key (e.g. cell.path)
    so random sampling is deterministic per-cell.
    """
    if not rules:
        return False, ""

    env = {
        "confidence": cell.confidence,
        "source_type": cell.source_type,
        "path": cell.path,
        "review_status": cell.review_status,
        "value": cell.value,
        "alt_count": len(cell.alternative_values or []),
        "is_flagged": cell.review_status == "flagged",
        "yoy": yoy if yoy is not None else 0.0,
    }

    for rule in rules:
        rule = (rule or "").strip()
        if not rule:
            continue
        try:
            if _match_random(rule, sample_seed):
                return True, f"random_sample hit: {rule}"
            if _match_path(rule, cell.path):
                return True, f"path match: {rule}"
            if _eval_expr(rule, env):
                return True, f"condition matched: {rule}"
        except Exception as e:
            logger.warning("Debate policy rule parse failed '%s': %s", rule, e)
            continue
    return False, ""


# ── Helpers ──────────────────────────────────────────────────

_RANDOM_RE = re.compile(r"random_sample\s*\(\s*([0-9.]+)\s*\)")


def _match_random(rule: str, seed: str | None) -> bool:
    m = _RANDOM_RE.search(rule)
    if not m:
        return False
    rate = float(m.group(1))
    rate = max(0.0, min(1.0, rate))
    r = random.Random(seed or rule)
    return r.random() < rate


_PATH_MATCH_RE = re.compile(r"path\s+matches\s+['\"]([^'\"]+)['\"]")


def _match_path(rule: str, path: str) -> bool:
    m = _PATH_MATCH_RE.search(rule)
    if not m:
        return False
    return fnmatch.fnmatchcase(path, m.group(1))


def _eval_expr(expr: str, env: dict[str, Any]) -> bool:
    # Normalize AND/OR/NOT to Python
    norm = expr
    norm = re.sub(r"\bAND\b", " and ", norm)
    norm = re.sub(r"\bOR\b", " or ", norm)
    norm = re.sub(r"\bNOT\b", " not ", norm)
    # Strip random_sample and path matches (handled elsewhere)
    norm = _RANDOM_RE.sub("False", norm)
    norm = _PATH_MATCH_RE.sub("False", norm)
    # Parse with ast in eval mode
    try:
        tree = ast.parse(norm, mode="eval")
    except SyntaxError:
        return False
    _validate_ast(tree)
    return bool(eval(compile(tree, "<debate_policy>", "eval"), {"__builtins__": {}}, {**_ALLOWED_FNS, **env}))


def _validate_ast(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        if isinstance(node, (ast.Call,)):
            if not isinstance(node.func, ast.Name) or node.func.id not in _ALLOWED_FNS:
                raise ValueError(f"disallowed call: {ast.dump(node)}")
        if isinstance(node, ast.Attribute):
            raise ValueError("attribute access not allowed")
        if isinstance(node, ast.Name):
            if node.id not in _ALLOWED_NAMES and node.id not in _ALLOWED_FNS:
                raise ValueError(f"disallowed name: {node.id}")
