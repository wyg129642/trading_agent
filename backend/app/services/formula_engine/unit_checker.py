"""Unit compatibility layer on top of the formula engine.

Not a full dimensional analysis system; just enforces the three rules
that catch the vast majority of modeling bugs:

1. ``$ + $`` is fine, ``$ + %`` is a unit error.
2. ``$ * %`` is fine (result has the ``$`` unit).
3. ``count * $/unit`` needs explicit ratio; we only recognize simple forms.

The checker runs on a ``ModelCell`` table snapshot (path -> unit +
formula) and reports ``UnitIssue`` records. It does not mutate anything.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .parser import (
    BinOp,
    BoolLit,
    FuncCall,
    Node,
    NumLit,
    PathRef,
    RangeRef,
    StrLit,
    UnaryOp,
    parse_formula,
)


@dataclass
class UnitIssue:
    cell_path: str
    message: str
    severity: str = "warn"


# Families — operands in the same family can be added/subtracted; crossing
# families is a warning.
UNIT_FAMILIES = {
    "money": {"$", "USD", "CNY", "HKD", "亿美元", "亿元", "万美元", "百万美元"},
    "share": {"%", "pct", "percent"},
    "count": {"万颗", "万块", "EB", "万片", "pieces", "units", "颗", "片"},
    "price_per_unit": {"美元/EB", "美元/颗", "美元/块", "$/unit"},
    "share_count": {"亿股", "shares"},
    "earnings_per_share": {"USD/share", "$/share"},
}


def _family(unit: str | None) -> str | None:
    if not unit:
        return None
    for fam, xs in UNIT_FAMILIES.items():
        if unit in xs:
            return fam
    return None


def check_units(
    cells: dict[str, tuple[str | None, str | None]],
) -> list[UnitIssue]:
    """Run unit checks.

    ``cells`` maps path -> (unit, formula). Cells without a formula are
    ignored (no operation => no unit contradiction).
    """
    issues: list[UnitIssue] = []
    for path, (unit, formula) in cells.items():
        if not formula:
            continue
        try:
            ast = parse_formula(formula)
        except Exception:
            continue
        try:
            _walk(ast, path, cells, issues)
        except _UnitMismatch as m:
            issues.append(UnitIssue(cell_path=path, message=m.message, severity="warn"))
    return issues


class _UnitMismatch(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


def _infer_unit(node: Node, cells: dict[str, tuple[str | None, str | None]]) -> str | None:
    if isinstance(node, NumLit):
        return None
    if isinstance(node, StrLit):
        return None
    if isinstance(node, BoolLit):
        return None
    if isinstance(node, PathRef):
        entry = cells.get(node.path)
        return entry[0] if entry else None
    if isinstance(node, RangeRef):
        s = cells.get(node.start)
        return s[0] if s else None
    if isinstance(node, UnaryOp):
        return _infer_unit(node.operand, cells)
    if isinstance(node, FuncCall):
        # SUM / AVERAGE / MIN / MAX preserve the first arg's unit
        if node.args:
            return _infer_unit(node.args[0], cells)
        return None
    if isinstance(node, BinOp):
        lu = _infer_unit(node.left, cells)
        ru = _infer_unit(node.right, cells)
        if node.op in {"+", "-"}:
            if lu and ru and _family(lu) != _family(ru):
                raise _UnitMismatch(f"{lu!r} {node.op} {ru!r} cross families")
            return lu or ru
        if node.op == "*":
            # money × share = money; count × price = money
            fam_l, fam_r = _family(lu), _family(ru)
            if fam_l == "share" and fam_r in ("money", "count"):
                return ru
            if fam_r == "share" and fam_l in ("money", "count"):
                return lu
            if {fam_l, fam_r} == {"count", "price_per_unit"}:
                return "money"
            return lu or ru
        if node.op == "/":
            if lu and ru and _family(lu) == _family(ru):
                return "%"  # ratio
            if _family(lu) == "money" and _family(ru) == "count":
                return "price_per_unit"
            return lu
    return None


def _walk(
    node: Node,
    cell_path: str,
    cells: dict[str, tuple[str | None, str | None]],
    issues: list[UnitIssue],
) -> None:
    _infer_unit(node, cells)  # may raise _UnitMismatch
    if isinstance(node, (UnaryOp,)):
        _walk(node.operand, cell_path, cells, issues)
    elif isinstance(node, BinOp):
        _walk(node.left, cell_path, cells, issues)
        _walk(node.right, cell_path, cells, issues)
    elif isinstance(node, FuncCall):
        for a in node.args:
            _walk(a, cell_path, cells, issues)
