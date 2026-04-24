"""Formula engine — AST evaluator + cell store + incremental recompute."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable

from .graph import CycleError, DependencyGraph
from .parser import (
    BinOp,
    BoolLit,
    FormulaParseError,
    FuncCall,
    Node,
    NumLit,
    PathRef,
    RangeRef,
    StrLit,
    UnaryOp,
    extract_paths,
    parse_formula,
)
from .functions import EXCEL_FUNCTIONS

__all__ = [
    "FormulaEngine",
    "CycleError",
    "FormulaError",
    "FormulaParseError",
]


class FormulaError(Exception):
    """Evaluation-time error, e.g. #DIV/0!, #REF!, #N/A."""


@dataclass
class _CellEntry:
    value: Any = None
    formula: str | None = None
    ast: Node | None = None
    deps: list[str] = field(default_factory=list)
    error: str | None = None  # stringified error code if eval failed
    dirty: bool = False


class FormulaEngine:
    """Store + evaluator of a set of dot-path ``ModelCell`` formulas.

    This class is synchronous and in-memory. The recipe engine pulls cells
    out of Postgres, hydrates an engine, evaluates, and writes back.
    """

    def __init__(self, range_resolver: Callable[[str, str], list[str]] | None = None) -> None:
        self._cells: dict[str, _CellEntry] = {}
        self._graph = DependencyGraph()
        # Optional resolver for range references (for SUM(a:z) etc.)
        self._range_resolver = range_resolver or _default_range_resolver
        # Custom functions take precedence over builtins
        self._local_functions: dict[str, Callable[..., Any]] = {}

    # ── custom function injection ──────────────────────────────

    def register_function(self, name: str, fn: Callable[..., Any]) -> None:
        self._local_functions[name.upper()] = fn

    # ── cell mutation ──────────────────────────────────────────

    def set_cell(
        self,
        path: str,
        *,
        value: Any = None,
        formula: str | None = None,
    ) -> None:
        """Set a cell's value or formula. Passing both is an error."""
        if value is not None and formula is not None:
            raise ValueError("Pass either value or formula, not both")
        entry = self._cells.setdefault(path, _CellEntry())
        self._graph.add_node(path)
        if formula is not None:
            try:
                ast = parse_formula(formula)
            except FormulaParseError as e:
                entry.formula = formula
                entry.ast = None
                entry.error = f"#NAME? {e}"
                entry.value = None
                entry.deps = []
                self._graph.set_dependencies(path, [])
                self._mark_downstream_dirty(path)
                return
            deps = extract_paths(ast)
            entry.formula = formula
            entry.ast = ast
            entry.deps = deps
            entry.error = None
            entry.value = None
            entry.dirty = True
            self._graph.set_dependencies(path, deps)
        else:
            entry.formula = None
            entry.ast = None
            entry.deps = []
            entry.value = value
            entry.error = None
            entry.dirty = False
            self._graph.set_dependencies(path, [])
        self._mark_downstream_dirty(path)

    def remove_cell(self, path: str) -> None:
        self._cells.pop(path, None)
        self._graph.remove_node(path)

    # ── queries ────────────────────────────────────────────────

    def get(self, path: str) -> Any:
        """Return the current (possibly stale) value for a path."""
        e = self._cells.get(path)
        if e is None:
            return None
        return e.value

    def error(self, path: str) -> str | None:
        e = self._cells.get(path)
        return e.error if e else None

    def paths(self) -> list[str]:
        return list(self._cells.keys())

    def dependencies(self, path: str) -> list[str]:
        e = self._cells.get(path)
        return list(e.deps) if e else []

    # ── cycle detection ────────────────────────────────────────

    def find_cycle(self) -> list[str] | None:
        return self._graph.find_cycle()

    # ── evaluation ─────────────────────────────────────────────

    def evaluate_all(self, *, strict: bool = False) -> dict[str, Any]:
        """Evaluate every formula in topological order.

        If ``strict`` is True, raise on the first error. Otherwise errors
        are recorded on each cell (`entry.error`) and propagate via the
        value being ``None``.
        """
        cycle = self._graph.find_cycle()
        if cycle:
            raise CycleError(cycle)

        order = self._graph.topo_order()
        for path in order:
            self._evaluate_one(path, strict=strict)
        return {p: e.value for p, e in self._cells.items()}

    def evaluate_subset(self, paths: list[str], *, strict: bool = False) -> dict[str, Any]:
        """Evaluate only the transitive closure of ``paths`` + their downstream."""
        cycle = self._graph.find_cycle()
        if cycle:
            raise CycleError(cycle)
        affected: set[str] = set(paths)
        for p in paths:
            affected |= self._graph.transitive_dependents(p)
        order = [p for p in self._graph.topo_order() if p in affected]
        for p in order:
            self._evaluate_one(p, strict=strict)
        return {p: self._cells[p].value for p in order if p in self._cells}

    # ── internals ──────────────────────────────────────────────

    def _evaluate_one(self, path: str, *, strict: bool) -> None:
        e = self._cells.get(path)
        if e is None:
            return
        if e.ast is None:
            # Hard-coded value; keep as-is
            e.error = None
            return
        try:
            e.value = self._eval_node(e.ast)
            e.error = None
        except FormulaError as err:
            e.value = None
            e.error = str(err)
            if strict:
                raise
        except ZeroDivisionError:
            e.value = None
            e.error = "#DIV/0!"
            if strict:
                raise FormulaError("#DIV/0!")
        except ValueError as err:
            e.value = None
            msg = str(err)
            if not msg.startswith("#"):
                msg = f"#VALUE! {msg}"
            e.error = msg
            if strict:
                raise FormulaError(msg) from err
        except KeyError as err:
            e.value = None
            e.error = f"#REF! unknown cell {err}"
            if strict:
                raise FormulaError(e.error) from err
        except Exception as err:
            e.value = None
            e.error = f"#ERROR! {err}"
            if strict:
                raise FormulaError(e.error) from err
        finally:
            e.dirty = False

    def _eval_node(self, node: Node) -> Any:
        if isinstance(node, NumLit):
            return node.value
        if isinstance(node, StrLit):
            return node.value
        if isinstance(node, BoolLit):
            return node.value
        if isinstance(node, PathRef):
            return self._resolve_path(node.path)
        if isinstance(node, RangeRef):
            return self._resolve_range(node.start, node.end)
        if isinstance(node, UnaryOp):
            v = self._eval_node(node.operand)
            return self._apply_unary(node.op, v)
        if isinstance(node, BinOp):
            if node.op == "&":
                # Excel concatenation: coerce to strings
                l = self._eval_node(node.left)
                r = self._eval_node(node.right)
                return ("" if l is None else str(l)) + ("" if r is None else str(r))
            l = self._eval_node(node.left)
            r = self._eval_node(node.right)
            return self._apply_binop(node.op, l, r)
        if isinstance(node, FuncCall):
            return self._eval_call(node)
        raise FormulaError(f"#ERROR! unknown node type {type(node).__name__}")

    def _apply_unary(self, op: str, v: Any) -> Any:
        if op == "-":
            return -_as_number(v)
        if op == "+":
            return +_as_number(v)
        raise FormulaError(f"#ERROR! unary {op}")

    def _apply_binop(self, op: str, l: Any, r: Any) -> Any:
        if op in {"==", "!=", "<", "<=", ">", ">="}:
            # Excel comparison — numeric if both coerce
            try:
                ln, rn = _as_number(l), _as_number(r)
                if op == "==":
                    return ln == rn
                if op == "!=":
                    return ln != rn
                if op == "<":
                    return ln < rn
                if op == "<=":
                    return ln <= rn
                if op == ">":
                    return ln > rn
                if op == ">=":
                    return ln >= rn
            except (TypeError, ValueError):
                # Fall back to string compare
                ls, rs = str(l), str(r)
                if op == "==":
                    return ls == rs
                if op == "!=":
                    return ls != rs
                if op == "<":
                    return ls < rs
                if op == "<=":
                    return ls <= rs
                if op == ">":
                    return ls > rs
                if op == ">=":
                    return ls >= rs
        ln = _as_number(l)
        rn = _as_number(r)
        if op == "+":
            return ln + rn
        if op == "-":
            return ln - rn
        if op == "*":
            return ln * rn
        if op == "/":
            if rn == 0:
                raise ZeroDivisionError
            return ln / rn
        if op == "^":
            return ln ** rn
        if op == "%":
            return ln / 100.0
        raise FormulaError(f"#ERROR! binop {op}")

    def _eval_call(self, node: FuncCall) -> Any:
        fn_name = node.name.upper()

        # IFERROR / IFNA special case — must catch child errors
        if fn_name in ("IFERROR", "IFNA") and len(node.args) >= 2:
            try:
                return self._eval_node(node.args[0])
            except Exception:
                return self._eval_node(node.args[1])

        fn = self._local_functions.get(fn_name) or EXCEL_FUNCTIONS.get(fn_name)
        if fn is None:
            raise FormulaError(f"#NAME? unknown function {fn_name}")
        args = [self._eval_node(a) for a in node.args]
        try:
            result = fn(*args)
        except TypeError as e:
            raise FormulaError(f"#VALUE! {fn_name}: {e}")
        return result

    def _resolve_path(self, path: str) -> Any:
        e = self._cells.get(path)
        if e is None:
            raise KeyError(path)
        if e.error:
            raise FormulaError(e.error)
        return e.value

    def _resolve_range(self, start: str, end: str) -> list[Any]:
        paths = self._range_resolver(start, end)
        out: list[Any] = []
        for p in paths:
            e = self._cells.get(p)
            if e is None:
                # Missing cells in a range are skipped (Excel treats as blank)
                out.append(None)
                continue
            if e.error:
                raise FormulaError(e.error)
            out.append(e.value)
        return out

    # ── dirty tracking ─────────────────────────────────────────

    def _mark_downstream_dirty(self, path: str) -> None:
        for p in self._graph.transitive_dependents(path):
            e = self._cells.get(p)
            if e is not None:
                e.dirty = True


def _as_number(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        f = float(v)
        if math.isnan(f):
            raise FormulaError("#NUM! NaN")
        return f
    if isinstance(v, str):
        s = v.strip().rstrip("%")
        try:
            n = float(s)
            return n / 100.0 if v.rstrip().endswith("%") else n
        except ValueError:
            raise ValueError(f"cannot convert {v!r} to number")
    raise ValueError(f"unsupported numeric type: {type(v).__name__}")


def _default_range_resolver(start: str, end: str) -> list[str]:
    """Best-effort range expansion.

    Handles the common case ``segment.foo.1`` : ``segment.foo.3`` ->
    ``[segment.foo.1, segment.foo.2, segment.foo.3]`` by expanding the
    trailing integer. Falls back to ``[start, end]``.
    """
    start_parts = start.split(".")
    end_parts = end.split(".")
    if (
        len(start_parts) == len(end_parts)
        and start_parts[:-1] == end_parts[:-1]
    ):
        try:
            a = int(start_parts[-1])
            b = int(end_parts[-1])
        except ValueError:
            return [start, end]
        if a <= b:
            return [".".join(start_parts[:-1] + [str(i)]) for i in range(a, b + 1)]
        return [".".join(start_parts[:-1] + [str(i)]) for i in range(a, b - 1, -1)]
    return [start, end]
