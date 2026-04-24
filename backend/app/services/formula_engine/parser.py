"""Formula parser — tokenize and build an AST for dot-path expressions.

Grammar (informal, closely mirroring a subset of Excel plus dot-path refs)::

    expr     := term (('+' | '-') term)*
    term     := factor (('*' | '/') factor)*
    factor   := unary ('^' unary)*
    unary    := ('-' | '+')? primary
    primary  := number | string | bool | path | call | '(' expr ')'
    call     := IDENT '(' args? ')'
    args     := expr (',' expr)*
    path     := IDENT ('.' IDENT_OR_NUM)+         # or ranged via ':' (for SUM)
    range    := path ':' path

Path tokens allow identifiers of the form ``segment.HDD.rev.FY26`` — any
non-whitespace, non-operator characters separated by dots. We also allow
hyphens and digits inside segments (e.g. ``FY26Q1``).

A formula must start with ``=``; anything else is returned as a literal by
the caller (not this module).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "parse_formula",
    "parse_dependencies",
    "extract_paths",
    "Node",
    "NumLit",
    "StrLit",
    "BoolLit",
    "PathRef",
    "RangeRef",
    "BinOp",
    "UnaryOp",
    "FuncCall",
]


# ── AST nodes ────────────────────────────────────────────────────

@dataclass
class Node:
    pass


@dataclass
class NumLit(Node):
    value: float


@dataclass
class StrLit(Node):
    value: str


@dataclass
class BoolLit(Node):
    value: bool


@dataclass
class PathRef(Node):
    path: str


@dataclass
class RangeRef(Node):
    """A:B range — both endpoints resolve to cell paths.

    At evaluate time, the engine determines what the range "expands to"
    via a caller-provided resolver. By default we support two styles:

      1. `SUM(a.1:a.3)` – if both endpoints share a parent path and the
         last segment is numeric/periodic, expand to the sequence.
      2. `SUM(a:b)` – fall back to treating this as {a, b} (list of two).

    The simpler semantics are fine for our use — most "ranges" are
    sums of siblings the recipe engine already lays out in order.
    """
    start: str
    end: str


@dataclass
class UnaryOp(Node):
    op: str
    operand: Node


@dataclass
class BinOp(Node):
    op: str
    left: Node
    right: Node


@dataclass
class FuncCall(Node):
    name: str
    args: list[Node] = field(default_factory=list)


# ── Tokenizer ────────────────────────────────────────────────────

# Order matters: longer operators first
_TOKEN_SPEC = [
    ("NUMBER", r"\d+(?:\.\d+)?(?:[eE][+-]?\d+)?%?"),
    ("STRING", r'"(?:[^"\\]|\\.)*"'),
    # A "PATH" here is a dotted identifier. Underscores, hyphens, and digits
    # allowed inside each segment. Unicode letters allowed so Chinese paths
    # are legal (we use dot-paths aggressively in recipes).
    ("PATH", r"[A-Za-z_一-鿿][\w一-鿿-]*(?:\.[A-Za-z_一-鿿0-9][\w一-鿿-]*)+"),
    ("IDENT", r"[A-Za-z_][A-Za-z_0-9]*"),
    ("OP", r"<>|<=|>=|==|!=|&&|\|\||[+\-*/^%=<>:&,()!]"),
    ("WS", r"\s+"),
]
_TOKEN_RE = re.compile("|".join(f"(?P<{k}>{v})" for k, v in _TOKEN_SPEC))


class FormulaParseError(Exception):
    pass


@dataclass
class _Tok:
    kind: str
    value: str
    pos: int


def _tokenize(src: str) -> list[_Tok]:
    toks: list[_Tok] = []
    i = 0
    while i < len(src):
        m = _TOKEN_RE.match(src, i)
        if not m:
            raise FormulaParseError(f"Unexpected character at position {i}: {src[i]!r}")
        kind = m.lastgroup
        value = m.group()
        i = m.end()
        if kind == "WS":
            continue
        toks.append(_Tok(kind, value, m.start()))
    toks.append(_Tok("EOF", "", len(src)))
    return toks


# ── Recursive descent parser ─────────────────────────────────────

class _Parser:
    def __init__(self, toks: list[_Tok], src: str):
        self.toks = toks
        self.i = 0
        self.src = src

    def peek(self, k: int = 0) -> _Tok:
        return self.toks[self.i + k]

    def eat(self, kind: str | None = None, value: str | None = None) -> _Tok:
        t = self.toks[self.i]
        if kind and t.kind != kind:
            raise FormulaParseError(
                f"Expected {kind} got {t.kind}({t.value!r}) at pos {t.pos}"
            )
        if value and t.value != value:
            raise FormulaParseError(
                f"Expected {value!r} got {t.value!r} at pos {t.pos}"
            )
        self.i += 1
        return t

    def parse(self) -> Node:
        n = self.parse_expr()
        if self.peek().kind != "EOF":
            t = self.peek()
            raise FormulaParseError(
                f"Unexpected trailing token {t.value!r} at pos {t.pos}"
            )
        return n

    # grammar
    def parse_expr(self) -> Node:
        return self.parse_comparison()

    def parse_comparison(self) -> Node:
        left = self.parse_additive()
        while self.peek().kind == "OP" and self.peek().value in {
            "=", "==", "<>", "!=", "<", ">", "<=", ">="
        }:
            op = self.eat().value
            # Excel uses '=' for equality in formulas; normalize to '=='
            if op == "=":
                op = "=="
            if op == "<>":
                op = "!="
            right = self.parse_additive()
            left = BinOp(op=op, left=left, right=right)
        return left

    def parse_additive(self) -> Node:
        left = self.parse_term()
        while self.peek().kind == "OP" and self.peek().value in {"+", "-", "&"}:
            op = self.eat().value
            right = self.parse_term()
            left = BinOp(op=op, left=left, right=right)
        return left

    def parse_term(self) -> Node:
        left = self.parse_power()
        while self.peek().kind == "OP" and self.peek().value in {"*", "/", "%"}:
            op = self.eat().value
            right = self.parse_power()
            left = BinOp(op=op, left=left, right=right)
        return left

    def parse_power(self) -> Node:
        left = self.parse_unary()
        if self.peek().kind == "OP" and self.peek().value == "^":
            self.eat()
            right = self.parse_power()  # right-assoc
            return BinOp(op="^", left=left, right=right)
        return left

    def parse_unary(self) -> Node:
        if self.peek().kind == "OP" and self.peek().value in {"+", "-"}:
            op = self.eat().value
            operand = self.parse_unary()
            return UnaryOp(op=op, operand=operand)
        return self.parse_primary()

    def parse_primary(self) -> Node:
        t = self.peek()
        if t.kind == "NUMBER":
            self.eat()
            v = t.value
            if v.endswith("%"):
                return NumLit(value=float(v[:-1]) / 100.0)
            return NumLit(value=float(v))
        if t.kind == "STRING":
            self.eat()
            return StrLit(value=_unescape_string(t.value[1:-1]))
        if t.kind == "OP" and t.value == "(":
            self.eat()
            n = self.parse_expr()
            self.eat("OP", ")")
            return n
        if t.kind == "IDENT":
            # Function call or bool literal or bare identifier (treated as path)
            name = t.value
            upper = name.upper()
            if upper in ("TRUE", "FALSE"):
                self.eat()
                return BoolLit(value=(upper == "TRUE"))
            self.eat()
            if self.peek().kind == "OP" and self.peek().value == "(":
                return self.parse_call(name)
            # Bare identifier = path of length 1
            return PathRef(path=name)
        if t.kind == "PATH":
            self.eat()
            path = t.value
            # Handle ranges: path ':' path
            if self.peek().kind == "OP" and self.peek().value == ":":
                self.eat()
                end_tok = self.peek()
                if end_tok.kind not in ("PATH", "IDENT"):
                    raise FormulaParseError(
                        f"Expected PATH after ':' at pos {end_tok.pos}"
                    )
                self.eat()
                return RangeRef(start=path, end=end_tok.value)
            return PathRef(path=path)
        raise FormulaParseError(
            f"Unexpected token {t.kind}({t.value!r}) at pos {t.pos}"
        )

    def parse_call(self, name: str) -> FuncCall:
        self.eat("OP", "(")
        args: list[Node] = []
        if not (self.peek().kind == "OP" and self.peek().value == ")"):
            args.append(self.parse_expr())
            while self.peek().kind == "OP" and self.peek().value == ",":
                self.eat()
                args.append(self.parse_expr())
        self.eat("OP", ")")
        return FuncCall(name=name.upper(), args=args)


def _unescape_string(s: str) -> str:
    return s.replace('\\"', '"').replace("\\\\", "\\").replace("\\n", "\n").replace("\\t", "\t")


# ── Public entry points ──────────────────────────────────────────

def parse_formula(formula: str) -> Node:
    """Parse a formula string (with or without leading =) into an AST."""
    if formula is None:
        raise FormulaParseError("formula is None")
    f = formula.strip()
    if f.startswith("="):
        f = f[1:]
    if not f:
        raise FormulaParseError("empty formula")
    return _Parser(_tokenize(f), f).parse()


def extract_paths(node: Node) -> list[str]:
    """Return all cell paths referenced by an AST (de-duplicated, ordered)."""
    seen: dict[str, None] = {}
    stack: list[Node] = [node]
    while stack:
        n = stack.pop()
        if isinstance(n, PathRef):
            seen.setdefault(n.path, None)
        elif isinstance(n, RangeRef):
            seen.setdefault(n.start, None)
            seen.setdefault(n.end, None)
        elif isinstance(n, (UnaryOp,)):
            stack.append(n.operand)
        elif isinstance(n, BinOp):
            stack.append(n.left)
            stack.append(n.right)
        elif isinstance(n, FuncCall):
            stack.extend(reversed(n.args))
    return list(seen.keys())


def parse_dependencies(formula: str) -> list[str]:
    """Parse a formula and return its cell-path dependencies."""
    return extract_paths(parse_formula(formula))
