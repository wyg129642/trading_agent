"""Tests for the formula parser."""
from __future__ import annotations

import pytest

from backend.app.services.formula_engine.parser import (
    BinOp,
    FormulaParseError,
    FuncCall,
    NumLit,
    PathRef,
    RangeRef,
    StrLit,
    UnaryOp,
    extract_paths,
    parse_dependencies,
    parse_formula,
)


def test_number_literal():
    ast = parse_formula("=42")
    assert isinstance(ast, NumLit)
    assert ast.value == 42

    ast = parse_formula("=1.5e3")
    assert ast.value == 1500


def test_percent_literal():
    ast = parse_formula("=35%")
    assert isinstance(ast, NumLit)
    assert ast.value == pytest.approx(0.35)


def test_path_reference():
    ast = parse_formula("=segment.HDD.rev.FY26")
    assert isinstance(ast, PathRef)
    assert ast.path == "segment.HDD.rev.FY26"


def test_simple_arithmetic():
    ast = parse_formula("=1+2*3")
    # must respect precedence: 1 + (2*3) = 7
    assert isinstance(ast, BinOp)
    assert ast.op == "+"
    assert isinstance(ast.right, BinOp) and ast.right.op == "*"


def test_power_right_assoc():
    ast = parse_formula("=2^3^2")
    # Right-associative: 2^(3^2) = 512
    assert isinstance(ast, BinOp) and ast.op == "^"
    assert isinstance(ast.right, BinOp) and ast.right.op == "^"


def test_unary_minus():
    ast = parse_formula("=-5 + 3")
    assert isinstance(ast, BinOp)
    assert isinstance(ast.left, UnaryOp) and ast.left.op == "-"


def test_function_call():
    ast = parse_formula("=SUM(a.1, a.2, a.3)")
    assert isinstance(ast, FuncCall)
    assert ast.name == "SUM"
    assert len(ast.args) == 3


def test_range_reference():
    ast = parse_formula("=SUM(a.1:a.5)")
    assert isinstance(ast, FuncCall) and ast.name == "SUM"
    r = ast.args[0]
    assert isinstance(r, RangeRef)
    assert r.start == "a.1" and r.end == "a.5"


def test_string_literal():
    ast = parse_formula('="Hello"')
    assert isinstance(ast, StrLit)
    assert ast.value == "Hello"


def test_comparison_normalization():
    ast = parse_formula("=a.b = 5")
    assert isinstance(ast, BinOp) and ast.op == "=="


def test_extract_paths():
    paths = parse_dependencies("=segment.HDD.rev.FY26 * segment.HDD.margin.FY26")
    assert "segment.HDD.rev.FY26" in paths
    assert "segment.HDD.margin.FY26" in paths
    assert len(paths) == 2


def test_extract_paths_in_range():
    paths = parse_dependencies("=SUM(a.1:a.3)")
    assert "a.1" in paths and "a.3" in paths


def test_empty_formula():
    with pytest.raises(FormulaParseError):
        parse_formula("=")


def test_unbalanced_paren():
    with pytest.raises(FormulaParseError):
        parse_formula("=(1+2")


def test_bare_equals_prefix_optional():
    # Both work
    assert parse_formula("=1+1")
    assert parse_formula("1+1")


def test_nested_functions():
    ast = parse_formula("=IF(a.1 > 10, SUM(b.1:b.3), AVG(c.1))")
    assert isinstance(ast, FuncCall) and ast.name == "IF"


def test_chinese_path():
    paths = parse_dependencies("=业务.收入.FY26 * 业务.毛利率.FY26")
    assert "业务.收入.FY26" in paths
    assert "业务.毛利率.FY26" in paths
