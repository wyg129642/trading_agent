"""Integration tests for the formula engine."""
from __future__ import annotations

import pytest

from backend.app.services.formula_engine import (
    CycleError,
    FormulaEngine,
    FormulaError,
)


def test_basic_arithmetic():
    e = FormulaEngine()
    e.set_cell("a", value=10)
    e.set_cell("b", value=3)
    e.set_cell("c", formula="=a + b")
    e.set_cell("d", formula="=a * b")
    e.evaluate_all()
    assert e.get("c") == 13
    assert e.get("d") == 30


def test_cascading_formula():
    """The Excel-example workflow: volume * asp = rev, rev * margin = ni, ni/shares = eps."""
    e = FormulaEngine()
    e.set_cell("volume.FY26", value=100)
    e.set_cell("asp.FY26", value=500)
    e.set_cell("rev.FY26", formula="=volume.FY26 * asp.FY26 / 10000")  # 亿美元
    e.set_cell("margin.FY26", value=0.4)
    e.set_cell("ni.FY26", formula="=rev.FY26 * margin.FY26")
    e.set_cell("shares.FY26", value=2.0)
    e.set_cell("eps.FY26", formula="=ni.FY26 / shares.FY26")
    e.evaluate_all()
    assert e.get("rev.FY26") == 5.0
    assert e.get("ni.FY26") == 2.0
    assert e.get("eps.FY26") == 1.0


def test_range_sum():
    e = FormulaEngine()
    for i in range(1, 5):
        e.set_cell(f"segment.part.{i}", value=float(i * 10))
    e.set_cell("segment.total", formula="=SUM(segment.part.1:segment.part.4)")
    e.evaluate_all()
    assert e.get("segment.total") == 100.0


def test_cycle_detection():
    e = FormulaEngine()
    e.set_cell("a", formula="=b + 1")
    e.set_cell("b", formula="=a + 1")
    cyc = e.find_cycle()
    assert cyc is not None
    assert set(cyc) == {"a", "b"}

    with pytest.raises(CycleError):
        e.evaluate_all()


def test_self_loop_detected():
    e = FormulaEngine()
    e.set_cell("a", formula="=a + 1")
    assert e.find_cycle() is not None


def test_incremental_recompute():
    e = FormulaEngine()
    e.set_cell("a", value=1.0)
    e.set_cell("b", value=2.0)
    e.set_cell("c", formula="=a + b")
    e.set_cell("d", formula="=c * 10")
    e.evaluate_all()
    assert e.get("d") == 30.0
    e.set_cell("a", value=5.0)
    e.evaluate_subset(["a"])
    assert e.get("d") == 70.0


def test_division_by_zero():
    e = FormulaEngine()
    e.set_cell("a", value=10)
    e.set_cell("b", value=0)
    e.set_cell("c", formula="=a / b")
    e.evaluate_all()
    assert e.error("c") == "#DIV/0!"
    assert e.get("c") is None


def test_unknown_reference():
    e = FormulaEngine()
    e.set_cell("a", formula="=unknown_cell * 2")
    e.evaluate_all()
    assert e.error("a") is not None
    assert "#REF!" in e.error("a")


def test_if_function():
    e = FormulaEngine()
    e.set_cell("x", value=10)
    e.set_cell("y", formula='=IF(x > 5, "big", "small")')
    e.evaluate_all()
    assert e.get("y") == "big"


def test_iferror():
    e = FormulaEngine()
    e.set_cell("x", value=10)
    e.set_cell("y", value=0)
    e.set_cell("z", formula="=IFERROR(x/y, 0)")
    e.evaluate_all()
    assert e.get("z") == 0


def test_percent_literal():
    e = FormulaEngine()
    e.set_cell("rev", value=100)
    e.set_cell("ni", formula="=rev * 10%")
    e.evaluate_all()
    assert e.get("ni") == 10.0


def test_averageifs():
    e = FormulaEngine()
    e.set_cell("a.1", value=10)
    e.set_cell("a.2", value=20)
    e.set_cell("a.3", value=30)
    e.set_cell("flag.1", value=1)
    e.set_cell("flag.2", value=0)
    e.set_cell("flag.3", value=1)
    e.set_cell("r", formula="=AVERAGEIFS(a.1:a.3, flag.1:flag.3, 1)")
    e.evaluate_all()
    assert e.get("r") == 20.0


def test_vlookup():
    e = FormulaEngine()
    e.register_function("GET_TABLE", lambda: [
        ["LITE", 100, 200],
        ["AAPL", 180, 170],
        ["NVDA", 500, 600],
    ])
    e.set_cell("key", value="NVDA")
    e.set_cell("v", formula="=VLOOKUP(key, GET_TABLE(), 2)")
    e.evaluate_all()
    assert e.get("v") == 500


def test_round():
    e = FormulaEngine()
    e.set_cell("a", formula="=ROUND(3.14159, 2)")
    e.set_cell("b", formula="=ROUND(2.5, 0)")
    e.evaluate_all()
    assert e.get("a") == pytest.approx(3.14)
    assert e.get("b") == pytest.approx(3.0)  # banker's rounding: 2.5 → 2 on some Pythons; ROUND uses round()


def test_cagr():
    e = FormulaEngine()
    e.set_cell("r", formula="=CAGR(100, 200, 5)")
    e.evaluate_all()
    assert e.get("r") == pytest.approx(2 ** (1 / 5) - 1, abs=1e-6)


def test_yoy():
    e = FormulaEngine()
    e.set_cell("r", formula="=YOY(120, 100)")
    e.evaluate_all()
    assert e.get("r") == pytest.approx(0.2)


def test_large_graph_perf(benchmark=None):
    """2000-cell graph should evaluate in a reasonable time."""
    import time
    e = FormulaEngine()
    for i in range(1000):
        e.set_cell(f"input.{i}", value=float(i))
    for i in range(1000):
        e.set_cell(f"out.{i}", formula=f"=input.{i} * 2 + 1")
    t0 = time.time()
    e.evaluate_all()
    dt = time.time() - t0
    assert e.get("out.999") == 999 * 2 + 1
    # Generous bound to avoid flakes in CI
    assert dt < 2.0, f"2000-cell eval took {dt:.2f}s"


def test_concat_strings():
    e = FormulaEngine()
    e.set_cell("name", value="LITE")
    e.set_cell("greet", formula='=CONCAT("Hello ", name)')
    e.evaluate_all()
    assert e.get("greet") == "Hello LITE"


def test_excel_operators():
    """Excel supports & for string concat."""
    e = FormulaEngine()
    e.set_cell("a", value="Hello")
    e.set_cell("b", value="World")
    e.set_cell("c", formula='=a & " " & b')
    e.evaluate_all()
    assert e.get("c") == "Hello World"


def test_sumifs():
    e = FormulaEngine()
    e.register_function("DATA", lambda: [10, 20, 30, 40])
    e.register_function("CAT", lambda: ["A", "B", "A", "B"])
    e.set_cell("total_a", formula='=SUMIFS(DATA(), CAT(), "A")')
    e.evaluate_all()
    assert e.get("total_a") == 40


def test_nested_if():
    e = FormulaEngine()
    e.set_cell("growth", value=0.3)
    e.set_cell("flag", formula='=IF(growth > 0.5, "hyper", IF(growth > 0.1, "high", "stable"))')
    e.evaluate_all()
    assert e.get("flag") == "high"


def test_change_formula_recomputes():
    e = FormulaEngine()
    e.set_cell("a", value=10)
    e.set_cell("b", formula="=a + 1")
    e.evaluate_all()
    assert e.get("b") == 11
    e.set_cell("b", formula="=a * 2")
    e.evaluate_all()
    assert e.get("b") == 20
