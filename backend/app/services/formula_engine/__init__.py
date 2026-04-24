"""Formula Engine — Excel-compatible expression evaluator for ModelCell graphs.

Public API:

    from backend.app.services.formula_engine import (
        FormulaEngine, CycleError, FormulaError, parse_dependencies,
    )

    engine = FormulaEngine()
    engine.set_cell("rev.FY26", value=100.0)
    engine.set_cell("margin.FY26", value=0.4)
    engine.set_cell("ni.FY26", formula="=rev.FY26 * margin.FY26")
    engine.evaluate_all()
    print(engine.get("ni.FY26"))  # 40.0

Design:

* Dot-path identifiers natively (no A1 translation needed — we control the DSL).
* Tarjan SCC cycle detection before evaluation.
* Topological evaluation with dirty-bit propagation for incremental recompute.
* ~80 Excel functions (stats, logical, lookup, math, text, financial).
* Unit-awareness is a separate check pass (``unit_checker.py``), not part of eval.
* Deterministic float semantics — we use Python floats; percent values are
  stored as fractions (0.35 = 35%) consistent with the Excel convention.
"""
from .evaluator import FormulaEngine, CycleError, FormulaError, FormulaParseError
from .parser import parse_formula, parse_dependencies, extract_paths
from .graph import DependencyGraph
from . import functions as _functions  # register builtins
from .functions import register_function, EXCEL_FUNCTIONS

__all__ = [
    "FormulaEngine",
    "CycleError",
    "FormulaError",
    "FormulaParseError",
    "parse_formula",
    "parse_dependencies",
    "extract_paths",
    "DependencyGraph",
    "register_function",
    "EXCEL_FUNCTIONS",
]
