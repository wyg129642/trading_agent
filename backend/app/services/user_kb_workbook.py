"""Multi-sheet workbook helpers for the knowledge-base workspace.

Workbook schema (stored as ``spreadsheet_data`` on the Mongo doc):

.. code-block:: json

    {
      "active_sheet_id": "sheet-1",
      "sheets": [
        {
          "id": "sheet-1",
          "name": "估值表",
          "rows": 22,
          "cols": 8,
          "cells": { "A1": {"v": "..."} | {"f": "=A2+1"}, ... },
          "col_widths": [160, 60, 90, 90, ...]
        }
      ]
    }

**Backwards compatibility.** Existing rows have the old flat shape::

    {"rows": 22, "cols": 8, "cells": {...}, "col_widths": [...]}

Readers (``normalize_for_read``) synthesize a one-sheet workbook so the
frontend only deals with the new shape. Writers (``validate_for_write``)
accept either; flat payloads are wrapped into a single-sheet workbook on
the way in, so the stored row is eventually upgraded.

**Size caps.** Enforced at write time:

* ≤ 10 sheets per workbook
* ≤ 500 rows × 50 cols per sheet
* ≤ 200 000 cells total (summed across sheets) — the JSON would exceed
  ~5 MB before hitting this, which is already extreme for an internal
  valuation model.
"""
from __future__ import annotations

import re
import uuid
from datetime import date
from typing import Any

# ── Caps ──────────────────────────────────────────────────────────

MAX_SHEETS = 10
MAX_ROWS_PER_SHEET = 500
MAX_COLS_PER_SHEET = 50
MAX_CELLS_TOTAL = 200_000
# Names are plain strings in the UI — cap at a reasonable length; the UI
# itself clips display, but we want to avoid unbounded data on disk.
MAX_SHEET_NAME_LEN = 64
# Reject cell values longer than this; formulas too (the formula language
# is arithmetic, so anything over a few hundred chars is abuse).
MAX_CELL_STRING_LEN = 4096

A1_RE = re.compile(r"^[A-Z]{1,3}[1-9][0-9]{0,3}$")


class WorkbookValidationError(ValueError):
    """Raised when a workbook payload is malformed or exceeds caps."""


# ── Default 5-year DCF template ──────────────────────────────────

# Kept as a module-level constant so it can be reused both by the
# holdings-sync seeding path and by the "install DCF skill" flow.
# Matches the old DEFAULT_VALUATION_TEMPLATE shape almost 1:1 — the new
# workbook just wraps it under sheets[0].

_DCF_CELLS: dict[str, dict[str, Any]] = {
    # Header row
    "A1": {"v": "指标 / Metric"},
    "B1": {"v": "单位"},
    "C1": {"v": "2023A"},
    "D1": {"v": "2024A"},
    "E1": {"v": "2025E"},
    "F1": {"v": "2026E"},
    "G1": {"v": "2027E"},
    "H1": {"v": "2028E"},
    # Revenue
    "A2": {"v": "营业收入"}, "B2": {"v": "亿元"},
    "A3": {"v": "  同比增长率"}, "B3": {"v": "%"},
    "C3": {"f": "=IFERROR((C2-B2)/B2*100,0)"},
    "D3": {"f": "=IFERROR((D2-C2)/C2*100,0)"},
    "E3": {"f": "=IFERROR((E2-D2)/D2*100,0)"},
    "F3": {"f": "=IFERROR((F2-E2)/E2*100,0)"},
    "G3": {"f": "=IFERROR((G2-F2)/F2*100,0)"},
    "H3": {"f": "=IFERROR((H2-G2)/G2*100,0)"},
    # Gross margin
    "A4": {"v": "毛利润"}, "B4": {"v": "亿元"},
    "A5": {"v": "  毛利率"}, "B5": {"v": "%"},
    "C5": {"f": "=IFERROR(C4/C2*100,0)"},
    "D5": {"f": "=IFERROR(D4/D2*100,0)"},
    "E5": {"f": "=IFERROR(E4/D2*100,0)"},
    "F5": {"f": "=IFERROR(F4/F2*100,0)"},
    "G5": {"f": "=IFERROR(G4/G2*100,0)"},
    "H5": {"f": "=IFERROR(H4/H2*100,0)"},
    # Operating margin
    "A6": {"v": "营业利润"}, "B6": {"v": "亿元"},
    "A7": {"v": "  营业利润率"}, "B7": {"v": "%"},
    "C7": {"f": "=IFERROR(C6/C2*100,0)"},
    "D7": {"f": "=IFERROR(D6/D2*100,0)"},
    "E7": {"f": "=IFERROR(E6/E2*100,0)"},
    "F7": {"f": "=IFERROR(F6/F2*100,0)"},
    "G7": {"f": "=IFERROR(G6/G2*100,0)"},
    "H7": {"f": "=IFERROR(H6/H2*100,0)"},
    # Net income
    "A8": {"v": "净利润"}, "B8": {"v": "亿元"},
    "A9": {"v": "  净利率"}, "B9": {"v": "%"},
    "C9": {"f": "=IFERROR(C8/C2*100,0)"},
    "D9": {"f": "=IFERROR(D8/D2*100,0)"},
    "E9": {"f": "=IFERROR(E8/E2*100,0)"},
    "F9": {"f": "=IFERROR(F8/F2*100,0)"},
    "G9": {"f": "=IFERROR(G8/G2*100,0)"},
    "H9": {"f": "=IFERROR(H8/H2*100,0)"},
    # Cash flow
    "A11": {"v": "经营活动现金流"}, "B11": {"v": "亿元"},
    "A12": {"v": "资本开支"}, "B12": {"v": "亿元"},
    "A13": {"v": "自由现金流"}, "B13": {"v": "亿元"},
    "C13": {"f": "=C11-C12"},
    "D13": {"f": "=D11-D12"},
    "E13": {"f": "=E11-E12"},
    "F13": {"f": "=F11-F12"},
    "G13": {"f": "=G11-G12"},
    "H13": {"f": "=H11-H12"},
    # Valuation assumptions
    "A15": {"v": "估值假设"},
    "A16": {"v": "  折现率 WACC"}, "B16": {"v": "%"}, "C16": {"v": "10"},
    "A17": {"v": "  永续增长率 g"}, "B17": {"v": "%"}, "C17": {"v": "3"},
    "A19": {"v": "内在价值"},
    "A20": {"v": "  现值 (亿元)"},
    "A21": {"v": "  每股价值 (元)"},
    "A22": {"v": "  总股本 (亿股)"},
}


def default_valuation_sheet(sheet_id: str = "sheet-1", name: str = "估值表") -> dict:
    """Return a single-sheet dict for the canonical DCF template."""
    return {
        "id": sheet_id,
        "name": name,
        "rows": 22,
        "cols": 8,
        "cells": {k: dict(v) for k, v in _DCF_CELLS.items()},
        "col_widths": [160, 60, 90, 90, 90, 90, 90, 90],
    }


def default_valuation_workbook() -> dict:
    """Return a full workbook with one sheet containing the DCF template."""
    sheet = default_valuation_sheet()
    return {
        "active_sheet_id": sheet["id"],
        "sheets": [sheet],
    }


def sensitivity_sheet(sheet_id: str = "sheet-2", name: str = "敏感性分析") -> dict:
    """A 2D WACC × g sensitivity table. Used by the sensitivity skill."""
    cells: dict[str, dict[str, Any]] = {
        "A1": {"v": "WACC\\g"},
        "B1": {"v": "2%"}, "C1": {"v": "2.5%"}, "D1": {"v": "3%"},
        "E1": {"v": "3.5%"}, "F1": {"v": "4%"},
        "A2": {"v": "8%"}, "A3": {"v": "9%"}, "A4": {"v": "10%"},
        "A5": {"v": "11%"}, "A6": {"v": "12%"},
    }
    return {
        "id": sheet_id, "name": name,
        "rows": 6, "cols": 6,
        "cells": cells,
        "col_widths": [80, 80, 80, 80, 80, 80],
    }


def three_statements_workbook() -> dict:
    """Three-statement scaffold — 10-yr IS / BS / CF headers, empty body."""
    years = ["2020A", "2021A", "2022A", "2023A", "2024A",
             "2025E", "2026E", "2027E", "2028E", "2029E"]

    def _with_header(sheet_id: str, name: str, row_labels: list[str]) -> dict:
        cells: dict[str, dict[str, Any]] = {"A1": {"v": "项目"}}
        for i, y in enumerate(years):
            cells[f"{chr(ord('B') + i)}1"] = {"v": y}
        for r, label in enumerate(row_labels):
            cells[f"A{r + 2}"] = {"v": label}
        return {
            "id": sheet_id, "name": name,
            "rows": max(10, len(row_labels) + 1),
            "cols": 1 + len(years),
            "cells": cells,
            "col_widths": [160] + [80] * len(years),
        }

    income = _with_header("is", "利润表", [
        "营业收入", "营业成本", "毛利润", "销售费用",
        "管理费用", "研发费用", "营业利润", "利润总额",
        "所得税", "净利润",
    ])
    balance = _with_header("bs", "资产负债表", [
        "货币资金", "应收账款", "存货", "流动资产合计",
        "固定资产", "无形资产", "资产总计",
        "短期借款", "应付账款", "流动负债合计",
        "长期借款", "负债合计", "股东权益",
    ])
    cash = _with_header("cf", "现金流量表", [
        "经营活动现金流入", "经营活动现金流出", "经营活动现金流净额",
        "投资活动现金流净额", "筹资活动现金流净额",
        "现金净增加额", "期初现金余额", "期末现金余额",
    ])
    return {
        "active_sheet_id": "is",
        "sheets": [income, balance, cash],
    }


# ── Normalization ─────────────────────────────────────────────────


def _coerce_sheet(sheet: dict[str, Any]) -> dict[str, Any]:
    """Shape-check a single sheet. Returns a clean copy."""
    if not isinstance(sheet, dict):
        raise WorkbookValidationError("each sheet must be an object")
    sid = str(sheet.get("id") or "").strip()
    if not sid:
        sid = f"sheet-{uuid.uuid4().hex[:8]}"
    name = str(sheet.get("name") or "Sheet1").strip()[:MAX_SHEET_NAME_LEN] or "Sheet1"
    rows = int(sheet.get("rows") or 0)
    cols = int(sheet.get("cols") or 0)
    if rows < 1 or rows > MAX_ROWS_PER_SHEET:
        raise WorkbookValidationError(
            f"sheet '{name}': rows {rows} out of 1..{MAX_ROWS_PER_SHEET}"
        )
    if cols < 1 or cols > MAX_COLS_PER_SHEET:
        raise WorkbookValidationError(
            f"sheet '{name}': cols {cols} out of 1..{MAX_COLS_PER_SHEET}"
        )
    raw_cells = sheet.get("cells") or {}
    if not isinstance(raw_cells, dict):
        raise WorkbookValidationError(
            f"sheet '{name}': cells must be a dict keyed by A1-style refs"
        )
    clean_cells: dict[str, dict[str, Any]] = {}
    for k, cell in raw_cells.items():
        if not isinstance(k, str) or not A1_RE.match(k):
            # Ignore malformed keys silently — common when frontend sends
            # an extra empty-edit cell keyed on e.g. "A1 ". We won't refuse
            # the whole save for a stray key.
            continue
        if not isinstance(cell, dict):
            continue
        v = cell.get("v")
        f = cell.get("f")
        out: dict[str, Any] = {}
        if v is not None:
            vs = str(v)
            if len(vs) > MAX_CELL_STRING_LEN:
                raise WorkbookValidationError(
                    f"sheet '{name}' cell {k}: value exceeds "
                    f"{MAX_CELL_STRING_LEN} chars"
                )
            out["v"] = vs
        if f is not None:
            fs = str(f)
            if len(fs) > MAX_CELL_STRING_LEN:
                raise WorkbookValidationError(
                    f"sheet '{name}' cell {k}: formula exceeds "
                    f"{MAX_CELL_STRING_LEN} chars"
                )
            if not fs.startswith("="):
                # Canonicalize: a formula must start with '='. If the caller
                # forgot, treat it as a raw value instead.
                out["v"] = out.get("v") or fs
            else:
                out["f"] = fs
        if out:
            clean_cells[k] = out

    col_widths = sheet.get("col_widths")
    if col_widths is not None and not isinstance(col_widths, list):
        col_widths = None
    if isinstance(col_widths, list):
        # Coerce to ints, cap length to sheet.cols, drop anything absurd.
        cleaned: list[int] = []
        for w in col_widths[:cols]:
            try:
                iw = int(w)
            except Exception:
                iw = 0
            if iw < 20 or iw > 600:
                iw = 0
            cleaned.append(iw)
        col_widths = cleaned

    return {
        "id": sid,
        "name": name,
        "rows": rows,
        "cols": cols,
        "cells": clean_cells,
        "col_widths": col_widths,
    }


def validate_for_write(data: Any) -> dict:
    """Accept either a legacy flat shape or a multi-sheet shape; return
    the canonical multi-sheet shape for persistence.

    Raises :class:`WorkbookValidationError` on malformed input.
    """
    if not isinstance(data, dict):
        raise WorkbookValidationError("workbook payload must be an object")

    # Flat legacy shape → wrap in a single sheet.
    if "sheets" not in data and ("cells" in data or "rows" in data):
        sheet = _coerce_sheet({
            "id": "sheet-1",
            "name": str(data.get("title") or "估值表"),
            "rows": int(data.get("rows") or 22),
            "cols": int(data.get("cols") or 8),
            "cells": data.get("cells") or {},
            "col_widths": data.get("col_widths") or None,
        })
        return {"active_sheet_id": sheet["id"], "sheets": [sheet]}

    raw_sheets = data.get("sheets")
    if not isinstance(raw_sheets, list) or not raw_sheets:
        raise WorkbookValidationError("workbook must contain at least one sheet")
    if len(raw_sheets) > MAX_SHEETS:
        raise WorkbookValidationError(
            f"too many sheets: {len(raw_sheets)} > {MAX_SHEETS}"
        )

    seen_ids: set[str] = set()
    cleaned_sheets: list[dict[str, Any]] = []
    total_cells = 0
    for s in raw_sheets:
        sheet = _coerce_sheet(s)
        if sheet["id"] in seen_ids:
            sheet["id"] = f"sheet-{uuid.uuid4().hex[:8]}"
        seen_ids.add(sheet["id"])
        total_cells += len(sheet["cells"])
        if total_cells > MAX_CELLS_TOTAL:
            raise WorkbookValidationError(
                f"total populated cells {total_cells} exceeds {MAX_CELLS_TOTAL}"
            )
        cleaned_sheets.append(sheet)

    active = str(data.get("active_sheet_id") or "").strip()
    if active not in seen_ids:
        active = cleaned_sheets[0]["id"]
    return {"active_sheet_id": active, "sheets": cleaned_sheets}


def normalize_for_read(data: Any) -> dict:
    """Return a multi-sheet workbook shape regardless of what's stored.

    Never raises — treats malformed persisted data as an empty workbook with
    the DCF default. Used by the GET paths that must not fail if a legacy
    row has unexpected fields.
    """
    if not isinstance(data, dict):
        return default_valuation_workbook()
    if "sheets" in data and isinstance(data["sheets"], list) and data["sheets"]:
        # Fast-path: already multi-sheet. Still run a defensive coerce on
        # each sheet so a corrupt row doesn't crash the UI.
        try:
            return validate_for_write(data)
        except WorkbookValidationError:
            return default_valuation_workbook()
    # Legacy shape — wrap into single sheet.
    try:
        return validate_for_write(data)
    except WorkbookValidationError:
        return default_valuation_workbook()


# ── Template interpolation (used by skill install) ───────────────


_INTERP_RE = re.compile(r"\{\{\s*([a-z_][a-z0-9_]*)\s*\}\}")


def interpolate(s: str, variables: dict[str, Any]) -> str:
    """Replace ``{{name}}`` tokens in ``s`` with values from ``variables``.

    Unknown tokens pass through unchanged. All values are stringified. Used
    for skill templates — variables are whitelisted at the call site, so
    this function itself does not enforce a whitelist.
    """
    if not s or "{{" not in s:
        return s

    def sub(m: re.Match[str]) -> str:
        k = m.group(1)
        if k in variables:
            return str(variables[k])
        return m.group(0)

    return _INTERP_RE.sub(sub, s)


def interpolate_workbook(workbook: dict, variables: dict[str, Any]) -> dict:
    """Apply ``interpolate`` to every cell value and formula in a workbook."""
    out = {"active_sheet_id": workbook.get("active_sheet_id"), "sheets": []}
    for sheet in workbook.get("sheets") or []:
        new_cells: dict[str, dict[str, Any]] = {}
        for k, cell in (sheet.get("cells") or {}).items():
            c: dict[str, Any] = {}
            if "v" in cell:
                c["v"] = interpolate(str(cell["v"]), variables)
            if "f" in cell:
                c["f"] = interpolate(str(cell["f"]), variables)
            new_cells[k] = c
        out["sheets"].append({**sheet, "cells": new_cells})
    return out


def default_variables(
    *,
    stock_name: str = "",
    ticker: str = "",
    market: str = "",
    user_name: str = "",
) -> dict[str, str]:
    """The whitelist of interpolation variables available to skill templates."""
    return {
        "stock_name": stock_name or "",
        "ticker": ticker or "",
        "market": market or "",
        "today": date.today().isoformat(),
        "user_name": user_name or "",
    }
