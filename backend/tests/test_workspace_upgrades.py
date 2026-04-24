"""Pure-Python tests for the workspace redesign backend.

Covers the new primitives added in the workspace-v2 cut:

* Multi-sheet workbook schema (``user_kb_workbook.validate_for_write`` +
  ``normalize_for_read``) — shape validation, caps, legacy-wrap.
* Template interpolation used by skill install.
* Skill factories (DCF / 三张报表 / 敏感性 / 同业对比).

We deliberately avoid Mongo / Postgres here — those paths are covered by
the existing ``test_user_kb_service.py`` integration tests against a
running Mongo container. Unit-testing the pure helpers keeps this file
fast (< 20 ms) and runnable without any services up.
"""
from __future__ import annotations

import pytest

from backend.app.services import user_kb_workbook as wb
from backend.app.services import kb_skills_service as ks


# ── Default workbook ────────────────────────────────────────────


def test_default_valuation_workbook_shape():
    w = wb.default_valuation_workbook()
    assert set(w.keys()) == {"active_sheet_id", "sheets"}
    assert len(w["sheets"]) == 1
    sheet = w["sheets"][0]
    assert sheet["name"] == "估值表"
    assert sheet["rows"] >= 22
    assert sheet["cols"] == 8
    assert "A1" in sheet["cells"]
    # Workbook is JSON-serializable end to end.
    import json
    json.dumps(w)


def test_three_statements_workbook_has_three_sheets():
    w = wb.three_statements_workbook()
    assert len(w["sheets"]) == 3
    assert {s["name"] for s in w["sheets"]} == {"利润表", "资产负债表", "现金流量表"}


# ── validate_for_write ──────────────────────────────────────────


def test_validate_for_write_accepts_legacy_flat_shape():
    flat = {"rows": 3, "cols": 2, "cells": {"A1": {"v": "x"}, "B2": {"f": "=A1"}}}
    got = wb.validate_for_write(flat)
    assert len(got["sheets"]) == 1
    s = got["sheets"][0]
    assert s["rows"] == 3 and s["cols"] == 2
    assert s["cells"]["A1"] == {"v": "x"}
    assert s["cells"]["B2"] == {"f": "=A1"}


def test_validate_for_write_canonicalizes_missing_formula_prefix():
    # Formula without a leading = gets coerced to a value, not a formula —
    # the storage shape is strict even if the caller was sloppy.
    flat = {"rows": 1, "cols": 1, "cells": {"A1": {"f": "A1+1"}}}
    got = wb.validate_for_write(flat)
    cell = got["sheets"][0]["cells"]["A1"]
    assert "f" not in cell
    assert cell["v"] == "A1+1"


def test_validate_for_write_rejects_oversized_sheet():
    with pytest.raises(wb.WorkbookValidationError):
        wb.validate_for_write({"rows": 501, "cols": 2, "cells": {}})
    with pytest.raises(wb.WorkbookValidationError):
        wb.validate_for_write({"rows": 5, "cols": 100, "cells": {}})


def test_validate_for_write_caps_sheet_count():
    many = {"sheets": [wb.default_valuation_sheet(f"s{i}", f"S{i}") for i in range(11)]}
    with pytest.raises(wb.WorkbookValidationError):
        wb.validate_for_write(many)


def _col_label(i: int) -> str:
    # 0 → A, 25 → Z, 26 → AA ... matches the A1 regex the validator accepts.
    s = ""
    while True:
        s = chr(ord("A") + (i % 26)) + s
        i = i // 26 - 1
        if i < 0:
            break
    return s


def test_validate_for_write_caps_total_cells():
    # Build 5 sheets × 50 (cols) × 500 (rows) = 125 000 cells each → 625 000
    # total, well above the 200 000 cap. Cells use valid A1 keys so the
    # per-key regex passes; the cap is what trips first.
    def mk_sheet(sid: str) -> dict:
        cells: dict[str, dict] = {}
        for c in range(50):
            col = _col_label(c)
            for r in range(1, 501):
                cells[f"{col}{r}"] = {"v": "x"}
        return {"id": sid, "name": sid, "rows": 500, "cols": 50, "cells": cells}

    # 9 sheets × (50 × 500 = 25 000 cells each) = 225 000 — above the 200 000
    # total-cell cap, below the 10-sheet cap, so the cell-count branch is
    # exercised specifically.
    huge = {"sheets": [mk_sheet(f"s{i}") for i in range(9)]}
    with pytest.raises(wb.WorkbookValidationError):
        wb.validate_for_write(huge)


def test_validate_for_write_ignores_malformed_cell_keys():
    # Malformed keys shouldn't kill the whole save — they get dropped.
    got = wb.validate_for_write({
        "rows": 2, "cols": 2,
        "cells": {
            "A1": {"v": "ok"},
            "not-a-cell-ref": {"v": "ignored"},
            "": {"v": "ignored-too"},
        },
    })
    cells = got["sheets"][0]["cells"]
    assert "A1" in cells
    assert "not-a-cell-ref" not in cells


def test_validate_for_write_dedupes_sheet_ids():
    # Two sheets claiming the same id — validator assigns a fresh id to
    # the duplicate so downstream code can key on id safely.
    a = wb.default_valuation_sheet("dup", "A")
    b = wb.default_valuation_sheet("dup", "B")
    got = wb.validate_for_write({"sheets": [a, b]})
    ids = [s["id"] for s in got["sheets"]]
    assert len(set(ids)) == 2


def test_validate_for_write_falls_back_to_first_sheet_for_bad_active_id():
    a = wb.default_valuation_sheet("real", "A")
    got = wb.validate_for_write({"sheets": [a], "active_sheet_id": "does-not-exist"})
    assert got["active_sheet_id"] == "real"


# ── normalize_for_read ──────────────────────────────────────────


def test_normalize_for_read_handles_none_and_junk():
    # Never raises, always returns the canonical shape.
    assert isinstance(wb.normalize_for_read(None), dict)
    assert "sheets" in wb.normalize_for_read(None)
    # Junk payload also survives.
    assert "sheets" in wb.normalize_for_read({"bogus": True})
    assert "sheets" in wb.normalize_for_read([])


def test_normalize_for_read_passes_through_workbook_shape():
    w = wb.default_valuation_workbook()
    got = wb.normalize_for_read(w)
    assert got["sheets"][0]["id"] == w["sheets"][0]["id"]


def test_normalize_for_read_wraps_legacy_flat():
    flat = {"rows": 2, "cols": 2, "cells": {"A1": {"v": "x"}}}
    got = wb.normalize_for_read(flat)
    assert len(got["sheets"]) == 1
    assert got["sheets"][0]["cells"]["A1"]["v"] == "x"


# ── Interpolation ───────────────────────────────────────────────


def test_interpolate_replaces_known_tokens():
    s = wb.interpolate("Hello {{stock_name}} at {{today}}", {
        "stock_name": "英伟达", "today": "2026-04-23",
    })
    assert s == "Hello 英伟达 at 2026-04-23"


def test_interpolate_passes_unknown_tokens_through():
    assert wb.interpolate("x={{unknown}}", {}) == "x={{unknown}}"


def test_interpolate_workbook_applies_to_every_cell():
    wbk = {
        "active_sheet_id": "s1",
        "sheets": [{
            "id": "s1", "name": "s",
            "rows": 1, "cols": 2,
            "cells": {
                "A1": {"v": "{{stock_name}}"},
                "B1": {"f": "={{ticker}}+1"},
            },
        }],
    }
    got = wb.interpolate_workbook(wbk, {"stock_name": "NV", "ticker": "A1"})
    assert got["sheets"][0]["cells"]["A1"]["v"] == "NV"
    assert got["sheets"][0]["cells"]["B1"]["f"] == "=A1+1"


def test_default_variables_whitelist():
    vs = wb.default_variables(stock_name="英伟达", ticker="NVDA", market="美股")
    assert vs["stock_name"] == "英伟达"
    assert vs["ticker"] == "NVDA"
    assert vs["market"] == "美股"
    assert "today" in vs  # auto-injected
    # Defaults to empty string for anything omitted.
    assert vs["user_name"] == ""


# ── Skill factories ─────────────────────────────────────────────


def test_skill_factories_return_valid_workbooks():
    for name in ["dcf_standard", "three_statements", "dcf_with_sensitivity",
                 "sensitivity_only", "peer_comparison"]:
        wbk = ks.factory(name)
        assert wbk is not None, f"factory {name} returned None"
        # Must validate as a canonical workbook.
        validated = wb.validate_for_write(wbk)
        assert len(validated["sheets"]) >= 1
        assert validated["active_sheet_id"] in {s["id"] for s in validated["sheets"]}


def test_skill_factory_unknown_name_returns_none():
    assert ks.factory("nonexistent_template") is None


# ── System skill definitions ────────────────────────────────────


def test_system_skills_have_valid_spec_shape():
    # The ``_SYSTEM_SKILLS`` list is used by ``ensure_system_skills`` at
    # startup; each entry's spec must validate without reaching Mongo or
    # Postgres. A missing field here would mean a broken startup.
    from backend.app.services.kb_skills_service import _SYSTEM_SKILLS, _validate_spec
    assert len(_SYSTEM_SKILLS) >= 4
    for defn in _SYSTEM_SKILLS:
        assert "slug" in defn and defn["slug"]
        assert "name" in defn and defn["name"]
        assert "spec" in defn
        _validate_spec(defn["spec"])
