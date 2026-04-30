"""Multi-Bagger Hunt screener API.

Reads outputs from the standalone stock-filtering pipeline at
/home/ygwang/stock-filtering/data/outputs/<asof>/. Each as-of date contains:
  - screener_full.csv      (universe with every metric)
  - watchlist_top<N>.csv   (composite-ranked top N)
  - watchlist_primed.csv   (Stage-1 score >= threshold)
  - stage2_triggers.csv    (today's breakouts)
  - report.html            (human-readable digest)
  - run_meta.json          (config snapshot + regime + counts)

This router exposes the CSVs as JSON for the React UI. It also surfaces a
trigger endpoint for admins to kick off a fresh run (subprocess).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import subprocess
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

from backend.app.deps import get_current_admin, get_current_user

router = APIRouter()
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration — where stockfilter writes its outputs.
# ---------------------------------------------------------------------------

SCREENER_HOME = Path(
    os.environ.get("STOCKFILTER_HOME", "/home/ygwang/stock-filtering")
)
OUTPUTS_DIR = SCREENER_HOME / "data" / "outputs"
PYTHON_BIN = os.environ.get("STOCKFILTER_PYTHON", "python3")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class AsofEntry(BaseModel):
    asof: str
    has_full: bool
    has_top: bool
    has_primed: bool
    has_stage2: bool
    has_meta: bool
    has_html: bool
    rows_screener: int | None = None
    rows_primed: int | None = None
    rows_stage2: int | None = None
    regime: str | None = None
    wrote_at: float | None = None


class TriggerRequest(BaseModel):
    asof: str | None = None
    top: int = 50
    limit: int | None = None
    years: int | None = None


class TriggerResponse(BaseModel):
    started: bool
    pid: int | None = None
    asof: str | None = None
    cmd: list[str]
    note: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_outputs_root() -> None:
    if not OUTPUTS_DIR.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Screener outputs directory missing: {OUTPUTS_DIR}. "
                   f"Run `stockfilter screen` first.",
        )


def _list_asof_dirs() -> list[Path]:
    if not OUTPUTS_DIR.exists():
        return []
    return sorted(
        [p for p in OUTPUTS_DIR.iterdir() if p.is_dir() and p.name.isdigit()],
        key=lambda p: p.name,
        reverse=True,
    )


def _read_meta(asof_dir: Path) -> dict[str, Any] | None:
    meta = asof_dir / "run_meta.json"
    if not meta.exists():
        return None
    try:
        return json.loads(meta.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("failed to parse run_meta.json at %s: %s", asof_dir, e)
        return None


def _find_top_csv(asof_dir: Path) -> Path | None:
    """The top-N watchlist filename varies (`watchlist_top10.csv`, _top50.csv)."""
    cands = sorted(asof_dir.glob("watchlist_top*.csv"))
    return cands[0] if cands else None


def _df_to_records(df: pd.DataFrame, max_rows: int | None = None) -> list[dict]:
    """JSON-safe records — replace NaN/Inf, cap rows, keep numeric types."""
    if df is None or df.empty:
        return []
    if max_rows is not None and len(df) > max_rows:
        df = df.head(max_rows)
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notna(df), None)
    records = df.to_dict(orient="records")
    # Last-mile sanitize: `NaN` may still leak through for object columns;
    # explicitly convert any remaining NaN floats.
    for r in records:
        for k, v in list(r.items()):
            if isinstance(v, float) and math.isnan(v):
                r[k] = None
    return records


def _resolve_asof(asof: str | None) -> Path:
    """Pick the latest asof if not given; raise 404 if asof has no outputs."""
    _ensure_outputs_root()
    if asof is None:
        dirs = _list_asof_dirs()
        if not dirs:
            raise HTTPException(404, "No screener outputs found yet")
        return dirs[0]
    target = OUTPUTS_DIR / asof
    if not target.exists():
        raise HTTPException(404, f"No outputs for asof={asof}")
    return target


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/asof-list", response_model=list[AsofEntry])
def asof_list() -> list[AsofEntry]:
    """All as-of dates with screener outputs, newest first."""
    out = []
    for d in _list_asof_dirs():
        meta = _read_meta(d) or {}
        out.append(AsofEntry(
            asof=d.name,
            has_full=(d / "screener_full.csv").exists(),
            has_top=_find_top_csv(d) is not None,
            has_primed=(d / "watchlist_primed.csv").exists(),
            has_stage2=(d / "stage2_triggers.csv").exists(),
            has_meta=(d / "run_meta.json").exists(),
            has_html=(d / "report.html").exists(),
            rows_screener=meta.get("rows_screener"),
            rows_primed=meta.get("rows_primed"),
            rows_stage2=meta.get("rows_stage2"),
            regime=meta.get("regime"),
            wrote_at=meta.get("wrote_at"),
        ))
    return out


@router.get("/meta")
def meta(asof: str | None = None) -> dict:
    """Read run_meta.json for the given asof (or latest)."""
    d = _resolve_asof(asof)
    m = _read_meta(d)
    if m is None:
        raise HTTPException(404, f"run_meta.json missing for {d.name}")
    # Drop the giant config_snapshot from the payload by default — UI shows
    # the regime + summary; full config is available via /config endpoint.
    light = {k: v for k, v in m.items() if k != "config_snapshot"}
    light["asof"] = d.name
    return light


@router.get("/full")
def full(
    asof: str | None = None,
    limit: int = Query(500, ge=1, le=10000),
    only_passed: bool = False,
    min_stage1: float | None = None,
) -> dict:
    """Full screener output (capped to `limit` rows). Use filters to slim it."""
    d = _resolve_asof(asof)
    fp = d / "screener_full.csv"
    if not fp.exists():
        raise HTTPException(404, "screener_full.csv missing")
    df = pd.read_csv(fp)
    if min_stage1 is not None and "stage1_score" in df.columns:
        df = df[df["stage1_score"].astype(float) >= min_stage1]
    if only_passed and "quality_pass" in df.columns:
        df = df[df["quality_pass"].astype(str).str.lower().isin(["true", "1"])]
    return {
        "asof": d.name,
        "total": int(len(df)),
        "rows": _df_to_records(df, max_rows=limit),
    }


@router.get("/watchlist")
def watchlist(
    asof: str | None = None,
    kind: Literal["top", "primed", "stage2"] = "top",
) -> dict:
    """Pre-built watchlists: `top` (composite), `primed` (stage-1), `stage2`."""
    d = _resolve_asof(asof)
    if kind == "top":
        fp = _find_top_csv(d)
    elif kind == "primed":
        fp = d / "watchlist_primed.csv"
    elif kind == "stage2":
        fp = d / "stage2_triggers.csv"
    else:
        raise HTTPException(400, f"unknown kind={kind}")

    if fp is None or not fp.exists():
        return {"asof": d.name, "kind": kind, "total": 0, "rows": []}

    df = pd.read_csv(fp)
    return {
        "asof": d.name,
        "kind": kind,
        "filename": fp.name,
        "total": int(len(df)),
        "rows": _df_to_records(df),
    }


@router.get("/report-html")
def report_html(asof: str | None = None) -> dict:
    """Inline the HTML report for embedding in an iframe-less view."""
    d = _resolve_asof(asof)
    fp = d / "report.html"
    if not fp.exists():
        raise HTTPException(404, "report.html missing")
    return {"asof": d.name, "html": fp.read_text(encoding="utf-8")}


@router.post("/trigger", response_model=TriggerResponse)
async def trigger(
    body: TriggerRequest,
    user=Depends(get_current_admin),
) -> TriggerResponse:
    """Admin-only: spawn a stockfilter screen run as a detached subprocess.

    Does NOT block — returns immediately with the PID. The caller polls
    /asof-list to detect when the new outputs land. For long full-universe
    runs (~15 min), this is the right model.
    """
    if not SCREENER_HOME.exists():
        raise HTTPException(503, f"STOCKFILTER_HOME missing: {SCREENER_HOME}")

    cmd = [
        PYTHON_BIN, "-W", "ignore", "-m", "stockfilter.cli", "screen",
        "--top", str(body.top),
    ]
    if body.asof:
        cmd += ["--asof", body.asof]
    if body.limit is not None:
        cmd += ["--limit", str(body.limit)]
    if body.years is not None:
        cmd += ["--years", str(body.years)]

    log_dir = SCREENER_HOME / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    out_log = log_dir / "trigger_last.log"

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(SCREENER_HOME),
            stdout=open(out_log, "wb"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except FileNotFoundError as e:
        raise HTTPException(500, f"failed to launch screener: {e}")

    return TriggerResponse(
        started=True, pid=proc.pid, asof=body.asof,
        cmd=cmd,
        note=f"running detached; tail {out_log}",
    )


@router.get("/log-tail")
def log_tail(lines: int = Query(50, ge=1, le=500),
             user=Depends(get_current_user)) -> dict:
    """Tail the last screener trigger log — useful for the UI 'last run' panel."""
    fp = SCREENER_HOME / "data" / "logs" / "trigger_last.log"
    if not fp.exists():
        return {"exists": False, "lines": []}
    try:
        text = fp.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        raise HTTPException(500, f"failed to read log: {e}")
    tail = text.splitlines()[-lines:]
    return {"exists": True, "lines": tail, "path": str(fp)}
