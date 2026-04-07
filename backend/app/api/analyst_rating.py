"""Analyst / Brokerage Rating API — serves backtested analyst scoring data.

Reads pre-computed CSV files from the comment_analyze pipeline and serves
analyst rankings, brokerage rankings, and individual report scores.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from backend.app.deps import get_current_user
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()

# Path to the analyst ranking output directory
_OUTPUT_DIR = Path("/home/ygwang/comment_analyze/output")


# ─── Response schemas ────────────────────────────────────────────

class AnalystEntry(BaseModel):
    rank: int
    analyst_id: str
    analyst_name: str
    org_name: str
    total_calls: int
    hits: int
    hit_rate: float
    avg_excess_return: float
    avg_confidence: float
    bullish_count: int
    bearish_count: int


class BrokerageEntry(BaseModel):
    rank: int
    org_name: str
    total_calls: int
    hits: int
    hit_rate: float
    avg_excess_return: float
    num_analysts: int


class ReportEntry(BaseModel):
    report_id: int
    ticker: str
    title: str
    authors: str
    org_name: str
    publish_date: str
    signal: str
    confidence: float
    reason: str
    excess_return_5d: float | None = None
    excess_return_20d: float | None = None
    excess_return_60d: float | None = None
    return_5d: float | None = None
    return_20d: float | None = None
    return_60d: float | None = None


class AnalystRatingResponse(BaseModel):
    analysts: list[AnalystEntry]
    brokerages: list[BrokerageEntry]
    reports: list[ReportEntry]
    total_analysts: int
    total_brokerages: int
    total_reports: int
    orgs: list[str]


# ─── CSV parsing helpers ────────────────────────────────────────

def _safe_float(val: str, default: float | None = None) -> float | None:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val: str, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _read_analyst_csv(window: str) -> list[AnalystEntry]:
    path = _OUTPUT_DIR / f"analyst_ranking_{window}.csv"
    if not path.exists():
        return []
    entries = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            aid = row.get("analyst_id", "")
            # analyst_id format: "Name @ Org"
            parts = aid.split(" @ ", 1)
            analyst_name = parts[0].strip() if parts else aid
            org_name = parts[1].strip() if len(parts) > 1 else ""
            entries.append(AnalystEntry(
                rank=i,
                analyst_id=aid,
                analyst_name=analyst_name,
                org_name=org_name,
                total_calls=_safe_int(row.get("total_calls", "0")),
                hits=_safe_int(row.get("hits", "0")),
                hit_rate=_safe_float(row.get("hit_rate", "0"), 0.0),
                avg_excess_return=_safe_float(row.get("avg_excess_return", "0"), 0.0),
                avg_confidence=_safe_float(row.get("avg_confidence", "0"), 0.0),
                bullish_count=_safe_int(row.get("bullish_count", "0")),
                bearish_count=_safe_int(row.get("bearish_count", "0")),
            ))
    return entries


def _read_brokerage_csv(window: str) -> list[BrokerageEntry]:
    path = _OUTPUT_DIR / f"brokerage_ranking_{window}.csv"
    if not path.exists():
        return []
    entries = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            entries.append(BrokerageEntry(
                rank=i,
                org_name=row.get("org_name", ""),
                total_calls=_safe_int(row.get("total_calls", "0")),
                hits=_safe_int(row.get("hits", "0")),
                hit_rate=_safe_float(row.get("hit_rate", "0"), 0.0),
                avg_excess_return=_safe_float(row.get("avg_excess_return", "0"), 0.0),
                num_analysts=_safe_int(row.get("num_analysts", "0")),
            ))
    return entries


def _read_reports_csv() -> list[ReportEntry]:
    path = _OUTPUT_DIR / "all_reports_scored.csv"
    if not path.exists():
        return []
    entries = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entries.append(ReportEntry(
                report_id=_safe_int(row.get("report_id", "0")),
                ticker=row.get("ticker", ""),
                title=row.get("title", ""),
                authors=row.get("authors", ""),
                org_name=row.get("org_name", ""),
                publish_date=row.get("publish_date", ""),
                signal=row.get("signal", ""),
                confidence=_safe_float(row.get("confidence", "0"), 0.0),
                reason=row.get("reason", ""),
                excess_return_5d=_safe_float(row.get("excess_return_5d")),
                excess_return_20d=_safe_float(row.get("excess_return_20d")),
                excess_return_60d=_safe_float(row.get("excess_return_60d")),
                return_5d=_safe_float(row.get("return_5d")),
                return_20d=_safe_float(row.get("return_20d")),
                return_60d=_safe_float(row.get("return_60d")),
            ))
    return entries


# ─── Endpoints ──────────────────────────────────────────────────

@router.get("", response_model=AnalystRatingResponse)
async def get_analyst_rating(
    window: str = Query("5d", pattern="^(5d|20d|60d)$"),
    min_calls: int = Query(3, ge=1),
    org: str = Query("", description="Filter by brokerage name"),
    user: User = Depends(get_current_user),
):
    """Return analyst and brokerage rankings for a given time window."""
    analysts = _read_analyst_csv(window)
    brokerages = _read_brokerage_csv(window)
    reports = _read_reports_csv()

    # Apply min_calls filter
    analysts = [a for a in analysts if a.total_calls >= min_calls]
    brokerages = [b for b in brokerages if b.total_calls >= min_calls]

    # Apply org filter
    if org:
        analysts = [a for a in analysts if org in a.org_name]
        brokerages = [b for b in brokerages if org in b.org_name]
        reports = [r for r in reports if org in r.org_name]

    # Re-rank after filtering
    for i, a in enumerate(analysts, 1):
        a.rank = i
    for i, b in enumerate(brokerages, 1):
        b.rank = i

    # Collect unique org names for filter dropdown
    all_brokerages = _read_brokerage_csv(window)
    orgs = sorted(set(b.org_name for b in all_brokerages if b.org_name))

    return AnalystRatingResponse(
        analysts=analysts,
        brokerages=brokerages,
        reports=reports,
        total_analysts=len(analysts),
        total_brokerages=len(brokerages),
        total_reports=len(reports),
        orgs=orgs,
    )


@router.get("/analyst/{analyst_id}/reports", response_model=list[ReportEntry])
async def get_analyst_reports(
    analyst_id: str,
    user: User = Depends(get_current_user),
):
    """Return all reports for a specific analyst."""
    reports = _read_reports_csv()
    # analyst_id format: "Name @ Org" — match against authors + org_name
    parts = analyst_id.split(" @ ", 1)
    name = parts[0].strip()
    org = parts[1].strip() if len(parts) > 1 else ""

    matched = []
    for r in reports:
        if name in r.authors and (not org or org == r.org_name):
            matched.append(r)

    # Sort by publish_date descending
    matched.sort(key=lambda r: r.publish_date, reverse=True)
    return matched
