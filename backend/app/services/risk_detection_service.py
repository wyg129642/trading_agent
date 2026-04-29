"""Risk detection service — reads from detect/ project's SQLite risk store.

The detect crawler+classifier pipeline lives at /home/ygwang/detect and
maintains a SQLite database with per-stock risk signals + composite scores.
This service exposes read-only queries used by the admin Risk Detection UI.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)

DETECT_DB_PATH = os.environ.get(
    "DETECT_DB_PATH", "/home/ygwang/detect/data/risk.db"
)


@contextmanager
def _conn():
    """Read-only connection to detect's SQLite store."""
    if not os.path.exists(DETECT_DB_PATH):
        raise FileNotFoundError(
            f"detect risk DB not found at {DETECT_DB_PATH}; is the detect crawler running?"
        )
    uri = f"file:{DETECT_DB_PATH}?mode=ro"
    c = sqlite3.connect(uri, uri=True, timeout=10)
    c.row_factory = sqlite3.Row
    try:
        yield c
    finally:
        c.close()


# ── CSV parsing ─────────────────────────────────────────────────────────

def parse_candidate_csv(content: bytes | str) -> list[dict[str, Any]]:
    """Parse the open.csv format produced by the quant strategy.

    Expected columns include `secID`, `secShortName`, `tradeDate`, `type`, `actClosePrice`.
    Same secID may appear multiple times (different `type` buy tiers); we keep all rows
    but report risk per unique secID.
    """
    if isinstance(content, bytes):
        content = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(content))
    rows: list[dict[str, Any]] = []
    for r in reader:
        sec_id = (r.get("secID") or r.get("code") or r.get("代码") or "").strip()
        if not sec_id:
            continue
        sec_id = sec_id.zfill(6)
        rows.append({
            "code": sec_id,
            "name": (r.get("secShortName") or r.get("名称") or "").strip(),
            "trade_date": (r.get("tradeDate") or "").strip(),
            "type": (r.get("type") or "").strip(),
            "buy_price": (r.get("buy_price") or "").strip(),
            "actClosePrice": (r.get("actClosePrice") or "").strip(),
        })
    return rows


def dedupe_codes(rows: list[dict[str, Any]]) -> list[str]:
    seen, out = set(), []
    for r in rows:
        c = r["code"]
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ── Queries against detect.risk.db ──────────────────────────────────────

def get_risks_for_codes(codes: list[str]) -> dict[str, dict[str, Any]]:
    """Return {code: risk_summary} for codes that have any active risk."""
    if not codes:
        return {}
    placeholders = ",".join("?" * len(codes))
    out: dict[str, dict[str, Any]] = {}
    with _conn() as c:
        rows = c.execute(
            f"""SELECT r.stock_code, r.composite_score, r.tier, r.hard_blocks,
                       r.active_types, r.details, r.updated_at,
                       s.name AS stock_name, s.st_status, s.list_status
                FROM risk_list r
                LEFT JOIN stocks s ON s.code = r.stock_code
                WHERE r.stock_code IN ({placeholders})
                ORDER BY r.composite_score DESC""",
            tuple(codes),
        ).fetchall()
        for row in rows:
            d = dict(row)
            try:
                d["active_types"] = json.loads(d["active_types"] or "[]")
                d["details"] = json.loads(d["details"] or "[]")
            except json.JSONDecodeError:
                d["active_types"], d["details"] = [], []
            out[d["stock_code"]] = d
    return out


def get_recent_news_for_codes(codes: list[str], lookback_days: int = 14, per_stock: int = 10) -> dict[str, list[dict[str, Any]]]:
    """Return {code: [news/announcement items]} from sources table.

    Items include both 公告 (cninfo, eastmoney:notice, tushare:*) and 新闻 (eastmoney:stocknews, cls:telegraph).
    """
    if not codes:
        return {}
    placeholders = ",".join("?" * len(codes))
    out: dict[str, list[dict[str, Any]]] = {c: [] for c in codes}
    with _conn() as c:
        rows = c.execute(
            f"""SELECT stock_code, source, title, url, body, category, published_at, fetched_at
                FROM sources
                WHERE stock_code IN ({placeholders})
                  AND COALESCE(published_at, fetched_at) >= datetime('now', '-{int(lookback_days)} days')
                ORDER BY COALESCE(published_at, fetched_at) DESC""",
            tuple(codes),
        ).fetchall()
        for row in rows:
            code = row["stock_code"]
            if len(out[code]) >= per_stock:
                continue
            out[code].append({
                "source": row["source"],
                "title": row["title"],
                "url": row["url"],
                "category": row["category"],
                "published_at": row["published_at"],
                "fetched_at": row["fetched_at"],
                "body_preview": (row["body"] or "")[:200] if row["body"] else None,
            })
    return out


def health() -> dict[str, Any]:
    """Quick stats on the underlying detect DB."""
    try:
        with _conn() as c:
            stats = {
                "db_path": DETECT_DB_PATH,
                "sources": c.execute("SELECT COUNT(*) FROM sources").fetchone()[0],
                "signals_active": c.execute(
                    "SELECT COUNT(*) FROM signals WHERE active=1"
                ).fetchone()[0],
                "risk_list_size": c.execute("SELECT COUNT(*) FROM risk_list").fetchone()[0],
                "by_tier": dict(
                    c.execute(
                        "SELECT tier, COUNT(*) FROM risk_list GROUP BY tier"
                    ).fetchall()
                ),
                "last_crawl": dict(
                    c.execute(
                        """SELECT source, MAX(finished_at)
                           FROM crawl_runs
                           WHERE finished_at IS NOT NULL
                           GROUP BY source"""
                    ).fetchall()
                ),
            }
        return {"ok": True, **stats}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── High-level "scan a CSV" orchestrator ────────────────────────────────

def scan_candidates(
    csv_content: bytes | str,
    min_block_score: float = 70.0,
    block_tiers: tuple[str, ...] = ("HARD", "HIGH"),
    lookback_days: int = 14,
    news_per_stock: int = 10,
) -> dict[str, Any]:
    rows = parse_candidate_csv(csv_content)
    codes = dedupe_codes(rows)
    risks = get_risks_for_codes(codes)
    news = get_recent_news_for_codes(codes, lookback_days, news_per_stock)

    # Group input rows by code, preserving each tier (rows can have multiple types)
    by_code: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_code.setdefault(r["code"], []).append(r)

    items = []
    risky, kept = 0, 0
    by_tier_count: dict[str, int] = {}
    for code in codes:
        risk = risks.get(code)
        is_blocked = bool(
            risk and (
                risk["composite_score"] >= min_block_score
                or risk["tier"] in block_tiers
            )
        )
        item = {
            "code": code,
            "name": (risk or {}).get("stock_name") or by_code[code][0].get("name"),
            "input_rows": by_code[code],
            "trade_date": by_code[code][0].get("trade_date"),
            "buy_price": by_code[code][0].get("buy_price"),
            "actClosePrice": by_code[code][0].get("actClosePrice"),
            "types": [r["type"] for r in by_code[code]],
            "blocked": is_blocked,
            "risk": risk,
            "news": news.get(code, []),
        }
        items.append(item)
        if is_blocked:
            risky += 1
            t = risk["tier"]
            by_tier_count[t] = by_tier_count.get(t, 0) + 1
        else:
            kept += 1

    # Sort: blocked first, then by composite_score desc
    items.sort(key=lambda x: (
        not x["blocked"],
        -((x["risk"] or {}).get("composite_score", 0) or 0),
    ))

    return {
        "summary": {
            "total_rows": len(rows),
            "unique_codes": len(codes),
            "risky": risky,
            "kept": kept,
            "by_tier": by_tier_count,
            "block_settings": {
                "min_block_score": min_block_score,
                "block_tiers": list(block_tiers),
            },
        },
        "items": items,
    }
