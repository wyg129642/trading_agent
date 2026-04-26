"""SentimenTrader market sentiment indicators.

Reads from MongoDB `sentimentrader.indicators`, populated by
`crawl/sentimentrader/scraper.py`. Three indicators: Smart/Dumb Money
Confidence Spread, CNN Fear & Greed Model, QQQ Optix. Updated once daily
after US market close.

Source: https://sentimentrader.com (paid subscription — user-owned).
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timezone
from email.utils import formatdate, parsedate_to_datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, Response
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

# Must match SENTIMENTRADER_IMAGE_DIR in crawl/sentimentrader/scraper.py
_IMAGE_DIR = Path(os.environ.get("SENTIMENTRADER_IMAGE_DIR",
                                  "/home/ygwang/crawl_data/sentimentrader_images"))

_VALID_SLUGS = {"smart_dumb_spread", "cnn_fear_greed", "etf_qqq", "smart_dumb"}

logger = logging.getLogger(__name__)
router = APIRouter()

# Indicator response is daily-fresh data; cache 60s to absorb the page-load
# burst (4 cards × N users hitting at the same time) and skip the remote-Mongo
# round-trip when the event loop is contended.
_INDICATORS_CACHE_TTL = 60.0
_indicators_cache: dict[str, Any] = {"ts": 0.0, "payload": None}

# Chart PNGs are ~110 KB each, total <500 KB — keyed by (slug, mtime) so a
# fresh scraper run invalidates automatically.
_image_cache: dict[str, tuple[float, bytes, str]] = {}  # slug -> (mtime, bytes, etag)


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    settings = get_settings()
    # Re-use alphapai URI since all crawlers live on the same local mongo host.
    return AsyncIOMotorClient(settings.sentimentrader_mongo_uri, tz_aware=True)


def _mongo_db() -> AsyncIOMotorDatabase:
    settings = get_settings()
    return _mongo_client()[settings.sentimentrader_mongo_db]


# Sparkline resolution — ~90 trading days ≈ 4.5 months back. Enough to show
# trend without bloating the payload.
SPARKLINE_POINTS = 90


def _to_date_iso(ts_ms: int | float | None) -> str | None:
    if not ts_ms:
        return None
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).date().isoformat()
    except Exception:
        return None


def _shape_indicator(doc: dict) -> dict:
    """Convert a MongoDB indicator document into the public API shape."""
    hist = doc.get("history_trimmed") or []
    bench = doc.get("benchmark_trimmed") or []
    # Keep the most-recent SPARKLINE_POINTS data points — the UI renders these.
    sparkline = [
        {"t": _to_date_iso(p[0]), "v": p[1]}
        for p in hist[-SPARKLINE_POINTS:]
        if isinstance(p, (list, tuple)) and len(p) >= 2
    ]
    benchmark_spark = [
        {"t": _to_date_iso(p[0]), "v": p[1]}
        for p in bench[-SPARKLINE_POINTS:]
        if isinstance(p, (list, tuple)) and len(p) >= 2
    ]
    slug = doc.get("slug")
    # Chart image (PNG screenshot of the real Highcharts rendering). Only
    # expose the URL if the file actually exists on disk — older docs created
    # before screenshot capture was added won't have one.
    image_url = None
    shot = doc.get("screenshot_path")
    if shot and slug in _VALID_SLUGS and Path(shot).exists():
        # Relative to the axios `api` baseURL (/api) — the frontend uses
        # `api.get(image_url)` so we don't want to include the /api prefix here.
        image_url = f"/sentimentrader/chart/{slug}.png"

    # Optional secondary indicator (dual-line charts like smart_dumb).
    secondary = None
    if doc.get("secondary_indicator_name"):
        sec_hist = doc.get("secondary_history_trimmed") or []
        secondary = {
            "name": doc["secondary_indicator_name"],
            "latest_value": doc.get("secondary_latest_value"),
            "sparkline": [
                {"t": _to_date_iso(p[0]), "v": p[1]}
                for p in sec_hist[-SPARKLINE_POINTS:]
                if isinstance(p, (list, tuple)) and len(p) >= 2
            ],
        }

    return {
        "slug": slug,
        "name": doc.get("name") or doc.get("indicator_name") or doc.get("slug"),
        "indicator_name": doc.get("indicator_name"),
        "chart_title": doc.get("chart_title"),
        "latest_value": doc.get("latest_value"),
        "latest_date": _to_date_iso(doc.get("latest_ts_ms")),
        "benchmark": {
            "name": doc.get("benchmark_name"),
            "value": doc.get("latest_benchmark_value"),
        },
        "image_url": image_url,
        "sparkline": sparkline,
        "benchmark_sparkline": benchmark_spark,
        "secondary": secondary,
        "source_url": doc.get("source_url"),
        "updated_at": (doc.get("updated_at").isoformat() if hasattr(doc.get("updated_at"), "isoformat") else doc.get("updated_at")),
    }


class IndicatorsResponse(BaseModel):
    indicators: list[dict]
    source: str
    source_url: str
    updated_at: str | None


@router.get("/indicators", response_model=IndicatorsResponse)
async def list_indicators(user: User = Depends(get_current_user)):
    """Return the current snapshot of all SentimenTrader indicators.

    Response preserves insertion order (smart/dumb spread → fear&greed → qqq)
    so the frontend can render cards left-to-right consistently.

    Cached for ``_INDICATORS_CACHE_TTL`` seconds so the dashboard's burst load
    (4 cards × every authed user) doesn't repeatedly drag a 4-doc/3000-point
    payload over the wire from remote Mongo only to slice it down to 90 points
    in ``_shape_indicator``. The underlying scraper writes once per day.
    """
    now = time.monotonic()
    cached = _indicators_cache.get("payload")
    if cached is not None and (now - _indicators_cache["ts"]) < _INDICATORS_CACHE_TTL:
        return cached

    settings = get_settings()
    db = _mongo_db()
    # 2026-04-23 迁移: sentimentrader 合并到 funda DB,collection 加前缀
    # `sentimentrader_indicators`(由 settings.sentimentrader_collection 配置)
    col = db[getattr(settings, "sentimentrader_collection", "indicators")]
    # Explicit slug order so the UI is deterministic. Keep the two smart/dumb
    # cards adjacent — spread on the left (derivative), raw confidences next.
    slug_order = ["smart_dumb_spread", "smart_dumb", "cnn_fear_greed", "etf_qqq"]
    # $slice tail: only the most recent SPARKLINE_POINTS data points are ever
    # rendered, but the docs hold ~750 each. Pulling 750×4 arrays from remote
    # Mongo for a 90-point sparkline was ~700 ms wire time + JSON encode.
    projection = {
        "history_trimmed": {"$slice": -SPARKLINE_POINTS},
        "benchmark_trimmed": {"$slice": -SPARKLINE_POINTS},
        "secondary_history_trimmed": {"$slice": -SPARKLINE_POINTS},
    }
    docs = await col.find({"slug": {"$in": slug_order}}, projection).to_list(length=10)
    by_slug = {d["slug"]: d for d in docs}

    shaped = [_shape_indicator(by_slug[s]) for s in slug_order if s in by_slug]
    if not shaped:
        raise HTTPException(
            status_code=503,
            detail="sentimentrader data not yet scraped — run crawl/sentimentrader/scraper.py first",
        )

    # The most recent updated_at across all indicators. Best proxy for "data freshness".
    latest = max(
        (d.get("updated_at") for d in docs if d.get("updated_at")),
        default=None,
    )
    payload = {
        "indicators": shaped,
        "source": "SentimenTrader",
        "source_url": "https://sentimentrader.com",
        "updated_at": latest.isoformat() if hasattr(latest, "isoformat") else None,
    }
    _indicators_cache["payload"] = payload
    _indicators_cache["ts"] = now
    return payload


@router.get("/chart/{slug}.png")
async def chart_image(
    slug: str,
    request: Request,
    user: User = Depends(get_current_user),
):
    """Serve the latest Playwright screenshot of the real Highcharts chart.

    Fresh scrapes (hourly cron) overwrite the file on disk in `_IMAGE_DIR`,
    so disk is always the authoritative source. The 2026-04-23 GridFS
    migration left a one-shot snapshot in funda's bucket but ongoing runs
    don't write there — the unified `stream_pdf_or_file` heuristic also
    can't reconcile sentimentrader's `<platform>/<dir>/<file>` GridFS
    filename with the `_IMAGE_DIR`-rooted disk layout. Serve disk only.

    Caches bytes in-process keyed by file mtime (~440 KB total for all 4
    PNGs). Sets ``Cache-Control: private, max-age=300`` + ``ETag`` so the
    browser short-circuits subsequent loads with a conditional request and
    we return ``304 Not Modified`` without re-reading the file.
    """
    if slug not in _VALID_SLUGS:
        raise HTTPException(status_code=404, detail="unknown indicator slug")
    file_path = _IMAGE_DIR / f"{slug}.png"
    try:
        st = file_path.stat()
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"screenshot missing — run crawl/sentimentrader/scraper.py to refresh",
        )

    cached = _image_cache.get(slug)
    if cached and cached[0] == st.st_mtime:
        _, body, etag = cached
    else:
        body = file_path.read_bytes()
        etag = f'W/"{slug}-{int(st.st_mtime)}-{len(body)}"'
        _image_cache[slug] = (st.st_mtime, body, etag)

    last_modified = formatdate(st.st_mtime, usegmt=True)

    inm = request.headers.get("if-none-match")
    if inm and etag in [v.strip() for v in inm.split(",")]:
        return Response(
            status_code=304,
            headers={
                "ETag": etag,
                "Last-Modified": last_modified,
                "Cache-Control": "private, max-age=300",
            },
        )
    ims = request.headers.get("if-modified-since")
    if ims:
        try:
            ims_dt = parsedate_to_datetime(ims)
            if ims_dt and ims_dt.timestamp() >= int(st.st_mtime):
                return Response(
                    status_code=304,
                    headers={
                        "ETag": etag,
                        "Last-Modified": last_modified,
                        "Cache-Control": "private, max-age=300",
                    },
                )
        except (TypeError, ValueError):
            pass

    return Response(
        content=body,
        media_type="image/png",
        headers={
            "ETag": etag,
            "Last-Modified": last_modified,
            "Cache-Control": "private, max-age=300",
            "Content-Length": str(len(body)),
        },
    )
