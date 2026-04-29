"""Risk Detection API (admin only).

Quant strategy uploads its candidate-stock CSV (open.csv format).
We cross-reference each secID against the detect crawler's risk store
and return per-stock risk types, composite score, and supporting news/announcements.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from backend.app.deps import get_current_admin
from backend.app.models.user import User
from backend.app.services import risk_detection_service as svc

logger = logging.getLogger(__name__)
router = APIRouter()


class FilterCodesRequest(BaseModel):
    codes: list[str]
    min_block_score: float = 70.0
    block_tiers: list[str] = ["HARD", "HIGH"]
    lookback_days: int = 14


@router.get("/health")
async def risk_health(admin: User = Depends(get_current_admin)) -> dict[str, Any]:
    """Show detect DB stats — confirms crawlers are alive."""
    return svc.health()


@router.post("/scan-csv")
async def scan_csv(
    file: UploadFile = File(...),
    min_block_score: float = Form(70.0),
    block_tiers: str = Form("HARD,HIGH"),
    lookback_days: int = Form(14),
    news_per_stock: int = Form(10),
    admin: User = Depends(get_current_admin),
) -> dict[str, Any]:
    """Upload the quant strategy's candidate CSV (open.csv format).

    Returns: summary + per-stock risk + supporting news/announcements.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 5MB)")
    tiers = tuple(t.strip() for t in block_tiers.split(",") if t.strip())
    try:
        result = svc.scan_candidates(
            content, min_block_score=min_block_score,
            block_tiers=tiers, lookback_days=lookback_days,
            news_per_stock=news_per_stock,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.exception("risk-scan-csv failed")
        raise HTTPException(status_code=500, detail=str(e))
    return result


@router.post("/filter")
async def filter_codes(
    body: FilterCodesRequest,
    admin: User = Depends(get_current_admin),
) -> dict[str, Any]:
    """JSON variant: pass code list directly (no CSV)."""
    if not body.codes:
        raise HTTPException(status_code=400, detail="codes is empty")
    risks = svc.get_risks_for_codes(body.codes)
    news = svc.get_recent_news_for_codes(body.codes, body.lookback_days, per_stock=10)
    items, risky, kept = [], 0, 0
    by_tier: dict[str, int] = {}
    for code in body.codes:
        r = risks.get(code)
        blocked = bool(r and (r["composite_score"] >= body.min_block_score or r["tier"] in body.block_tiers))
        items.append({
            "code": code, "blocked": blocked,
            "name": (r or {}).get("stock_name"),
            "risk": r, "news": news.get(code, []),
        })
        if blocked:
            risky += 1; by_tier[r["tier"]] = by_tier.get(r["tier"], 0) + 1
        else:
            kept += 1
    items.sort(key=lambda x: (not x["blocked"], -((x["risk"] or {}).get("composite_score", 0) or 0)))
    return {
        "summary": {"unique_codes": len(body.codes), "risky": risky, "kept": kept,
                    "by_tier": by_tier,
                    "block_settings": {"min_block_score": body.min_block_score,
                                        "block_tiers": body.block_tiers}},
        "items": items,
    }


@router.get("/stock/{code}")
async def stock_detail(code: str, lookback_days: int = 30, admin: User = Depends(get_current_admin)) -> dict[str, Any]:
    """One stock — full risk + recent news/announcements."""
    code = code.strip().zfill(6)
    risks = svc.get_risks_for_codes([code])
    news = svc.get_recent_news_for_codes([code], lookback_days=lookback_days, per_stock=50)
    return {"code": code, "risk": risks.get(code), "news": news.get(code, [])}
