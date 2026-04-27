"""REST API exposing Gangtise Datapipe data (mongodb `gangtise-full.dp_*`).

Datapipe is the official Gangtise data-sync client (Java jar at
124.71.193.17:9200) running locally in `mode=down` at
`/home/ygwang/crawl_data/gangtise_datapipe/`. It pushes XMLs into
`work/download/<product>/`; `crawl/gangtise/datapipe_importer.py` parses each
XML and upserts rows into `gangtise-full.dp_<product>` collections.

13 products are subscribed, each with its own schema. We surface them through
a single endpoint set, with `PRODUCT_CONFIG` mapping per-product field names
(title / time / ticker / body) into a uniform brief shape.

Sort is always `update_time` desc (Long yyyymmddhhmmss, present on every row).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


# Per-product config: how to render a heterogeneous dp_<product> row in a
# uniform list view. `time` is the source-of-truth publish time (varies by
# product); we always sort by `update_time` (Long yyyymmddhhmmss) which every
# product has. `ticker` is the field that holds a stock identifier when the
# product is stock-scoped (used by ?ticker= filter).
PRODUCT_CONFIG: dict[str, dict[str, Any]] = {
    "QAirmcninfo": {
        "label_cn": "互动易问答", "label_en": "IR Q&A (互动易)",
        "title": "question", "body": "answer", "time": "message_date",
        "ticker": "security_code", "name": "security_name",
    },
    "QAmessagerecord": {
        "label_cn": "上证 e 互动", "label_en": "SSE e-Interaction",
        "title": "question", "body": "answer", "time": "message_date",
        "ticker": "security_code", "name": "security_name",
    },
    "QAtelconferce": {
        "label_cn": "业绩说明会问答", "label_en": "Earnings Call Q&A",
        "title": "question", "body": "answer", "time": "message_date",
        "ticker": "security_code", "name": "security_name",
    },
    "fina_calendar": {
        "label_cn": "财经日历", "label_en": "Macro Calendar",
        "title": "field_name", "body": None, "time": "publish_date",
        "ticker": None, "name": "country_region",
    },
    "investmtcal": {
        "label_cn": "投资日历", "label_en": "Investment Calendar",
        "title": "event_details", "body": None, "time": "publish_date",
        "ticker": "event_tag", "name": "event_type",
    },
    "minutsofcompsurvey": {
        "label_cn": "调研纪要", "label_en": "Company Surveys",
        "title": "mting_title", "body": "mting_summary", "time": "publish_date",
        "ticker": "trade_code", "name": "security_name",
    },
    "news_financial": {
        "label_cn": "财联社收评", "label_en": "Cailianpress Daily",
        "title": "news_title", "body": "summary_org", "time": "pub_time",
        "ticker": None, "name": "media_name",
    },
    "news_financialflash": {
        "label_cn": "财经快讯", "label_en": "Financial Flash",
        "title": "summary_org", "body": "summary_org", "time": "pub_time",
        "ticker": None, "name": "media_name",
    },
    "news_skthottopics": {
        "label_cn": "股吧热门话题", "label_en": "Forum Hot Topics",
        "title": "topic", "body": "topic_txt", "time": "pub_time",
        "ticker": None, "name": "media_name",
    },
    "opinion_statistic": {
        "label_cn": "一致预期统计", "label_en": "Consensus Forecast",
        "title": "security_name", "body": None, "time": "stat_date",
        "ticker": "trade_code", "name": "security_name",
    },
    "postinfo_xq": {
        "label_cn": "雪球发帖", "label_en": "Xueqiu Posts",
        "title": "post_abstract", "body": "post_txt", "time": "pub_time",
        "ticker": None, "name": "post_userid",
    },
    "scheduleofalln": {
        "label_cn": "路演会议日程", "label_en": "Roadshow Schedule",
        "title": "schdl_title", "body": "schdl_guest", "time": "schdl_time",
        "ticker": "lsted_compy", "name": "indsty_type",
    },
    "stkproperterms": {
        "label_cn": "概念归属", "label_en": "Concept Tags",
        "title": "propern_name", "body": None, "time": None,
        "ticker": None, "name": "propern_type",
    },
}


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().datapipe_mongo_uri, tz_aware=True)


def _mongo_db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().datapipe_mongo_db]


def _coll_name(product: str) -> str:
    return f"dp_{product}"


def _truncate(s: Any, n: int) -> str:
    if not isinstance(s, str):
        return ""
    return s[:n] + ("…" if len(s) > n else "")


def _fmt_time(value: Any) -> str | None:
    """Datapipe rows carry update_time as Long (yyyymmddhhmmss). Other time
    fields can be int (yyyymmdd), Long (yyyymmddhhmmss), or str ("2026/04/24
    16:04:52" / "20260423"). Normalize to ISO-ish 'YYYY-MM-DD HH:MM' for UI."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    try:
        v = int(value)
    except (TypeError, ValueError):
        return str(value)
    s = str(v)
    if len(s) == 8:                       # yyyymmdd
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    if len(s) == 14:                      # yyyymmddhhmmss
        return f"{s[:4]}-{s[4:6]}-{s[6:8]} {s[8:10]}:{s[10:12]}"
    if len(s) == 12:                      # yyyymmddhhmm
        return f"{s[:4]}-{s[4:6]}-{s[6:8]} {s[8:10]}:{s[10:12]}"
    return s


def _extras(product: str, doc: dict) -> dict:
    """Per-product fields beyond the uniform brief, used for richer table cells.
    Q&A → full question/answer (untruncated, the table renders Q/A two-line).
    opinion_statistic → numeric columns (覆盖券商 / 推荐 / 涨跌幅).
    scheduleofalln → 会议时间 + 类型 + 行业.
    minutsofcompsurvey → 调研类型 + 上市公司 + 调研机构数.
    """
    if product in ("QAirmcninfo", "QAmessagerecord", "QAtelconferce"):
        return {
            "question": doc.get("question") or "",
            "answer": doc.get("answer") or "",
        }
    if product == "opinion_statistic":
        return {
            "cvg_num": doc.get("cvg_num"),
            "recom_num": doc.get("recom_num"),
            "chg_pct": doc.get("chg_pct"),
            "is_trade": bool(doc.get("is_trade")),
            "stat_date": _fmt_time(doc.get("stat_date")),
        }
    if product == "scheduleofalln":
        return {
            "schdl_time": _fmt_time(doc.get("schdl_time")),
            "schdl_type": doc.get("schdl_type"),
            "indsty_type": doc.get("indsty_type"),
            "lead_agency": doc.get("lead_agency"),
        }
    if product == "minutsofcompsurvey":
        survey_persons = doc.get("survey_person") or ""
        # 用 ; 或 ; 或 \n 分隔机构名,数一下到底有几家
        n_orgs = len([s for s in str(survey_persons).replace(";", ";").replace("\n", ";").split(";") if s.strip()])
        return {
            "survey_type": doc.get("survey_type"),
            "n_survey_orgs": n_orgs,
            "encoding_garbled": True,  # known: source XML mojibake on mting_summary
        }
    if product == "news_skthottopics":
        return {
            "topic_tags": doc.get("topic_tags") or "",
            "read_n": doc.get("read_n"),
            "comments_n": doc.get("comments_n"),
        }
    if product == "postinfo_xq":
        return {
            "comments_n": doc.get("comments_n"),
            "likes_n": doc.get("likes_n"),
            "shares_n": doc.get("shares_n"),
            "source_link": doc.get("source_link"),
        }
    if product == "fina_calendar":
        return {
            "country_region": doc.get("country_region"),
            "currency": doc.get("currency"),
            "importance_level": doc.get("importance_level"),
            "pre_value": doc.get("pre_value"),
            "unit": doc.get("unit"),
        }
    if product == "investmtcal":
        return {
            "event_type": doc.get("event_type"),
            "event_source": doc.get("event_source"),
        }
    return {}


def _brief(product: str, doc: dict) -> dict:
    cfg = PRODUCT_CONFIG[product]
    title_raw = doc.get(cfg["title"]) if cfg["title"] else None
    body_raw = doc.get(cfg["body"]) if cfg["body"] else None
    time_raw = doc.get(cfg["time"]) if cfg["time"] else None
    ticker_raw = doc.get(cfg["ticker"]) if cfg["ticker"] else None
    name_raw = doc.get(cfg["name"]) if cfg["name"] else None

    return {
        "id": str(doc.get("_id")),
        "product": product,
        "title": _truncate(str(title_raw or ""), 200),
        "preview": _truncate(str(body_raw or ""), 280),
        "time": _fmt_time(time_raw),
        "update_time": _fmt_time(doc.get("update_time")),
        "ticker": str(ticker_raw) if ticker_raw is not None else None,
        "ticker_name": str(name_raw) if name_raw is not None else None,
        "op_mode": doc.get("op_mode"),
        "deleted": bool(doc.get("_dp_deleted")),
        "imported_at": doc.get("_dp_imported_at"),
        "extras": _extras(product, doc),
    }


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class ProductInfo(BaseModel):
    product: str
    label_cn: str
    label_en: str
    has_ticker: bool
    count: int
    today: int
    latest_update_time: str | None


class ProductsResponse(BaseModel):
    products: list[ProductInfo]


class ItemListResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    page_size: int
    has_next: bool


class StatsResponse(BaseModel):
    total: int
    products: int
    today_total: int
    last_7_days: list[dict]
    importer_state: dict[str, Any] | None


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@router.get("/products", response_model=ProductsResponse)
async def list_products(user: User = Depends(get_current_user)):
    """Per-product metadata + row count + today count + latest update_time."""
    db = _mongo_db()
    today_yyyymmdd = int(datetime.now(timezone.utc).strftime("%Y%m%d") + "000000")

    out: list[ProductInfo] = []
    for product, cfg in PRODUCT_CONFIG.items():
        coll = db[_coll_name(product)]
        count = await coll.estimated_document_count()
        today = await coll.count_documents({"update_time": {"$gte": today_yyyymmdd}})
        latest = await coll.find_one({}, sort=[("update_time", -1)],
                                      projection={"update_time": 1})
        out.append(ProductInfo(
            product=product,
            label_cn=cfg["label_cn"],
            label_en=cfg["label_en"],
            has_ticker=cfg["ticker"] is not None,
            count=count,
            today=today,
            latest_update_time=_fmt_time(latest.get("update_time") if latest else None),
        ))
    return ProductsResponse(products=out)


@router.get("/items", response_model=ItemListResponse)
async def list_items(
    product: str = Query(..., description="Datapipe product name (see /products)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Substring on title or body"),
    ticker: str | None = Query(None, description="Stock code filter (regex on configured ticker field)"),
    user: User = Depends(get_current_user),
):
    if product not in PRODUCT_CONFIG:
        raise HTTPException(400, f"Unknown product: {product}")
    cfg = PRODUCT_CONFIG[product]
    coll = _mongo_db()[_coll_name(product)]

    match: dict[str, Any] = {}
    ors: list[dict] = []
    if q:
        if cfg["title"]:
            ors.append({cfg["title"]: {"$regex": q, "$options": "i"}})
        if cfg["body"]:
            ors.append({cfg["body"]: {"$regex": q, "$options": "i"}})
    if ticker and cfg["ticker"]:
        # ticker fields are sometimes int (e.g. security_code=300210) and
        # sometimes string (e.g. trade_code='001360.SZ'); allow both via $or
        ors.append({cfg["ticker"]: {"$regex": ticker, "$options": "i"}})
        try:
            ors.append({cfg["ticker"]: int(ticker)})
        except (TypeError, ValueError):
            pass
    if ors:
        match["$or"] = ors

    # Hide soft-deleted rows (importer marks _dp_deleted=true on op_mode=2).
    match["_dp_deleted"] = {"$ne": True}

    total = await coll.count_documents(match)
    cursor = (
        coll.find(match)
        .sort("update_time", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_brief(product, d) async for d in cursor]
    return ItemListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/items/{product}/{item_id}")
async def get_item(
    product: str,
    item_id: str,
    user: User = Depends(get_current_user),
):
    if product not in PRODUCT_CONFIG:
        raise HTTPException(400, f"Unknown product: {product}")
    coll = _mongo_db()[_coll_name(product)]

    # _id is heterogeneous: some are int (e.g. fina_calendar), some are
    # Long-as-Python-int (e.g. QAirmcninfo's hashed ids), some are nested.
    # Try str → int fallbacks.
    doc = await coll.find_one({"_id": item_id})
    if not doc:
        try:
            doc = await coll.find_one({"_id": int(item_id)})
        except (TypeError, ValueError):
            pass
    if not doc:
        raise HTTPException(404, "Item not found")

    return {
        **_brief(product, doc),
        "raw": {k: (v if not isinstance(v, datetime) else v.isoformat()) for k, v in doc.items() if k != "_id"},
    }


@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    db = _mongo_db()
    today_yyyymmdd = int(datetime.now(timezone.utc).strftime("%Y%m%d") + "000000")

    total = 0
    today_total = 0
    for product in PRODUCT_CONFIG:
        coll = db[_coll_name(product)]
        total += await coll.estimated_document_count()
        today_total += await coll.count_documents({"update_time": {"$gte": today_yyyymmdd}})

    # last_7_days: aggregate per product across last 7 calendar days using
    # the import timestamp (_dp_imported_at).
    last7: dict[str, dict[str, int]] = {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    for product in PRODUCT_CONFIG:
        coll = db[_coll_name(product)]
        pipeline = [
            {"$match": {"_dp_imported_at": {"$gte": cutoff}}},
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$_dp_imported_at"}},
                "n": {"$sum": 1},
            }},
        ]
        async for d in coll.aggregate(pipeline):
            date = d["_id"]
            last7.setdefault(date, {p: 0 for p in PRODUCT_CONFIG})
            last7[date][product] = d["n"]
    last7_sorted = sorted(last7.items())[-7:]
    last_7_list = [{"date": d, **{k: int(v) for k, v in counts.items()}, "total": sum(counts.values())}
                   for d, counts in last7_sorted]

    # Importer health: dp_state has the per-file import log; aggregate
    # last-run summary.
    state_coll = db["dp_state"]
    state_total = await state_coll.count_documents({"ok": True})
    state_errors = await state_coll.count_documents({"ok": False})
    latest_state = await state_coll.find_one(
        {"ok": True}, sort=[("imported_at", -1)],
        projection={"product": 1, "imported_at": 1, "rows": 1},
    )
    importer_state = {
        "files_imported_ok": state_total,
        "files_with_errors": state_errors,
        "last_imported_product": (latest_state or {}).get("product"),
        "last_imported_at": (latest_state.get("imported_at").isoformat()
                             if latest_state and isinstance(latest_state.get("imported_at"), datetime)
                             else None),
    }

    return StatsResponse(
        total=total,
        products=len(PRODUCT_CONFIG),
        today_total=today_total,
        last_7_days=last_7_list,
        importer_state=importer_state,
    )
