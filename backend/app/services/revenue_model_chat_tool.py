"""Chat tool that creates + kicks off a revenue-modeling RecipeRun.

Exposed as ``trigger_revenue_model(ticker, ...)`` so the AI research chat
can bootstrap a full structured model from a natural-language ask like
"帮我拆一下 LITE.US FY25E-FY27E 的收入".

The tool returns a model_id + a direct URL so the frontend can render a
"查看建模" button inline in the chat stream.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)


TRIGGER_REVENUE_MODEL_TOOLS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "trigger_revenue_model",
            "description": (
                "创建并启动一个结构化收入拆分建模任务 (RevenueModel + Recipe run). "
                "当用户明确要求 '帮我拆 XXX 公司的收入 / 做收入建模 / 分板块预测' 时调用. "
                "返回 model_id 和 URL — 用户在 /modeling/{model_id} 页面看到完整 spreadsheet. "
                "默认用当前行业的 public recipe. 如果没有匹配 pack, 会退化到 'generic' 行业."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "股票代码, 必填. 例: 'LITE.US' / '600519.SH' / '0700.HK'",
                    },
                    "company_name": {
                        "type": "string",
                        "description": "公司名 (中文或英文).",
                    },
                    "industry": {
                        "type": "string",
                        "description": (
                            "行业 pack slug (optical_modules / hdd / semi_equipment / ...). "
                            "留空则根据 ticker 自动猜."
                        ),
                    },
                    "fiscal_periods": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "财年区间, 如 ['FY25E','FY26E','FY27E']. 不填用 pack 默认.",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "True = 不调 LLM 只跑骨架 (测试用), 默认 false",
                    },
                },
                "required": ["ticker", "company_name"],
            },
        },
    },
]


TRIGGER_REVENUE_MODEL_SYSTEM_PROMPT = """\
## trigger_revenue_model

当用户明确要求为某支股票做 **结构化收入拆分建模**（不是单轮问答）时，调用此工具。
典型触发语: "帮我拆一下 X 的收入", "建一个 Y 的模型", "做 Z FY25-27 预测".

工具会创建一个可视化 spreadsheet + 后台启动 8 步 recipe:
读纪要 → 拆板块 → 分类增长 → 抽历史 → 对标同行 → 量×价/指引 → margin级联 → 一致预期核对 → CoVe 验证.

返回后请告诉用户:
1. 已创建 model_id
2. 跳转链接 `/modeling/{model_id}`
3. 预计 3-8 分钟完成 (跨多步 LLM + 工具调用)
4. 用户可以在 spreadsheet 里锁定/修改任一 cell, 反馈会纳入 Playbook 自进化
"""


async def execute_tool(
    name: str,
    arguments: dict[str, Any],
    citation_tracker: Any,
) -> str:
    if name != "trigger_revenue_model":
        return f"Unknown tool: {name}"

    ticker = str(arguments.get("ticker") or "").strip()
    company_name = str(arguments.get("company_name") or "").strip()
    industry = str(arguments.get("industry") or "").strip()
    fiscal_periods = arguments.get("fiscal_periods") or []
    dry_run = bool(arguments.get("dry_run", False))

    if not ticker or not company_name:
        return "缺少 ticker 或 company_name."

    # Lazy imports to avoid circular dependencies
    from backend.app.core.database import async_session_factory
    from backend.app.models.recipe import Recipe, RecipeRun
    from backend.app.models.revenue_model import RevenueModel
    from backend.app.services.recipe_engine import run_recipe
    from industry_packs import pack_registry
    from sqlalchemy import select

    # Auto-pick industry if not provided
    if not industry:
        industry = _guess_industry(ticker) or "optical_modules"
    pack = pack_registry.get(industry)
    if not pack:
        # fallback: pick first available public pack
        packs = pack_registry.list()
        if packs:
            industry = packs[0].slug
            pack = packs[0]

    # Default periods
    if not fiscal_periods:
        if pack:
            fiscal_periods = list((pack.meta or {}).get("default_periods") or ["FY25E", "FY26E", "FY27E"])
        else:
            fiscal_periods = ["FY25E", "FY26E", "FY27E"]

    try:
        async with async_session_factory() as db:
            # Pick a public recipe for this industry (most recent version)
            q = (
                select(Recipe)
                .where(Recipe.industry == industry, Recipe.is_public == True)  # noqa
                .order_by(Recipe.version.desc())
                .limit(1)
            )
            rec = (await db.execute(q)).scalar_one_or_none()
            if not rec:
                return (
                    f"未找到 {industry} 行业的 public recipe. "
                    f"请先 admin 导入 pack: `POST /api/recipes/import-pack/{industry}`."
                )

            # Default owner — must be a real user; we rely on the chat caller
            # identity being passed via environment. Since tool executor
            # has no user context, we defer ownership to a 'system' user if
            # such is configured, else we require the caller to supply.
            from backend.app.models.user import User as _User
            sys_user_q = select(_User).where(_User.email == "system@trading-intel").limit(1)
            sys_user = (await db.execute(sys_user_q)).scalar_one_or_none()
            if not sys_user:
                # Fall back: first admin user
                admin_q = select(_User).where(_User.role == "admin").limit(1)
                sys_user = (await db.execute(admin_q)).scalar_one_or_none()
            if not sys_user:
                return "无可用 owner 账号（system 或 admin）。请联系管理员创建 system@trading-intel 用户."

            model = RevenueModel(
                ticker=ticker,
                company_name=company_name,
                industry=industry,
                fiscal_periods=list(fiscal_periods),
                title=f"{company_name} ({ticker})",
                notes="Triggered from AI chat",
                base_currency="USD" if ticker.upper().endswith(".US") else "CNY",
                owner_user_id=sys_user.id,
                status="running",
                recipe_id=rec.id,
                recipe_version=rec.version,
            )
            db.add(model)
            await db.flush()
            run = RecipeRun(
                recipe_id=rec.id, recipe_version=rec.version,
                model_id=model.id, ticker=model.ticker,
                status="pending", settings={"dry_run": dry_run},
            )
            db.add(run)
            await db.commit()
            await db.refresh(model)
            await db.refresh(run)

            # Kick off async run
            asyncio.create_task(run_recipe(run.id, dry_run=dry_run))

            # Register a citation so the model can cite
            try:
                citation_tracker._register(
                    f"revenue_model:{model.id}",
                    {
                        "title": f"{company_name} 收入拆分模型",
                        "url": f"/modeling/{model.id}",
                        "website": "Trading Intelligence",
                        "date": "",
                        "source_type": "revenue_model",
                        "doc_type": "建模任务",
                    },
                )
            except Exception:
                pass

            return (
                f"✅ 已创建收入建模任务.\n"
                f"- model_id: `{model.id}`\n"
                f"- 行业: `{industry}`\n"
                f"- 区间: {', '.join(fiscal_periods)}\n"
                f"- Recipe: `{rec.name}` v{rec.version}\n"
                f"- 建模页面: /modeling/{model.id}\n"
                f"- 预计 3-8 分钟完成. 建模期间可以继续问问题."
            )
    except Exception as e:
        logger.exception("trigger_revenue_model failed")
        return f"创建建模任务失败: {e}"


def _guess_industry(ticker: str) -> str | None:
    """Cheap heuristic — match ticker against each pack's ticker_patterns."""
    try:
        from industry_packs import pack_registry
        for p in pack_registry.list():
            for pat in (p.meta or {}).get("ticker_patterns") or []:
                if ticker.upper() == str(pat).upper():
                    return p.slug
    except Exception:
        pass
    return None
