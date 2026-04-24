"""Funda (funda.ai) password login — email + password → cookie jar."""

from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from auto_login_common import (  # noqa: E402
    DEFAULT_EMAIL,
    extract_cookie_string,
    parse_and_run,
    run_login,
)

LOGIN_URL = "https://funda.ai/"
# 登录成功后跳转到 reports 主页, 让 /data-sources CDP 凭证实时预览能看到真实内容
# (原来登录完落在 funda.ai/ 的营销首页, 视觉上是空白, 用户看不出登录是否成功).
POST_LOGIN_URL = "https://funda.ai/reports"


def _make_extractor(existing_api_key: str):
    # 跟踪是否已经跳转过, 避免 poll 循环里重复 goto/wait.
    state: dict = {"navigated": False}

    async def extract(page, context):
        cookie = await extract_cookie_string(context)
        if "session-token" not in cookie:
            return None
        out = {"cookie": cookie}
        if existing_api_key:
            out["api_key"] = existing_api_key

        # 登录判定通过后, 切到 /reports 让实时预览展示内容列表.
        # extract 返回 creds 后 run_login 立刻进 finally 关浏览器, 所以必须
        # 在 return 前 goto + wait_for 渲染完, 否则 screencast 上只会看到空白一瞬.
        if not state["navigated"]:
            state["navigated"] = True
            try:
                await page.goto(POST_LOGIN_URL, wait_until="domcontentloaded", timeout=15000)
                # 等 SPA 渲染出 report 列表 — 4s 差不多够了 (tRPC fetchInfinite 回来 + 卡片布局)
                await page.wait_for_timeout(4000)
            except Exception:
                # 不阻塞登录成功 — 跳转失败就只影响可视化
                pass
        return out
    return extract


async def run(session_id: str, payload: dict) -> int:
    email = (payload.get("email") or payload.get("identifier") or payload.get("phone") or "").strip()
    password = payload.get("password") or ""
    if not email or not password:
        print("ERROR: email and password required", file=sys.stderr)
        return 2

    creds_path = SCRIPT_DIR / "credentials.json"
    existing_api_key = ""
    if creds_path.exists():
        try:
            existing_api_key = (json.loads(creds_path.read_text()) or {}).get("api_key", "")
        except Exception:
            pass

    return await run_login(
        platform="funda",
        session_id=session_id,
        login_url=LOGIN_URL,
        identifier=email,
        password=password,
        extract=_make_extractor(existing_api_key),
        credentials_path=creds_path,
        identifier_sels=DEFAULT_EMAIL,
        user_data_dir=SCRIPT_DIR / "playwright_data",
    )


if __name__ == "__main__":
    parse_and_run(run)
