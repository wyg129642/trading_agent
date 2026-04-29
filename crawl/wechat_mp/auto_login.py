"""微信公众号管理后台扫码登录 (mp.weixin.qq.com).

登录态包含两部分:
  1. URL 中的 ?token=NNNNNNN  — 后续所有 cgi-bin 接口的必填查询参数
  2. cookies (slave_user / data_ticket / data_bizuin / bizuin / pass_ticket 等)
     — 必须与 token 配对使用

扫码后页面会从 https://mp.weixin.qq.com/cgi-bin/loginpage 跳到
https://mp.weixin.qq.com/cgi-bin/home?t=home/index&lang=zh_CN&token=NNN —
我们就在 URL 出现 token 参数时把 cookies + token 一起捞下来。
"""

from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from auto_login_common import (  # noqa: E402
    parse_and_run,
    run_qr_login,
)

LOGIN_URL = "https://mp.weixin.qq.com/"


async def _extract(page, context):
    """登录成功 ⇔ URL 含 ?token=… 且 home 页就绪。"""
    try:
        url = page.url
    except Exception:
        return None
    qs = parse_qs(urlparse(url).query)
    token_list = qs.get("token") or []
    if not token_list:
        return None
    token = token_list[0]
    if not token or not token.isdigit():
        return None
    try:
        cookies = await context.cookies()
    except Exception:
        cookies = []
    cookies = [
        {k: c[k] for k in ("name", "value", "domain", "path") if k in c}
        for c in cookies
        if c.get("domain", "").endswith("weixin.qq.com")
    ]
    if not cookies:
        return None
    return {"token": token, "cookies": cookies, "login_url": url}


async def run(session_id: str, payload: dict) -> int:
    creds_path = SCRIPT_DIR / "credentials.json"
    return await run_qr_login(
        platform="wechat_mp",
        session_id=session_id,
        login_url=LOGIN_URL,
        extract=_extract,
        credentials_path=creds_path,
        user_data_dir=SCRIPT_DIR / "playwright_data",
    )


if __name__ == "__main__":
    parse_and_run(run)
