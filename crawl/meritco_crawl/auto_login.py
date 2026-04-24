"""Meritco (久谦中台) QR-scan auto-login.

Default mode is QR (the site has heavy password-path 2FA). The auth token
is a 32-hex string transmitted as an HTTP request header on every call to
`/matrix-search/forum/...`. We install a Playwright request-listener before
any navigation and capture the token off the first matching request.

If the user's cached cookies are still valid, visiting the login URL
auto-redirects to the forum, the listener fires, and we grab the token
without ever showing a QR.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from auto_login_common import (  # noqa: E402
    DEFAULT_QR_ELEMENT,
    DEFAULT_QR_TAB,
    DEFAULT_USER_AGENT,
    extract_cookies_list,
    parse_and_run,
    run_login,
    run_qr_login,
)

LOGIN_URL = "https://research.meritco-group.com/login?redirect=classic"
FORUM_URL = "https://research.meritco-group.com/forum?forumType=2"


def _make_extractor():
    """Return an `extract(page, context)` coroutine that watches the page's
    network traffic for a forum-API call and reads the `token` header off it.

    State (`seen`, listener-installed flag, forced-nav flag) is captured in
    closure cells so the same function reference works across the poll loop.
    """
    seen: dict[str, str] = {}
    listener_installed = [False]
    forced_nav_done = [False]

    async def on_request(request):
        if "/matrix-search/forum/" not in request.url:
            return
        tok = request.headers.get("token")
        if tok and len(tok) >= 24 and "token" not in seen:
            seen["token"] = tok
            seen["user_agent"] = request.headers.get("user-agent") or DEFAULT_USER_AGENT

    async def extract(page, context):
        # Install listener on first call — earlier would be better (to catch
        # auto-redirect API calls before they fire) but we don't have a hook
        # point before page.goto in the shared helper. See `forced_nav_done`
        # below for the compensation.
        if not listener_installed[0]:
            page.on("request", lambda req: asyncio.create_task(on_request(req)))
            listener_installed[0] = True

        if "token" in seen:
            # Capture the cookie jar alongside the RSA-signed token so viewer
            # mode can replay the browser session. scraper.py only reads
            # `token` + `user_agent`; the extra keys are ignored there.
            try:
                cookies = await extract_cookies_list(
                    context, domain_hint="meritco-group.com"
                )
            except Exception:
                cookies = []
            return {"token": seen["token"],
                    "user_agent": seen["user_agent"],
                    "cookies": cookies}

        # If URL has moved past /login — either because cached cookies let us
        # in, or because the user scanned the QR and Meritco redirected — a
        # forum page visit *now* will fire fresh API calls that our listener
        # (just installed) can catch. Do this at most once.
        url = (page.url or "")
        if "/login" not in url and not forced_nav_done[0]:
            forced_nav_done[0] = True
            try:
                await page.goto(FORUM_URL, wait_until="domcontentloaded", timeout=15000)
            except Exception:
                pass

        return None

    return extract


async def run(session_id: str, payload: dict) -> int:
    mode = (payload.get("mode") or "qr").lower()
    creds_path = SCRIPT_DIR / "credentials.json"
    user_data = SCRIPT_DIR / "playwright_data"

    # Treat empty-password "password" mode as SMS. Meritco's "password" tab
    # is actually SMS (phone + code), no password field at all.
    if mode in ("sms", "password") and not payload.get("password"):
        mode = "sms"

    if mode == "qr":
        return await run_qr_login(
            platform="meritco",
            session_id=session_id,
            login_url=LOGIN_URL,
            extract=_make_extractor(),
            credentials_path=creds_path,
            user_data_dir=user_data,
            # Known-good selectors for Meritco, pinned first so the scanner
            # hits them immediately instead of walking through the CN-generic
            # 30-entry default list.
            qr_tab_sels=['text=微信扫码', '[role="tab"]:has-text("微信扫码")']
                        + list(DEFAULT_QR_TAB),
            qr_elem_sels=[
                # Meritco embeds WeChat's qrconnect iframe (open.weixin.qq.com)
                # inside #wx_qrcode. The iframe renders the QR server-side;
                # Playwright's element.screenshot() captures the composited
                # pixels regardless of origin.
                '#wx_qrcode iframe',
                '#wx_qrcode',
                '[class*="qr-wrapper"]:not([style*="display: none"]) iframe',
                '[class*="qr-wrapper"]:not([style*="display: none"])',
                # Generic fallbacks
                '[class*="qr-wrapper"] img',
                '[class*="qr-wrapper"] canvas',
            ] + list(DEFAULT_QR_ELEMENT),
            ready_sels=['text=微信扫码', '[class*="login"]'],
        )

    # SMS / password fallback. run_login's SMS path kicks in when password=""
    # and automatically handles the "同意接受短信" checkbox before clicking
    # the 获取验证码 button.
    phone = (payload.get("phone") or payload.get("identifier") or "").strip()
    password = payload.get("password") or ""
    if not phone:
        print("ERROR: phone required", file=sys.stderr)
        return 2
    return await run_login(
        platform="meritco",
        session_id=session_id,
        login_url=LOGIN_URL,
        identifier=phone,
        password=password,
        extract=_make_extractor(),
        credentials_path=creds_path,
        user_data_dir=user_data,
        sms_tab_sels=['text=短信登录', 'text=短信登陆'] + list(__import__("auto_login_common").DEFAULT_SMS_TAB),
    )


if __name__ == "__main__":
    parse_and_run(run)
