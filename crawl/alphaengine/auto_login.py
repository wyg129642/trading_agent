"""AlphaEngine (www.alphaengine.top) password login via Playwright.

Extracts two localStorage keys:
  - ``token``          — access JWT, 30-day validity
  - ``refresh_token``  — used for silent token renewal (Authorization-Refresh header
                         against /api/v1/kmpadmin/auth/refresh)

Both are persisted to ``credentials.json``. The scraper's file-locked refresh
helper (see ``scraper.refresh_with_file_lock``) rotates these every 6h or on
401 without further user action, as long as this platform keeps at least one
watcher alive.

Password + optional SMS OTP: alphaengine uses Tencent CAPTCHA (TCaptcha.js)
on every submit. We don't solve the CAPTCHA programmatically — the user
usually has to complete a slider. When the CAPTCHA appears in the headful
browser, run_login blocks up to OTP_WAIT_SECONDS for the user to finish.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from auto_login_common import (  # noqa: E402
    extract_localstorage,
    parse_and_run,
    run_login,
    run_qr_login,
)

# Landing page that surfaces the login modal. The SPA guards every route with
# an auth check, so any protected deep-link works; summary-center is the one
# users visit most and matches what the scraper reads.
LOGIN_URL = "https://www.alphaengine.top/#/home/page"


async def _extract(page, context):
    """Return both tokens only after the user is actually logged in.

    ``token`` alone isn't enough — we also need ``refresh_token`` so the
    scraper can rotate the session without the user re-logging in every
    30 days. The SPA writes both the moment the login POST succeeds.
    """
    data = await extract_localstorage(page, ["token", "refresh_token"])
    tok = data.get("token")
    rtok = data.get("refresh_token")
    if not tok or not tok.startswith("eyJ"):
        return None
    out = {"token": tok}
    if rtok and rtok.startswith("eyJ"):
        out["refresh_token"] = rtok
    return out


async def run(session_id: str, payload: dict) -> int:
    mode = (payload.get("mode") or "password").lower()
    creds_path = SCRIPT_DIR / "credentials.json"

    # alphaengine.top doesn't have a WeChat QR login tab in the modal, but
    # future-proof in case it's added. Password is the primary flow today.
    if mode == "qr":
        return await run_qr_login(
            platform="alphaengine",
            session_id=session_id,
            login_url=LOGIN_URL,
            extract=_extract,
            credentials_path=creds_path,
        )

    phone = (payload.get("phone") or payload.get("identifier") or "").strip()
    password = payload.get("password") or ""
    if not phone:
        print("ERROR: phone required", file=sys.stderr)
        return 2
    return await run_login(
        platform="alphaengine",
        session_id=session_id,
        login_url=LOGIN_URL,
        identifier=phone,
        password=password,
        extract=_extract,
        credentials_path=creds_path,
        user_data_dir=SCRIPT_DIR / "playwright_data",
    )


if __name__ == "__main__":
    parse_and_run(run)
