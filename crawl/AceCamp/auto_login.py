"""AceCamp (本营) password login. Token = full cookie jar."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from auto_login_common import (  # noqa: E402
    extract_cookie_string,
    parse_and_run,
    run_login,
    run_qr_login,
)

LOGIN_URL = "https://www.acecamptech.com/"


async def _extract(page, context):
    """Return saved creds only when the session is bound to an actual user.

    Prior version accepted any `_ace_camp_tech_production_session` cookie, but
    Rails sets that cookie on first visit for ANONYMOUS guests too. That bug
    caused login sessions to 'succeed' with anonymous credentials: the scraper
    pulled only free articles (paid ones came back as ~100-char title-only
    previews), and the real-time-viewer correctly showed "Log in / Sign up".

    Authoritative check: hit `api.acecamptech.com/api/v1/users/me` with the
    current cookie jar. Anonymous session → `data: null`. Logged-in → user dict.
    """
    cookie = await extract_cookie_string(context)
    if "_ace_camp_tech_production_session" not in cookie and "user_token" not in cookie:
        return None

    # Verify via users/me — the SPA uses this exact call to decide auth.
    try:
        probe = await context.request.get(
            "https://api.acecamptech.com/api/v1/users/me"
            "?get_follows=true&with_owner=true&with_resume=true&version=2.0",
            headers={
                "Accept": "application/json",
                "Origin": "https://www.acecamptech.com",
                "Referer": "https://www.acecamptech.com/",
            },
        )
    except Exception:
        return None
    if not probe.ok:
        return None
    try:
        body = await probe.json()
    except Exception:
        return None
    user = (body or {}).get("data") if isinstance(body, dict) else None
    if not user:
        # Logged-out session — don't persist garbage creds.
        return None

    return {"cookie": cookie}


async def run(session_id: str, payload: dict) -> int:
    mode = (payload.get("mode") or "password").lower()
    creds_path = SCRIPT_DIR / "credentials.json"

    if mode == "qr":
        return await run_qr_login(
            platform="acecamp",
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
        platform="acecamp",
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
