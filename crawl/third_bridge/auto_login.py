"""Third Bridge password login — email + password → cookie jar (best effort)."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from auto_login_common import (  # noqa: E402
    DEFAULT_EMAIL,
    DEFAULT_USER_AGENT,
    extract_cookie_string,
    parse_and_run,
    run_login,
)

LOGIN_URL = "https://forum.thirdbridge.com/zh/home/all"


async def _extract(page, context):
    cookie = await extract_cookie_string(context)
    if "tb_forum_authenticated_prod" not in cookie:
        return None
    return {"cookie": cookie, "user_agent": DEFAULT_USER_AGENT}


async def run(session_id: str, payload: dict) -> int:
    email = (payload.get("email") or payload.get("identifier") or payload.get("phone") or "").strip()
    password = payload.get("password") or ""
    if not email or not password:
        print("ERROR: email and password required", file=sys.stderr)
        return 2
    return await run_login(
        platform="thirdbridge",
        session_id=session_id,
        login_url=LOGIN_URL,
        identifier=email,
        password=password,
        extract=_extract,
        credentials_path=SCRIPT_DIR / "credentials.json",
        identifier_sels=DEFAULT_EMAIL,
        user_data_dir=SCRIPT_DIR / "playwright_data",
    )


if __name__ == "__main__":
    parse_and_run(run)
