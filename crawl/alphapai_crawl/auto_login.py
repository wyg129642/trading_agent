"""AlphaPai password login.

localStorage.USER_AUTH_TOKEN (JWT, 30-day exp).
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

LOGIN_URL = "https://alphapai-web.rabyte.cn/"


async def _extract(page, context):
    data = await extract_localstorage(page, ["USER_AUTH_TOKEN"])
    tok = data.get("USER_AUTH_TOKEN")
    return {"token": tok} if tok else None


async def run(session_id: str, payload: dict) -> int:
    mode = (payload.get("mode") or "password").lower()
    creds_path = SCRIPT_DIR / "credentials.json"

    if mode == "qr":
        return await run_qr_login(
            platform="alphapai",
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
        platform="alphapai",
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
