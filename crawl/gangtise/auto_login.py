"""Gangtise (岗底斯) auto-login.

Preferred mode is QR-code — the site layers password + SMS 2FA checks,
which breaks headless automation. QR scan via the Gangtise mobile app
sidesteps everything.

Password flow is kept as a fallback when the payload contains a password.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from auto_login_common import (  # noqa: E402
    DEFAULT_QR_ELEMENT,
    DEFAULT_QR_TAB,
    extract_localstorage,
    parse_and_run,
    run_login,
    run_qr_login,
)

LOGIN_URL = "https://open.gangtise.com/"


async def _extract(page, context):
    # Primary: classic password-login + web-QR keys
    data = await extract_localstorage(
        page, ["G_token", "G_user", "G_cnfr_uid", "G_user_key", "G_tenantId"],
    )

    # Fallback 1: scan ALL of localStorage + sessionStorage for any UUID-shaped
    # value under a *token*/*user*/*auth* key. WeChat-scan login on Gangtise
    # might not hit the same G_token key we know from password-login.
    if not data.get("G_token"):
        try:
            all_stores = await page.evaluate("""() => {
              const out = {ls: {}, ss: {}};
              try { for (let i=0;i<localStorage.length;i++){
                const k=localStorage.key(i); out.ls[k]=localStorage.getItem(k)||'';
              } } catch(e) {}
              try { for (let i=0;i<sessionStorage.length;i++){
                const k=sessionStorage.key(i); out.ss[k]=sessionStorage.getItem(k)||'';
              } } catch(e) {}
              return out;
            }""") or {}
        except Exception:
            all_stores = {}
        import re as _re
        uuid_re = _re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        merged = {}
        merged.update(all_stores.get("ls") or {})
        merged.update(all_stores.get("ss") or {})
        for k, v in merged.items():
            if not isinstance(v, str):
                continue
            if k == "G_token" or (uuid_re.match(v) and any(x in k.lower() for x in ("token", "auth"))):
                data["G_token"] = v
                break
        for k, v in merged.items():
            if "user" in k.lower() and isinstance(v, str) and v.startswith("{"):
                data.setdefault("G_user", v)
                break

    # Fallback 2: cookies. If Gangtise keeps the UUID in a cookie only.
    if not data.get("G_token"):
        try:
            cookies = await context.cookies()
        except Exception:
            cookies = []
        for c in cookies:
            name = (c.get("name") or "").lower()
            val = c.get("value") or ""
            if name in ("token", "g_token", "access_token") and val:
                data["G_token"] = val
                break

    tok = data.get("G_token")
    if not tok:
        return None

    user = {}
    if data.get("G_user"):
        try:
            user = json.loads(data["G_user"]) or {}
        except (ValueError, TypeError):
            user = {}

    return {
        "token": tok,
        "uid": str(user.get("uid") or data.get("G_cnfr_uid") or ""),
        "user_key": str(user.get("user_key") or data.get("G_user_key") or ""),
        "tenant_id": str(user.get("tenant_id") or data.get("G_tenantId") or ""),
    }


async def run(session_id: str, payload: dict) -> int:
    mode = (payload.get("mode") or "qr").lower()
    password = payload.get("password") or ""

    # QR is the default. Falling back to password requires an explicit mode
    # plus a password (just a phone isn't enough for a password-only site).
    if mode == "password" and password:
        phone = (payload.get("phone") or payload.get("identifier") or "").strip()
        if not phone:
            print("ERROR: phone required for password mode", file=sys.stderr)
            return 2
        return await run_login(
            platform="gangtise",
            session_id=session_id,
            login_url=LOGIN_URL,
            identifier=phone,
            password=password,
            extract=_extract,
            credentials_path=SCRIPT_DIR / "credentials.json",
        )

    return await run_qr_login(
        platform="gangtise",
        session_id=session_id,
        login_url=LOGIN_URL,
        extract=_extract,
        credentials_path=SCRIPT_DIR / "credentials.json",
        user_data_dir=SCRIPT_DIR / "playwright_data",
        # Known-good selectors for Gangtise — putting the actual matches
        # first avoids scanning through 30+ generic fallbacks.
        qr_tab_sels=['[class*="type-entrance"]:has-text("微信")', 'text=微信登录']
                    + list(DEFAULT_QR_TAB),
        qr_elem_sels=['img[src^="data:image/png;base64"]']
                     + list(DEFAULT_QR_ELEMENT),
        ready_sels=['[class*="type-entrance"]', 'input[placeholder*="手机"]'],
    )


if __name__ == "__main__":
    parse_and_run(run)
