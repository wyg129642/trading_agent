"""Jinmen (进门财经) password login. Token historically lived in
``localStorage.JM_AUTH_INFO`` (base64-encoded JSON containing ``webtoken``,
``uid``, ``organizationId``).

The platform may quietly rotate the storage key (`JM_AUTH_INFO` →
`USER_AUTH_INFO` / `JIM_AUTH_INFO` / etc.), so we scan all of localStorage
for any base64 blob whose decoded JSON has the canonical shape, and as a
final safety net also poke the CDP network log for a `json_user_login_*`
or `json_user_info_*` response carrying the same fields. That makes the
extract robust against both UI redirects (login page → activity page didn't
fire) and storage-key drift."""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from auto_login_common import (  # noqa: E402
    dump_all_localstorage,
    extract_cookies_list,
    extract_localstorage,
    network_log,
    parse_and_run,
    run_login,
    run_qr_login,
)

LOGIN_URL = "https://brm.comein.cn/"

# Known storage key the scraper currently consumes. We always normalize
# captures back into this key so scraper.py keeps working unchanged.
PRIMARY_KEY = "JM_AUTH_INFO"

# Heuristic candidates we'll also check.
_FALLBACK_KEYS = ("USER_AUTH_INFO", "JIM_AUTH_INFO", "AUTH_INFO", "JM_USER_INFO")


def _looks_like_jm_blob(value: str) -> bool:
    """A JM blob is base64-encoded JSON with at least
    ``value.webtoken`` (or ``value.token``) + ``value.uid``."""
    if not value or len(value) < 100:
        return False
    try:
        decoded = base64.b64decode(value).decode("utf-8")
        data = json.loads(decoded)
    except Exception:
        return False
    inner = (data or {}).get("value") or {}
    has_token = bool(inner.get("webtoken") or inner.get("token"))
    has_uid = bool(inner.get("uid"))
    return has_token and has_uid


def _extract_token_from_login_response(net) -> str | None:
    """Look at the most recent json_user_login_* / json_user_info_* responses
    and rebuild the JM_AUTH_INFO blob. Useful when the user logged in
    successfully (server returned 200) but the SPA redirect hasn't yet
    written the blob to localStorage — the network response always contains
    the same data."""
    for entry in reversed(net or []):
        url = entry.get("url") or ""
        body = entry.get("response_body") or ""
        if "json_user_login" not in url and "json_user_info" not in url:
            continue
        if not body or "webtoken" not in body:
            continue
        try:
            payload = json.loads(body)
        except Exception:
            continue
        # API returns {data:{webtoken, uid, organizationId, ...}}
        d = (payload or {}).get("data") or {}
        webtoken = d.get("webtoken") or d.get("token")
        uid = d.get("uid")
        if not (webtoken and uid):
            continue
        # Re-wrap into the legacy localStorage shape so scraper.parse_auth
        # keeps working unchanged.
        synthetic = {
            "value": {
                "uid": str(uid),
                "webtoken": webtoken,
                "organizationId": d.get("organizationId") or d.get("orgId"),
                "phonenumber": d.get("phoneNumber") or d.get("phonenumber"),
                "logintype": "1",
            },
            "expire": None,
            "isClear": True,
        }
        return base64.b64encode(
            json.dumps(synthetic, ensure_ascii=False).encode("utf-8")
        ).decode("ascii")
    return None


async def _extract(page, context):
    # Always grab the full cookie jar alongside the auth blob — viewer mode
    # needs both (localStorage seed + session cookies) to faithfully
    # reproduce a logged-in UI state. Scraper.py ignores the `cookies` key
    # (it only reads `token`), so this is additive and harmless.
    try:
        cookies = await extract_cookies_list(context, domain_hint="comein.cn")
    except Exception:
        cookies = []

    def _with_cookies(res):
        if res and cookies:
            res["cookies"] = cookies
        return res

    # 1) Fast path — primary localStorage key
    data = await extract_localstorage(page, [PRIMARY_KEY, *_FALLBACK_KEYS])
    for k in (PRIMARY_KEY, *_FALLBACK_KEYS):
        blob = data.get(k)
        if blob and _looks_like_jm_blob(blob):
            return _with_cookies({"token": blob})

    # 2) Wider scan — any localStorage key whose value LOOKS like a JM blob.
    #    Catches future key rotations without code changes.
    all_ls = await dump_all_localstorage(page)
    for k, v in all_ls.items():
        if _looks_like_jm_blob(v):
            return _with_cookies({"token": v})

    # 3) Network fallback — rebuild the blob from json_user_login response.
    net = network_log(context)
    blob = _extract_token_from_login_response(net)
    if blob:
        return _with_cookies({"token": blob})

    return None


async def run(session_id: str, payload: dict) -> int:
    mode = (payload.get("mode") or "password").lower()
    creds_path = SCRIPT_DIR / "credentials.json"

    if mode == "qr":
        return await run_qr_login(
            platform="jinmen",
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
        platform="jinmen",
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
