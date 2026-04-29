"""Data-source credential management (admin-only).

Exposes 7 crawler platforms' credentials for inspection, manual refresh via
pasted token, and — where supported — automated login via Playwright that runs
out-of-process.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, Field

from backend.app.deps import get_current_user
from backend.app.models.user import User
from backend.app.services import auto_login_runner, credential_manager, crawler_manager
from backend.app.services import cdp_screencast_session as screencast

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────────────────


class PlatformListItem(BaseModel):
    key: str
    display_name: str
    supports_auto_login: bool
    login_hint: str
    login_identifier: str = "phone"
    login_needs_password: bool = False
    login_mode: str = "password"
    supports_qr_login: bool = False
    has_saved_login: bool = False
    saved_identifier: str = ""
    has_credentials: bool
    credentials_path: str
    last_refreshed: str | None
    token_fields: dict[str, str]
    health: str
    health_detail: str
    health_checked_at: str | None
    last_data_at: str | None = None
    data_age_hours: float | None = None
    data_total: int | None = None
    # Set when the platform ran a content-quality probe (currently AceCamp only).
    # ratio=0.0 → all recent docs have real content; ratio≥0.7 → detail 被封.
    content_empty_ratio: float | None = None
    content_sample_size: int | None = None


class ManualTokenBody(BaseModel):
    # Each platform has its own field layout (see PLATFORMS spec). The UI sends
    # whatever fields the user typed; we intersect with known token_fields.
    fields: dict[str, str] = Field(default_factory=dict)


class LoginBody(BaseModel):
    # Shape varies per platform; all strings.
    # - If `mode="qr"`, identifier/password are ignored (QR scan drives it).
    # - If `mode="password"`, phone/email + password are required.
    # - If `mode="sms"` (or password omitted), the script takes the SMS-OTP path.
    phone: str | None = None
    email: str | None = None
    password: str | None = None
    mode: str | None = None  # "qr" | "password" | "sms" — None → spec default
    remember: bool = False   # persist identifier+password for re-use


class SavedLoginBody(BaseModel):
    # Used by POST /login/saved — no body needed in practice, but accept empty.
    pass


class OtpBody(BaseModel):
    code: str = Field(min_length=1, max_length=32)


# ── Endpoints ────────────────────────────────────────────────────────────


@router.get("", response_model=list[PlatformListItem])
async def list_data_sources(_: User = Depends(get_current_user)) -> list[dict[str, Any]]:
    statuses = await credential_manager.status_all()
    return [asdict(s) for s in statuses]


@router.get("/ingestion-daily")
async def ingestion_daily(
    days: int = Query(14, ge=1, le=60),
    _: User = Depends(get_current_user),
) -> dict[str, Any]:
    """Daily ingestion counts per platform (CST-bucketed)."""
    return await credential_manager.ingestion_daily_series(days=days)


@router.get("/{key}", response_model=PlatformListItem)
async def get_data_source(key: str, _: User = Depends(get_current_user)):
    try:
        status = await credential_manager.status_with_health(key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return asdict(status)


@router.post("/{key}/token", response_model=PlatformListItem)
async def set_manual_token(
    key: str,
    body: ManualTokenBody,
    _: User = Depends(get_current_user),
):
    """Overwrite credentials.json with whatever fields the admin pasted.

    We merge with any existing fields the user didn't touch so that partial
    updates (e.g. rotate only `cookie`, keep `api_key`) work.
    """
    try:
        spec = credential_manager.get_platform(key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    allowed = set(spec.token_fields)
    incoming = {k: v for k, v in body.fields.items() if k in allowed and v}
    if not incoming:
        raise HTTPException(
            status_code=400,
            detail=f"No valid fields provided. Accepted: {sorted(allowed)}",
        )

    existing = credential_manager.read_credentials(key)
    existing.update(incoming)
    credential_manager.write_credentials(key, existing)

    status = await credential_manager.status_with_health(key)
    return asdict(status)


@router.post("/{key}/login")
async def start_auto_login(
    request: Request,
    key: str,
    body: LoginBody,
    _: User = Depends(get_current_user),
):
    """Kick off Playwright-driven login. Returns a session_id for polling."""
    try:
        spec = credential_manager.get_platform(key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    if not spec.supports_auto_login:
        raise HTTPException(
            status_code=400,
            detail=f"{spec.display_name} 不支持自动登录, 请手动粘贴 token",
        )

    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")

    mode = (body.mode or spec.login_mode or "password").lower()
    identifier = body.email if spec.login_identifier == "email" else body.phone
    identifier = (identifier or "").strip()

    payload: dict[str, Any] = {"mode": mode}
    if identifier:
        payload["phone"] = identifier
        payload["email"] = identifier
        payload["identifier"] = identifier
    if body.password:
        payload["password"] = body.password

    # Persist identifier+password if the user asked ("记住密码").
    if body.remember and identifier and body.password:
        credential_manager.write_saved_login(key, identifier, body.password)

    try:
        session_id = await auto_login_runner.launch(redis, key, payload)
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {"session_id": session_id, "status": "STARTING", "mode": mode}


@router.post("/{key}/login/saved")
async def start_login_with_saved(
    request: Request,
    key: str,
    _: User = Depends(get_current_user),
):
    """Trigger auto-login using the stored identifier+password."""
    try:
        spec = credential_manager.get_platform(key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    if not spec.supports_auto_login:
        raise HTTPException(status_code=400, detail="不支持自动登录")

    saved = credential_manager.read_saved_login(key)
    if not saved or not saved.get("identifier") or not saved.get("password"):
        raise HTTPException(status_code=404, detail="尚未保存密码")

    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")

    identifier = saved["identifier"]
    payload: dict[str, Any] = {
        "mode": "password",
        "phone": identifier,
        "email": identifier,
        "identifier": identifier,
        "password": saved["password"],
    }
    try:
        session_id = await auto_login_runner.launch(redis, key, payload)
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"session_id": session_id, "status": "STARTING", "mode": "password"}


@router.delete("/{key}/saved-login")
async def forget_saved_login(key: str, _: User = Depends(get_current_user)):
    try:
        credential_manager.get_platform(key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    removed = credential_manager.delete_saved_login(key)
    return {"removed": removed}


@router.get("/{key}/login/{session_id}")
async def get_login_status(
    request: Request,
    key: str,
    session_id: str,
    _: User = Depends(get_current_user),
):
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    return await auto_login_runner.get_status(redis, key, session_id)


# ── Crawler lifecycle (scraper.py --watch) ───────────────────────────────


@router.get("/{key}/crawler")
async def crawler_status(
    request: Request,
    key: str,
    _: User = Depends(get_current_user),
):
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    try:
        return await crawler_manager.status(redis, key)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{key}/crawler/start")
async def crawler_start(
    request: Request,
    key: str,
    _: User = Depends(get_current_user),
):
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    try:
        return await crawler_manager.start(redis, key, force=True)
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{key}/crawler/stop")
async def crawler_stop(
    request: Request,
    key: str,
    _: User = Depends(get_current_user),
):
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    try:
        return await crawler_manager.stop(redis, key)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# ── QR refresh ───────────────────────────────────────────────────────────


@router.post("/{key}/login/{session_id}/refresh-qr")
async def refresh_qr(
    request: Request,
    key: str,
    session_id: str,
    _: User = Depends(get_current_user),
):
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    await auto_login_runner.request_qr_refresh(redis, session_id)
    return {"ok": True}


@router.post("/{key}/login/{session_id}/otp")
async def submit_login_otp(
    request: Request,
    key: str,
    session_id: str,
    body: OtpBody,
    _: User = Depends(get_current_user),
):
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    await auto_login_runner.submit_otp(redis, session_id, body.code)
    return {"ok": True}


# ── CDP screencast (remote browser via Chrome DevTools screencast) ───────
#
# For platforms where headless detection or layered consent steps block
# auto-login, we expose a "remote browser" — Chromium runs on the server,
# we push JPEG frames via WebSocket, and the user's mouse/keyboard events
# flow back over the same socket. Same idea as noVNC but no X server
# required, so no sudo needed.


def _screencast_extract_for(platform_key: str):
    """Resolve the platform's extract callback from its auto_login.py.

    Supports two styles:
      - module-level `_extract(page, context)` (alphapai, gangtise, jinmen,
        acecamp, thirdbridge)
      - `_make_extractor(...)` factory that returns an extract callable
        (funda, meritco — because they need closure state like existing
        api_key or a network-header trap)

    For factory style, we try to call `_make_extractor()` with no args,
    and fall back to reading `credentials.json` to supply needed fields.
    """
    import importlib.util
    import sys as _sys
    import json as _json

    from backend.app.services.auto_login_runner import AUTO_LOGIN_SCRIPTS, _CRAWL_DIR

    if platform_key not in AUTO_LOGIN_SCRIPTS:
        return None
    script_path = _CRAWL_DIR / AUTO_LOGIN_SCRIPTS[platform_key]
    if not script_path.exists():
        return None
    mod_name = f"crawl_auto_login_{platform_key}"
    spec = importlib.util.spec_from_file_location(mod_name, str(script_path))
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    _sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)

    # 1) Simple case: module has _extract directly.
    fn = getattr(mod, "_extract", None)
    if fn is not None:
        return fn

    # 2) Factory case: _make_extractor(...). Build one.
    factory = getattr(mod, "_make_extractor", None)
    if factory is None:
        return None
    # Try zero-arg call first (meritco).
    try:
        return factory()
    except TypeError:
        pass
    # Funda-style: factory wants an api_key. Pull from credentials.json.
    existing_api_key = ""
    try:
        creds_path = script_path.parent / "credentials.json"
        if creds_path.exists():
            existing_api_key = (
                _json.loads(creds_path.read_text(encoding="utf-8")) or {}
            ).get("api_key", "")
    except Exception:
        pass
    try:
        return factory(existing_api_key)
    except Exception:
        return None


def _login_url_for(platform_key: str) -> str | None:
    """Cheap: just import the platform's auto_login module and read LOGIN_URL."""
    from backend.app.services.auto_login_runner import AUTO_LOGIN_SCRIPTS, _CRAWL_DIR
    if platform_key not in AUTO_LOGIN_SCRIPTS:
        return None
    script_path = _CRAWL_DIR / AUTO_LOGIN_SCRIPTS[platform_key]
    if not script_path.exists():
        return None
    # Naive regex parse — avoids triggering side effects.
    import re
    try:
        src = script_path.read_text(encoding="utf-8")
        m = re.search(r'LOGIN_URL\s*=\s*["\']([^"\']+)["\']', src)
        return m.group(1) if m else None
    except Exception:
        return None


@router.post("/{key}/screencast/start")
async def screencast_start(
    key: str,
    _: User = Depends(get_current_user),
):
    """Spawn a remote-browser session. Returns session_id to pass to WS URL."""
    try:
        spec = credential_manager.get_platform(key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    extract_fn = _screencast_extract_for(key)
    if extract_fn is None:
        raise HTTPException(status_code=400, detail=f"平台 {key} 缺少 _extract 回调")
    login_url = _login_url_for(key)
    if login_url is None:
        raise HTTPException(status_code=400, detail=f"平台 {key} 缺少 LOGIN_URL")

    try:
        sess = await screencast.create_session(
            platform=key,
            login_url=login_url,
            credentials_path=spec.credentials_path,
            extract_fn=extract_fn,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"启动失败: {exc}")

    return {
        "session_id": sess.session_id,
        "status": sess.status,
        "message": sess.status_msg,
        "viewport": [sess.viewport_w, sess.viewport_h],
    }


# ── Viewer mode: open platform logged-in for side-by-side data comparison ──

# Per-platform section URLs the user can jump directly into for data
# comparison. First entry is the default when no section is specified.
# Keys match the scraper category/type names where possible so the
# frontend can match up with its own data tabs.
_VIEWER_SECTIONS: dict[str, list[tuple[str, str, str]]] = {
    # key, label, deep-link URL
    "gangtise": [
        # Always bootstrap at portal root — our post_nav_fn flips the hash
        # to the requested section once the portal's activeProduct is set.
        # The stand-alone /research/ /summary/ /chief/ SPA subdomains don't
        # render under headless Chromium even with token injected.
        ("research", "研报",    "https://open.gangtise.com/"),
        ("summary",  "纪要",    "https://open.gangtise.com/"),
        ("chief",    "首席观点", "https://open.gangtise.com/"),
    ],
    "alphapai": [
        ("roadshow", "会议/路演",  "https://alphapai-web.rabyte.cn/reading/home/meeting"),
        ("comment",  "券商点评",   "https://alphapai-web.rabyte.cn/reading/home/comment"),
        ("report",   "券商研报",   "https://alphapai-web.rabyte.cn/reading/home/point"),
        ("wechat",   "社媒/微信",  "https://alphapai-web.rabyte.cn/reading/social-media"),
    ],
    "funda": [
        ("home", "主页", "https://funda.ai/"),
    ],
    "acecamp": [
        # AceCamp SPA 路由 `/latest/:type` — type ∈ {all, minute, original,
        # stock, industry, repost} 对应平台 notes_types / recent_visit_types
        # 字典. 深链接直接进对应内容流, 避免 `/` 根页面只显示游客 landing
        # (用户以为"号被封"的根因就是 viewer 默认落在 `/`).
        ("latest_all",      "最新 · 全部",     "https://www.acecamptech.com/latest/all"),
        ("latest_minute",   "最新 · 纪要",     "https://www.acecamptech.com/latest/minute"),
        ("latest_original", "最新 · 文章/研报", "https://www.acecamptech.com/latest/original"),
        ("community",       "观点社区",        "https://www.acecamptech.com/community"),
        ("event_calendar",  "路演 · 日历",     "https://www.acecamptech.com/event/calendar"),
        ("collection",      "我的收藏",        "https://www.acecamptech.com/personalCenter/collection"),
        ("home",            "主页",            "https://www.acecamptech.com/"),
    ],
    "thirdbridge": [
        ("home", "全部论坛", "https://forum.thirdbridge.com/zh/home/all"),
    ],
    "jinmen": [
        ("meetings",         "会议纪要",        "https://brm.comein.cn/#/conference"),
        ("reports",          "研报 (国内)",     "https://brm.comein.cn/#/research-report"),
        ("oversea_realtime", "外资研报 · 实时", "https://brm.comein.cn/reportManage/index?tabType=oversea&subTabType=realtime"),
        ("oversea_delay",    "外资研报 · 延时", "https://brm.comein.cn/reportManage/index?tabType=oversea&subTabType=delay"),
        ("home",             "主页",           "https://brm.comein.cn/"),
    ],
    "meritco": [
        # 必须走 `/classic/forum?forumType=N` 进入 — 直接访问 `/forum?forumType=N`
        # SPA 只加载骨架不触发 list API, 右侧面板会留白. `/classic/*` 入口走完
        # 路由 init 才会 fire `/matrix-search/forum/select/list` + company/industries
        # + calendar, 然后 SPA 自己 replace URL 为 `/forum?...`. (2026-04-24 验证)
        ("t2",   "专业内容 (纪要+研报)", "https://research.meritco-group.com/classic/forum?forumType=2"),
        ("t3",   "久谦自研",          "https://research.meritco-group.com/classic/forum?forumType=3"),
        ("t1",   "活动",              "https://research.meritco-group.com/classic/forum?forumType=1"),
        ("home", "主页",              "https://research.meritco-group.com/classic/forum"),
    ],
    "alphaengine": [
        # Alphaengine is a hash-routed SPA — `/#/summary-center` renders all
        # 4 tabs; `?code=X` isn't a real router param (404s). The tab select
        # happens client-side after the hash route loads; user clicks through.
        ("summary",       "纪要",    "https://www.alphaengine.top/#/summary-center"),
        ("chinaReport",   "国内研报", "https://www.alphaengine.top/#/summary-center"),
        ("foreignReport", "海外研报", "https://www.alphaengine.top/#/summary-center"),
        ("news",          "资讯",    "https://www.alphaengine.top/#/summary-center"),
    ],
    # meritco: auth via per-request RSA-signed header + uncaptured cookie
    # jar; viewer injection not supported. Listed elsewhere.
}


def _viewer_section_url(platform_key: str, section: str | None) -> str | None:
    """Resolve (platform, section) → deep-link URL. Missing section falls back
    to the first entry; missing platform returns None.
    """
    entries = _VIEWER_SECTIONS.get(platform_key) or []
    if not entries:
        return None
    if section:
        for key, _, url in entries:
            if key == section:
                return url
    return entries[0][2]


def _parse_cookie_header(cookie_str: str, domain: str) -> list[dict]:
    """Turn a `k=v; k2=v2; …` header into Playwright cookie dicts."""
    out: list[dict] = []
    for part in (cookie_str or "").split(";"):
        if "=" not in part:
            continue
        name, _, value = part.strip().partition("=")
        if not name:
            continue
        out.append({
            "name": name, "value": value,
            "domain": domain, "path": "/",
            "httpOnly": False, "secure": True,
            "sameSite": "Lax",
        })
    return out


def _build_ls_inject_script(entries: dict[str, str], guard_keys: list[str]) -> str:
    """Build an init-script that:
      1. Monkey-patches Storage.prototype.clear / removeItem to protect
         ``guard_keys`` — many SPAs call localStorage.clear() during boot,
         which wipes our just-set tokens. We make those a no-op for the
         keys we care about.
      2. Seeds the entries into localStorage on every document start.
      3. Re-seeds every 500 ms for the first ~15 seconds in case the SPA
         bypasses the prototype hook (e.g. direct descriptor assignment).
    """
    import json as _j
    seeds = "\n".join(
        f"  try {{ localStorage.setItem({_j.dumps(k)}, {_j.dumps(v)}); }} catch(e) {{}}"
        for k, v in entries.items() if v
    )
    guard = _j.dumps(guard_keys)
    return f"""
(() => {{
  const GUARD = {guard};
  try {{
    const oClear = Storage.prototype.clear;
    Storage.prototype.clear = function() {{
      // keep our guard keys, wipe the rest — preserves SPA's ability to
      // reset non-auth state while we survive.
      const saved = {{}};
      for (const k of GUARD) {{
        try {{ saved[k] = this.getItem(k); }} catch(e) {{}}
      }}
      try {{ oClear.call(this); }} catch(e) {{}}
      for (const [k,v] of Object.entries(saved)) {{
        if (v != null) try {{ this.setItem(k, v); }} catch(e) {{}}
      }}
    }};
    const oRemove = Storage.prototype.removeItem;
    Storage.prototype.removeItem = function(k) {{
      if (GUARD.indexOf(k) >= 0) return;
      return oRemove.apply(this, arguments);
    }};
  }} catch(e) {{}}

{seeds}

  // Defensive re-seed: if the page somehow wipes anyway (e.g. direct
  // reassignment of window.localStorage), keep re-injecting for ~15s.
  let ticks = 0;
  const iv = setInterval(() => {{
    ticks++;
{seeds}
    if (ticks > 30) clearInterval(iv);
  }}, 500);
}})();
"""


def _viewer_inject_for(platform_key: str, creds: dict):
    """Returns an async ``inject_fn(context)`` that pre-seeds saved creds so
    the browser lands already-logged-in. Returns None if the platform's
    creds can't be safely injected.
    """
    # Gangtise — localStorage: G_token (UUID) + G_cnfr_uid + G_user_key + G_tenantId
    if platform_key == "gangtise":
        token = (creds.get("token") or "").strip()
        if not token:
            return None
        entries = {
            "G_token":      token,
            "token":        token,
            "G_cnfr_uid":   (creds.get("uid") or "").strip(),
            "G_user_key":   (creds.get("user_key") or "").strip(),
            "G_tenantId":   (creds.get("tenant_id") or "").strip(),
        }
        guard = list(entries.keys()) + ["G_user"]
        script = _build_ls_inject_script(entries, guard)

        async def _inject(context):
            await context.add_init_script(script)
        return _inject

    # AlphaPai — localStorage USER_AUTH_TOKEN (JWT)
    if platform_key == "alphapai":
        token = (creds.get("token") or creds.get("USER_AUTH_TOKEN") or "").strip()
        if not token:
            return None
        entries = {"USER_AUTH_TOKEN": token, "token": token}
        script = _build_ls_inject_script(entries, list(entries.keys()))

        async def _inject(context):
            await context.add_init_script(script)
        return _inject

    # AlphaEngine — localStorage.token (JWT, eyJ 开头)
    if platform_key == "alphaengine":
        token = (creds.get("token") or "").strip()
        if not token:
            return None
        entries = {"token": token}
        script = _build_ls_inject_script(entries, list(entries.keys()))

        async def _inject(context):
            await context.add_init_script(script)
        return _inject

    # Jinmen — localStorage.JM_AUTH_INFO (base64-JSON blob) + full cookie
    # jar from the login flow. Either on its own is insufficient for the
    # SPA: localStorage gets the SPA through auth-check, cookies carry the
    # session state the API endpoints validate.
    if platform_key == "jinmen":
        import base64 as _b64, json as _json
        jm_auth = (creds.get("JM_AUTH_INFO") or creds.get("token") or "").strip()
        saved_cookies = creds.get("cookies") if isinstance(creds.get("cookies"), list) else []
        if not jm_auth:
            # Fall back to the scraper's hardcoded module default
            try:
                from pathlib import Path as _P
                scraper_src = (_P(__file__).resolve().parents[3] /
                               "crawl" / "jinmen" / "scraper.py").read_text(encoding="utf-8")
                import re as _re
                m = _re.search(r'^JM_AUTH_INFO\s*=\s*"([^"]+)"', scraper_src, _re.M)
                if m:
                    jm_auth = m.group(1)
            except Exception:
                pass
        if not jm_auth and not saved_cookies:
            return None
        entries = {}
        if jm_auth:
            entries["JM_AUTH_INFO"] = jm_auth
        script = _build_ls_inject_script(entries, list(entries.keys())) if entries else None

        # Synthesize the .comein.cn session cookies from the JM_AUTH_INFO blob.
        # Confirmed 2026-04-22: localStorage alone does NOT log the SPA in;
        # `webtoken` cookie on .comein.cn is required to get past /login redirect.
        # We layer this *before* any saved_cookies from a real login flow so the
        # latter (richer) jar takes precedence when present.
        synth_cookies: list[dict] = []
        if jm_auth:
            try:
                inner = (_json.loads(_b64.b64decode(jm_auth).decode("utf-8")) or {}).get("value") or {}
                webtoken = inner.get("webtoken") or inner.get("token") or ""
                jid = inner.get("jid") or ""
                uid = str(inner.get("uid") or "")
                if webtoken:
                    for name in ("webtoken", "token"):
                        synth_cookies.append({
                            "name": name, "value": webtoken,
                            "domain": ".comein.cn", "path": "/",
                        })
                if jid:
                    synth_cookies.append({
                        "name": "jid", "value": jid,
                        "domain": ".comein.cn", "path": "/",
                    })
                if uid:
                    synth_cookies.append({
                        "name": "uid", "value": uid,
                        "domain": ".comein.cn", "path": "/",
                    })
            except Exception:
                pass

        async def _inject(context):
            if script:
                await context.add_init_script(script)
            # Synthesized first (best-effort), then real saved_cookies overwrite
            # any name collisions thanks to add_cookies' replace-by-name semantics.
            if synth_cookies:
                try:
                    await context.add_cookies(synth_cookies)
                except Exception:
                    pass
            if saved_cookies:
                try:
                    await context.add_cookies(saved_cookies)
                except Exception:
                    pass
        return _inject

    # Meritco — the SPA reads `localStorage.token` + `localStorage["X-User-Type"]`
    # on boot (confirmed 2026-04-24 by grepping app.d55168a2.js for getItem).
    # Just replaying cookies isn't enough — the saved cookie jar usually only
    # contains the pseudo-cookie `X-User-Type=default` (no real session cookie
    # since meritco auth is purely header-based). We must seed localStorage with
    # the same 32-hex `token` scraper.py sends in HTTP headers. Any captured
    # cookies are replayed too for completeness.
    if platform_key == "meritco":
        token = (creds.get("token") or "").strip()
        if not token:
            return None
        entries = {
            "token": token,
            "X-User-Type": "default",
        }
        script = _build_ls_inject_script(entries, list(entries.keys()))

        saved_cookies = creds.get("cookies") if isinstance(creds.get("cookies"), list) else []

        async def _inject(context):
            await context.add_init_script(script)
            if saved_cookies:
                try:
                    await context.add_cookies(saved_cookies)
                except Exception:
                    pass
        return _inject

    # Funda + AceCamp + ThirdBridge — cookie jar, paste raw as Cookie header
    if platform_key in ("funda", "acecamp", "thirdbridge"):
        raw_cookie = (creds.get("cookie") or "").strip()
        if not raw_cookie:
            return None
        domain_map = {
            "funda": ".funda.ai",
            "acecamp": ".acecamptech.com",
            "thirdbridge": ".thirdbridge.com",
        }
        domain = domain_map[platform_key]
        cookies = _parse_cookie_header(raw_cookie, domain)
        if not cookies:
            return None

        async def _inject(context):
            try:
                await context.add_cookies(cookies)
            except Exception:
                pass
        return _inject

    return None


@router.get("/{key}/viewer/sections")
async def viewer_sections(
    key: str,
    _: User = Depends(get_current_user),
):
    """List the deep-link sections available for in-viewer navigation."""
    entries = _VIEWER_SECTIONS.get(key) or []
    return {
        "sections": [
            {"key": k, "label": label, "url": url}
            for k, label, url in entries
        ],
    }


@router.post("/{key}/viewer/start")
async def viewer_start(
    key: str,
    section: str | None = None,
    _: User = Depends(get_current_user),
):
    """Spawn a remote-browser session pre-loaded with saved credentials,
    pointed at the requested section's deep-link URL. Reuses the screencast
    WS stack. Pass `?section=xxx` to jump to a specific list view
    (e.g. gangtise research / summary / chief).
    """
    try:
        spec = credential_manager.get_platform(key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    # Need saved creds, otherwise just hit /screencast/start to log in first.
    # Jinmen has no credentials.json (scraper uses a hardcoded JM_AUTH_INFO
    # module const); _viewer_inject_for("jinmen", {}) will fall back to that.
    import json as _json
    creds: dict = {}
    if spec.credentials_path.exists():
        try:
            creds = _json.loads(spec.credentials_path.read_text(encoding="utf-8"))
        except Exception:
            creds = {}
    if not creds and key != "jinmen":
        raise HTTPException(
            status_code=400,
            detail=f"平台 {key} 尚无凭证, 请先用 '登录' 写入 credentials.json",
        )

    inject_fn = _viewer_inject_for(key, creds)
    if inject_fn is None:
        raise HTTPException(
            status_code=400,
            detail=f"平台 {key} 暂不支持凭证注入实时查看 "
                   "(凭证为服务端签名/加密形态, 需在新浏览器里手动登录一次)",
        )

    target_url = _viewer_section_url(key, section) or _login_url_for(key)
    if not target_url:
        raise HTTPException(status_code=400, detail=f"平台 {key} 缺少浏览 URL")

    # Gangtise's SPA subdomain pages (/research/ etc) don't render headless —
    # we always land on portal root and hop to the section's hash after
    # activeProduct is set by the portal bootstrap.
    post_nav_fn = None
    if key == "gangtise":
        _gangtise_hash = {
            "research": "#/research",
            "summary":  "#/meeting",
            "chief":    "#/opinion",
        }.get(section or "research", "#/research")

        async def _gangtise_hop(page):
            import asyncio as _a
            # Portal needs ~3s to set up activeProduct, then we flip hash.
            await _a.sleep(3)
            try:
                await page.evaluate(f"window.location.hash = {_gangtise_hash!r}")
            except Exception:
                pass

        post_nav_fn = _gangtise_hop

    try:
        sess = await screencast.create_session(
            platform=key,
            login_url=target_url,
            credentials_path=spec.credentials_path,
            extract_fn=None,
            inject_fn=inject_fn,
            post_nav_fn=post_nav_fn,
            mode="viewer",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"启动失败: {exc}")

    return {
        "session_id": sess.session_id,
        "status": sess.status,
        "message": sess.status_msg,
        "viewport": [sess.viewport_w, sess.viewport_h],
        "target_url": target_url,
        "section": section,
    }


@router.delete("/{key}/screencast/{session_id}")
async def screencast_stop(
    key: str,
    session_id: str,
    _: User = Depends(get_current_user),
):
    await screencast.drop_session(session_id)
    return {"closed": True}


@router.post("/{key}/screencast/{session_id}/extract-now")
async def screencast_extract_now(
    key: str,
    session_id: str,
    _: User = Depends(get_current_user),
):
    """User-triggered "I've logged in, grab it now" — runs extract() with
    a short retry window (10s @ 500ms cadence) to cover race with
    localStorage hydration after navigation."""
    sess = screencast.get_session(session_id)
    if sess is None:
        raise HTTPException(status_code=404, detail="session not found")
    if sess.status in ("SUCCESS", "CLOSED"):
        return {"ok": True, "status": sess.status, "message": sess.status_msg}

    import asyncio as _aio
    last_error: str | None = None
    deadline = time.time() + 10.0
    while time.time() < deadline:
        try:
            creds = await sess.extract_fn(sess.page, sess.context)
            last_error = None
        except Exception as exc:
            creds = None
            last_error = str(exc)[:200]
        if creds:
            await sess._finish(creds)
            # Peek at crawler PID for a nice UX message.
            crawler_pid = None
            try:
                import redis.asyncio as aioredis
                from backend.app.config import get_settings
                # Env-scoped: staging reads its own Redis DB so it never
                # tags prod's crawler PIDs onto a staging login event.
                r = aioredis.from_url(get_settings().redis_url, decode_responses=True)
                crawler_pid = await r.hget(
                    f"crawler:{key}", f"pid:default",
                ) or await r.hget(f"crawler:{key}", f"pid:t2") or ""
                await r.aclose()
            except Exception:
                pass
            return {
                "ok": True,
                "status": sess.status,
                "message": sess.status_msg,
                "credential_keys": list(creds.keys()),
                "crawler_pid": crawler_pid,
            }
        await _aio.sleep(0.5)

    # Still nothing. Snapshot current state so the user gets actionable feedback.
    snap: dict[str, Any] = {
        "ok": False,
        "status": sess.status,
        "message": "10 秒内未检测到有效凭证",
    }
    if last_error:
        snap["extract_error"] = last_error
    try:
        snap["url"] = sess.page.url
    except Exception:
        pass
    try:
        snap["localStorage_keys"] = await sess.page.evaluate(
            "() => Object.keys(localStorage).slice(0, 40)"
        )
    except Exception:
        pass
    return snap


@router.get("/{key}/screencast/{session_id}")
async def screencast_status(
    key: str,
    session_id: str,
    _: User = Depends(get_current_user),
):
    sess = screencast.get_session(session_id)
    if sess is None:
        return {"status": "NOT_FOUND"}
    return {
        "session_id": session_id,
        "status": sess.status,
        "message": sess.status_msg,
        "viewport": [sess.viewport_w, sess.viewport_h],
        "uptime_s": int(time.time() - sess.started_at) if sess.started_at else 0,
    }


@router.get("/{key}/screencast/{session_id}/probe")
async def screencast_probe(
    key: str,
    session_id: str,
    _: User = Depends(get_current_user),
):
    """Snapshot the current Chromium state — URL, localStorage keys, cookie
    count, and a peek at the extract callback. Useful for diagnosing why
    a seemingly successful user scan isn't triggering SUCCESS."""
    sess = screencast.get_session(session_id)
    if sess is None:
        raise HTTPException(status_code=404, detail="session not found")
    if sess.page is None:
        raise HTTPException(status_code=503, detail="page not ready")
    out: dict[str, Any] = {"status": sess.status, "message": sess.status_msg}
    try:
        out["url"] = sess.page.url
    except Exception as exc:
        out["url_error"] = str(exc)
    try:
        ls_keys = await sess.page.evaluate(
            "() => Object.keys(localStorage).slice(0, 40)"
        )
        out["localStorage_keys"] = ls_keys
    except Exception as exc:
        out["localStorage_error"] = str(exc)
    try:
        out["cookie_count"] = len(await sess.context.cookies())
    except Exception as exc:
        out["cookie_error"] = str(exc)
    # Try extract once, non-fatal.
    try:
        creds = await sess.extract_fn(sess.page, sess.context)
        out["extract_returned_creds"] = bool(creds)
        if creds:
            out["extract_keys"] = list(creds.keys())
    except Exception as exc:
        out["extract_error"] = str(exc)[:200]
    return out


@router.get("/{key}/screencast/{session_id}/network")
async def screencast_network(
    key: str,
    session_id: str,
    since_seq: int = 0,
    search: str | None = None,
    full: int = 0,
    limit: int = 100,
    _: User = Depends(get_current_user),
):
    """DevTools-lite Network panel — returns captured XHR/fetch calls since
    the given seq. Frontend polls this and renders a scrollable request log.

    Args:
      since_seq: return only entries with seq > this (incremental fetch)
      search:    substring filter on URL (e.g. 'queryOpinionList')
      full:      1 = include full response body; 0 = only preview (default)
      limit:     max entries to return
    """
    sess = screencast.get_session(session_id)
    if sess is None:
        raise HTTPException(status_code=404, detail="session not found")
    entries = getattr(sess, "_network_log", [])
    out = []
    for e in entries:
        if e.get("seq", 0) <= since_seq:
            continue
        if search and search.lower() not in (e.get("url") or "").lower():
            continue
        # Strip internal fields + optionally body for payload size
        clean = {k: v for k, v in e.items() if not k.startswith("_")}
        if not full:
            clean.pop("response_body", None)
        out.append(clean)
        if len(out) >= limit:
            break
    return {
        "count": len(out),
        "max_seq": sess._network_seq,
        "entries": out,
    }


@router.websocket("/{key}/screencast/{session_id}/ws")
async def screencast_ws(
    websocket: WebSocket,
    key: str,
    session_id: str,
):
    """Bidirectional frame/input bridge.

    Server → client messages:
      {"type": "frame", "data": "<base64 jpeg>", "w": int, "h": int}
      {"type": "status", "status": "SUCCESS"|"FAILED", "message": "..."}

    Client → server messages (all JSON):
      {"type": "mouse", "action": "down"|"up"|"move", "x": number, "y": number, "button": "left"|"right", "clickCount": 1}
      {"type": "wheel", "x": number, "y": number, "deltaX": number, "deltaY": number}
      {"type": "key", "action": "down"|"up", "key": "a", "code": "KeyA", "text": "a", "modifiers": 0}
      {"type": "type", "text": "hello"}

    We don't enforce auth on the WS itself — session_id is random 16-hex and
    only valid during its lifetime — but for production this should check a
    JWT via query param.
    """
    sess = screencast.get_session(session_id)
    if sess is None:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()

    async def _pump_frames():
        # Fast path: ship JPEG frames as raw binary WS messages instead of
        # JSON-wrapped base64. Three wins:
        #  1. ~33% fewer bytes (no base64 inflation)
        #  2. zero JSON serialise on server / zero parse on client per frame
        #  3. browser can pipe Blob → <img.src> with one URL.createObjectURL
        # Status/heartbeat/non-frame messages still go via send_json for
        # backwards-compat with the existing client-side dispatcher.
        import base64
        while sess.status != "CLOSED":
            try:
                msg = await asyncio.wait_for(sess.frame_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # Send a heartbeat so client knows we're alive.
                try:
                    await websocket.send_json({"type": "heartbeat",
                                               "status": sess.status,
                                               "message": sess.status_msg})
                except Exception:
                    return
                continue
            try:
                if msg.get("type") == "frame":
                    raw = msg.get("data")
                    if isinstance(raw, str):
                        try:
                            jpeg_bytes = base64.b64decode(raw)
                        except Exception:
                            # Malformed data — fall back to JSON path so the
                            # client at least sees the original payload.
                            await websocket.send_json(msg)
                            continue
                        await websocket.send_bytes(jpeg_bytes)
                    else:
                        await websocket.send_json(msg)
                else:
                    await websocket.send_json(msg)
            except Exception:
                return
            if msg.get("type") == "status" and msg.get("status") in ("SUCCESS", "FAILED"):
                return

    pump_task = asyncio.create_task(_pump_frames())
    try:
        while True:
            try:
                evt = await websocket.receive_json()
            except WebSocketDisconnect:
                break
            if isinstance(evt, dict):
                await sess.input_queue.put(evt)
    finally:
        pump_task.cancel()
        try:
            await websocket.close()
        except Exception:
            pass


# (asyncio + time imports moved to top of module)
