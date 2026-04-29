"""Shared Playwright auto-login helpers.

Primary flow is **password login**. If the site surfaces an OTP / CAPTCHA
field after password submit (e.g. "login from new device" 2FA), we fall
through to an OTP wait — the frontend prompts the user and the code is
pushed back via Redis BLPOP.

Why one unified flow instead of separate SMS / password paths:
  - Most CN platforms let you log in with just password most of the time;
    the SMS step only appears on new device/IP.
  - A unified `run_login` means every platform wrapper is ~30 lines.
  - SMS-only platforms (rare) can still work if the script finds a SMS tab
    automatically and there's no password saved.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Sequence

CRAWL_DIR = Path(__file__).resolve().parent
LOG_DIR = CRAWL_DIR.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
# 5 min wait loop + 60s slack for boot / extraction / shutdown, so the
# outer subprocess watchdog (auto_login_runner's `timeout=...`) should be
# at least 370s. We keep them aligned.
OVERALL_TIMEOUT = 360
OTP_WAIT_SECONDS = 180

# playwright-stealth — lazy-imported because it's optional; if missing, we
# just skip and hope the site doesn't fingerprint. On CN platforms with WAF
# it almost always matters.
try:
    from playwright_stealth import Stealth as _Stealth  # type: ignore
    _STEALTH = _Stealth()
except Exception:
    _STEALTH = None


# Per-platform browser-context locale. Mismatch between IP geolocation and
# navigator.language is a widely-used bot-fingerprint tell (TDC / Akamai /
# PerimeterX / Cloudflare Turnstile), and also affects SPA UI language.
# Our server IP is Shanghai Telecom; CN sites stay zh-CN, US/international
# sites stay en-US to match their expected user base.
PLATFORM_LOCALE: dict[str, dict[str, str]] = {
    # platform_key: {locale, timezone_id, accept_lang}
    "alphapai":    {"locale": "zh-CN", "timezone_id": "Asia/Shanghai",
                    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6"},
    "gangtise":    {"locale": "zh-CN", "timezone_id": "Asia/Shanghai",
                    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6"},
    "jinmen":      {"locale": "zh-CN", "timezone_id": "Asia/Shanghai",
                    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6"},
    "meritco":     {"locale": "zh-CN", "timezone_id": "Asia/Shanghai",
                    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6"},
    "acecamp":     {"locale": "zh-CN", "timezone_id": "Asia/Shanghai",
                    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6"},
    "alphaengine": {"locale": "zh-CN", "timezone_id": "Asia/Shanghai",
                    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6"},
    "thirdbridge": {"locale": "zh-CN", "timezone_id": "Asia/Shanghai",
                    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6"},
    # 微信公众号管理后台 mp.weixin.qq.com — 扫码登录,session ~4 天后失效
    "wechat_mp":   {"locale": "zh-CN", "timezone_id": "Asia/Shanghai",
                    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6"},
    # US sites — match US user baseline to avoid locale-IP mismatch the other
    # way (Shanghai IP + US locale is uncommon but so is CN locale for
    # Funda/SentimenTrader's subscriber base).
    "funda":          {"locale": "en-US", "timezone_id": "America/New_York",
                       "accept_lang": "en-US,en;q=0.9"},
    "sentimentrader": {"locale": "en-US", "timezone_id": "America/New_York",
                       "accept_lang": "en-US,en;q=0.9"},
}

# Default falls back to zh-CN since our server is in Shanghai — safer match
# for any new CN platform added later.
_CN_DEFAULT_LOCALE = {
    "locale": "zh-CN",
    "timezone_id": "Asia/Shanghai",
    "accept_lang": "zh-CN,zh;q=0.9,en;q=0.6",
}


def context_opts_for(platform: str) -> dict:
    """Returns Playwright `new_context` kwargs for this platform: user_agent,
    locale, timezone_id, and matching Accept-Language header. Use when
    creating contexts for login / viewer / scraping to keep fingerprints
    aligned with the platform's expected user locale."""
    cfg = PLATFORM_LOCALE.get(platform, _CN_DEFAULT_LOCALE)
    return {
        "user_agent": DEFAULT_USER_AGENT,
        "locale": cfg["locale"],
        "timezone_id": cfg["timezone_id"],
        "extra_http_headers": {"Accept-Language": cfg["accept_lang"]},
    }


async def apply_stealth(page, platform: str | None = None) -> None:
    """Install anti-detection patches on the given page.

    Safe no-op if playwright-stealth isn't installed. Idempotent — stealth
    itself checks a marker key on the page to avoid double-application.
    If ``platform`` is passed, ``navigator.languages`` is also patched to
    match that platform's expected locale (zh-CN for most, en-US for Funda /
    SentimenTrader).
    """
    if _STEALTH is not None:
        try:
            await _STEALTH.apply_stealth_async(page)
        except Exception:
            # Never fail login just because stealth threw.
            pass
    # Playwright's locale= only patches navigator.language, not .languages —
    # leaving ['en-US','en'] in the tuple even when .language='zh-CN'. Align
    # both: a real user's languages list usually starts with their UI locale.
    cfg = PLATFORM_LOCALE.get(platform or "", _CN_DEFAULT_LOCALE)
    if cfg["locale"].startswith("zh"):
        langs = ["zh-CN", "zh", "en"]
    else:
        langs = ["en-US", "en"]
    import json as _json
    try:
        await page.add_init_script(f"""
          try {{
            Object.defineProperty(Navigator.prototype, 'languages', {{
              get: () => {_json.dumps(langs)},
              configurable: true,
            }});
          }} catch(e) {{}}
        """)
    except Exception:
        pass

# ── Default DOM selectors — permissive, most-specific first ──────────────

DEFAULT_PASSWORD_TAB = [
    'text=密码登录',
    'text=密码登陆',
    'text=账号登录',
    'text=账号登陆',
    'text=Password',
    '[role="tab"]:has-text("密码")',
    'a:has-text("密码登录")',
    'a:has-text("密码登陆")',
    'span:has-text("密码登录")',
    'span:has-text("密码登陆")',
]
DEFAULT_SMS_TAB = [
    # Users write 登陆 / 登录 interchangeably — try both.
    'text=验证码登录',
    'text=验证码登陆',
    'text=短信登录',
    'text=短信登陆',
    'text=手机登录',
    'text=手机登陆',
    'text=动态码登录',
    '[role="tab"]:has-text("验证码")',
    'a:has-text("验证码登录")',
    'a:has-text("验证码登陆")',
    'span:has-text("验证码登录")',
    'span:has-text("验证码登陆")',
]
DEFAULT_PHONE = [
    'input[placeholder*="手机"]',
    'input[placeholder*="账号"]',
    'input[placeholder*="phone" i]',
    'input[name="phone"]',
    'input[name="mobile"]',
    'input[name="username"]',
    'input[name="account"]',
    'input[type="tel"]',
]
DEFAULT_EMAIL = [
    'input[type="email"]',
    'input[name="email"]',
    'input[placeholder*="邮箱"]',
    'input[placeholder*="email" i]',
    'input[name="username"]',
]
DEFAULT_PASSWORD = [
    'input[type="password"]',
    'input[name="password"]',
    'input[placeholder*="密码"]',
]
DEFAULT_OTP = [
    'input[placeholder*="验证码"]',
    'input[placeholder*="code" i]',
    'input[name="code"]',
    'input[name="smsCode"]',
    'input[name="captcha"]',
    'input[name="verifyCode"]',
]
DEFAULT_SEND_CODE = [
    'button:has-text("获取验证码")',
    'button:has-text("发送验证码")',
    'a:has-text("获取验证码")',
    'span:has-text("获取验证码")',
    'button:has-text("Send Code")',
]
DEFAULT_SUBMIT = [
    'button:has-text("登录")',
    'button:has-text("登 录")',
    'button:has-text("登陆")',
    'button:has-text("登 陆")',
    'button:has-text("Login")',
    'button:has-text("Sign In")',
    'button[type="submit"]',
]
DEFAULT_CONSENT_CHECKBOX = [
    # Many CN login forms have an "I agree to receive SMS / accept terms"
    # checkbox that must be ticked BEFORE the "获取验证码" button enables.
    # Meritco is one of these. Order: labels → visible unchecked checkboxes.
    'label:has-text("同意接受短信")',
    'label:has-text("同意接收短信")',
    'label:has-text("同意并接受")',
    'label:has-text("我已阅读并同意")',
    'label:has-text("已阅读并同意")',
    'label:has-text("同意")',
    '[class*="agreement"] input[type="checkbox"]',
    '[class*="agree"] input[type="checkbox"]',
    '[class*="protocol"] input[type="checkbox"]',
    'input[type="checkbox"]:not(:checked)',
]
DEFAULT_CONSENT_AGREE = [
    # Most-specific phrases first (no ambiguity).
    'button:text-is("同意并继续")',
    'button:text-is("我已阅读并同意")',
    'button:text-is("我同意")',
    'button:text-is("同意")',
    'button:text-is("我已阅读")',
    'button:text-is("接受")',
    'button:text-is("Accept")',
    'button:text-is("Agree")',
    # Ant Design / custom wrappers — often <div class="ant-btn"> or role=button.
    '.ant-btn-primary:text-is("同意")',
    '.ant-btn:text-is("同意")',
    '[role="button"]:text-is("同意")',
    '[class*="btn-primary"]:text-is("同意")',
    '[class*="confirm-btn"]:text-is("同意")',
    # Anchor / span / div styled as button.
    'a:text-is("同意")',
    'span:text-is("同意并继续")',
    'span:text-is("我同意")',
    'span:text-is("同意")',
    'div:text-is("同意")',
    # Ancestor contains 同意 AND sits inside a dialog/modal — narrows to real
    # dialogs, avoids clicking random "同意" links elsewhere on the page.
    '[class*="dialog"] button:text-is("同意")',
    '[class*="modal"] button:text-is("同意")',
    '[class*="popup"] button:text-is("同意")',
    '[class*="dialog"] [role="button"]:text-is("同意")',
    '[class*="dialog"] span:text-is("同意")',
    # Class-name heuristics.
    '[class*="agree"][class*="btn"]:not([class*="disagree"])',
    '[class*="confirm"]:not([class*="cancel"])',
]
DEFAULT_QR_TAB = [
    # Explicit text — Gangtise uses "微信登录" on a <div class="type-entrance">.
    # Put this first because it's the highest-signal CN pattern.
    '[class*="type-entrance"]:has-text("微信")',
    '[class*="entrance"]:has-text("微信")',
    '[class*="entrance"]:has-text("扫码")',
    'text=微信登录',
    'text=微信登陆',
    'text=微信扫码',
    'text=扫码登录',
    'text=扫码登陆',
    'text=二维码登录',
    'text=二维码登陆',
    'text=扫一扫',
    'div:has-text("微信登录") >> nth=0',
    '[role="tab"]:has-text("扫码")',
    '[role="tab"]:has-text("微信")',
    'a:has-text("扫码登录")',
    'a:has-text("扫码登陆")',
    'a:has-text("微信登录")',
    'span:has-text("扫码登录")',
    'span:has-text("扫码登陆")',
    'span:has-text("微信登录")',
    # Icon-only fallbacks
    'img[src*="wechat" i]',
    'img[src*="weixin" i]',
    'img[alt*="微信"]',
    'img[alt*="扫码"]',
    'img[alt*="qr" i]',
    '[class*="wechat"]',
    '[class*="weixin"]',
    '[class*="qrcode"]',
    'svg[class*="qrcode"]',
    'use[xlink:href="#icon-qrcode"]',
]
# When we're on the QR view, these isolate the actual QR image or canvas.
# Selectors for "QR has expired, click to refresh" overlays / buttons.
DEFAULT_QR_REFRESH = [
    'text=点击刷新',
    'text=刷新二维码',
    'text=刷新',
    'text=重新获取',
    'text=已过期',
    'text=Refresh',
    'button:has-text("刷新")',
    'button:has-text("Refresh")',
    '[class*="refresh"]',
    '[class*="expired"]',
    'a:has-text("刷新")',
    'span:has-text("刷新")',
]
DEFAULT_QR_ELEMENT = [
    '.qr-code canvas',
    '.qrcode canvas',
    '[class*="qrcode"] canvas',
    '[class*="qr-code"] canvas',
    '[class*="QrCode"] canvas',
    'canvas[class*="qr" i]',
    'img[src^="data:image"][class*="qr" i]',
    'img[src*="qrcode"]',
    '[class*="login-qrcode"] img',
    '[class*="login-qrcode"] canvas',
    '[class*="qrCodeBox"] img',
    '[class*="qrCodeBox"] canvas',
    # Raw fallbacks — last-resort after QR switch usually there is exactly
    # one canvas / data-URI img on the page.
    'img[src^="data:image/png;base64"]',
    'canvas',
]


# ── Redis helpers ────────────────────────────────────────────────────────


def build_redis_client():
    import redis.asyncio as aioredis
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return aioredis.from_url(url, decode_responses=True)


async def update_status(redis, platform: str, session_id: str, **fields) -> None:
    key = f"login:{platform}:{session_id}"
    await redis.hset(key, mapping=fields)
    await redis.expire(key, 600)


async def wait_otp(redis, session_id: str, timeout: int = OTP_WAIT_SECONDS) -> str | None:
    key = f"login_otp:{session_id}"
    res = await redis.blpop(key, timeout=timeout)
    if res is None:
        return None
    _, code = res
    return code if isinstance(code, str) else code.decode()


async def consume_refresh_signal(redis, session_id: str) -> bool:
    """Non-blocking check for a user-triggered QR refresh. Drains the queue."""
    key = f"login_refresh:{session_id}"
    popped = False
    while True:
        res = await redis.lpop(key)
        if res is None:
            break
        popped = True
    return popped


# ── Playwright helpers ───────────────────────────────────────────────────


async def first_visible(page, selectors: Sequence[str], timeout_ms: int = 2000):
    """Probe each selector briefly in sequence, returning the first hit.

    Per-selector timeout is kept low (≤timeout_ms/N) because our defaults list
    20+ candidates — waiting the full budget on each would compound into
    minutes. We split the budget so the total stays predictable.
    """
    if not selectors:
        return None
    per = max(200, timeout_ms // len(selectors))
    for sel in selectors:
        try:
            el = await page.wait_for_selector(sel, state="visible", timeout=per)
            if el:
                return sel
        except Exception:
            continue
    return None


async def click_any(page, selectors: Sequence[str], timeout_ms: int = 1500) -> bool:
    sel = await first_visible(page, selectors, timeout_ms=timeout_ms)
    if not sel:
        return False
    try:
        await page.click(sel)
        return True
    except Exception:
        return False


async def snapshot(page, platform: str, session_id: str, reason: str) -> None:
    """Dump screenshot + HTML + a small DOM scrape so we can debug blind DOMs."""
    try:
        png = LOG_DIR / f"auto_login_{platform}_{session_id}_{reason}.png"
        await page.screenshot(path=str(png), full_page=True)
    except Exception:
        pass
    try:
        html_path = LOG_DIR / f"auto_login_{platform}_{session_id}_{reason}.html"
        html = await page.content()
        html_path.write_text(html, encoding="utf-8")
    except Exception:
        pass
    try:
        # A tiny summary: visible buttons/links/tabs and canvas/img counts.
        # Often enough to see why selectors missed.
        hints = await page.evaluate("""
            () => {
              const q = (s) => Array.from(document.querySelectorAll(s));
              const vis = (el) => {
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
              };
              const txt = (el) => (el.innerText || el.textContent || el.alt || '').trim().slice(0, 60);
              return {
                buttons: q('button').filter(vis).map(txt).filter(Boolean).slice(0, 30),
                links: q('a').filter(vis).map(txt).filter(Boolean).slice(0, 30),
                tabs: q('[role="tab"]').filter(vis).map(txt).filter(Boolean).slice(0, 30),
                icons_with_title: q('[title], [aria-label]').filter(vis)
                   .map(e => e.getAttribute('title') || e.getAttribute('aria-label'))
                   .filter(Boolean).slice(0, 30),
                canvas_count: q('canvas').length,
                img_count: q('img').length,
                qr_hint_classes: q('[class*="qr" i], [class*="QR"], [class*="scan" i]')
                   .map(e => e.className).slice(0, 10),
              };
            }
        """)
        (LOG_DIR / f"auto_login_{platform}_{session_id}_{reason}.hints.json").write_text(
            json.dumps(hints, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


async def extract_cookie_string(context) -> str:
    cookies = await context.cookies()
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies if c.get("name"))


async def extract_cookies_list(context, domain_hint: str | None = None) -> list[dict]:
    """Return Playwright-shaped cookie objects so viewer-mode can replay them
    via ``context.add_cookies()``. Unlike ``extract_cookie_string``, this
    preserves domain / path / expires / httpOnly / secure / sameSite — all
    needed for the server to accept the session cookie.

    If ``domain_hint`` is given, filter to cookies matching that domain (for
    platforms that embed third-party trackers we don't want to replay).
    """
    raw = await context.cookies()
    out: list[dict] = []
    for c in raw or []:
        if not c.get("name"):
            continue
        if domain_hint and domain_hint not in (c.get("domain") or ""):
            continue
        entry = {"name": c["name"], "value": c.get("value", "")}
        for k in ("domain", "path", "expires", "httpOnly", "secure", "sameSite"):
            if k in c and c[k] not in (None, -1):
                entry[k] = c[k]
        entry.setdefault("path", "/")
        out.append(entry)
    return out


async def extract_localstorage(page, keys: Sequence[str]) -> dict[str, str]:
    """Read specific localStorage keys.

    Uses `window.__origLocalStorage` if present (captured by the CDP
    screencast init script) to bypass any page-level override. Falls back
    to plain `localStorage` for flows that didn't inject that hook.
    """
    out: dict[str, str] = {}
    for k in keys:
        try:
            val = await page.evaluate(
                "(k) => { const ls = window.__origLocalStorage || localStorage;"
                " try { return ls.getItem(k); } catch(e) { return null; } }",
                k,
            )
            if val:
                out[k] = val
        except Exception:
            continue
    return out


async def dump_all_localstorage(page) -> dict[str, str]:
    """Read every key/value pair in localStorage. Useful when a platform
    rotates the auth-blob key name and we need to scan with regex."""
    try:
        return await page.evaluate("""() => {
          const ls = window.__origLocalStorage || localStorage;
          const out = {};
          try {
            for (let i = 0; i < ls.length; i++) {
              const k = ls.key(i);
              try { out[k] = ls.getItem(k) || ''; } catch(e) {}
            }
          } catch(e) {}
          return out;
        }""") or {}
    except Exception:
        return {}


def network_log(context) -> list[dict]:
    """Return the CDP screencast session's recent XHR/fetch ring buffer, or
    `[]` when called outside a screencast (e.g. from auto_login_runner's
    headless subprocess). The buffer is mutated in place by the session, so
    callers should iterate without long-term references."""
    sess = getattr(context, "_screencast_session", None)
    if sess is None:
        return []
    return list(getattr(sess, "_network_log", None) or [])


async def _finish_creds(
    redis, platform: str, session_id: str,
    creds: dict,
    extra_credential_fields: dict | None,
    credentials_path: Path,
    message: str = "登录成功",
) -> None:
    """Write credentials.json + mark session SUCCESS. Shared by fast-path and
    normal paths so both branches emit identical status."""
    if extra_credential_fields:
        creds.update(extra_credential_fields)
    creds["updated_at"] = datetime.now(timezone.utc).isoformat()
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_path.write_text(
        json.dumps(creds, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    await update_status(
        redis, platform, session_id,
        status="SUCCESS",
        message=f"{message}, 凭证写入 {credentials_path.name}",
        ended_at=datetime.now(timezone.utc).isoformat(),
        qr_image="",
    )


# ── Unified login flow ───────────────────────────────────────────────────


ExtractFn = Callable[[Any, Any], Awaitable[dict | None]]


# Risk-control lockout detection — when the page text contains any of these
# phrases, we STOP immediately instead of looping. Each retry after lockout
# typically extends the cooldown and can escalate from "10 min" → "1 hour"
# → "permanent ban".
_LOCKOUT_PHRASES = (
    "输入验证码错误次数过多", "验证码错误次数过多", "账号暂时锁定",
    "暂时锁定", "账号已锁定", "账户已锁定", "请稍后再试", "尝试次数过多",
    "frequently", "too many attempts", "temporarily locked",
)


class CaptchaLockout(RuntimeError):
    """Platform risk-control triggered a CAPTCHA / account lockout.
    Caller must abort the current login attempt and NOT retry."""


async def _check_lockout(page) -> None:
    """Read body innerText; raise CaptchaLockout if any phrase matches.
    Best-effort (DOM errors swallowed)."""
    try:
        txt = await page.evaluate(
            "() => (document.body && document.body.innerText || '').slice(0, 4000)"
        )
    except Exception:
        return
    if not isinstance(txt, str):
        return
    low = txt.lower()
    for phrase in _LOCKOUT_PHRASES:
        if phrase.lower() in low:
            raise CaptchaLockout(
                f"平台风控锁定 ({phrase}) — 停止登录流以免继续触发。"
                f"等 10 分钟后再试, 或改用浏览器 cookie 直接覆盖 credentials.json"
            )


async def run_login(
    *,
    platform: str,
    session_id: str,
    login_url: str,
    identifier: str,
    password: str,
    extract: ExtractFn,
    credentials_path: Path,
    identifier_sels: Sequence[str] = DEFAULT_PHONE,
    password_sels: Sequence[str] = DEFAULT_PASSWORD,
    password_tab_sels: Sequence[str] = DEFAULT_PASSWORD_TAB,
    sms_tab_sels: Sequence[str] = DEFAULT_SMS_TAB,
    otp_sels: Sequence[str] = DEFAULT_OTP,
    send_code_sels: Sequence[str] = DEFAULT_SEND_CODE,
    submit_sels: Sequence[str] = DEFAULT_SUBMIT,
    extra_credential_fields: dict | None = None,
    wait_after_goto_ms: int = 1200,
    user_data_dir: Path | None = None,
    skip_if_logged_in_s: float = 2.0,
) -> int:
    """Universal login flow.

    Strategy:
      1. Goto login URL, wait for SPA to settle.
      2. Try to switch to password tab (silent if no tabs exist).
      3. Fill identifier.
      4. If password available AND password input visible → fill + submit.
      5. If after submit an OTP field appears → OTP_NEEDED → BLPOP → fill + resubmit.
      6. If password path not available (no password arg) → SMS fallback:
         click SMS tab, click send-code, OTP_NEEDED → BLPOP → fill → submit.
      7. Poll extract() until it returns non-empty creds or times out.
    """
    redis = build_redis_client()
    has_pwd = bool(password)

    await update_status(
        redis, platform, session_id,
        status="RUNNING",
        message="正在启动 Chromium…",
        mode="password" if has_pwd else "sms",
    )

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        await update_status(
            redis, platform, session_id,
            status="FAILED",
            message="Playwright 未安装",
            ended_at=datetime.now(timezone.utc).isoformat(),
        )
        await redis.aclose()
        return 3

    async with async_playwright() as pw:
        try:
            launch_args = ["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            # Platform-aware context opts (locale + timezone + Accept-Language).
            # CN platforms → zh-CN / Asia/Shanghai to match our server's
            # Shanghai Telecom IP. US platforms (Funda, SentimenTrader) →
            # en-US / New_York to match their expected subscriber base.
            # IP↔locale mismatch is a classic TDC / WAF fingerprint tell.
            _ctx_opts = context_opts_for(platform)
            if user_data_dir is not None:
                user_data_dir.mkdir(parents=True, exist_ok=True)
                context = await pw.chromium.launch_persistent_context(
                    user_data_dir=str(user_data_dir),
                    headless=True,
                    args=launch_args,
                    **_ctx_opts,
                )
                browser = context.browser
            else:
                browser = await pw.chromium.launch(headless=True, args=launch_args)
                context = await browser.new_context(**_ctx_opts)
        except Exception as exc:
            await update_status(
                redis, platform, session_id,
                status="FAILED",
                message=f"Chromium 启动失败: {exc}",
                ended_at=datetime.now(timezone.utc).isoformat(),
            )
            await redis.aclose()
            return 4

        pages = context.pages if user_data_dir is not None else []
        page = pages[0] if pages else await context.new_page()
        # Anti-detection — masks navigator.webdriver, chrome.runtime, WebGL,
        # canvas, plugin list, permissions, plus navigator.languages aligned
        # to the platform's locale (CN for most, EN for Funda/SentimenTrader).
        await apply_stealth(page, platform=platform)

        try:
            await update_status(redis, platform, session_id, message="加载登录页…")
            await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(wait_after_goto_ms)

            # 风控锁定早探 — 刚落地就显示"锁定"说明之前已被触发, 不要再提交
            # 任何验证码 / 密码, 直接退出等冷却.
            await _check_lockout(page)

            # Dismiss any blocking user-agreement / privacy modal before
            # trying to interact with the login form itself.
            await click_any(page, DEFAULT_CONSENT_AGREE, timeout_ms=1500)
            await page.wait_for_timeout(300)

            # Fast path: cached cookies may auto-log-in on page load.
            if skip_if_logged_in_s > 0:
                end = asyncio.get_event_loop().time() + skip_if_logged_in_s
                while asyncio.get_event_loop().time() < end:
                    try:
                        maybe = await extract(page, context)
                        if maybe:
                            await _finish_creds(
                                redis, platform, session_id, maybe,
                                extra_credential_fields, credentials_path,
                                message="已从缓存会话直接登录",
                            )
                            return 0
                    except Exception:
                        pass
                    await page.wait_for_timeout(400)

            # Try to switch to password tab first (default for most sites is
            # password, but some default to SMS).
            if has_pwd:
                await click_any(page, password_tab_sels, timeout_ms=1500)
                await page.wait_for_timeout(400)

            # Fill identifier.
            ident_sel = await first_visible(page, identifier_sels, timeout_ms=6000)
            if not ident_sel:
                await snapshot(page, platform, session_id, "no_identifier_input")
                raise RuntimeError("找不到账号输入框 (DOM 变更?)")
            await page.fill(ident_sel, identifier)

            if has_pwd:
                pwd_sel = await first_visible(page, password_sels, timeout_ms=3000)
                if not pwd_sel:
                    await snapshot(page, platform, session_id, "no_password_input")
                    raise RuntimeError("找不到密码输入框 (页面可能只支持验证码)")
                await page.fill(pwd_sel, password)

                submit_sel = await first_visible(page, submit_sels, timeout_ms=3000)
                if not submit_sel:
                    await snapshot(page, platform, session_id, "no_submit_button")
                    raise RuntimeError("找不到登录按钮")
                await update_status(redis, platform, session_id, message="提交密码…")
                await page.click(submit_sel)
                await page.wait_for_timeout(1500)

                # Probe for a post-password OTP prompt (2FA / new device).
                otp_sel = await first_visible(page, otp_sels, timeout_ms=3000)
                if otp_sel:
                    await update_status(
                        redis, platform, session_id,
                        status="OTP_NEEDED",
                        needs_otp="1",
                        message="需要短信验证码, 请查收并输入",
                    )
                    code = await wait_otp(redis, session_id)
                    if not code:
                        raise RuntimeError("等待验证码超时 (3 分钟)")
                    await page.fill(otp_sel, code.strip())
                    await update_status(
                        redis, platform, session_id,
                        status="RUNNING",
                        needs_otp="0",
                        message="验证码已填写, 提交…",
                    )
                    submit2 = await first_visible(page, submit_sels, timeout_ms=2000)
                    if submit2:
                        await page.click(submit2)
            else:
                # SMS-only path: click SMS tab, request code, wait, fill, submit.
                await click_any(page, sms_tab_sels, timeout_ms=2000)
                await page.wait_for_timeout(400)
                # Consent checkbox — Meritco and similar sites disable the
                # "获取验证码" button until you've ticked "同意接受短信". Click
                # whatever matches; harmless if no checkbox exists.
                consented = await click_any(page, DEFAULT_CONSENT_CHECKBOX, timeout_ms=1500)
                await update_status(
                    redis, platform, session_id,
                    message=f"点击获取验证码… (consent={consented})",
                )
                if not await click_any(page, send_code_sels, timeout_ms=4000):
                    await snapshot(page, platform, session_id, "no_send_code_button")
                    raise RuntimeError("找不到获取验证码按钮 (该平台可能不支持 SMS 登录)")
                await update_status(
                    redis, platform, session_id,
                    status="OTP_NEEDED",
                    needs_otp="1",
                    message="验证码已发送, 请查收并输入",
                )
                code = await wait_otp(redis, session_id)
                if not code:
                    raise RuntimeError("等待验证码超时 (3 分钟)")
                otp_sel = await first_visible(page, otp_sels, timeout_ms=3000)
                if not otp_sel:
                    await snapshot(page, platform, session_id, "no_otp_input_after_send")
                    raise RuntimeError("找不到验证码输入框")
                await page.fill(otp_sel, code.strip())
                await update_status(
                    redis, platform, session_id,
                    status="RUNNING",
                    needs_otp="0",
                    message="验证码已填写, 提交…",
                )
                submit_sel = await first_visible(page, submit_sels, timeout_ms=2000)
                if submit_sel:
                    await page.click(submit_sel)

            # Poll for successful login via the extractor.
            # 每 ~1s 探一次风控 banner;命中即抛 CaptchaLockout, 由外层 except 标 FAILED 退出.
            deadline = asyncio.get_event_loop().time() + 35
            creds: dict | None = None
            lockout_check_counter = 0
            while asyncio.get_event_loop().time() < deadline:
                try:
                    creds = await extract(page, context)
                except Exception:
                    creds = None
                if creds:
                    break
                lockout_check_counter += 1
                if lockout_check_counter % 2 == 0:  # 每 ~1.4s 探一次
                    await _check_lockout(page)
                await page.wait_for_timeout(700)

            if not creds:
                await snapshot(page, platform, session_id, "no_creds_after_login")
                # 提交后页面可能刚渲染出锁定 banner — 最后一次明确探测, 提供精确错因
                await _check_lockout(page)
                raise RuntimeError("登录后未提取到凭证 (密码错 / 验证码错 / 风控)")

            await _finish_creds(
                redis, platform, session_id, creds,
                extra_credential_fields, credentials_path,
                message="登录成功",
            )
            return 0

        except CaptchaLockout as exc:
            # 明确风控锁定 — 前端收到这条消息应停止自动重试, 等冷却或走人工登录
            await update_status(
                redis, platform, session_id,
                status="LOCKED_OUT",
                message=str(exc)[:400],
                ended_at=datetime.now(timezone.utc).isoformat(),
                lockout="1",
            )
            return 5
        except Exception as exc:
            await update_status(
                redis, platform, session_id,
                status="FAILED",
                message=str(exc)[:400],
                ended_at=datetime.now(timezone.utc).isoformat(),
            )
            return 1
        finally:
            try:
                await context.close()
            except Exception:
                pass
            if user_data_dir is None:
                try:
                    await browser.close()
                except Exception:
                    pass
            await redis.aclose()


# Backwards-compat alias — older platform wrappers imported run_sms_login.
run_sms_login = run_login


async def run_qr_login(
    *,
    platform: str,
    session_id: str,
    login_url: str,
    extract: ExtractFn,
    credentials_path: Path,
    qr_tab_sels: Sequence[str] = DEFAULT_QR_TAB,
    qr_elem_sels: Sequence[str] = DEFAULT_QR_ELEMENT,
    qr_refresh_sels: Sequence[str] = DEFAULT_QR_REFRESH,
    # Ready signal: the earliest selector whose presence means the login card
    # has actually rendered. `first_visible` against this list exits as soon as
    # any match — no arbitrary fixed timeouts.
    ready_sels: Sequence[str] | None = None,
    extra_credential_fields: dict | None = None,
    poll_timeout_s: int = 300,
    user_data_dir: Path | None = None,
    skip_if_logged_in_s: float = 1.2,  # short — cached-session auto-login is instant
    # How long to let the SPA settle after reload/goto before probing the DOM.
    # Also used by the hard-refresh path.
    wait_after_goto_ms: int = 1200,
) -> int:
    """QR-code login.

    1. Goto login page, wait for SPA to hydrate.
    2. Click the QR-login tab if one exists (many sites need that).
    3. Find the QR <canvas>/<img>, screenshot it, push PNG (base64 data-URI)
       into Redis so the frontend can render it.
    4. Poll `extract(page, context)` — once the user scans + confirms in the
       mobile app, localStorage/cookies populate and extract returns creds.
    5. Write credentials.json, mark SUCCESS.

    If the QR element rotates every N seconds (common), we re-capture and
    re-push every 25s while waiting.
    """
    import base64

    redis = build_redis_client()
    await update_status(
        redis, platform, session_id,
        status="RUNNING",
        message="正在启动 Chromium…",
        mode="qr",
    )

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        await update_status(
            redis, platform, session_id,
            status="FAILED",
            message="Playwright 未安装",
            ended_at=datetime.now(timezone.utc).isoformat(),
        )
        await redis.aclose()
        return 3

    async with async_playwright() as pw:
        # Persistent context: reuses cookies + JS cache across runs.
        # If user logged in before, cookies may skip the QR step entirely.
        try:
            launch_args = ["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            # Platform-aware context opts (see run_login).
            _ctx_opts = context_opts_for(platform)
            if user_data_dir is not None:
                user_data_dir.mkdir(parents=True, exist_ok=True)
                context = await pw.chromium.launch_persistent_context(
                    user_data_dir=str(user_data_dir),
                    headless=True,
                    args=launch_args,
                    **_ctx_opts,
                )
                browser = context.browser
            else:
                browser = await pw.chromium.launch(headless=True, args=launch_args)
                context = await browser.new_context(**_ctx_opts)
        except Exception as exc:
            await update_status(
                redis, platform, session_id,
                status="FAILED",
                message=f"Chromium 启动失败: {exc}",
                ended_at=datetime.now(timezone.utc).isoformat(),
            )
            await redis.aclose()
            return 4

        # Reuse an existing about:blank page if persistent context provided one.
        pages = context.pages if user_data_dir is not None else []
        page = pages[0] if pages else await context.new_page()
        # Anti-detection — masks navigator.webdriver, chrome.runtime, WebGL,
        # canvas, plugin list, permissions, plus navigator.languages aligned
        # to the platform's locale (CN for most, EN for Funda/SentimenTrader).
        await apply_stealth(page, platform=platform)

        try:
            t_start = asyncio.get_event_loop().time()
            def elapsed() -> float:
                return asyncio.get_event_loop().time() - t_start
            def log(msg: str) -> None:
                print(f"[{elapsed():5.1f}s] {platform}:{session_id} {msg}", flush=True)

            log("browser ready, starting navigation")
            await update_status(redis, platform, session_id, message="加载登录页…")
            await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
            log("domcontentloaded")

            # Wait for a specific "ready" selector instead of a blind timer —
            # return as soon as the login form renders. The ready list is
            # the union of QR-tab, password-tab and phone-input selectors —
            # any of them appearing means the card is up.
            if ready_sels is None:
                ready_list = list(qr_tab_sels)[:8] + list(DEFAULT_PHONE)[:4] + list(DEFAULT_PASSWORD_TAB)[:4]
            else:
                ready_list = list(ready_sels)
            await first_visible(page, ready_list, timeout_ms=10000)
            log("login card visible (first_visible on ready_list)")

            # Dismiss any user-agreement / privacy modal that blocks the
            # login flow (e.g. Gangtise shows a "同意 / 不同意" dialog that
            # intercepts clicks + halts the scan-login JS polling).
            dismissed_consent = await click_any(
                page, DEFAULT_CONSENT_AGREE, timeout_ms=2000
            )
            if dismissed_consent:
                log("consent modal dismissed (clicked 同意 or equivalent)")
                await update_status(
                    redis, platform, session_id,
                    message="已同意用户协议弹窗,继续加载二维码…",
                )
                await page.wait_for_timeout(500)

            # Fast path: maybe persistent cookies already auto-logged us in.
            if skip_if_logged_in_s > 0:
                end = asyncio.get_event_loop().time() + skip_if_logged_in_s
                while asyncio.get_event_loop().time() < end:
                    try:
                        maybe = await extract(page, context)
                        if maybe:
                            await _finish_creds(
                                redis, platform, session_id, maybe,
                                extra_credential_fields, credentials_path,
                                message="已从缓存会话直接登录, 无需扫码",
                            )
                            return 0
                    except Exception:
                        pass
                    await page.wait_for_timeout(300)

            log("fast-path done, searching QR tab")
            clicked = await click_any(page, qr_tab_sels, timeout_ms=2000)
            log(f"QR tab click result={clicked}")
            # CN QR endpoints typically need 1-2s to mint + serve the image
            # after the tab click (e.g. mp.gangtise/meritco scan-login API).
            await page.wait_for_timeout(1500)

            # Budget raised — stealth init can add 1-2s of first-paint delay.
            qr_sel = await first_visible(page, qr_elem_sels, timeout_ms=10000)
            log(f"QR element found: {qr_sel}")
            if not qr_sel:
                await snapshot(page, platform, session_id, "no_qr_element")
                raise RuntimeError("找不到二维码元素 (页面可能未显示 QR 选项)")

            async def push_qr() -> bool:
                """Re-locate the QR element from scratch and screenshot it.

                We don't cache the element reference — canvases get replaced on
                auto-rotation and old handles become detached.
                """
                for sel in qr_elem_sels:
                    try:
                        el = await page.wait_for_selector(sel, state="visible", timeout=1500)
                    except Exception:
                        continue
                    if not el:
                        continue
                    try:
                        png_bytes = await el.screenshot()
                    except Exception:
                        continue
                    b64 = base64.b64encode(png_bytes).decode("ascii")
                    await update_status(
                        redis, platform, session_id,
                        status="QR_NEEDED",
                        needs_otp="0",
                        qr_image=f"data:image/png;base64,{b64}",
                        qr_captured_at=str(int(asyncio.get_event_loop().time())),
                        message="请用 App 扫描二维码登录",
                    )
                    return True
                return False

            await push_qr()

            async def do_refresh(reason: str, hard: bool) -> None:
                """Manual refresh (hard=True) reloads the page — gives a fresh
                server-minted sceneId. Auto refresh (hard=False) ONLY takes a
                new screenshot; we don't click any "refresh" button on the
                page because doing so would rotate the sceneId server-side
                and invalidate a QR the user may be about to scan (or just
                scanned)."""
                if hard:
                    await update_status(
                        redis, platform, session_id,
                        message=f"正在重新加载登录页 ({reason})…",
                    )
                    try:
                        await page.reload(wait_until="domcontentloaded", timeout=20000)
                        await page.wait_for_timeout(wait_after_goto_ms)
                        await click_any(page, qr_tab_sels, timeout_ms=5000)
                        await page.wait_for_timeout(1200)
                    except Exception as exc:
                        await update_status(
                            redis, platform, session_id,
                            message=f"重载失败: {exc}",
                        )
                        return
                # Soft-refresh → just re-screenshot. Site's own JS rotates
                # the canvas on its schedule; we passively capture.

                ok = await push_qr()
                await update_status(
                    redis, platform, session_id,
                    message=f"二维码已刷新 ({reason}, push_ok={ok})",
                )

            # Poll for completion. Auto-refresh every 25s, plus honor any
            # manual-refresh signal. Also track URL + cookies — the clearest
            # signal that scan landed server-side is a page navigation or
            # a new session cookie, even before localStorage hydrates.
            deadline = asyncio.get_event_loop().time() + poll_timeout_s
            last_refresh = asyncio.get_event_loop().time()
            initial_url = page.url
            last_url = initial_url
            nav_detected_at: float | None = None
            try:
                initial_cookie_names = {c["name"] for c in await context.cookies()}
            except Exception:
                initial_cookie_names = set()
            saw_new_cookies = False
            creds: dict | None = None

            consent_dismissed_count = [0]
            while asyncio.get_event_loop().time() < deadline:
                # Dismiss any consent / agreement modal that may pop up at
                # any time (Gangtise shows "同意 / 不同意" AFTER QR render and
                # blocks scan-status JS polling). Cheap; harmless if absent.
                try:
                    if await click_any(page, DEFAULT_CONSENT_AGREE, timeout_ms=250):
                        consent_dismissed_count[0] += 1
                        await update_status(
                            redis, platform, session_id,
                            message=f"关闭用户协议弹窗 (第 {consent_dismissed_count[0]} 次)",
                        )
                        await page.wait_for_timeout(400)
                        # After dismissal, grab a fresh QR — the previous one
                        # may have gone stale while the modal was blocking.
                        await push_qr()
                except Exception:
                    pass

                try:
                    creds = await extract(page, context)
                except Exception:
                    creds = None
                if creds:
                    break

                # 风控 / CAPTCHA 锁探测 — 撞上即抛 CaptchaLockout 退出, 不再 poll
                await _check_lockout(page)

                # URL-change detection — navigation away from login almost
                # always means the scan was accepted.
                try:
                    cur_url = page.url
                except Exception:
                    cur_url = last_url
                if cur_url != last_url:
                    last_url = cur_url
                    if cur_url != initial_url and nav_detected_at is None:
                        nav_detected_at = asyncio.get_event_loop().time()
                        await snapshot(page, platform, session_id, "post_scan_nav")
                        await update_status(
                            redis, platform, session_id,
                            message=f"检测到跳转 → {cur_url[:80]} · 等待凭证落地…",
                        )

                # Cookie delta — fires even when URL stays the same.
                try:
                    cur_cookies = {c["name"] for c in await context.cookies()}
                    new_cookies = cur_cookies - initial_cookie_names
                    if new_cookies and not saw_new_cookies:
                        saw_new_cookies = True
                        await update_status(
                            redis, platform, session_id,
                            message=f"检测到新 cookie: {sorted(list(new_cookies))[:4]}",
                        )
                        # Snapshot so we can see the post-scan page state
                        # (often a consent modal blocking token extraction).
                        await snapshot(page, platform, session_id, "post_scan_cookie")
                except Exception:
                    pass

                # After detected nav, poll hard for 20s — localStorage
                # usually populates within that window.
                if nav_detected_at is not None:
                    elapsed_nav = asyncio.get_event_loop().time() - nav_detected_at
                    if elapsed_nav < 20:
                        await page.wait_for_timeout(400)
                        continue
                    if 20 <= elapsed_nav < 22:
                        await snapshot(page, platform, session_id, "post_nav_no_creds_20s")

                # Manual refresh — priority over auto.
                if await consume_refresh_signal(redis, session_id):
                    await do_refresh("manual", hard=True)
                    last_refresh = asyncio.get_event_loop().time()
                    initial_url = page.url
                    last_url = initial_url
                    nav_detected_at = None
                else:
                    now = asyncio.get_event_loop().time()
                    # 60s cadence — long enough that Gangtise-style scene
                    # lifecycles aren't churned mid-scan, but short enough
                    # to keep the image fresh if canvas auto-rotates.
                    if now - last_refresh > 60 and nav_detected_at is None:
                        await do_refresh("auto", hard=False)
                        last_refresh = now
                await page.wait_for_timeout(1500)

            if not creds:
                await snapshot(page, platform, session_id, "qr_timeout_no_creds")
                hint = ""
                if nav_detected_at is not None:
                    hint = " (页面已跳转但凭证未出现, 可能 localStorage 在 iframe 内)"
                elif saw_new_cookies:
                    hint = " (新 cookie 已设置但 JS 未落 localStorage, 可能需要完整页面刷新)"
                raise RuntimeError(f"等待扫码超时 (5 分钟){hint}")

            await _finish_creds(
                redis, platform, session_id, creds,
                extra_credential_fields, credentials_path,
                message="扫码登录成功",
            )
            return 0

        except CaptchaLockout as exc:
            await update_status(
                redis, platform, session_id,
                status="LOCKED_OUT",
                message=str(exc)[:400],
                ended_at=datetime.now(timezone.utc).isoformat(),
                lockout="1",
            )
            return 5
        except Exception as exc:
            await update_status(
                redis, platform, session_id,
                status="FAILED",
                message=str(exc)[:400],
                ended_at=datetime.now(timezone.utc).isoformat(),
            )
            return 1
        finally:
            # Persistent context: closing it is enough; it owns the browser.
            # Non-persistent: close context first, then browser.
            try:
                await context.close()
            except Exception:
                pass
            if user_data_dir is None:
                try:
                    await browser.close()
                except Exception:
                    pass
            await redis.aclose()


def parse_and_run(coro_factory: Callable[[str, dict], Awaitable[int]]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--session-id", required=True)
    p.add_argument("--payload", required=True)
    args = p.parse_args()
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        print(f"ERROR: invalid --payload JSON: {exc}", file=sys.stderr)
        sys.exit(2)

    try:
        rc = asyncio.run(
            asyncio.wait_for(coro_factory(args.session_id, payload), timeout=OVERALL_TIMEOUT)
        )
    except asyncio.TimeoutError:
        print("ERROR: overall timeout", file=sys.stderr)
        sys.exit(5)
    sys.exit(rc)
