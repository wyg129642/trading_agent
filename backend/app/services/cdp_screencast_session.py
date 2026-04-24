"""In-process CDP-screencast login sessions.

Each session wraps a running Playwright Chromium + a CDP channel. We subscribe
to `Page.screencastFrame` and push JPEG bytes into an asyncio.Queue; the
WebSocket handler (in data_sources.py) drains the queue to the client. Input
events flow the other way — the WS handler puts them on `input_queue` and the
session task dispatches them via `Input.dispatchMouseEvent` / `dispatchKeyEvent`.

Separately, a polling task calls the platform's `extract(page, context)` every
second; when it returns a non-empty dict, we write credentials.json and
mark the session SUCCESS.
"""

from __future__ import annotations

import asyncio
import base64
import json
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

# Reuse consent + stealth + extract helpers + selector lists.
import sys as _sys
_CRAWL_DIR = Path(__file__).resolve().parents[3] / "crawl"
if str(_CRAWL_DIR) not in _sys.path:
    _sys.path.insert(0, str(_CRAWL_DIR))
import auto_login_common as alc  # noqa: E402

_SESSIONS: dict[str, "ScreencastSession"] = {}

# Map KeyboardEvent.key → Windows virtual key code.
# CDP's Input.dispatchKeyEvent needs this for non-printable keys to actually
# take effect — without it, Backspace/arrows/Tab fire the KeyboardEvent but
# don't delete text or move the caret. Covers the keys typical login forms
# need; printable keys are handled via `text`.
_VK_CODES: dict[str, int] = {
    "Backspace": 8,
    "Tab": 9,
    "Enter": 13,
    "Shift": 16,
    "Control": 17,
    "Alt": 18,
    "Pause": 19,
    "CapsLock": 20,
    "Escape": 27,
    "Space": 32,
    "PageUp": 33,
    "PageDown": 34,
    "End": 35,
    "Home": 36,
    "ArrowLeft": 37,
    "ArrowUp": 38,
    "ArrowRight": 39,
    "ArrowDown": 40,
    "Insert": 45,
    "Delete": 46,
    "Meta": 91,
    "ContextMenu": 93,
    "F1": 112, "F2": 113, "F3": 114, "F4": 115,
    "F5": 116, "F6": 117, "F7": 118, "F8": 119,
    "F9": 120, "F10": 121, "F11": 122, "F12": 123,
}

# ── Warm browser pool ────────────────────────────────────────────────────
#
# Launching Chromium takes 2-4s on this machine. Keeping one live browser
# around and creating new contexts inside it drops session startup to ~1s.
# The browser is closed when no sessions have used it for IDLE_CLOSE_S.

_WARM_LOCK = asyncio.Lock()
_WARM: dict[str, Any] = {"pw": None, "browser": None, "last_used": 0.0}
IDLE_CLOSE_S = 600  # 10 min


async def _ensure_warm_browser():
    """Return a reusable Chromium. Creates one if stale/missing."""
    async with _WARM_LOCK:
        browser = _WARM.get("browser")
        if browser is not None:
            try:
                _ = browser.contexts  # ping
                _WARM["last_used"] = time.time()
                return browser
            except Exception:
                _WARM["browser"] = None
        from playwright.async_api import async_playwright
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=True,
            # Dropped `--headless=new` — it broke AlphaPai's SPA render on
            # our Chromium (DOM had content but nothing painted). Classic
            # headless works fine across all 7 platforms.
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--proxy-server=direct://",
                "--proxy-bypass-list=*",
            ],
        )
        _WARM["pw"] = pw
        _WARM["browser"] = browser
        _WARM["last_used"] = time.time()
        return browser


async def _idle_cleanup_loop():
    """Close warm browser after IDLE_CLOSE_S of no sessions."""
    while True:
        await asyncio.sleep(60)
        async with _WARM_LOCK:
            browser = _WARM.get("browser")
            if browser is None:
                continue
            if _SESSIONS:
                _WARM["last_used"] = time.time()
                continue
            if time.time() - _WARM.get("last_used", 0) > IDLE_CLOSE_S:
                try:
                    await browser.close()
                except Exception:
                    pass
                try:
                    pw = _WARM.get("pw")
                    if pw:
                        await pw.stop()
                except Exception:
                    pass
                _WARM["browser"] = None
                _WARM["pw"] = None


@dataclass
class ScreencastSession:
    """One live Chromium + CDP channel, addressable by session_id.

    Two operating modes:
      - ``login`` (default): navigate to login URL, poll extract_fn every 3s
        until creds land, then write credentials.json and auto-start scraper.
      - ``viewer``: pre-inject already-saved creds via ``inject_fn`` so the
        page loads already-logged-in; no polling, no finalize. Session stays
        alive until the client WS disconnects or an idle timeout.
    """

    session_id: str
    platform: str
    login_url: str
    credentials_path: Path
    extract_fn: Callable[[Any, Any], Awaitable[dict | None]] | None = None
    # Optional pre-nav hook for viewer mode. Receives (context) and should
    # set cookies + localStorage (via context.add_init_script) from creds.
    inject_fn: Callable[[Any], Awaitable[None]] | None = None
    # Optional post-nav hook — called after goto settles. Typical use:
    # Gangtise needs to bootstrap at portal root for activeProduct, then
    # switch hash to the target section.
    post_nav_fn: Callable[[Any], Awaitable[None]] | None = None
    mode: str = "login"  # "login" | "viewer"
    # 1600x900 is a reasonable default for viewer-mode data comparison
    # (user's drawer is ~1100px anyway, so no point streaming 2560×1200).
    # Login mode may override to 2560×1200 for AlphaPai's off-screen login card.
    viewport_w: int = 1600
    viewport_h: int = 900

    # Runtime state, populated by start()
    pw_ctx: Any = None
    browser: Any = None
    context: Any = None
    page: Any = None
    cdp: Any = None
    extras: dict[str, Any] = field(default_factory=dict)  # credential extras to merge

    # Frame queue kept deliberately small (3) — we don't want to buffer
    # stale frames when the WS client is slower than Chromium's paint cadence.
    # On overflow `_on_frame` drops the oldest and inserts the freshest,
    # prioritising *recency* over *completeness* for perceived latency.
    frame_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=3))
    input_queue: asyncio.Queue = field(default_factory=asyncio.Queue)

    # Lifecycle
    started_at: float = 0.0
    status: str = "INIT"  # INIT | STREAMING | SUCCESS | FAILED | CLOSED
    status_msg: str = ""
    _tasks: list[asyncio.Task] = field(default_factory=list)
    _creds: dict | None = None

    # DevTools-lite network capture: ring buffer of recent XHR/fetch calls.
    # Populated by Playwright `request`/`response` listeners. Frontend polls
    # it via `/screencast/{sid}/network` for a live "Network panel".
    # Keyed by request id (hash of url + request timestamp); inflight entries
    # get merged with response data when it lands.
    _network_log: list[dict] = field(default_factory=list)
    _network_seq: int = 0

    async def start(self) -> None:
        """Warm pool → context + page → install hooks → start screencast →
        kick navigation in the background. Designed for <2s TTFF (time to
        first frame) on warm pool, <5s cold."""
        self.status = "RUNNING"
        self.status_msg = "启动 Chromium…"
        self.started_at = time.time()

        # Reuse a warm browser. Skip pw_ctx tracking — warm pool owns it.
        self.browser = await _ensure_warm_browser()
        # Platform-aware locale: zh-CN/Shanghai for CN sites, en-US/NY for
        # US sites (Funda, SentimenTrader). See PLATFORM_LOCALE in
        # auto_login_common for the full mapping. Rationale: IP↔locale
        # mismatch is a TDC/WAF fingerprint tell; also CN sites read
        # navigator.language to pick UI language (EN pages without this).
        _opts = alc.context_opts_for(self.platform)
        self.context = await self.browser.new_context(
            viewport={"width": self.viewport_w, "height": self.viewport_h},
            **_opts,
        )

        # Viewer mode: pre-inject saved credentials into the fresh context
        # BEFORE opening any page, so the page loads already-logged-in.
        # Attach a back-reference so extract_fn can reach the session's
        # network log and storage hooks without changing the public
        # extract(page, context) signature. Use a private dunder-style
        # name to avoid colliding with anything Playwright/Chromium adds.
        self.context._screencast_session = self  # type: ignore[attr-defined]

        # inject_fn is typically: set cookies via context.add_cookies()
        # and localStorage via context.add_init_script().
        if self.inject_fn is not None:
            try:
                await self.inject_fn(self.context)
            except Exception as exc:
                import logging
                logging.getLogger("cdp_screencast").warning(
                    "[%s:%s] inject_fn failed: %s",
                    self.platform, self.session_id, exc,
                )

        self.page = await self.context.new_page()
        await alc.apply_stealth(self.page, platform=self.platform)

        # DevTools-lite network capture — attach request/response listeners to
        # every new frame. Filter to XHR/fetch (the interesting API calls) and
        # skip static assets (js/css/images/fonts) that drown the log.
        def _on_request(req):
            try:
                rtype = req.resource_type
                # Capture XHR/fetch/document (SPA hash-routes sometimes trigger
                # only document requests), but skip static assets.
                if rtype in ("image", "font", "stylesheet", "media",
                             "websocket", "manifest", "texttrack",
                             "script", "other", "preflight"):
                    return
                url = req.url
                # Skip tracking / analytics / fingerprint noise
                if any(s in url for s in ("/sinahq/", "/obsproxy/", ".png", ".jpg",
                                           ".woff", ".svg", "/favicon",
                                           "/tdc/", "cef.alibabausercontent")):
                    return
                self._network_seq += 1
                seq = self._network_seq
                try:
                    post_data = req.post_data
                except Exception:
                    post_data = None
                entry = {
                    "seq": seq,
                    "ts": time.time(),
                    "method": req.method,
                    "url": url,
                    "resource_type": rtype,
                    "post_data": (post_data[:4000] if isinstance(post_data, str) else None),
                    "status": None,
                    "response_body": None,
                    "response_preview": None,
                    "response_time_ms": None,
                    "_req_obj_id": id(req),
                }
                self._network_log.append(entry)
                # Ring buffer cap
                if len(self._network_log) > 200:
                    self._network_log[:] = self._network_log[-200:]
            except Exception:
                pass

        async def _on_response_async(resp):
            try:
                req = resp.request
                rtype = req.resource_type
                if rtype not in ("xhr", "fetch"):
                    return
                # Find matching entry (last one with that request obj id)
                target = None
                for e in reversed(self._network_log):
                    if e.get("_req_obj_id") == id(req):
                        target = e
                        break
                if target is None:
                    return
                target["status"] = resp.status
                target["response_time_ms"] = int((time.time() - target["ts"]) * 1000)
                # Only grab body for JSON-ish responses, up to 20KB
                ct = (resp.headers.get("content-type") or "").lower()
                if "json" in ct or "text" in ct or "javascript" in ct:
                    try:
                        text = await resp.text()
                    except Exception:
                        text = None
                    if text is not None:
                        target["response_body"] = text[:20000]
                        target["response_preview"] = text[:500]
            except Exception:
                pass

        self.page.on("request", _on_request)
        self.page.on("response", lambda r: asyncio.create_task(_on_response_async(r)))

        # Hook localStorage.setItem + cookie writes — only needed in login
        # mode, where we're trying to notice when a login completes. In
        # viewer mode we already injected creds pre-nav, no need to watch.
        if self.mode == "login":
            await self.page.expose_binding("_notifyStorageWrite", self._on_storage_write)
        await self.page.add_init_script("""
          (() => {
            // Save pristine Storage references + method refs before any page
            // script runs. Some sites replace window.localStorage with a
            // proxy/null to break headless probes — these pristine refs let
            // us bypass that.
            try {
              window.__origLocalStorage = window.localStorage;
              window.__origSessionStorage = window.sessionStorage;
              window.__storageKey = Storage.prototype.key;
              window.__storageGet = Storage.prototype.getItem;
              window.__storageLen = Object.getOwnPropertyDescriptor(
                Storage.prototype, 'length'
              );
            } catch(e) {}
            const origSet = Storage.prototype.setItem;
            Storage.prototype.setItem = function(k, v) {
              try { origSet.call(this, k, v); } catch(e) {}
              try {
                if (window._notifyStorageWrite) {
                  window._notifyStorageWrite({
                    kind: this === sessionStorage ? 'sessionStorage' : 'localStorage',
                    key: String(k),
                    value: (v == null ? '' : String(v)).slice(0, 1200),
                  });
                }
              } catch(e) {}
            };
            // Poll-based fallback — catches direct property assignment
            // (`localStorage.foo = 'bar'`) that bypasses Storage.prototype.setItem
            // on some engines. 500ms is fast enough to feel instant.
            let _seen = {};
            const _snapshot = (store, kind) => {
              try {
                for (let i = 0; i < store.length; i++) {
                  const k = store.key(i);
                  const v = store.getItem(k) || '';
                  const prev = _seen[kind + ':' + k];
                  if (prev !== v) {
                    _seen[kind + ':' + k] = v;
                    if (window._notifyStorageWrite) {
                      window._notifyStorageWrite({
                        kind: kind,
                        key: k,
                        value: v.slice(0, 1200),
                      });
                    }
                  }
                }
              } catch(e) {}
            };
            setInterval(() => {
              _snapshot(localStorage, 'localStorage');
              _snapshot(sessionStorage, 'sessionStorage');
            }, 500);
            // Also catch document.cookie assignments.
            try {
              const protoDesc = Object.getOwnPropertyDescriptor(Document.prototype, 'cookie');
              if (protoDesc && protoDesc.configurable) {
                Object.defineProperty(Document.prototype, 'cookie', {
                  configurable: true,
                  get: function() { return protoDesc.get.call(this); },
                  set: function(v) {
                    try { protoDesc.set.call(this, v); } catch(e) {}
                    try {
                      if (window._notifyStorageWrite) {
                        window._notifyStorageWrite({
                          kind: 'cookie',
                          key: '',
                          value: String(v).slice(0, 400),
                        });
                      }
                    } catch(e) {}
                  },
                });
              }
            } catch(e) {}
          })();
        """)

        # CRITICAL FOR PERCEIVED SPEED — open CDP + start screencast BEFORE
        # navigation. That way the user sees the browser load the login page
        # live (like a normal tab) instead of waiting in a spinner.
        self.cdp = await self.context.new_cdp_session(self.page)
        self.cdp.on("Page.screencastFrame", self._on_frame)
        # 2026-04-22 latency tuning:
        #  - everyNthFrame=2 halves encode pressure (30fps→15fps) — fine
        #    for browsing text/tables, imperceptible for side-by-side data.
        #  - quality 45 cuts JPEG bytes another 20% vs 55; on 1600×900
        #    frames look clean, font rendering intact.
        #  - maxWidth/Height capped at viewport so no oversampling.
        await self.cdp.send("Page.startScreencast", {
            "format": "jpeg",
            "quality": 45,
            "maxWidth": self.viewport_w,
            "maxHeight": self.viewport_h,
            "everyNthFrame": 2,
        })
        self.status = "STREAMING"
        self.status_msg = "屏幕流就绪, 正在跳转登录页…"

        # Nav happens in the background — start() returns immediately so the
        # HTTP handler doesn't block. `_bootstrap_page` handles goto + consent
        # + QR-tab click + delayed poll.
        self._tasks.append(asyncio.create_task(self._bootstrap_page()))
        self._tasks.append(asyncio.create_task(self._input_loop()))
        # Credentials polling only makes sense in login mode. Viewer mode
        # is a passive browse session — no finalize, no auto-scraper spawn.
        if self.mode == "login" and self.extract_fn is not None:
            self._tasks.append(asyncio.create_task(self._poll_credentials()))

    async def _bootstrap_page(self) -> None:
        """Goto login URL + best-effort consent/tab clicks. Runs async so the
        caller's start() returns quickly and screencast starts streaming
        during navigation."""
        import logging
        log = logging.getLogger("cdp_screencast")
        try:
            # `wait_until="commit"` returns as soon as the document commits
            # (request sent, response starting) — usually <300ms vs 1-3s for
            # domcontentloaded. Frames already flowing, user sees the load.
            await self.page.goto(self.login_url, wait_until="commit", timeout=30000)
        except Exception as exc:
            log.warning("[%s:%s] goto err: %s", self.platform, self.session_id, exc)
            self.status_msg = f"页面加载异常: {str(exc)[:80]}"
            return

        # Settle for up to 8s on domcontentloaded — the page may not be
        # fully parsed but is usable.
        try:
            await self.page.wait_for_load_state("domcontentloaded", timeout=8000)
        except Exception:
            pass

        # SPA hydration wait — many login pages (AlphaPai/Funda/Gangtise)
        # render just a loader shell on domcontentloaded. Wait until actual
        # interactive content is present (input/button/canvas) OR give up
        # at ~8s so user sees whatever rendered so far.
        try:
            await self.page.wait_for_function(
                """() => {
                  const body = document.body;
                  if (!body) return false;
                  // "Real" content: any interactive element OR >300 bytes of text
                  if (body.innerText && body.innerText.length > 300) return true;
                  return !!document.querySelector(
                    'input, button, canvas, a[href*="login" i], [class*="login"]'
                  );
                }""",
                timeout=8000,
            )
        except Exception:
            pass

        # Login-only niceties — in viewer mode the user is already logged
        # in and we want the raw target page, not the login UI.
        if self.mode == "login":
            try:
                await alc.click_any(self.page, alc.DEFAULT_CONSENT_AGREE, timeout_ms=1200)
            except Exception:
                pass
            try:
                await alc.click_any(self.page, alc.DEFAULT_QR_TAB, timeout_ms=1500)
            except Exception:
                pass
            self.status_msg = "登录页就绪, 等待扫码或输入账号"
        else:
            self.status_msg = "实时平台就绪, 可浏览对比数据"
            if self.post_nav_fn is not None:
                try:
                    await self.post_nav_fn(self.page)
                except Exception as exc:
                    import logging
                    logging.getLogger("cdp_screencast").warning(
                        "[%s:%s] post_nav_fn failed: %s",
                        self.platform, self.session_id, exc,
                    )

    def _on_storage_write(self, source, data: dict) -> None:
        """JS hook callback. Schedule an extract attempt — Playwright's
        binding is sync at the JS boundary, but we can kick an async task
        to do the actual work without blocking the page."""
        import logging
        log = logging.getLogger("cdp_screencast")
        log.info(
            "[%s:%s] storage-write %s key=%s",
            self.platform, self.session_id,
            data.get("kind"), str(data.get("key"))[:40],
        )
        # Don't finalize on every write — only if extract() says creds are
        # now present. Fires async.
        asyncio.create_task(self._try_finalize_once(reason=f"hook/{data.get('kind')}"))

    async def _try_finalize_once(self, reason: str = "poll") -> None:
        """Attempt extract once; on success, finalize. Idempotent."""
        if self.status in ("SUCCESS", "CLOSED"):
            return
        import logging
        log = logging.getLogger("cdp_screencast")
        try:
            creds = await self.extract_fn(self.page, self.context)
        except Exception as exc:
            log.debug("[%s:%s] extract error (%s): %s",
                      self.platform, self.session_id, reason, exc)
            return
        if creds and self.status not in ("SUCCESS", "CLOSED"):
            log.info(
                "[%s:%s] creds found via %s, finalizing",
                self.platform, self.session_id, reason,
            )
            await self._finish(creds)

    def _on_frame(self, event: dict) -> None:
        """CDP handler — non-async. Ack immediately and buffer frame for WS."""
        session_id = event.get("sessionId")
        data = event.get("data")  # base64 JPEG
        # Schedule the ack on the running loop so CDP keeps sending.
        asyncio.create_task(self._ack_frame(session_id))
        if data is None:
            return
        # Drop if queue is full — don't backpressure Chromium.
        try:
            self.frame_queue.put_nowait({
                "type": "frame",
                "data": data,
                "w": self.viewport_w,
                "h": self.viewport_h,
                "ts": time.time(),
            })
        except asyncio.QueueFull:
            # Drain one stale frame and try again — keeps latency low.
            try:
                self.frame_queue.get_nowait()
                self.frame_queue.put_nowait({
                    "type": "frame",
                    "data": data,
                    "w": self.viewport_w,
                    "h": self.viewport_h,
                    "ts": time.time(),
                })
            except Exception:
                pass

    async def _ack_frame(self, session_id) -> None:
        try:
            await self.cdp.send("Page.screencastFrameAck", {"sessionId": session_id})
        except Exception:
            pass

    async def _input_loop(self) -> None:
        """Forward queued input events to Chromium via CDP."""
        try:
            while self.status in ("STREAMING", "RUNNING"):
                try:
                    evt = await asyncio.wait_for(self.input_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                try:
                    await self._dispatch(evt)
                except Exception as exc:
                    # Non-fatal — log to status msg, keep going.
                    self.status_msg = f"input dispatch err: {exc}"
        except asyncio.CancelledError:
            pass

    async def _dispatch(self, evt: dict) -> None:
        """Translate a UI input event dict into a CDP call.

        Expected event shapes:
          {type: 'mouse', action: 'down'|'up'|'move', x, y, button}
          {type: 'wheel', x, y, deltaX, deltaY}
          {type: 'key', action: 'down'|'up', key, code, text, modifiers}
        """
        t = evt.get("type")
        if t == "mouse":
            action = evt.get("action", "move")
            cdp_type = {"down": "mousePressed", "up": "mouseReleased", "move": "mouseMoved"}.get(action, "mouseMoved")
            await self.cdp.send("Input.dispatchMouseEvent", {
                "type": cdp_type,
                "x": float(evt.get("x", 0)),
                "y": float(evt.get("y", 0)),
                "button": evt.get("button", "left") if action != "move" else "none",
                "clickCount": int(evt.get("clickCount", 1)),
                "modifiers": int(evt.get("modifiers", 0)),
            })
        elif t == "wheel":
            await self.cdp.send("Input.dispatchMouseEvent", {
                "type": "mouseWheel",
                "x": float(evt.get("x", 0)),
                "y": float(evt.get("y", 0)),
                "deltaX": float(evt.get("deltaX", 0)),
                "deltaY": float(evt.get("deltaY", 0)),
            })
        elif t == "key":
            action = evt.get("action", "down")
            key = evt.get("key", "")
            code = evt.get("code", "")
            text = evt.get("text")
            vk = _VK_CODES.get(key)
            # Non-printable named keys (Backspace, arrows, Tab, Enter, …) need
            # windowsVirtualKeyCode/nativeVirtualKeyCode, otherwise Chromium
            # dispatches the KeyboardEvent but performs no action (no char
            # removed, no caret move). For printable keys CDP figures the
            # code from `text` automatically.
            cdp_type = "keyDown" if action == "down" else "keyUp"
            if text and cdp_type == "keyDown":
                # A real character — use rawKeyDown+char pair so the input
                # element receives `beforeinput` and actually inserts the text.
                await self.cdp.send("Input.dispatchKeyEvent", {
                    "type": "keyDown",
                    "key": key,
                    "code": code,
                    "text": text,
                    "unmodifiedText": text,
                    "modifiers": int(evt.get("modifiers", 0)),
                    **({"windowsVirtualKeyCode": vk,
                        "nativeVirtualKeyCode": vk} if vk else {}),
                })
            else:
                args = {
                    "type": cdp_type,
                    "key": key,
                    "code": code,
                    "modifiers": int(evt.get("modifiers", 0)),
                }
                if vk:
                    args["windowsVirtualKeyCode"] = vk
                    args["nativeVirtualKeyCode"] = vk
                if text is not None:
                    args["text"] = text
                    args["unmodifiedText"] = text
                await self.cdp.send("Input.dispatchKeyEvent", args)
        elif t == "type" or t == "paste":
            # Paste (Ctrl+V from local) or direct text injection — insert
            # text at the current caret. Input.insertText fires proper
            # `input`/`beforeinput` events, so React-controlled inputs update.
            text = evt.get("text", "")
            if text:
                await self.cdp.send("Input.insertText", {"text": text})
        elif t == "copy-request":
            # Ctrl+C from local → ship remote page selection to the client so
            # it can stuff it into the local clipboard.
            try:
                text = await self.page.evaluate(
                    "() => (window.getSelection ? window.getSelection().toString() : '')"
                )
            except Exception:
                text = ""
            try:
                self.frame_queue.put_nowait({
                    "type": "copy-response",
                    "text": text or "",
                })
            except asyncio.QueueFull:
                pass

    # Short phrases that mean "risk control locked this account" — keep
    # matching generous (platforms vary) but conservative enough to not
    # false-positive on normal UI copy. When one hits, we STOP the session
    # immediately to avoid trigger-happy screencast loops stacking more
    # failed verifications on top of the lockout.
    _LOCKOUT_PHRASES = (
        "输入验证码错误次数过多", "验证码错误次数过多", "账号暂时锁定",
        "暂时锁定", "账号已锁定", "账户已锁定", "请稍后再试", "尝试次数过多",
        "frequently", "too many attempts", "temporarily locked",
    )

    async def _detect_lockout(self) -> str | None:
        """Return a short lockout message if the current page displays any
        of the risk-control phrases above; else None. Cheap & best-effort —
        errors swallowed, runs at most every few ticks."""
        try:
            txt = await self.page.evaluate(
                "() => (document.body && document.body.innerText || '').slice(0, 4000)"
            )
        except Exception:
            return None
        if not isinstance(txt, str):
            return None
        low = txt.lower()
        for phrase in self._LOCKOUT_PHRASES:
            if phrase.lower() in low:
                return phrase
        return None

    async def _poll_credentials(self) -> None:
        """Poll `extract` every second until it returns creds, then finalize."""
        import logging
        log = logging.getLogger("cdp_screencast")
        try:
            deadline = time.time() + 420  # 7 min hard cap (matches runner budget)
            tick = 0
            last_url = ""
            last_error: str | None = None
            while self.status in ("STREAMING", "RUNNING") and time.time() < deadline:
                tick += 1
                # 3s polling cadence now — JS hook is the primary signal; this
                # is just a safety net if the hook missed something.
                await asyncio.sleep(3.0)
                # CAPTCHA / 账号锁定探测 — 撞上立刻退出, 不再 tick.
                # 每 5 tick (~15s) 检查一次, 不必 per-tick (DOM 串 4k 字符, 有成本).
                if tick % 5 == 0:
                    lock = await self._detect_lockout()
                    if lock:
                        self.status = "FAILED"
                        self.status_msg = (
                            f"账号触发风控锁定 ({lock}). 已停止会话避免继续触发 "
                            f"— 等 10 分钟后再试, 或直接粘贴浏览器 cookie 覆盖 credentials.json."
                        )
                        log.warning(
                            "[%s:%s] LOCKOUT detected at tick=%d phrase=%r — aborting session",
                            self.platform, self.session_id, tick, lock,
                        )
                        return
                try:
                    creds = await self.extract_fn(self.page, self.context)
                    last_error = None
                except Exception as exc:
                    creds = None
                    last_error = str(exc)[:200]
                # Emit a diag line every 5s so the backend log reflects progress.
                if tick % 5 == 0:
                    try:
                        cur_url = self.page.url
                    except Exception:
                        cur_url = "?"
                    # Dump localStorage keys every 10s for visibility.
                    if tick % 10 == 0:
                        try:
                            # Use our pristine refs (captured in init_script)
                            # to dodge any page-level localStorage overrides.
                            probe = await self.page.evaluate("""() => {
                              const ls = window.__origLocalStorage || localStorage;
                              const ss = window.__origSessionStorage || sessionStorage;
                              const ls_keys = [];
                              try { for (let i=0;i<ls.length;i++){
                                ls_keys.push(ls.key(i));
                              } } catch(e) {}
                              const ss_keys = [];
                              try { for (let i=0;i<ss.length;i++){
                                ss_keys.push(ss.key(i));
                              } } catch(e) {}
                              let g_token = '';
                              try { g_token = ls.getItem('G_token') || ''; } catch(e) {}
                              return {
                                ls_keys: ls_keys.slice(0, 30),
                                ss_keys: ss_keys.slice(0, 20),
                                ls_len: ls.length,
                                ss_len: ss.length,
                                g_token: g_token,
                                url: location.href,
                              };
                            }""") or {}
                            g_token = probe.get("g_token") or ""
                            log.info(
                                "[%s:%s] tick=%d url=%s LS(%s)=[%s] SS(%s)=[%s] G_token=%s…(%dB)",
                                self.platform, self.session_id, tick,
                                (probe.get("url") or "")[:80],
                                str(probe.get("ls_len")),
                                ",".join(probe.get("ls_keys") or [])[:200],
                                str(probe.get("ss_len")),
                                ",".join(probe.get("ss_keys") or [])[:120],
                                g_token[:20], len(g_token),
                            )
                        except Exception as e:
                            log.info("[%s:%s] tick=%d probe_err=%s",
                                     self.platform, self.session_id, tick, e)
                    if cur_url != last_url or last_error:
                        log.info(
                            "[%s:%s] poll tick=%d url=%s creds=%s err=%s",
                            self.platform, self.session_id, tick,
                            cur_url[:100], bool(creds), last_error,
                        )
                        last_url = cur_url
                if creds:
                    log.info("[%s:%s] extract returned creds via poll, finalizing",
                             self.platform, self.session_id)
                    await self._finish(creds)
                    return
            # Timed out without creds.
            if self.status not in ("SUCCESS", "FAILED", "CLOSED"):
                self.status = "FAILED"
                self.status_msg = "登录超时 (7 分钟)"
        except asyncio.CancelledError:
            pass

    async def _finish(self, creds: dict) -> None:
        """Success path — write credentials.json, flip status to SUCCESS,
        auto-start the platform's crawler (same behavior as the subprocess
        login flow in auto_login_runner)."""
        if self.extras:
            creds.update(self.extras)
        creds["updated_at"] = datetime.utcnow().isoformat() + "Z"
        self.credentials_path.parent.mkdir(parents=True, exist_ok=True)
        self.credentials_path.write_text(
            json.dumps(creds, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._creds = creds
        self.status = "SUCCESS"
        self.status_msg = "登录成功, 凭证已写入"

        # Kick off the crawler — same as auto_login_runner's SUCCESS path,
        # so the Meritco/Gangtise/… scraper picks up the fresh token
        # immediately. Best-effort; failure here doesn't undo the login.
        try:
            import redis.asyncio as aioredis
            from backend.app.services import crawler_manager
            from backend.app.config import get_settings
            # Use the env-scoped URL so staging stays on its own Redis DB
            # (prod=DB0, staging=DB1) when kicking off crawler-managers.
            r = aioredis.from_url(
                get_settings().redis_url,
                decode_responses=True,
            )
            try:
                info = await crawler_manager.start(r, self.platform, force=True)
                self.status_msg = f"登录成功, 爬虫 PID={info.get('pid')} 已启动"
            finally:
                await r.aclose()
        except Exception as exc:
            import logging
            logging.getLogger("cdp_screencast").warning(
                "[%s:%s] crawler auto-start failed: %s",
                self.platform, self.session_id, exc,
            )
            self.status_msg = f"登录成功, 但爬虫启动失败: {exc}"

        # Push a terminal "status" event to the queue so WS clients see it
        # before we close.
        try:
            self.frame_queue.put_nowait({
                "type": "status",
                "status": "SUCCESS",
                "message": self.status_msg,
            })
        except asyncio.QueueFull:
            pass

    async def close(self) -> None:
        if self.status in ("CLOSED",):
            return
        self.status = "CLOSED"
        for t in self._tasks:
            t.cancel()
        try:
            if self.cdp:
                await self.cdp.send("Page.stopScreencast")
                await self.cdp.detach()
        except Exception:
            pass
        # Only close the per-session context. The browser lives in the warm
        # pool and is reused by subsequent sessions; don't touch it here.
        try:
            if self.context:
                await self.context.close()
        except Exception:
            pass


# ── Registry ─────────────────────────────────────────────────────────────


async def create_session(
    platform: str,
    login_url: str,
    credentials_path: Path,
    extract_fn: Callable[[Any, Any], Awaitable[dict | None]] | None = None,
    viewport: tuple[int, int] = (2560, 1200),
    extras: dict[str, Any] | None = None,
    mode: str = "login",
    inject_fn: Callable[[Any], Awaitable[None]] | None = None,
    post_nav_fn: Callable[[Any], Awaitable[None]] | None = None,
) -> ScreencastSession:
    """Public factory. Returns a started ScreencastSession keyed by random id.

    mode="login"  — normal: watch for creds, finalize, spawn scraper.
    mode="viewer" — pre-inject saved creds and open the target URL so the
                    user can browse the original platform logged-in.
    """
    session_id = secrets.token_hex(8)
    sess = ScreencastSession(
        session_id=session_id,
        platform=platform,
        login_url=login_url,
        credentials_path=credentials_path,
        extract_fn=extract_fn,
        inject_fn=inject_fn,
        post_nav_fn=post_nav_fn,
        mode=mode,
        viewport_w=viewport[0],
        viewport_h=viewport[1],
        extras=extras or {},
    )
    _SESSIONS[session_id] = sess
    try:
        await sess.start()
    except Exception as exc:
        sess.status = "FAILED"
        sess.status_msg = f"启动失败: {exc}"
        await sess.close()
        raise
    return sess


def get_session(session_id: str) -> ScreencastSession | None:
    return _SESSIONS.get(session_id)


async def drop_session(session_id: str) -> None:
    sess = _SESSIONS.pop(session_id, None)
    if sess:
        await sess.close()


async def drop_all() -> None:
    for sid in list(_SESSIONS.keys()):
        await drop_session(sid)
