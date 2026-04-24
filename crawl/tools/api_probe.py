#!/usr/bin/env python3
"""API probe — reusable tool to capture a platform's real UI API calls.

Purpose: when a scraper returns fewer items than the UI shows, the UI is
usually hitting a newer/different endpoint or body shape. Instead of
guessing, spin up a real Chromium with our saved creds injected, navigate
through each section of the site, and sniff every XHR/fetch the UI fires.

Usage:
    python3 crawl/tools/api_probe.py <platform>        # probe one platform
    python3 crawl/tools/api_probe.py --all             # probe all 7
    python3 crawl/tools/api_probe.py <platform> --diff # also diff vs scraper

Writes JSON snapshots to ``crawl/tools/probe_output/<platform>_<ts>.json``
with one row per unique (method, path, body-hash) call:

    {
      "platform": "gangtise",
      "total": 55,
      "unique": 38,
      "calls": [
        {"method": "POST", "url": "/app/...", "req_body": "...",
         "resp_status": 200, "resp_preview": "..."},
        ...
      ]
    }

Design decisions:
  - Use Playwright directly (not our CDP screencast) so we stay headless
    and can run unattended.
  - Inject creds via backend's `_viewer_inject_for` dispatcher — single
    source of truth for per-platform cred→localStorage/cookie mapping.
  - Per-frame XHR/fetch hook via `context.add_init_script`; then scrape
    `window.__xhr_log` from every frame (iframes count — many portals
    render sub-apps as iframes).
  - Post-nav click-through: for portals with sidebar tabs, emulate the
    user clicking each one so the right sub-SPA mounts and fires data
    queries.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Make the backend's viewer inject dispatcher importable
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from playwright.async_api import async_playwright  # noqa: E402

# Silence the Clash proxy that intercepts local-loopback / CN CDN
for _k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
           "all_proxy", "ALL_PROXY"):
    os.environ.pop(_k, None)

OUT_DIR = Path(__file__).parent / "probe_output"
OUT_DIR.mkdir(exist_ok=True)


# Per-platform probe plan.
#
# Each entry describes how to drive the UI so every tab fires its data-load
# XHR. Entries:
#   start_url:  first navigation (usually landing / login-redirect target)
#   settle_s:   how long to wait after start_url (for SPA to bootstrap state)
#   steps:      list of actions to take after the initial load. Each is one
#               of:
#                 {"goto": "url"}          — navigate directly
#                 {"hash": "#/foo"}        — set location.hash (same-origin)
#                 {"click": "selector"}    — click an element by text or CSS
#                 {"wait": seconds}        — sleep
#   filter_keywords: URL substrings indicating an API call worth keeping
#                    (drops static assets / telemetry)
PROBE_PLANS: dict[str, dict] = {
    "gangtise": {
        "start_url": "https://open.gangtise.com/",
        "settle_s": 6,
        "steps": [
            # Click each sidebar tab so its sub-SPA iframe mounts.
            {"click": 'text="观点"'}, {"wait": 8},
            {"click": 'text="研报"'}, {"wait": 8},
            {"click": 'text="纪要"'}, {"wait": 8},
            {"click": 'text="会议"'}, {"wait": 6},
            {"click": 'text="专家"'}, {"wait": 5},
            {"click": 'text="题材"'}, {"wait": 5},
        ],
        "filter_keywords": ["/application/"],
    },
    "alphapai": {
        "start_url": "https://alphapai-web.rabyte.cn/reading/home/meeting",
        "settle_s": 8,
        "steps": [
            {"goto": "https://alphapai-web.rabyte.cn/reading/home/comment"},
            {"wait": 6},
            {"goto": "https://alphapai-web.rabyte.cn/reading/home/point"},
            {"wait": 6},
            {"goto": "https://alphapai-web.rabyte.cn/reading/social-media"},
            {"wait": 6},
            {"goto": "https://alphapai-web.rabyte.cn/reading/home/meeting"},
            {"wait": 4},
        ],
        "filter_keywords": ["/external/", "/api/"],
    },
    "meritco": {
        "start_url": "https://research.meritco-group.com/forum?forumType=2",
        "settle_s": 10,
        "steps": [
            {"wait": 6},
            {"goto": "https://research.meritco-group.com/forum?forumType=3"},
            {"wait": 8},
            {"goto": "https://research.meritco-group.com/forum?forumType=1"},
            {"wait": 6},
        ],
        # UI uses matrix-search API; include /api/ fallback for newer paths
        "filter_keywords": ["/matrix-search/", "/api/"],
    },
    "jinmen": {
        "start_url": "https://brm.comein.cn/",
        "settle_s": 10,
        "steps": [
            {"goto": "https://brm.comein.cn/#/conference"},
            {"wait": 6},
            {"goto": "https://brm.comein.cn/#/research-report"},
            {"wait": 6},
        ],
        "filter_keywords": ["/api/", "/brm/", "/matrix-search/"],
    },
    "funda": {
        # funda's i18n redirect sends /research → /zh/research. Bigger waits
        # because tRPC batched request fires after React hydrates fully.
        "start_url": "https://funda.ai/zh/research",
        "settle_s": 12,
        "steps": [
            {"wait": 6},
            {"goto": "https://funda.ai/zh/earnings"}, {"wait": 10},
            {"goto": "https://funda.ai/zh/transcripts"}, {"wait": 10},
            {"goto": "https://funda.ai/zh/research"}, {"wait": 8},
        ],
        # funda.ai + api.funda.ai + trpc-style batched paths
        "filter_keywords": ["/api/", "/trpc/"],
    },
    "acecamp": {
        # AceCamp UI is on acecamptech.com but API is at api.acecamptech.com.
        # Same-origin cookie works on api.* subdomain via CORS + credentials.
        "start_url": "https://www.acecamptech.com/",
        "settle_s": 10,
        "steps": [
            {"goto": "https://www.acecamptech.com/events"}, {"wait": 10},
            {"goto": "https://www.acecamptech.com/articles"}, {"wait": 10},
        ],
        "filter_keywords": ["/api/"],
    },
    "thirdbridge": {
        "start_url": "https://forum.thirdbridge.com/zh/home/all",
        "settle_s": 10,
        "steps": [
            {"wait": 8},
        ],
        "filter_keywords": ["/api/"],
    },
    "alphaengine": {
        # Hash-routed SPA — tab switching is client-side after page renders
        "start_url": "https://www.alphaengine.top/#/summary-center",
        "settle_s": 8,
        "steps": [
            # Click each tab (if clickable) to trigger its streamSearch SSE
            {"click": 'text="国内研报"'}, {"wait": 6},
            {"click": 'text="海外研报"'}, {"wait": 6},
            {"click": 'text="资讯"'},     {"wait": 6},
            {"click": 'text="纪要"'},     {"wait": 4},
        ],
        "filter_keywords": ["/api/"],
    },
}


XHR_LOGGER_JS = """
(() => {
  // Per-frame XHR + fetch interceptor. Writes to `window.__xhr_log` (array).
  // Idempotent: reinstall-safe if add_init_script runs multiple times.
  if (window.__xhr_installed) return;
  window.__xhr_installed = true;
  window.__xhr_log = [];

  const orig_fetch = window.fetch;
  window.fetch = async function(url, opts) {
    const e = {kind: 'fetch', ts: Date.now(),
               url: String(url),
               method: ((opts||{}).method || 'GET'),
               body: ((opts||{}).body ? String((opts||{}).body).slice(0, 4000) : null)};
    try {
      const r = await orig_fetch.apply(this, arguments);
      try { e.status = r.status; } catch(_e) {}
      // Don't consume the response body for the app — just clone
      try {
        const clone = r.clone();
        const ct = (clone.headers.get('content-type') || '').toLowerCase();
        if (ct.includes('json') || ct.includes('text') || ct.includes('javascript')) {
          e.resp = (await clone.text()).slice(0, 6000);
        }
      } catch(_e) {}
      window.__xhr_log.push(e);
      return r;
    } catch(err) {
      e.error = String(err);
      window.__xhr_log.push(e);
      throw err;
    }
  };

  const OX = XMLHttpRequest.prototype.open;
  const OS = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function(method, url) {
    this.__log = {kind: 'xhr', ts: Date.now(),
                  url: String(url), method: String(method)};
    return OX.apply(this, arguments);
  };
  XMLHttpRequest.prototype.send = function(body) {
    if (this.__log) {
      this.__log.body = body ? String(body).slice(0, 4000) : null;
      this.addEventListener('loadend', () => {
        try { this.__log.status = this.status; } catch(_e) {}
        try {
          const ct = (this.getResponseHeader('content-type') || '').toLowerCase();
          if (ct.includes('json') || ct.includes('text') || ct.includes('javascript')) {
            this.__log.resp = (this.responseText || '').slice(0, 6000);
          }
        } catch(_e) {}
        window.__xhr_log.push(this.__log);
      });
    }
    return OS.apply(this, arguments);
  };
})();
"""


async def probe_platform(platform: str) -> dict:
    """Run the probe plan for one platform, return collected calls."""
    from backend.app.api.data_sources import _viewer_inject_for  # lazy
    from backend.app.services import credential_manager

    plan = PROBE_PLANS.get(platform)
    if plan is None:
        return {"platform": platform, "error": f"no probe plan defined"}

    # Load creds the same way viewer does
    try:
        spec = credential_manager.get_platform(platform)
        creds: dict = {}
        if spec.credentials_path.exists():
            creds = json.loads(spec.credentials_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"platform": platform, "error": f"creds load: {exc}"}

    inject_fn = _viewer_inject_for(platform, creds)
    if inject_fn is None:
        return {"platform": platform,
                "error": "no injector (server-signed auth or missing creds)",
                "note": "probe would show login page only — skip"}

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
    )
    ctx = await browser.new_context(
        viewport={"width": 1600, "height": 900},
        locale="zh-CN", timezone_id="Asia/Shanghai",
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
    )

    # Install cred + logger init scripts (order matters: creds first, then
    # logger runs in every frame on every navigation).
    try:
        await inject_fn(ctx)
    except Exception as exc:
        print(f"  [{platform}] inject_fn failed: {exc}", file=sys.stderr)
    await ctx.add_init_script(XHR_LOGGER_JS)

    page = await ctx.new_page()

    # Drive the plan
    started = time.time()
    try:
        await page.goto(plan["start_url"], wait_until="domcontentloaded",
                        timeout=25000)
    except Exception as exc:
        print(f"  [{platform}] initial goto failed: {exc}", file=sys.stderr)
    await asyncio.sleep(plan.get("settle_s", 6))

    for step in plan.get("steps", []):
        try:
            if "goto" in step:
                await page.goto(step["goto"], wait_until="domcontentloaded",
                                timeout=25000)
            elif "hash" in step:
                await page.evaluate(
                    f"window.location.hash = {json.dumps(step['hash'][1:])}"
                )
            elif "click" in step:
                try:
                    await page.click(step["click"], timeout=4000)
                except Exception:
                    # Silent — not all tabs exist for all users
                    pass
            elif "wait" in step:
                await asyncio.sleep(step["wait"])
        except Exception as exc:
            print(f"  [{platform}] step {step} err: {exc}", file=sys.stderr)

    # Let any inflight XHR settle
    await asyncio.sleep(3)

    # Health snapshot — detect login-redirect / empty-page fail modes so
    # the user sees WHY a platform has 0 useful calls.
    final_url = ""
    body_len = 0
    auth_state = "ok"
    try:
        final_url = page.url
        body_len = await page.evaluate(
            "() => document.body ? document.body.innerText.length : 0"
        )
    except Exception:
        pass
    lower_url = final_url.lower()
    if any(kw in lower_url for kw in ("/login", "redirect_uri=", "signin", "auth=")):
        auth_state = "login_redirect"
    elif body_len < 100:
        auth_state = "empty_page"

    # Collect from all frames
    all_calls: list[dict] = []
    for frame in page.frames:
        try:
            logs = await frame.evaluate("() => window.__xhr_log || []")
        except Exception:
            logs = []
        for row in logs:
            row["frame_url"] = frame.url
        all_calls.extend(logs)

    await browser.close()
    await pw.stop()

    # Filter + dedup
    keywords = plan.get("filter_keywords", [])
    filtered = []
    for c in all_calls:
        url = c.get("url") or ""
        # Extract path — works for both absolute and relative URLs
        path = url.split("?", 1)[0]
        if path.startswith("http://") or path.startswith("https://"):
            # https://host/rest/of/path → /rest/of/path
            tail = path.split("://", 1)[1]
            slash = tail.find("/")
            path = tail[slash:] if slash >= 0 else "/"
        elif not path.startswith("/"):
            path = "/" + path
        if keywords and not any(kw in path for kw in keywords):
            continue
        filtered.append({
            "method": c.get("method", "GET"),
            "url": url,
            "path": path,
            "req_body": c.get("body"),
            "resp_status": c.get("status"),
            "resp_preview": (c.get("resp") or "")[:1500],
            "frame": c.get("frame_url", "")[:80],
        })

    # Dedup by (method, path, body[:200]) — same call pattern counts once
    seen: set = set()
    unique: list = []
    for c in filtered:
        key = (c["method"], c["path"].split("?")[0],
               hashlib.md5(((c["req_body"] or "")[:200]).encode("utf-8")).hexdigest())
        if key in seen:
            continue
        seen.add(key)
        unique.append(c)

    return {
        "platform": platform,
        "elapsed_s": round(time.time() - started, 1),
        "total_xhr": len(all_calls),
        "filtered": len(filtered),
        "unique_paths": len(unique),
        "calls": unique,
        "final_url": final_url,
        "body_len": body_len,
        "auth_state": auth_state,
    }


def diff_against_scraper(platform: str, probe_result: dict) -> dict:
    """Given a probe result + the scraper's source, show UI paths missing
    from the scraper and scraper paths not seen in UI (likely dead)."""
    import re
    # Heuristic: plat key → scraper file
    plat_dir_map = {
        "alphapai": "alphapai_crawl",
        "gangtise": "gangtise",
        "meritco": "meritco_crawl",
        "jinmen": "jinmen",
        "funda": "funda",
        "acecamp": "AceCamp",
        "thirdbridge": "third_bridge",
    }
    d = plat_dir_map.get(platform)
    if not d:
        return {}
    scraper_src = REPO_ROOT / "crawl" / d / "scraper.py"
    if not scraper_src.exists():
        return {"error": f"{scraper_src} missing"}
    src = scraper_src.read_text(encoding="utf-8")
    scraper_paths = set()
    for m in re.finditer(r'["\'](/?[a-zA-Z0-9/_-]{4,}[a-zA-Z0-9])["\']', src):
        p = m.group(1)
        if not p.startswith("/"):
            p = "/" + p
        # Heuristic: keep plausible API paths only
        if not any(c in p for c in ("/api/", "/application/", "/matrix-search/",
                                     "/rpc/", "/external/", "/download/",
                                     "queryByCondition", "queryPage",
                                     "queryOpinionList")):
            continue
        scraper_paths.add(p.split("?")[0])

    ui_paths = {c["path"].split("?")[0] for c in probe_result.get("calls", [])}
    # Normalize both sides (strip trailing slashes, lowercase)
    def norm(s: set) -> set:
        return {p.rstrip("/").lower() for p in s}
    ui_norm = norm(ui_paths)
    sc_norm = norm(scraper_paths)

    only_ui = sorted(ui_norm - sc_norm)
    only_scraper = sorted(sc_norm - ui_norm)
    shared = sorted(ui_norm & sc_norm)
    return {
        "ui_paths_count": len(ui_norm),
        "scraper_paths_count": len(sc_norm),
        "shared": shared,
        "only_in_ui_(missing_from_scraper)": only_ui,
        "only_in_scraper_(probably_dead_or_not_exercised)": only_scraper,
    }


async def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("platform", nargs="?")
    p.add_argument("--all", action="store_true")
    p.add_argument("--diff", action="store_true",
                   help="also diff against scraper.py")
    p.add_argument("--concurrency", type=int, default=2,
                   help="parallel platforms when --all")
    args = p.parse_args()

    if args.all:
        targets = list(PROBE_PLANS.keys())
    elif args.platform:
        targets = [args.platform]
    else:
        print("usage: api_probe.py <platform> | --all  [--diff]")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = []

    sem = asyncio.Semaphore(max(1, args.concurrency))

    async def _run(pl):
        async with sem:
            print(f"[probe] {pl} starting…", file=sys.stderr)
            r = await probe_platform(pl)
            print(f"[probe] {pl}: {r.get('unique_paths', 0)} unique paths "
                  f"({r.get('filtered', 0)}/{r.get('total_xhr', 0)} xhr)",
                  file=sys.stderr)
            out = OUT_DIR / f"{pl}_{ts}.json"
            out.write_text(json.dumps(r, ensure_ascii=False, indent=2),
                           encoding="utf-8")
            if args.diff:
                r["_diff"] = diff_against_scraper(pl, r)
            return r

    results = await asyncio.gather(*[_run(pl) for pl in targets])

    # Print compact summary
    print("\n" + "=" * 60)
    print(f"Probe run {ts}")
    print("=" * 60)
    for r in results:
        pl = r.get("platform")
        err = r.get("error")
        if err:
            print(f"\n[{pl}] ERROR: {err}")
            continue
        auth = r.get("auth_state", "?")
        status_icon = {"ok": "✓", "login_redirect": "🔒", "empty_page": "⚪"}.get(auth, "?")
        print(f"\n[{pl}] {status_icon} auth={auth} · {r['unique_paths']} unique API paths "
              f"({r['filtered']}/{r['total_xhr']} xhr, {r['elapsed_s']}s)")
        if auth == "login_redirect":
            print(f"  🔒 redirected to login: {r.get('final_url', '')[:100]}")
            print(f"     → 需在数据源管理里给该平台重新登录, 刷新 credentials.json")
        elif auth == "empty_page":
            print(f"  ⚪ page body empty (body_len={r.get('body_len', 0)})")
            print(f"     → SPA 可能未渲染; 检查 start_url / inject 是否匹配 UI")
        d = r.get("_diff")
        if d and "error" not in d:
            gaps = d.get("only_in_ui_(missing_from_scraper)", [])
            if gaps:
                print(f"  ⚠ UI hits {len(gaps)} paths NOT in scraper:")
                for p in gaps[:15]:
                    print(f"    + {p}")
            stale = d.get("only_in_scraper_(probably_dead_or_not_exercised)", [])
            if stale:
                print(f"  ⓘ  scraper has {len(stale)} paths not seen in UI:")
                for p in stale[:6]:
                    print(f"    - {p}")


if __name__ == "__main__":
    asyncio.run(main())
