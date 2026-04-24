#!/usr/bin/env python3
"""
The Information — CDP 调试探针 (debug-only, 不进入 realtime / backfill).

目的:
  我 (Claude) 需要在写 scraper.py 之前, 先把 theinformation.com 的真实 API 结构
  摸清楚 — SPA 打什么请求 / 付费墙怎么落 / 登录流长什么样.
  这脚本启 Playwright headless Chromium, 访问一条 URL, 拦截所有 XHR/fetch,
  把 method/url/status/request-body/response-preview 落到 JSONL; 同时隔几秒截图.
  最后我通过读 screenshots 和 grep JSONL 得到 "真实 SPA 长什么样".

用法:
  # 公开探针 (不带 cookie, 先看首页+付费墙)
  python3 cdp_probe.py --url https://www.theinformation.com/ --dwell 20

  # 带已保存的 cookie (credentials.json), 验证登录态
  python3 cdp_probe.py --url https://www.theinformation.com/articles --creds \
      --dwell 25

  # 指定 label 分离输出文件 (多轮探测用)
  python3 cdp_probe.py --url https://www.theinformation.com/ \
      --label homepage_anon --dwell 15

  # 尝试多个 URL
  python3 cdp_probe.py --urls https://www.theinformation.com/,\
https://www.theinformation.com/articles,\
https://www.theinformation.com/briefing --dwell 12 --label public_tour

输出:
  debug_screenshots/<label>_<idx>.jpg      每 3s 一张截图
  debug_network.<label>.jsonl              所有 XHR/fetch 事件 (一行一条)
  debug_html/<label>_final.html            最终 DOM 快照

每行 JSONL 字段:
  {
    "ts": 1776404294.12,
    "method": "GET",
    "url": "...",
    "resource_type": "xhr|fetch|document|script|...",
    "status": 200,
    "request_headers": {...},
    "post_data": "<= 4KB string",
    "response_headers": {...},
    "response_preview": "<= 4KB string (content-type starts with application/ or text/)",
    "timing_ms": 123,
    "from_cache": false,
    "security": null
  }

NOTE: 对所有 XHR/document/fetch 都抓; 对 image/font/stylesheet 跳过 (噪声).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# 强制直连 (Clash on 7890 会吃 localhost 但不应该影响外网; 不过稳妥起见)
for _k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)
os.environ.setdefault("NO_PROXY", "localhost,127.0.0.1")

try:
    from playwright.async_api import async_playwright, Page, Request, Response
except ImportError:
    print("ERROR: playwright not installed. Run: /home/ygwang/miniconda3/envs/agent/bin/pip install playwright && playwright install chromium", file=sys.stderr)
    sys.exit(2)

SCRIPT_DIR = Path(__file__).resolve().parent
CREDS_FILE = SCRIPT_DIR / "credentials.json"
SHOTS_DIR = SCRIPT_DIR / "debug_screenshots"
HTML_DIR = SCRIPT_DIR / "debug_html"

SKIP_RESOURCE_TYPES = {"image", "font", "stylesheet", "media", "manifest"}
PREVIEW_CAP = 4096


def _load_creds() -> Dict[str, str]:
    if not CREDS_FILE.exists():
        return {}
    try:
        data = json.loads(CREDS_FILE.read_text())
    except Exception as e:
        print(f"warn: failed to parse credentials.json: {e}", file=sys.stderr)
        return {}
    return {k: v for k, v in data.items() if isinstance(v, str) and v and not k.startswith("_")}


def _parse_cookie_header(cookie_str: str, domain: str) -> List[Dict[str, str]]:
    """Cookie-string (key=value; key2=value2) → Playwright cookie objects."""
    cookies = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, _, value = part.partition("=")
        cookies.append({
            "name": name.strip(),
            "value": value.strip(),
            "domain": domain,
            "path": "/",
            "secure": True,
            "httpOnly": False,
            "sameSite": "Lax",
        })
    return cookies


class NetworkRecorder:
    def __init__(self, out_path: Path):
        self.out_path = out_path
        self._fp = out_path.open("w", encoding="utf-8")
        self._start_ms: Dict[str, float] = {}
        self.event_count = 0

    def on_request(self, request: Request) -> None:
        if request.resource_type in SKIP_RESOURCE_TYPES:
            return
        self._start_ms[request.url] = time.time() * 1000

    async def on_response(self, response: Response) -> None:
        request = response.request
        if request.resource_type in SKIP_RESOURCE_TYPES:
            return
        record: Dict[str, Any] = {
            "ts": round(time.time(), 3),
            "method": request.method,
            "url": request.url,
            "resource_type": request.resource_type,
            "status": response.status,
            "request_headers": dict(request.headers or {}),
            "post_data": None,
            "response_headers": dict(response.headers or {}),
            "response_preview": None,
            "timing_ms": None,
            "from_cache": response.from_service_worker,
        }
        # request body
        try:
            pd = request.post_data
            if pd:
                record["post_data"] = pd[:PREVIEW_CAP]
        except Exception:
            pass
        # response body preview (only for textual / structured bodies)
        ct = (response.headers or {}).get("content-type", "").lower()
        if any(ct.startswith(pfx) for pfx in ("application/json", "application/ld", "application/xml", "text/", "application/javascript", "application/x-ndjson")):
            try:
                body = await response.body()
                if body:
                    try:
                        record["response_preview"] = body.decode("utf-8", errors="replace")[:PREVIEW_CAP]
                    except Exception:
                        record["response_preview"] = f"<bytes len={len(body)}>"
            except Exception as e:
                record["response_preview"] = f"<body err: {e}>"
        else:
            record["response_preview"] = f"<non-text ct={ct[:40]}>"
        # timing
        t0 = self._start_ms.pop(request.url, None)
        if t0 is not None:
            record["timing_ms"] = int(time.time() * 1000 - t0)

        self._fp.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._fp.flush()
        self.event_count += 1

    def close(self):
        try:
            self._fp.close()
        except Exception:
            pass


async def probe_url(
    page: Page,
    url: str,
    dwell: float,
    label: str,
    shot_every_sec: float = 3.0,
    idx_base: int = 0,
) -> Dict[str, Any]:
    SHOTS_DIR.mkdir(parents=True, exist_ok=True)
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    t_start = time.time()
    print(f"[probe] → {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
    except Exception as e:
        print(f"  goto error: {e}")
    # 保持会话 dwell 秒, 定时截图
    idx = idx_base
    next_shot = 0.0
    while time.time() - t_start < dwell:
        now_el = time.time() - t_start
        if now_el >= next_shot:
            shot = SHOTS_DIR / f"{label}_{idx:02d}.jpg"
            try:
                await page.screenshot(path=str(shot), type="jpeg", quality=70, full_page=False)
                print(f"  📸 {shot.name} @ {now_el:.1f}s")
            except Exception as e:
                print(f"  screenshot err: {e}")
            idx += 1
            next_shot = now_el + shot_every_sec
        await asyncio.sleep(0.4)

    # final DOM dump
    try:
        html = await page.content()
        dump = HTML_DIR / f"{label}_final.html"
        dump.write_text(html, encoding="utf-8")
        print(f"  💾 {dump.name} ({len(html)} chars)")
    except Exception as e:
        print(f"  content err: {e}")

    # snapshot current URL (SPA 可能跳走)
    try:
        cur_url = page.url
    except Exception:
        cur_url = url
    return {"start_url": url, "final_url": cur_url, "screenshots_taken": idx - idx_base}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", help="Single URL to probe")
    ap.add_argument("--urls", help="Comma-separated URLs (quick multi-probe)")
    ap.add_argument("--dwell", type=float, default=18.0, help="Seconds to linger per URL")
    ap.add_argument("--label", default="probe", help="Output file label")
    ap.add_argument("--creds", action="store_true", help="Load credentials.json cookie into context")
    ap.add_argument("--user-agent", default="", help="Override UA (else use creds or Chrome default)")
    ap.add_argument("--locale", default="en-US", help="Browser locale")
    ap.add_argument("--timezone", default="America/New_York", help="Browser timezone")
    ap.add_argument("--headless", action="store_true", default=True)
    ap.add_argument("--no-headless", action="store_false", dest="headless")
    args = ap.parse_args()

    if not args.url and not args.urls:
        ap.error("Pass --url or --urls")

    urls = [args.url] if args.url else [u.strip() for u in args.urls.split(",") if u.strip()]

    creds = _load_creds() if args.creds else {}
    ua = args.user_agent or creds.get("user_agent") or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )

    net_path = SCRIPT_DIR / f"debug_network.{args.label}.jsonl"
    recorder = NetworkRecorder(net_path)
    print(f"[probe] label={args.label} dwell={args.dwell}s creds={'yes' if creds else 'no'} ua={ua[:50]}...")
    print(f"[probe] network → {net_path}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=args.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            user_agent=ua,
            locale=args.locale,
            timezone_id=args.timezone,
            viewport={"width": 1400, "height": 900},
        )
        # inject cookies
        if creds.get("cookie"):
            cookie_objs = _parse_cookie_header(creds["cookie"], ".theinformation.com")
            if cookie_objs:
                await context.add_cookies(cookie_objs)
                print(f"[probe] injected {len(cookie_objs)} cookies")

        page = await context.new_page()
        # hook network
        page.on("request", recorder.on_request)
        page.on("response", lambda r: asyncio.create_task(recorder.on_response(r)))

        summary = {"urls": [], "label": args.label}
        idx_base = 0
        for u in urls:
            res = await probe_url(page, u, args.dwell, f"{args.label}_{len(summary['urls'])}", idx_base=idx_base)
            idx_base += res["screenshots_taken"]
            summary["urls"].append(res)

        await context.close()
        await browser.close()

    recorder.close()
    summary["network_events"] = recorder.event_count
    summary["network_file"] = str(net_path)
    print(f"[probe] done. events={recorder.event_count}")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
