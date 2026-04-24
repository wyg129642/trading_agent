"""CDP probe for SemiAnalysis — opens headless Chromium at the archive page,
records every XHR for 20s, prints request/response shape. Run once while
building the scraper to confirm the SPA really uses /api/v1/archive the way
we assume (and nothing else).

Usage:
    HTTP_PROXY=http://127.0.0.1:7890 HTTPS_PROXY=http://127.0.0.1:7890 \\
    /home/ygwang/miniconda3/envs/agent/bin/python crawl/semianalysis/cdp_probe.py

The script runs headless by default. Set HEADFUL=1 for a visible browser
(requires X server / xvfb).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from playwright.async_api import async_playwright

ARCHIVE_URL = "https://newsletter.semianalysis.com/archive"
WAIT_SECONDS = 20


async def main() -> None:
    headful = os.environ.get("HEADFUL") == "1"
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")

    async with async_playwright() as pw:
        launch_kwargs: dict = {"headless": not headful}
        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}
        browser = await pw.chromium.launch(**launch_kwargs)
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = await ctx.new_page()

        records: list[dict] = []

        async def on_response(resp):
            try:
                url = resp.url
                if "newsletter.semianalysis.com" not in url and "substack.com" not in url:
                    return
                # Only JSON/XHR-ish
                ct = (resp.headers.get("content-type") or "").lower()
                if "json" not in ct and "/api/" not in url:
                    return
                body_preview = ""
                try:
                    txt = await resp.text()
                    body_preview = txt[:400]
                except Exception:
                    pass
                records.append({
                    "method": resp.request.method,
                    "status": resp.status,
                    "url": url,
                    "ct": ct,
                    "body_preview": body_preview,
                })
            except Exception:
                pass

        page.on("response", on_response)

        print(f"[cdp] GET {ARCHIVE_URL}  (proxy={proxy or 'none'}, headless={not headful})")
        try:
            await page.goto(ARCHIVE_URL, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            print(f"[cdp] navigation warning: {e}")

        # Trigger a scroll to force lazy-load of more archive pages
        for i in range(3):
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await asyncio.sleep(2)

        await asyncio.sleep(WAIT_SECONDS)

        # Print captured XHRs
        print(f"\n[cdp] captured {len(records)} JSON responses:\n")
        for r in records:
            print(f"  {r['status']:3d}  {r['method']:4s}  {r['url'][:120]}")
        print()

        # Print first archive + post responses in more detail
        seen_paths = set()
        for r in records:
            path = r["url"].split("?")[0].split("newsletter.semianalysis.com")[-1]
            if path in seen_paths:
                continue
            if "/api/v1/" not in path:
                continue
            seen_paths.add(path)
            print(f"[cdp] {path}  status={r['status']}  ct={r['ct']}")
            print(f"       preview: {r['body_preview'][:200]}\n")

        # Dump any cookies set
        cookies = await ctx.cookies()
        interesting = [c for c in cookies if
                       c["domain"].endswith("semianalysis.com") or c["domain"].endswith("substack.com")]
        print(f"[cdp] {len(interesting)} cookies on semianalysis/substack domains:")
        for c in interesting:
            print(f"   {c['name']:28s}  domain={c['domain']}  path={c['path']}  httpOnly={c.get('httpOnly')}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
