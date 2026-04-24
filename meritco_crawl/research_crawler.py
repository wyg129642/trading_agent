#!/usr/bin/env python3
"""
Meritco (久谦中台) /research/ section crawler.

Covers the "classic" research index at research.meritco-group.com/classic
(separate from /forum/ which is covered by crawler.py).

Endpoints:
  - /meritco-chatgpt/filter/list      — menu tree (type=12)
  - /matrix-search/research/article/search/v2   — paginated list per menuCode (needs X-My-Header)
  - /matrix-search/research/article/detail/v2   — article detail              (needs X-My-Header)
  - /matrix-search/research/organization/v2     — org list per menuCode
  - /matrix-search/research/download            — PDF (summary/report bytes)

X-My-Header: RSA-2048 PKCS#1 v1.5 of
  - search: token + keyword + str(page)
  - detail: token + articleId
(public key embedded in the frontend bundle — see RSA_PUB below.)

Module field:
  - search always uses "CLASSIC_ALL_SEARCH"
  - detail/download: "CLASSIC_JY_SEARCH" if platform=="纪要" else "CLASSIC_YB_SEARCH"
Download type: "report" for platform="研报" else "summary" (PDFs).
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_der_public_key

ROOT = Path(__file__).parent.resolve()
DATA_DIR = ROOT / "data" / "research"
CREDS_FILE = ROOT / "credentials.json"
API_BASE = "https://research.meritco-group.com"

RSA_PUB_B64 = (
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0q3O3srLBw1roKRa8D8DCUb5yy1u"
    "CZJV0WN20h7ePPj3QlUsJNKsIyuxptsV8ql2aBKjcm+tjLx8s+463m8PMTdqoJdFaabH+dxa"
    "3/0tSMZbyWFCnm0OLzGT4PhVXxTq9MNjjIh5DZFhX5NSPtQU8acmj2551vhzNpwnHqf6hgwV"
    "ZdCUASNqqp5kOA81DYekT5soFtlZMp/StpXUHa0Sxck1rFkpwjyk0YAXwAnsTdycJovwsnbX"
    "0jwFmLqNYW3qtJYKJr5yOHRgMaNojmR/TliA4DbroIMnChJs+5G4EFUInE6H6eTmi3CxJARD"
    "TY39MLjT8ZQGmLXdComHLCEoLwIDAQAB"
)


class AuthExpired(Exception):
    pass


# ─── signer ────────────────────────────────────────────────────────────────

class Signer:
    """RSA PKCS#1 v1.5 signer — replicates the frontend JSEncrypt.encrypt()."""

    def __init__(self, pubkey_b64: str) -> None:
        self._pk = load_der_public_key(base64.b64decode(pubkey_b64))

    def sign(self, plaintext: str) -> str:
        ct = self._pk.encrypt(plaintext.encode("utf-8"), padding.PKCS1v15())
        return base64.b64encode(ct).decode("ascii")


# ─── config & state ─────────────────────────────────────────────────────────

@dataclass
class Config:
    page_size: int = 50
    delay: float = 2.0
    max_retries: int = 5
    backoff_factor: float = 2.0
    timeout: float = 30.0
    max_list_pages: int = 0  # 0 = unlimited
    skip_pdfs: bool = False
    skip_details: bool = False
    skip_orgs: bool = False
    menucodes_filter: list[str] | None = None  # None = all leaf menus


@dataclass
class Progress:
    """Per-menuCode checkpoint."""

    completed_list_pages: dict[str, int] = field(default_factory=dict)
    # Global sets so we don't re-fetch the same articleId across menus.
    completed_detail_ids: set[str] = field(default_factory=set)
    completed_pdf_ids: set[str] = field(default_factory=set)
    completed_org_menus: set[str] = field(default_factory=set)
    started_at: str = ""
    last_update: str = ""

    @classmethod
    def load(cls, path: Path) -> "Progress":
        if not path.exists():
            return cls(started_at=datetime.now().isoformat())
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            completed_list_pages=raw.get("completed_list_pages", {}),
            completed_detail_ids=set(raw.get("completed_detail_ids", [])),
            completed_pdf_ids=set(raw.get("completed_pdf_ids", [])),
            completed_org_menus=set(raw.get("completed_org_menus", [])),
            started_at=raw.get("started_at", ""),
            last_update=raw.get("last_update", ""),
        )

    def save(self, path: Path) -> None:
        self.last_update = datetime.now().isoformat()
        path.write_text(
            json.dumps(
                {
                    "completed_list_pages": self.completed_list_pages,
                    "completed_detail_ids": sorted(self.completed_detail_ids),
                    "completed_pdf_ids": sorted(self.completed_pdf_ids),
                    "completed_org_menus": sorted(self.completed_org_menus),
                    "started_at": self.started_at,
                    "last_update": self.last_update,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )


# ─── HTTP client ────────────────────────────────────────────────────────────

class MeritcoResearchClient:
    def __init__(self, token: str, user_agent: str, signer: Signer, timeout: float = 30.0) -> None:
        self.token = token
        self.signer = signer
        common_headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN",
            "content-type": "application/json;charset=UTF-8",
            "origin": API_BASE,
            "referer": API_BASE + "/",
            "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "token": token,
            "user-agent": user_agent,
            "x-user-type": "",
        }
        # httpx: disable system proxies by passing proxy=None explicitly via transport
        self.client = httpx.Client(
            base_url=API_BASE,
            timeout=timeout,
            http2=False,
            headers=common_headers,
            trust_env=False,  # ignore HTTP(S)_PROXY env vars — user asked not to use proxy
        )

    def close(self) -> None:
        self.client.close()

    def _check(self, resp: httpx.Response) -> httpx.Response:
        if resp.status_code in (401, 403):
            raise AuthExpired(f"HTTP {resp.status_code}")
        resp.raise_for_status()
        return resp

    # --- menu tree --------------------------------------------------------

    def fetch_menu_tree(self, tree_type: str = "12") -> dict:
        body = {"type": tree_type, "platform": "RESEARCH_PC"}
        resp = self.client.post("/meritco-chatgpt/filter/list", json=body)
        self._check(resp)
        return resp.json()

    # --- article search ---------------------------------------------------

    def list_page(self, menu_code: str, page: int, page_size: int, trace_id: str, keyword: str = "") -> dict:
        body = {
            "page": page,
            "pageSize": page_size,
            "module": "CLASSIC_ALL_SEARCH",
            "contentTag": "",
            "traceId": trace_id,
            "publishTime": "",
            "codeIndustryId": "",
            "totalPage": "5",
            "sortColumn": "articleDate",
            "source": "",
            "reportTag": "全部标签",
            "menuCode": menu_code,
            "outCat1": "",
            "orgNameList": [],
            "outCat2": "",
            "keyword": keyword,
            "type": "",
            "platform": "RESEARCH_PC",
        }
        headers = {"x-my-header": self.signer.sign(self.token + keyword + str(page))}
        resp = self.client.post("/matrix-search/research/article/search/v2", json=body, headers=headers)
        self._check(resp)
        return resp.json()

    # --- article detail ---------------------------------------------------

    def detail(self, article_id: str, content_platform: str, source: str, module: str) -> dict:
        body = {
            "keyword": "",
            "articleId": article_id,
            "contentPlatform": content_platform,
            "source": source,
            "md5Key": None,
            "traceInfo": None,
            "module": module,
            "platform": "RESEARCH_PC",
        }
        headers = {"x-my-header": self.signer.sign(self.token + article_id)}
        resp = self.client.post("/matrix-search/research/article/detail/v2", json=body, headers=headers)
        self._check(resp)
        return resp.json()

    # --- organization list -------------------------------------------------

    def org_list(self, menu_code: str, page: int = 1, page_size: int = 30, keyword: str = "") -> dict:
        body = {
            "page": page,
            "pageSize": page_size,
            "traceId": f"org-{int(time.time()*1000)}",
            "publishTime": "",
            "menuCode": menu_code,
            "orgKeyword": keyword,
        }
        resp = self.client.post("/matrix-search/research/organization/v2", json=body)
        self._check(resp)
        return resp.json()

    # --- download (PDF bytes) --------------------------------------------

    def download(self, article_id: str, module: str, dtype: str = "summary") -> bytes:
        body = {"articleId": article_id, "module": module, "type": dtype, "platform": "RESEARCH_PC"}
        resp = self.client.post("/matrix-search/research/download", json=body)
        self._check(resp)
        return resp.content


# ─── retry wrapper ──────────────────────────────────────────────────────────

def with_retry(fn, config: Config, label: str, log: logging.Logger, *args, **kwargs):
    delay = config.delay
    for attempt in range(1, config.max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except AuthExpired:
            raise
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status in (429,) or status >= 500:
                wait = delay * (config.backoff_factor ** (attempt - 1))
                log.warning(f"  {label}: HTTP {status}, retry {attempt}/{config.max_retries} in {wait:.1f}s")
                time.sleep(wait)
            else:
                log.error(f"  {label}: HTTP {status} (non-retryable)")
                raise
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError, httpx.RemoteProtocolError) as e:
            wait = delay * (config.backoff_factor ** (attempt - 1))
            log.warning(f"  {label}: {type(e).__name__}, retry {attempt}/{config.max_retries} in {wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            log.error(f"  {label}: unexpected {type(e).__name__}: {e}")
            if attempt < config.max_retries:
                time.sleep(delay)
            else:
                raise
    raise RuntimeError(f"{label}: exhausted {config.max_retries} retries")


# ─── menu walk ──────────────────────────────────────────────────────────────

def collect_leaves(tree_root: dict) -> list[tuple[str, str]]:
    """Return [(menuCode, human/path)] for every leaf menuCode in the tree."""
    leaves: list[tuple[str, str]] = []

    def walk(node: dict, path: str) -> None:
        if not isinstance(node, dict):
            return
        name = node.get("name", "?")
        mc = node.get("menuCode", "")
        children = node.get("childList") or []
        p = f"{path}/{name}" if path else name
        if mc and not children:
            leaves.append((mc, p))
        for c in children:
            walk(c, p)

    for key, root in tree_root.items():
        walk(root, "")
    return leaves


# ─── main phases ────────────────────────────────────────────────────────────

def platform_to_module(platform: str | None) -> str:
    return "CLASSIC_JY_SEARCH" if platform == "纪要" else "CLASSIC_YB_SEARCH"


def platform_to_download_type(platform: str | None) -> str:
    return "report" if platform == "研报" else "summary"


def crawl_lists(client, config, progress, log, menu_code) -> list[dict]:
    """Paginate search/v2 for one menuCode. Save each page. Return records from all pages."""
    list_dir = DATA_DIR / "lists" / menu_code
    list_dir.mkdir(parents=True, exist_ok=True)
    start_page = progress.completed_list_pages.get(menu_code, 0) + 1
    trace_id = f"{int(time.time()*1000)}{random.randint(100000, 999999):06x}"
    page = start_page
    all_records: list[dict] = []

    log.info(f"[list:{menu_code}] start page={page} pageSize={config.page_size}")

    while True:
        if config.max_list_pages and page > config.max_list_pages:
            log.info(f"[list:{menu_code}] reached --max-list-pages={config.max_list_pages}, stop")
            break

        pf = list_dir / f"page_{page:04d}.json"
        if pf.exists():
            data = json.loads(pf.read_text(encoding="utf-8"))
        else:
            t0 = time.time()
            data = with_retry(
                client.list_page, config, f"list:{menu_code} p{page}", log,
                menu_code=menu_code, page=page, page_size=config.page_size, trace_id=trace_id,
            )
            if data.get("code") != 200:
                log.error(f"[list:{menu_code}] p{page}: code={data.get('code')} msg={data.get('message')!r}, stopping")
                break
            pf.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            log.info(f"[list:{menu_code}] p{page}: saved ({time.time()-t0:.1f}s)")
            time.sleep(config.delay + random.uniform(0, 0.5))

        result = data.get("result") or {}
        records = result.get("records") or []
        all_records.extend(records)
        progress.completed_list_pages[menu_code] = page
        if not records:
            log.info(f"[list:{menu_code}] empty page — done")
            break
        if len(records) < config.page_size:
            log.info(f"[list:{menu_code}] partial page ({len(records)} < {config.page_size}) — done")
            break
        page += 1

    # sanity: re-collect from disk (handles resumes)
    merged: list[dict] = []
    seen: set[str] = set()
    for pf in sorted(list_dir.glob("page_*.json")):
        d = json.loads(pf.read_text(encoding="utf-8"))
        for rec in (d.get("result") or {}).get("records") or []:
            aid = rec.get("articleId") or rec.get("id")
            if aid and aid not in seen:
                seen.add(aid)
                merged.append(rec)
    log.info(f"[list:{menu_code}] done: {len(merged)} unique records across {page-start_page+1} new pages")
    return merged


def crawl_details(client, config, progress, log, records: list[dict], menu_code: str) -> None:
    details_dir = DATA_DIR / "details"
    details_dir.mkdir(parents=True, exist_ok=True)
    progress_file = DATA_DIR / "progress.json"

    todo = [r for r in records if (r.get("articleId") or r.get("id")) not in progress.completed_detail_ids]
    log.info(f"[detail:{menu_code}] {len(todo)} to fetch ({len(progress.completed_detail_ids)} global done)")

    batch = 0
    for idx, rec in enumerate(todo, 1):
        aid = rec.get("articleId") or rec.get("id")
        if not aid:
            continue
        detail_file = details_dir / f"{aid}.json"
        if detail_file.exists():
            progress.completed_detail_ids.add(aid)
            continue

        platform = rec.get("platform") or ""
        source = rec.get("source") or ""
        module = platform_to_module(platform)

        try:
            data = with_retry(
                client.detail, config, f"detail {aid}", log, aid, platform, source, module
            )
        except AuthExpired:
            raise
        except Exception as e:
            log.error(f"[detail:{menu_code}] id={aid}: FAILED ({type(e).__name__}: {e}), skipping")
            continue

        if data.get("code") != 200:
            log.warning(f"[detail:{menu_code}] id={aid}: code={data.get('code')} msg={data.get('message')!r}")

        detail_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        progress.completed_detail_ids.add(aid)

        batch += 1
        if batch >= 20:
            progress.save(progress_file)
            batch = 0
            log.info(f"[detail:{menu_code}] {idx}/{len(todo)} ({100*idx/len(todo):.1f}%)")

        time.sleep(config.delay + random.uniform(0, 0.5))

    progress.save(progress_file)
    log.info(f"[detail:{menu_code}] done: {len(progress.completed_detail_ids)} total globally")


def crawl_pdfs(client, config, progress, log, records: list[dict], menu_code: str) -> None:
    pdf_dir = DATA_DIR / "pdfs"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    progress_file = DATA_DIR / "progress.json"

    todo = [r for r in records if (r.get("articleId") or r.get("id")) not in progress.completed_pdf_ids]
    log.info(f"[pdf:{menu_code}] {len(todo)} to download")

    batch = 0
    for idx, rec in enumerate(todo, 1):
        aid = rec.get("articleId") or rec.get("id")
        if not aid:
            continue
        out = pdf_dir / f"{aid}.pdf"
        if out.exists() and out.stat().st_size > 100:
            progress.completed_pdf_ids.add(aid)
            continue

        platform = rec.get("platform") or ""
        module = platform_to_module(platform)
        dtype = platform_to_download_type(platform)

        try:
            raw = with_retry(
                client.download, config, f"pdf {aid}", log, aid, module, dtype
            )
        except AuthExpired:
            raise
        except Exception as e:
            log.error(f"[pdf:{menu_code}] id={aid}: FAILED ({type(e).__name__}: {e}), skipping")
            continue

        # Check: PDF should start with %PDF or be JSON error
        if raw[:4] == b"%PDF":
            out.write_bytes(raw)
            progress.completed_pdf_ids.add(aid)
        else:
            # Likely JSON error (e.g. not entitled)
            err_path = pdf_dir / f"{aid}.err.json"
            try:
                j = json.loads(raw.decode("utf-8", "replace"))
                log.warning(f"[pdf:{menu_code}] id={aid}: non-PDF response code={j.get('code')} msg={j.get('message')!r}")
                err_path.write_text(json.dumps(j, ensure_ascii=False), encoding="utf-8")
            except Exception:
                err_path.write_bytes(raw[:2048])
                log.warning(f"[pdf:{menu_code}] id={aid}: non-PDF, {len(raw)} bytes (preview saved)")
            progress.completed_pdf_ids.add(aid)  # mark done so we don't retry forever

        batch += 1
        if batch >= 10:
            progress.save(progress_file)
            batch = 0
            log.info(f"[pdf:{menu_code}] {idx}/{len(todo)}")

        time.sleep(config.delay + random.uniform(0, 0.5))

    progress.save(progress_file)


def crawl_org(client, config, progress, log, menu_code: str) -> None:
    if menu_code in progress.completed_org_menus:
        return
    orgs_dir = DATA_DIR / "orgs"
    orgs_dir.mkdir(parents=True, exist_ok=True)
    out = orgs_dir / f"{menu_code}.json"
    if out.exists():
        progress.completed_org_menus.add(menu_code)
        return
    try:
        data = with_retry(client.org_list, config, f"org {menu_code}", log, menu_code)
    except AuthExpired:
        raise
    except Exception as e:
        log.error(f"[org:{menu_code}] FAILED ({type(e).__name__}: {e})")
        return
    out.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    progress.completed_org_menus.add(menu_code)
    log.info(f"[org:{menu_code}] saved")
    time.sleep(config.delay + random.uniform(0, 0.5))


# ─── entrypoint ─────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log_file = DATA_DIR / "crawl.log"
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # silence httpx / httpcore per-request chatter
    for name in ("httpx", "httpcore", "hpack"):
        logging.getLogger(name).setLevel(logging.WARNING)
    return logging.getLogger("meritco-research")


def load_credentials() -> tuple[str, str]:
    if not CREDS_FILE.exists():
        print(f"ERROR: {CREDS_FILE} not found.", file=sys.stderr)
        sys.exit(1)
    c = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
    t = (c.get("token") or "").strip()
    if not t:
        print(f"ERROR: no 'token' in {CREDS_FILE}", file=sys.stderr)
        sys.exit(1)
    return t, c.get("user_agent", "Mozilla/5.0")


def main() -> int:
    ap = argparse.ArgumentParser(description="Meritco /research/ crawler")
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--delay", type=float, default=2.0)
    ap.add_argument("--max-retries", type=int, default=5)
    ap.add_argument("--max-list-pages", type=int, default=0, help="0=unlimited per menuCode")
    ap.add_argument("--skip-details", action="store_true")
    ap.add_argument("--skip-pdfs", action="store_true")
    ap.add_argument("--skip-orgs", action="store_true")
    ap.add_argument("--menu", action="append", default=None,
                    help="restrict to specific menuCodes (repeatable). Default: all leaves.")
    ap.add_argument("--refresh-menu", action="store_true", help="re-fetch menu_tree_type12.json")
    args = ap.parse_args()

    config = Config(
        page_size=args.page_size,
        delay=args.delay,
        max_retries=args.max_retries,
        max_list_pages=args.max_list_pages,
        skip_details=args.skip_details,
        skip_pdfs=args.skip_pdfs,
        skip_orgs=args.skip_orgs,
        menucodes_filter=args.menu,
    )

    log = setup_logging()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    progress_file = DATA_DIR / "progress.json"
    progress = Progress.load(progress_file)

    token, ua = load_credentials()
    signer = Signer(RSA_PUB_B64)
    client = MeritcoResearchClient(token, ua, signer, timeout=config.timeout)

    def _sigint(signum, frame):
        log.warning("SIGINT received, saving progress and exiting")
        progress.save(progress_file)
        client.close()
        sys.exit(130)
    signal.signal(signal.SIGINT, _sigint)

    log.info("=== Meritco /research/ crawler start ===")

    # Load or fetch menu tree
    menu_file = DATA_DIR / "menu_tree_type12.json"
    if args.refresh_menu or not menu_file.exists():
        log.info("fetching menu tree")
        tree = client.fetch_menu_tree("12")
        if tree.get("code") != 200:
            log.error(f"menu fetch failed: {tree}")
            return 2
        menu_file.write_text(json.dumps(tree["result"], ensure_ascii=False, indent=2), encoding="utf-8")
    tree_root = json.loads(menu_file.read_text(encoding="utf-8"))
    leaves = collect_leaves(tree_root)
    log.info(f"menu: {len(leaves)} leaf menuCodes")

    if config.menucodes_filter:
        leaves = [(m, p) for m, p in leaves if m in config.menucodes_filter]
        log.info(f"filter applied: {len(leaves)} leaves after --menu filter")

    t0 = time.time()
    try:
        for i, (mc, path) in enumerate(leaves, 1):
            log.info(f"\n========== [{i}/{len(leaves)}] {mc}  ({path}) ==========")
            records = crawl_lists(client, config, progress, log, mc)
            progress.save(progress_file)

            if not config.skip_orgs:
                crawl_org(client, config, progress, log, mc)
                progress.save(progress_file)

            if not config.skip_details and records:
                crawl_details(client, config, progress, log, records, mc)

            if not config.skip_pdfs and records:
                crawl_pdfs(client, config, progress, log, records, mc)

    except AuthExpired as e:
        log.error(f"!!! TOKEN EXPIRED: {e}")
        log.error("!!! Refresh token in credentials.json and resume.")
        progress.save(progress_file)
        return 2
    except KeyboardInterrupt:
        progress.save(progress_file)
        return 130
    finally:
        client.close()

    log.info(f"=== DONE in {(time.time()-t0)/60:.1f} min. "
             f"Details: {len(progress.completed_detail_ids)} "
             f"PDFs: {len(progress.completed_pdf_ids)} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
