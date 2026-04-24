#!/usr/bin/env python3
"""
Meritco (久谦中台) forum crawler.

Pulls all items from https://research.meritco-group.com/matrix-search/forum/:
  1. Paginate /select/list to get list items (metadata)
  2. For each item, fetch /select/id?forumId=X to get full content
  3. Save raw JSON per list page + per detail, with checkpoint/resume.

Usage:
    python crawler.py                    # crawl type=2 (professional content)
    python crawler.py --type 1           # crawl different forumType
    python crawler.py --details-only     # skip list, only fetch missing details
    python crawler.py --lists-only       # only paginate lists, skip details
    python crawler.py --page-size 50 --delay 2.0

Credentials:
    Put your token in credentials.json. When it expires, re-capture from DevTools.
"""

from __future__ import annotations

import argparse
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

ROOT = Path(__file__).parent.resolve()
DATA_DIR = ROOT / "data"
CREDS_FILE = ROOT / "credentials.json"
API_BASE = "https://research.meritco-group.com"

# Default platformArr = all 7 professional content categories (纪要 + 研报 + 其他)
DEFAULT_PLATFORM_ARR = [
    "专业内容-纪要-国内市场-专家访谈",
    "专业内容-纪要-国内市场-业绩交流",
    "专业内容-纪要-国内市场-券商路演",
    "专业内容-纪要-海外市场",
    "专业内容-研报-国内市场",
    "专业内容-研报-海外市场",
    "专业内容-其他报告",
]


# ─── config & state ─────────────────────────────────────────────────────────

@dataclass
class Config:
    forum_type: int = 2
    page_size: int = 50
    delay: float = 2.0
    max_retries: int = 5
    backoff_factor: float = 2.0
    timeout: float = 30.0
    lists_only: bool = False
    details_only: bool = False
    max_list_pages: int = 0  # 0 = unlimited


@dataclass
class Progress:
    """Checkpoint state, persisted after every N items."""
    forum_type: int = 2
    last_list_page: int = 0
    total_items: int = 0
    completed_detail_ids: set[int] = field(default_factory=set)
    started_at: str = ""
    last_update: str = ""

    @classmethod
    def load(cls, path: Path, forum_type: int) -> "Progress":
        if not path.exists():
            return cls(forum_type=forum_type, started_at=datetime.now().isoformat())
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            forum_type=raw.get("forum_type", forum_type),
            last_list_page=raw.get("last_list_page", 0),
            total_items=raw.get("total_items", 0),
            completed_detail_ids=set(raw.get("completed_detail_ids", [])),
            started_at=raw.get("started_at", ""),
            last_update=raw.get("last_update", ""),
        )

    def save(self, path: Path) -> None:
        self.last_update = datetime.now().isoformat()
        path.write_text(
            json.dumps(
                {
                    "forum_type": self.forum_type,
                    "last_list_page": self.last_list_page,
                    "total_items": self.total_items,
                    "completed_detail_ids": sorted(self.completed_detail_ids),
                    "started_at": self.started_at,
                    "last_update": self.last_update,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )


# ─── HTTP client ────────────────────────────────────────────────────────────

class MeritcoClient:
    def __init__(self, token: str, user_agent: str, timeout: float = 30.0) -> None:
        self.token = token
        self.client = httpx.Client(
            base_url=API_BASE,
            timeout=timeout,
            http2=False,
            headers={
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
            },
        )

    def close(self) -> None:
        self.client.close()

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        resp = self.client.request(method, url, **kwargs)
        if resp.status_code == 401 or resp.status_code == 403:
            raise AuthExpired(f"HTTP {resp.status_code}: token expired or invalid")
        resp.raise_for_status()
        return resp

    def list_page(self, page: int, page_size: int, forum_type: int, trace_id: str) -> dict:
        # For type=2 the frontend explicitly sends all 7 professional-content tags.
        # For type=1 (events) and type=3 (Jiuqian's own research), those tags don't
        # apply — send an empty array so the server returns everything in that type.
        platform_arr = DEFAULT_PLATFORM_ARR if forum_type == 2 else []
        body = {
            "forumId": None,
            "page": page,
            "pageSize": page_size,
            "module": "CLASSIC_ALL_SEARCH",
            "contentTag": "",
            "traceId": trace_id,
            "publishTime": "",
            "codeIndustryId": "",
            "sortColumn": "articleDate",
            "source": "",
            "reportTag": "全部标签",
            "platformArr": platform_arr,
            "outCat1": "",
            "orgNameList": [],
            "outCat2": "",
            "keyword": "",
            "type": forum_type,
            "industryList": [],
            "expertType": "",
            "meetingStartTime": "",
            "meetingEndTime": "",
            "queryHotListFlag": False,
            "sort": 2,
            "platform": "RESEARCH_PC",
        }
        resp = self._request("POST", "/matrix-search/forum/select/list", json=body)
        return resp.json()

    def detail(self, forum_id: int) -> dict:
        resp = self._request(
            "POST",
            f"/matrix-search/forum/select/id?forumId={forum_id}",
            json={"platform": "RESEARCH_PC"},
        )
        return resp.json()


class AuthExpired(Exception):
    pass


# ─── retry wrapper ──────────────────────────────────────────────────────────

def with_retry(fn, config: Config, label: str, log: logging.Logger, *args, **kwargs):
    """Call fn with retry + exponential backoff. Raises AuthExpired immediately."""
    delay = config.delay
    for attempt in range(1, config.max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except AuthExpired:
            raise
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or status >= 500:
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


# ─── main phases ────────────────────────────────────────────────────────────

def crawl_lists(
    client: MeritcoClient,
    config: Config,
    progress: Progress,
    log: logging.Logger,
) -> list[int]:
    """Paginate /select/list, save each page to data/lists/. Returns all item IDs."""
    lists_dir = DATA_DIR / "lists"
    lists_dir.mkdir(parents=True, exist_ok=True)
    progress_file = DATA_DIR / f"progress_type{config.forum_type}.json"

    all_ids: list[int] = []
    trace_id = f"{int(time.time()*1000)}{random.randint(100000, 999999):06x}"
    start_page = progress.last_list_page + 1
    page = start_page

    log.info(f"[list] start page={page} pageSize={config.page_size} forumType={config.forum_type}")

    while True:
        if config.max_list_pages and page > config.max_list_pages:
            log.info(f"[list] reached --max-list-pages={config.max_list_pages}, stop")
            break

        page_file = lists_dir / f"type{config.forum_type}_page_{page:04d}.json"

        if page_file.exists():
            log.info(f"[list] page {page}: already saved, loading from disk")
            data = json.loads(page_file.read_text(encoding="utf-8"))
        else:
            t0 = time.time()
            data = with_retry(
                client.list_page,
                config, f"list page {page}", log,
                page=page, page_size=config.page_size,
                forum_type=config.forum_type, trace_id=trace_id,
            )
            elapsed = time.time() - t0
            code = data.get("code")
            if code != 200:
                log.error(f"[list] page {page}: code={code} message={data.get('message')!r}, stopping")
                break
            page_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=None),
                encoding="utf-8",
            )
            log.info(f"[list] page {page}: saved ({elapsed:.1f}s)")
            time.sleep(config.delay + random.uniform(0, 0.5))

        result = data.get("result") or {}
        items = result.get("forumList") or []
        total = result.get("total") or 0

        if total and progress.total_items != total:
            progress.total_items = total
            log.info(f"[list] total reported by server: {total}")

        page_ids = [item.get("id") for item in items if item.get("id") is not None]
        all_ids.extend(page_ids)

        log.info(f"[list] page {page}: {len(items)} items, running total {len(all_ids)}/{total}")

        progress.last_list_page = page
        progress.save(progress_file)

        if not items:
            log.info(f"[list] empty page, pagination complete")
            break
        if len(items) < config.page_size:
            log.info(f"[list] last page (partial: {len(items)} < {config.page_size})")
            break
        if total and len(all_ids) >= total:
            log.info(f"[list] reached total={total}, pagination complete")
            break

        page += 1

    # Sanity: collect ALL ids across saved list pages (for resume scenarios)
    all_ids_from_disk: list[int] = []
    for pf in sorted(lists_dir.glob(f"type{config.forum_type}_page_*.json")):
        d = json.loads(pf.read_text(encoding="utf-8"))
        for item in (d.get("result") or {}).get("forumList") or []:
            iid = item.get("id")
            if iid is not None:
                all_ids_from_disk.append(iid)

    unique_ids = list(dict.fromkeys(all_ids_from_disk))
    log.info(f"[list] done: {len(unique_ids)} unique IDs across {page-start_page+1} new pages")
    return unique_ids


def crawl_details(
    client: MeritcoClient,
    config: Config,
    progress: Progress,
    log: logging.Logger,
    all_ids: list[int],
) -> None:
    """Fetch /select/id?forumId=X for each id, save to data/details/."""
    details_dir = DATA_DIR / "details"
    details_dir.mkdir(parents=True, exist_ok=True)
    progress_file = DATA_DIR / f"progress_type{config.forum_type}.json"

    todo = [i for i in all_ids if i not in progress.completed_detail_ids]
    done_count = len(progress.completed_detail_ids)
    total = len(all_ids)

    log.info(f"[detail] {len(todo)} to fetch ({done_count}/{total} done)")

    t_start = time.time()
    batch = 0

    for idx, fid in enumerate(todo, 1):
        detail_file = details_dir / f"{fid}.json"

        if detail_file.exists():
            progress.completed_detail_ids.add(fid)
            continue

        try:
            data = with_retry(
                client.detail, config, f"detail {fid}", log, fid,
            )
        except AuthExpired:
            raise
        except Exception as e:
            log.error(f"[detail] id={fid}: FAILED ({type(e).__name__}: {e}), skipping")
            continue

        code = data.get("code")
        if code != 200:
            log.warning(f"[detail] id={fid}: code={code} message={data.get('message')!r}")
        detail_file.write_text(
            json.dumps(data, ensure_ascii=False), encoding="utf-8",
        )
        progress.completed_detail_ids.add(fid)

        batch += 1
        if batch >= 20:
            progress.save(progress_file)
            batch = 0
            elapsed = time.time() - t_start
            rate = idx / elapsed if elapsed > 0 else 0
            remain = (len(todo) - idx) / rate if rate > 0 else 0
            log.info(
                f"[detail] {done_count + idx}/{total} "
                f"({100*(done_count+idx)/total:.1f}%) "
                f"rate={rate:.2f}/s ETA={remain/60:.1f}min"
            )

        time.sleep(config.delay + random.uniform(0, 0.5))

    progress.save(progress_file)
    log.info(f"[detail] done: {len(progress.completed_detail_ids)}/{total}")


# ─── entrypoint ─────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    DATA_DIR.mkdir(exist_ok=True)
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
    return logging.getLogger("meritco")


def load_credentials() -> tuple[str, str]:
    if not CREDS_FILE.exists():
        print(f"ERROR: {CREDS_FILE} not found. Create it with your token.", file=sys.stderr)
        sys.exit(1)
    creds = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
    token = creds.get("token", "").strip()
    ua = creds.get("user_agent", "Mozilla/5.0")
    if not token:
        print(f"ERROR: no 'token' in {CREDS_FILE}", file=sys.stderr)
        sys.exit(1)
    return token, ua


def main() -> int:
    ap = argparse.ArgumentParser(description="Meritco forum crawler")
    ap.add_argument("--type", type=int, default=2, help="forumType (default 2=专业内容)")
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--delay", type=float, default=2.0, help="seconds between requests")
    ap.add_argument("--max-retries", type=int, default=5)
    ap.add_argument("--max-list-pages", type=int, default=0, help="0=unlimited")
    ap.add_argument("--lists-only", action="store_true")
    ap.add_argument("--details-only", action="store_true")
    ap.add_argument("--reset-progress", action="store_true", help="ignore checkpoint, start fresh")
    args = ap.parse_args()

    config = Config(
        forum_type=args.type,
        page_size=args.page_size,
        delay=args.delay,
        max_retries=args.max_retries,
        lists_only=args.lists_only,
        details_only=args.details_only,
        max_list_pages=args.max_list_pages,
    )

    log = setup_logging()
    DATA_DIR.mkdir(exist_ok=True)
    # Per-type progress file so crawls of different forumTypes don't overwrite each other.
    progress_file = DATA_DIR / f"progress_type{config.forum_type}.json"
    # Backward-compat: if old shared progress.json exists and matches this type, migrate.
    legacy = DATA_DIR / "progress.json"
    if legacy.exists() and not progress_file.exists():
        try:
            legacy_data = json.loads(legacy.read_text(encoding="utf-8"))
            if legacy_data.get("forum_type") == config.forum_type:
                legacy.rename(progress_file)
                log.info(f"migrated legacy progress.json -> {progress_file.name}")
        except Exception:
            pass

    if args.reset_progress and progress_file.exists():
        progress_file.unlink()
        log.info("reset progress file")

    progress = Progress.load(progress_file, config.forum_type)
    token, ua = load_credentials()

    log.info(f"=== Meritco crawler start (type={config.forum_type}, delay={config.delay}s) ===")

    client = MeritcoClient(token, ua, timeout=config.timeout)

    # Handle Ctrl-C cleanly
    def _sigint(signum, frame):
        log.warning("SIGINT received, saving progress and exiting")
        progress.save(progress_file)
        client.close()
        sys.exit(130)
    signal.signal(signal.SIGINT, _sigint)

    t0 = time.time()
    try:
        if config.details_only:
            # rebuild id list from saved list pages
            lists_dir = DATA_DIR / "lists"
            all_ids: list[int] = []
            for pf in sorted(lists_dir.glob(f"type{config.forum_type}_page_*.json")):
                d = json.loads(pf.read_text(encoding="utf-8"))
                for item in (d.get("result") or {}).get("forumList") or []:
                    if item.get("id") is not None:
                        all_ids.append(item["id"])
            all_ids = list(dict.fromkeys(all_ids))
            log.info(f"details-only mode: {len(all_ids)} IDs loaded from list pages")
        else:
            all_ids = crawl_lists(client, config, progress, log)

        if not config.lists_only and all_ids:
            crawl_details(client, config, progress, log, all_ids)

    except AuthExpired as e:
        log.error(f"!!! TOKEN EXPIRED: {e}")
        log.error("!!! Re-capture token from DevTools and update credentials.json, then resume.")
        progress.save(progress_file)
        return 2
    except KeyboardInterrupt:
        log.warning("KeyboardInterrupt")
        progress.save(progress_file)
        return 130
    finally:
        client.close()

    elapsed = time.time() - t0
    log.info(f"=== DONE in {elapsed/60:.1f} min. "
             f"Lists: {progress.last_list_page} pages. "
             f"Details: {len(progress.completed_detail_ids)}/{progress.total_items} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
