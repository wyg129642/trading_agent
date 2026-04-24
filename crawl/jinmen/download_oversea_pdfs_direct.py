#!/usr/bin/env python3
"""进门外资研报 PDF 纯直链下载器 (bypass 账号冻结).

不同于 `download_oversea_pdfs.py` (调 preview API 要认证) 或
`backfill_oversea_pdfs.py` (走 dump_oversea_report 也要认证),本脚本:

  1. **只读 MongoDB** 已入库的 `oversea_reports.original_url` 字段 —
     preview 阶段的元数据在库里已经全了 (1.5M 条, ~150.7 万缺 PDF)。
  2. **只 GET OSS 直链** `database.comein.cn/original-data/pdf/mndj_report/<md5>.pdf`
     — 这层 CDN 是公开的,不校验 jinmen 账号 / token / cookie。所以账号被冻
     (`code=201 已被冻结`) 也不影响。
  3. 文件落盘到 `/mnt/share/ygwang/overseas_pdf/YYYY-MM/<release_time_ms>_<rid>.pdf`,
     按月分桶避免单目录塞几十万文件。

并发 / 进度:
  - ThreadPoolExecutor(default=16)。CDN 比 preview API 宽松得多,并发 16-32 都安全。
  - 每 30 秒 flush 一次 `_progress_direct.json` 到脚本目录,`--resume` 从上次停
    的 `_id` 接着往下。
  - tqdm 实时进度条 (DL / skip / bad / err) + 每 200 条打一行清晰快照。

Usage::

    # 全量下载, 16 并发 (默认)
    python3 download_oversea_pdfs_direct.py

    # 看看小批量先跑通
    python3 download_oversea_pdfs_direct.py --max 200 --concurrency 4

    # 强制重下已有 PDF
    python3 download_oversea_pdfs_direct.py --force
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from pymongo import MongoClient
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
PROGRESS_FILE = SCRIPT_DIR / "_progress_oversea_direct.json"

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin",
)
MONGO_DB = os.environ.get("MONGO_DB", "jinmen-full")
COL = "oversea_reports"

# /mnt/ygwang 目前是 root-only, SMB share 下的 /mnt/share/ygwang/overseas_pdf
# 已经被 OP 设成 drwxrwxrwx 可写,作为默认落点。
DEFAULT_PDF_DIR = "/mnt/share/ygwang/overseas_pdf"

BJ_TZ = timezone(timedelta(hours=8))

_stop = False


def _sig(*_):
    global _stop
    _stop = True
    print("\n[signal] 收到 SIGINT/SIGTERM, 完成当前批后停", flush=True)


signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def _ym_bucket(release_ms: int | None) -> str:
    if not release_ms:
        return "unknown"
    try:
        return datetime.fromtimestamp(int(release_ms) / 1000, tz=BJ_TZ).strftime("%Y-%m")
    except (OSError, ValueError):
        return "unknown"


def _dest_path(root: Path, rid, release_ms: int | None) -> Path:
    # 命名包含 rid (主键) + release_ms (方便肉眼识别发布日期, 不冲突)
    ts = int(release_ms) if release_ms else 0
    return root / _ym_bucket(release_ms) / f"mndj_rtime_{ts}_{rid}.pdf"


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {"last_id": None, "downloaded": 0, "skipped": 0, "bad": 0, "err": 0, "bytes": 0,
            "started_at": datetime.now(timezone.utc).isoformat()}


def save_progress(p: dict) -> None:
    p["updated_at"] = datetime.now(timezone.utc).isoformat()
    PROGRESS_FILE.write_text(json.dumps(p, indent=2))


# Thread-local HTTP session — requests.Session 非线程安全
_tls = threading.local()


def get_http() -> requests.Session:
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        # 不让 Clash / 其它代理干扰 CDN 直链 (CN 侧 CDN 走代理反而慢 + 失败)
        s.trust_env = False
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Accept": "application/pdf,application/octet-stream,*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.6",
        })
        _tls.s = s
    return s


def download_one(url: str, dest: Path, timeout: float = 60.0) -> tuple[int, str]:
    """Return (bytes_written, error). 0 bytes + error == 失败。"""
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with get_http().get(url, stream=True, timeout=timeout) as r:
            if r.status_code != 200:
                return 0, f"HTTP {r.status_code}"
            it = r.iter_content(32768)
            first = next(it, b"")
            if not first.startswith(b"%PDF"):
                return 0, f"bad magic {first[:8].hex()}"
            dest.parent.mkdir(parents=True, exist_ok=True)
            written = 0
            with tmp.open("wb") as f:
                f.write(first); written += len(first)
                for chunk in it:
                    if not chunk:
                        continue
                    f.write(chunk); written += len(chunk)
            tmp.replace(dest)
            return written, ""
    except (requests.RequestException, OSError) as e:
        try:
            if tmp.exists(): tmp.unlink()
        except Exception:
            pass
        return 0, f"{type(e).__name__}: {str(e)[:60]}"


class Stats:
    __slots__ = ("lock", "dl", "skip", "bad", "err", "bytes_")

    def __init__(self):
        self.lock = threading.Lock()
        self.dl = self.skip = self.bad = self.err = self.bytes_ = 0

    def inc(self, kind: str, n: int = 1, bytes_: int = 0):
        with self.lock:
            setattr(self, kind, getattr(self, kind) + n)
            self.bytes_ += bytes_

    def snapshot(self) -> dict:
        with self.lock:
            return {"downloaded": self.dl, "skipped": self.skip,
                    "bad": self.bad, "err": self.err, "bytes": self.bytes_}


def process_doc(doc: dict, pdf_dir: Path, col, force: bool, stats: Stats) -> str:
    rid = doc["_id"]
    url = doc.get("original_url") or ""
    release_ms = doc.get("release_time_ms") or 0

    if not url.startswith("http"):
        stats.inc("bad")
        return "bad_url"

    dest = _dest_path(pdf_dir, rid, release_ms)

    # skip: DB 里已标 + 文件也在
    if not force:
        existing_path = doc.get("pdf_local_path") or ""
        existing_size = doc.get("pdf_size_bytes") or 0
        if existing_size > 0 and existing_path:
            ep = Path(existing_path)
            if ep.exists() and ep.stat().st_size > 0:
                stats.inc("skip")
                return "skip"
        # 文件已落在新位置但 DB 没记 — 也当成功, 只补 DB 字段
        if dest.exists() and dest.stat().st_size > 0:
            size = dest.stat().st_size
            col.update_one({"_id": rid}, {"$set": {
                "pdf_local_path": str(dest),
                "pdf_size_bytes": size,
                "pdf_download_error": "",
            }})
            stats.inc("skip")
            return "skip"

    n, err = download_one(url, dest)
    if n > 0:
        col.update_one({"_id": rid}, {"$set": {
            "pdf_local_path": str(dest),
            "pdf_size_bytes": n,
            "pdf_download_error": "",
        }})
        stats.inc("dl", bytes_=n)
        return "dl"
    col.update_one({"_id": rid}, {"$set": {
        "pdf_download_error": err,
    }})
    stats.inc("err")
    return f"err:{err}"


def main():
    ap = argparse.ArgumentParser(description="进门外资研报 PDF 直链下载器")
    ap.add_argument("--pdf-dir", default=DEFAULT_PDF_DIR,
                    help=f"PDF 落点 (默认 {DEFAULT_PDF_DIR})")
    ap.add_argument("--concurrency", type=int, default=16,
                    help="并发线程 (默认 16; CDN 直链宽容, 别超 32)")
    ap.add_argument("--max", type=int, default=0,
                    help="最多处理多少条 (0=全部)")
    ap.add_argument("--resume", action="store_true",
                    help="从 _progress_oversea_direct.json 的 last_id 接着下 (按 _id 升序)")
    ap.add_argument("--force", action="store_true",
                    help="强制重下已有 PDF")
    ap.add_argument("--order", choices=("newest", "oldest", "id"), default="newest",
                    help="遍历顺序: newest=release 倒序(默认, 新的优先) / "
                         "oldest=release 正序 / id=_id 升序(与 --resume 配套)")
    ap.add_argument("--batch-size", type=int, default=200,
                    help="Mongo cursor batch_size + 保存进度频率 (默认 200)")
    args = ap.parse_args()

    pdf_dir = Path(args.pdf_dir)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    cli = MongoClient(MONGO_URI)
    db = cli[MONGO_DB]
    col = db[COL]

    q = {"original_url": {"$regex": "^http"}}
    if not args.force:
        q["$or"] = [
            {"pdf_local_path": {"$in": [None, ""]}},
            {"pdf_local_path": {"$exists": False}},
            {"pdf_size_bytes": {"$lte": 0}},
            {"pdf_size_bytes": {"$exists": False}},
        ]
    total = col.count_documents(q)

    progress = load_progress()
    if args.resume and progress.get("last_id") is not None:
        q["_id"] = {"$gt": progress["last_id"]}
        sort_spec = [("_id", 1)]
        remaining = col.count_documents(q)
        print(f"[resume] last_id={progress['last_id']}, 剩 {remaining}/{total}")
    else:
        if args.order == "newest":
            sort_spec = [("release_time_ms", -1), ("_id", -1)]
        elif args.order == "oldest":
            sort_spec = [("release_time_ms", 1), ("_id", 1)]
        else:
            sort_spec = [("_id", 1)]

    print(f"[direct] dir={pdf_dir} concurrency={args.concurrency} "
          f"total={total} force={args.force} order={args.order}")

    projection = {"_id": 1, "original_url": 1, "release_time_ms": 1,
                  "pdf_local_path": 1, "pdf_size_bytes": 1, "title": 1}
    cur = col.find(q, projection).sort(sort_spec).batch_size(args.batch_size)
    limit = args.max or total

    stats = Stats()
    t0 = time.time()
    last_save = t0
    last_log = t0

    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        bar = tqdm(total=min(total, limit), desc="oversea PDF", unit="pdf",
                   dynamic_ncols=True, smoothing=0.1)
        in_flight: dict = {}
        processed = 0

        def _drain_some(max_pending: int):
            """完成一部分 future, 直到未完成数 ≤ max_pending。"""
            nonlocal processed, last_save, last_log
            while len(in_flight) > max_pending:
                done = None
                for fut in as_completed(in_flight):
                    done = fut
                    break
                if done is None:
                    break
                rid = in_flight.pop(done)
                try:
                    done.result()  # result 已进 stats
                except Exception as e:
                    stats.inc("err")
                    bar.write(f"  [ERR] rid={rid} {type(e).__name__}: {e}")
                processed += 1
                bar.update(1)
                snap = stats.snapshot()
                mb = snap["bytes"] / 1024 / 1024
                rate = processed / max(time.time() - t0, 1)
                bar.set_postfix_str(
                    f"DL={snap['downloaded']} skip={snap['skipped']} "
                    f"bad={snap['bad']} err={snap['err']} "
                    f"{mb:.0f}MB @ {rate:.1f}/s"
                )
                progress["last_id"] = rid
                progress.update(snap)
                now = time.time()
                if now - last_save > 30:
                    save_progress(progress)
                    last_save = now
                if now - last_log > 60:
                    bar.write(f"  [snapshot t+{int(now - t0)}s] "
                              f"DL={snap['downloaded']} skip={snap['skipped']} "
                              f"bad={snap['bad']} err={snap['err']} "
                              f"{mb:.0f}MB @ {rate:.1f}/s")
                    last_log = now

        try:
            for doc in cur:
                if _stop or processed + len(in_flight) >= limit:
                    break
                fut = ex.submit(process_doc, doc, pdf_dir, col, args.force, stats)
                in_flight[fut] = doc["_id"]
                # 控制 in-flight 上限, 不让所有 futures 一口气提交
                if len(in_flight) >= args.concurrency * 4:
                    _drain_some(args.concurrency * 2)
            # drain 余下
            _drain_some(0)
        finally:
            bar.close()

    save_progress(progress)
    cli.close()
    snap = stats.snapshot()
    elapsed = time.time() - t0
    mb = snap["bytes"] / 1024 / 1024
    print(f"\n[direct done] elapsed={elapsed:.0f}s  processed={processed}  "
          f"DL={snap['downloaded']}  skip={snap['skipped']}  "
          f"bad={snap['bad']}  err={snap['err']}  {mb:.0f}MB  "
          f"avg={processed/max(elapsed,1):.1f}/s")


if __name__ == "__main__":
    main()
