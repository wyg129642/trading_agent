#!/usr/bin/env python3
"""
进门财经 mndj_report PDF 批量下载器
====================================
范围: 由命令行参数指定，默认 27000001 ~ 27065000
并发: 20 连接
断点续传: 跳过已存在的文件，记录扫描进度
停止条件: 连续 N 个 404 则认为到达上界（可配置，稀疏区段需调高）
"""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path

import aiohttp
from tqdm import tqdm

# ============================================================
# 配置
# ============================================================

BASE_URL = "https://database.comein.cn/original-data/pdf/mndj_report/mndj_{id}.pdf"
OUTPUT_DIR = Path(__file__).parent / "jinmen-full-pdf-mndj-report"   # progress JSONs stay here
PDF_DIR = Path("/mnt/share/ygwang/pdf_full")                         # 主归档：NAS 2026-04 迁入
LEGACY_PDF_DIR = Path("/home/ygwang/crawl_data/pdf_full")            # 迁移过渡期：仍可能残留本地文件，exists 检查兜底

CONCURRENCY = 50           # 并发连接数（实测：50=0.1% err 稳定；150=23% err virgin territory；200=17% err。单 IP 限速导致高并发批量 timeout）
TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)


# ============================================================
# 进度管理
# ============================================================

def load_progress(progress_file: Path, start_id: int):
    if progress_file.exists():
        with open(progress_file, "r") as f:
            return json.load(f)
    return {"last_scanned_id": start_id - 1, "downloaded": 0, "skipped_404": 0}


def save_progress(progress_file: Path, progress):
    with open(progress_file, "w") as f:
        json.dump(progress, f)


# ============================================================
# 下载逻辑
# ============================================================

async def download_one(session: aiohttp.ClientSession, file_id: int, sem: asyncio.Semaphore):
    """下载单个 PDF，返回 (id, status): status = 'ok' | 'exists' | '404' | 'error'"""
    filename = f"mndj_{file_id}.pdf"
    filepath = PDF_DIR / filename

    if filepath.exists() and filepath.stat().st_size > 0:
        return file_id, "exists"
    # 迁移过渡期：本地 crawl_data/pdf_full 仍在向 NAS 搬，残留文件兜底跳过
    legacy_filepath = LEGACY_PDF_DIR / filename
    if legacy_filepath.exists() and legacy_filepath.stat().st_size > 0:
        return file_id, "exists"

    url = BASE_URL.format(id=file_id)

    async with sem:
        for attempt in range(3):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        filepath.write_bytes(content)
                        return file_id, "ok"
                    elif resp.status == 404:
                        return file_id, "404"
                    else:
                        if attempt < 2:
                            await asyncio.sleep(1 * (attempt + 1))
                        else:
                            return file_id, f"error_{resp.status}"
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < 2:
                    await asyncio.sleep(2 * (attempt + 1))
                else:
                    return file_id, f"error_{type(e).__name__}"

    return file_id, "error_exhausted"


async def run(start_id: int, end_id: int, consec_404_limit: int, progress_tag: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # 硬保护：/mnt/share 未挂载时 PDF_DIR 会落到根盘，绝对不允许
    if not Path("/mnt/share").is_mount():
        raise SystemExit("ERROR: /mnt/share 未挂载，拒绝启动以防 PDF 落盘到错误位置")
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    progress_file = OUTPUT_DIR / f"_progress_{progress_tag}.json"
    progress = load_progress(progress_file, start_id)

    resume_id = progress["last_scanned_id"] + 1
    if resume_id > start_id:
        print(f"断点续传: 从 {resume_id} 继续 (已下载 {progress['downloaded']})")
    else:
        resume_id = start_id

    total_range = end_id - resume_id + 1

    # 强制不使用代理
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, force_close=False)
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=TIMEOUT,
        trust_env=False,  # 忽略环境变量中的 proxy 设置
    ) as session:

        sem = asyncio.Semaphore(CONCURRENCY)
        stats = {
            "downloaded": progress.get("downloaded", 0),
            "skipped_exists": 0,
            "skipped_404": 0,
            "errors": 0,
        }
        consecutive_404 = 0
        last_save_time = time.time()

        pbar = tqdm(total=total_range, desc="下载进度", unit="file",
                    initial=0, dynamic_ncols=True)

        # 分批处理，每批 CONCURRENCY * 5 个
        batch_size = CONCURRENCY * 5
        current_id = resume_id

        while current_id <= end_id:
            batch_end = min(current_id + batch_size, end_id + 1)
            ids = list(range(current_id, batch_end))

            tasks = [download_one(session, fid, sem) for fid in ids]
            results = await asyncio.gather(*tasks)

            for file_id, status in sorted(results, key=lambda x: x[0]):
                if status == "ok":
                    stats["downloaded"] += 1
                    consecutive_404 = 0
                elif status == "exists":
                    stats["skipped_exists"] += 1
                    consecutive_404 = 0
                elif status == "404":
                    stats["skipped_404"] += 1
                    consecutive_404 += 1
                else:
                    stats["errors"] += 1
                    consecutive_404 = 0

            pbar.update(len(ids))
            pbar.set_postfix(
                ok=stats["downloaded"],
                exist=stats["skipped_exists"],
                miss=stats["skipped_404"],
                err=stats["errors"],
                c404=consecutive_404,
            )

            progress["last_scanned_id"] = batch_end - 1
            progress["downloaded"] = stats["downloaded"]
            progress["skipped_404"] = stats["skipped_404"]

            # 每 30 秒保存进度
            if time.time() - last_save_time > 30:
                save_progress(progress_file, progress)
                last_save_time = time.time()

            # 连续 404 超限 → 到达上界
            if consec_404_limit > 0 and consecutive_404 >= consec_404_limit:
                print(f"\n连续 {consecutive_404} 个 404，到达上界，停止扫描")
                break

            current_id = batch_end

        pbar.close()
        save_progress(progress_file, progress)

    print(f"\n{'='*60}")
    print(f"完成!")
    print(f"  新下载: {stats['downloaded']} 个 PDF")
    print(f"  已存在: {stats['skipped_exists']} (跳过)")
    print(f"  404:    {stats['skipped_404']}")
    print(f"  错误:   {stats['errors']}")
    print(f"  目录:   {PDF_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=27000001, help="起始 ID")
    parser.add_argument("--end", type=int, default=27065000, help="结束 ID（含）")
    parser.add_argument("--consec-404-limit", type=int, default=2000,
                        help="连续 404 早停阈值；设为 0 表示不早停（适合稀疏区段）")
    parser.add_argument("--tag", type=str, default="default",
                        help="进度文件标识，避免不同范围互相覆盖")
    args = parser.parse_args()

    print(f"扫描范围: mndj_{args.start}.pdf ~ mndj_{args.end}.pdf "
          f"({args.end - args.start + 1} 个 ID)")
    print(f"早停阈值: {args.consec_404_limit} (0=禁用)")
    print(f"进度文件: _progress_{args.tag}.json")

    asyncio.run(run(args.start, args.end, args.consec_404_limit, args.tag))
