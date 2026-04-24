#!/usr/bin/env python3
"""
Alpha派研报批量下载器 v2 — 同花顺概念 × 按周拆分
================================================
实验结论:
- 单次返回上限 ~40-60 条
- 按周拆分比按月多 6.3x 数据
- 概念查询比个股名效率高 25-30%
- 相近概念重叠率仅 1-2%

策略: 450个同花顺概念 × 26周 = 11,700 次请求
预计: 间隔2s ≈ 6.5小时, 去重后预估 5-8万条
支持断点续传（已有ID自动跳过）
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Set

sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                "alphapai-skill", "alphapai-research", "scripts"))
from alphapai_client import AlphaPaiClient, load_config

# ============================================================
# 配置
# ============================================================

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "downloaded_reports")
REPORT_TYPES = ["report", "foreign_report"]

END_DATE = datetime(2026, 4, 2)
START_DATE = datetime(2025, 10, 2)

# 按周拆分时间窗口
WEEK_WINDOWS = []
cur = START_DATE
while cur < END_DATE:
    nxt = cur + timedelta(days=7)
    if nxt > END_DATE:
        nxt = END_DATE
    WEEK_WINDOWS.append((cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")))
    cur = nxt

BASE_DELAY = 2.0
MAX_RETRIES = 5
BACKOFF_FACTOR = 2.0

# ============================================================
# 从文件加载同花顺概念列表
# ============================================================

def load_ths_concepts():
    """从已下载的同花顺概念文件加载"""
    path = os.path.join(OUTPUT_DIR, "ths_concept_names.txt")
    if not os.path.exists(path):
        print(f"ERROR: 概念文件不存在: {path}")
        print("请先运行概念下载步骤")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]
    return names

# ============================================================
# 工具函数
# ============================================================

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def save_jsonl(filepath, records):
    with open(filepath, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def load_existing_ids(*filepaths):
    """加载已有文件中的所有 ID 用于去重"""
    ids = set()
    for fp in filepaths:
        if os.path.exists(fp):
            with open(fp, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            ids.add(json.loads(line).get("id", ""))
                        except json.JSONDecodeError:
                            pass
    return ids

def call_with_retry(client, query, recall_type, start, end):
    delay = BASE_DELAY
    for attempt in range(MAX_RETRIES + 1):
        try:
            return client.recall_data(
                query=query, is_cut_off=False,
                recall_type=recall_type, start_time=start, end_time=end,
            )
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "rate" in err_str.lower():
                wait = delay * (BACKOFF_FACTOR ** attempt)
                print(f"    ⚠ 429 限流, 等 {wait:.0f}s (retry {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
            elif any(c in err_str for c in ["500", "502", "503"]):
                wait = delay * (BACKOFF_FACTOR ** attempt)
                print(f"    ⚠ 服务端错误, 等 {wait:.0f}s")
                time.sleep(wait)
            else:
                # JSON 解析错误等非致命错误，记录后继续
                if attempt < MAX_RETRIES:
                    time.sleep(delay)
                else:
                    return {}
    return {}

# ============================================================
# 进度追踪（断点续传）
# ============================================================

PROGRESS_FILE = os.path.join(OUTPUT_DIR, "v2_progress.json")

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"completed_tasks": []}

def save_progress(progress):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False)

# ============================================================
# 主逻辑
# ============================================================

def main():
    config = load_config()
    if not config:
        print("ERROR: 未找到 API 配置"); sys.exit(1)
    client = AlphaPaiClient(config)
    ensure_dir(OUTPUT_DIR)

    concepts = load_ths_concepts()

    report_file = os.path.join(OUTPUT_DIR, "report_国内研报.jsonl")
    foreign_file = os.path.join(OUTPUT_DIR, "foreign_report_海外研报.jsonl")
    log_file = os.path.join(OUTPUT_DIR, "download_log_v2.json")

    # 加载已有 ID（含 v1 数据）
    seen_ids = load_existing_ids(report_file, foreign_file)
    print(f"已加载 {len(seen_ids)} 条历史 ID")

    # 断点续传
    progress = load_progress()
    completed = set(progress.get("completed_tasks", []))
    if completed:
        print(f"断点续传: 已完成 {len(completed)} 个任务")

    total_tasks = len(WEEK_WINDOWS) * len(concepts)
    stats = {
        "total_requests": 0, "total_report": 0, "total_foreign_report": 0,
        "total_duplicates_skipped": 0, "errors": 0, "skipped_completed": 0,
    }

    print(f"{'='*60}")
    print(f"Alpha派研报下载器 v2")
    print(f"查询来源: 同花顺概念 {len(concepts)} 个")
    print(f"时间窗口: {len(WEEK_WINDOWS)} 周 ({START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')})")
    print(f"总请求数: {total_tasks} (已完成 {len(completed)}，剩余 {total_tasks - len(completed)})")
    print(f"预计耗时: ~{(total_tasks - len(completed)) * 2.5 / 3600:.1f} 小时")
    print(f"{'='*60}")

    t0 = time.time()
    task_idx = 0
    batch_new = 0  # 每 50 个任务打印一次汇总

    for win_idx, (win_start, win_end) in enumerate(WEEK_WINDOWS):
        print(f"\n{'─'*50}")
        print(f"周 [{win_idx+1}/{len(WEEK_WINDOWS)}]: {win_start} ~ {win_end}")
        print(f"{'─'*50}")

        for q_idx, concept in enumerate(concepts):
            task_idx += 1
            task_key = f"{win_start}|{concept}"

            # 断点跳过
            if task_key in completed:
                stats["skipped_completed"] += 1
                continue

            tag = f"[{task_idx}/{total_tasks}]"
            print(f"  {tag} \"{concept}\" ({win_start}~{win_end})", end="", flush=True)

            try:
                result = call_with_retry(client, concept, REPORT_TYPES, win_start, win_end)
                stats["total_requests"] += 1
                data_list = result.get("data", [])

                new_r, new_f, dup = [], [], 0
                for item in data_list:
                    iid = item.get("id", "")
                    if iid in seen_ids:
                        dup += 1
                        continue
                    seen_ids.add(iid)
                    item["_query"] = concept
                    item["_window"] = f"{win_start}~{win_end}"
                    t = item.get("type", "")
                    if t == "report":
                        new_r.append(item)
                    elif t == "foreign_report":
                        new_f.append(item)

                if new_r:
                    save_jsonl(report_file, new_r)
                    stats["total_report"] += len(new_r)
                if new_f:
                    save_jsonl(foreign_file, new_f)
                    stats["total_foreign_report"] += len(new_f)
                stats["total_duplicates_skipped"] += dup

                print(f"  → 内:{len(new_r)} 外:{len(new_f)} 重:{dup}")

                # 标记完成
                completed.add(task_key)
                batch_new += 1

                # 每 50 个任务保存一次进度
                if batch_new >= 50:
                    progress["completed_tasks"] = list(completed)
                    save_progress(progress)
                    batch_new = 0
                    elapsed = time.time() - t0
                    done = stats["total_requests"]
                    remaining = total_tasks - len(completed)
                    if done > 0:
                        eta = remaining * (elapsed / done) / 3600
                        print(f"  📊 进度: {len(completed)}/{total_tasks}, "
                              f"唯一: {len(seen_ids)}, ETA: {eta:.1f}h")

            except Exception as e:
                stats["errors"] += 1
                print(f"  ✗ {str(e)[:80]}")

            time.sleep(BASE_DELAY)

    # 最终保存
    progress["completed_tasks"] = list(completed)
    save_progress(progress)

    elapsed = time.time() - t0
    stats["elapsed_seconds"] = round(elapsed, 1)
    stats["unique_ids_total"] = len(seen_ids)

    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"完成！耗时 {elapsed/3600:.1f} 小时")
    print(f"  国内研报: {stats['total_report']} 条 (新增)")
    print(f"  海外研报: {stats['total_foreign_report']} 条 (新增)")
    print(f"  去重跳过: {stats['total_duplicates_skipped']} 条")
    print(f"  唯一ID:  {len(seen_ids)} 条 (含v1)")
    print(f"  错误:    {stats['errors']} 次")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
