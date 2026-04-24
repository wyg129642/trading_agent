#!/usr/bin/env python3
"""
Alpha派研报批量下载器
====================
通过 recall API 的语义搜索，用多样化查询 + 时间分片 + 去重，
尽可能全面地下载 report（国内研报）和 foreign_report（海外研报）数据。

策略：
1. 将6个月拆分为1个月的时间窗口（6个窗口）
2. 每个窗口内用多个行业/主题查询覆盖不同研报
3. isCutOff=false 获取完整内容
4. 遇到 429 用指数退避重试
5. 按 ID 全局去重
6. 分类型保存为 JSONL 文件
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Set

# 把 alphapai_client.py 所在目录加入路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                "alphapai-skill", "alphapai-research", "scripts"))
from alphapai_client import AlphaPaiClient, load_config

# ============================================================
# 配置
# ============================================================

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "downloaded_reports")
REPORT_TYPES = ["report", "foreign_report"]

# 时间范围：最近6个月
END_DATE = datetime(2026, 4, 2)
START_DATE = datetime(2025, 10, 2)

# 每个月的时间窗口
MONTH_WINDOWS = []
cur = START_DATE
while cur < END_DATE:
    next_month = cur + timedelta(days=30)
    if next_month > END_DATE:
        next_month = END_DATE
    MONTH_WINDOWS.append((cur.strftime("%Y-%m-%d"), next_month.strftime("%Y-%m-%d")))
    cur = next_month

# 限速：请求间隔(秒)，429 后退避
BASE_DELAY = 2.0        # 正常请求间隔
MAX_RETRIES = 5          # 最大重试次数
BACKOFF_FACTOR = 2.0     # 退避倍数

# ============================================================
# 查询列表 — 尽可能覆盖不同行业和主题
# ============================================================

QUERIES = [
    # === A股热门行业 ===
    "人工智能 AI 半导体",
    "新能源汽车 电动车 销量",
    "光伏 太阳能 组件",
    "锂电池 储能 电池材料",
    "白酒 消费 食品饮料",
    "医药 创新药 生物科技",
    "银行 金融 保险",
    "房地产 地产 物业",
    "军工 国防 航空航天",
    "电子 芯片 集成电路",
    "通信 5G 物联网",
    "计算机 软件 云计算",
    "钢铁 有色金属 铜铝",
    "化工 新材料 精细化工",
    "机械 工程机械 自动化",
    "汽车零部件 智能驾驶",
    "传媒 游戏 影视",
    "农业 养殖 种业",
    "电力 公用事业 核电",
    "交通运输 航运 快递物流",
    # === 热门个股 ===
    "贵州茅台 经营业绩",
    "宁德时代 电池技术",
    "比亚迪 新能源汽车",
    "中芯国际 半导体制造",
    "腾讯 互联网 社交",
    "阿里巴巴 电商 云",
    "华为 产业链",
    "中国平安 保险 银行",
    "招商银行 零售银行",
    "隆基绿能 光伏硅片",
    "迈瑞医疗 医疗器械",
    "海康威视 安防 AI",
    "恒瑞医药 创新药研发",
    "美的集团 家电 智能制造",
    "三一重工 工程机械",
    "中国中免 免税 消费",
    "药明康德 CXO 医药外包",
    "紫光国微 芯片设计",
    "中微公司 半导体设备",
    "北方华创 半导体设备",
    # === 宏观 & 策略 ===
    "宏观经济 GDP 政策",
    "A股策略 市场展望",
    "货币政策 降息 利率",
    "财政政策 国债 赤字",
    "中美关系 贸易",
    "港股 恒生指数",
    "美股 纳斯达克 标普",
    "汇率 人民币 美元",
    "大宗商品 原油 黄金",
    "ESG 绿色金融 碳中和",
    # === 海外 / 外资关注 ===
    "NVIDIA GPU AI chips",
    "Apple iPhone supply chain",
    "Tesla EV sales",
    "semiconductor TSMC foundry",
    "China economy outlook",
    "emerging markets Asia",
    "electric vehicle battery",
    "renewable energy solar wind",
    "luxury goods LVMH consumption",
    "global macro interest rates",
    # === 细分行业 ===
    "机器人 人形机器人 减速器",
    "算力 数据中心 服务器",
    "卫星互联网 低轨卫星",
    "量子计算 量子通信",
    "AIGC 大模型 GPT",
    "工业互联网 智能制造",
    "固态电池 钠离子电池",
    "CXO 生物医药外包",
    "中药 中成药 医保",
    "养老 康复 医疗服务",
    "消费电子 VR AR MR",
    "跨境电商 出海",
    "预制菜 餐饮供应链",
    "宠物经济 宠物食品",
    "煤炭 能源 天然气",
]

# ============================================================
# 工具函数
# ============================================================

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def save_jsonl(filepath: str, records: List[Dict]):
    """追加写入 JSONL"""
    with open(filepath, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def call_with_retry(client: AlphaPaiClient, query: str, recall_type: List[str],
                    start: str, end: str) -> Dict:
    """带重试和退避的 recall 调用"""
    delay = BASE_DELAY
    for attempt in range(MAX_RETRIES + 1):
        try:
            result = client.recall_data(
                query=query,
                is_cut_off=False,      # 完整内容，不截断
                recall_type=recall_type,
                start_time=start,
                end_time=end,
            )
            return result
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "rate" in err_str.lower():
                wait = delay * (BACKOFF_FACTOR ** attempt)
                print(f"    ⚠ 429 限流，等待 {wait:.1f}s 后重试 (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
            elif "500" in err_str or "502" in err_str or "503" in err_str:
                wait = delay * (BACKOFF_FACTOR ** attempt)
                print(f"    ⚠ 服务端错误 ({err_str[:60]}), 等待 {wait:.1f}s 重试")
                time.sleep(wait)
            else:
                print(f"    ✗ 请求失败: {err_str[:100]}")
                if attempt < MAX_RETRIES:
                    time.sleep(delay)
                else:
                    raise
    return {}


# ============================================================
# 主逻辑
# ============================================================

def main():
    config = load_config()
    if not config:
        print("ERROR: 未找到 API 配置，请先运行 config --set-key")
        sys.exit(1)

    client = AlphaPaiClient(config)
    ensure_dir(OUTPUT_DIR)

    # 输出文件
    report_file = os.path.join(OUTPUT_DIR, "report_国内研报.jsonl")
    foreign_file = os.path.join(OUTPUT_DIR, "foreign_report_海外研报.jsonl")
    log_file = os.path.join(OUTPUT_DIR, "download_log.json")

    # 全局去重集合
    seen_ids: Set[str] = set()

    # 如果之前已有数据，加载已有 ID 用于去重
    for fpath in [report_file, foreign_file]:
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            rec = json.loads(line)
                            seen_ids.add(rec.get("id", ""))
                        except json.JSONDecodeError:
                            pass
    if seen_ids:
        print(f"已加载 {len(seen_ids)} 条历史记录用于去重")

    stats = {
        "total_requests": 0,
        "total_report": 0,
        "total_foreign_report": 0,
        "total_duplicates_skipped": 0,
        "errors": 0,
        "time_windows": len(MONTH_WINDOWS),
        "queries_per_window": len(QUERIES),
    }

    total_tasks = len(MONTH_WINDOWS) * len(QUERIES)
    task_idx = 0

    print(f"=" * 60)
    print(f"Alpha派研报下载器")
    print(f"时间范围: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"时间窗口: {len(MONTH_WINDOWS)} 个")
    print(f"查询数量: {len(QUERIES)} 个")
    print(f"总请求数: {total_tasks}")
    print(f"数据类型: report(国内研报), foreign_report(海外研报)")
    print(f"输出目录: {OUTPUT_DIR}")
    print(f"=" * 60)

    start_time = time.time()

    for win_idx, (win_start, win_end) in enumerate(MONTH_WINDOWS):
        print(f"\n{'─' * 50}")
        print(f"时间窗口 [{win_idx+1}/{len(MONTH_WINDOWS)}]: {win_start} ~ {win_end}")
        print(f"{'─' * 50}")

        for q_idx, query in enumerate(QUERIES):
            task_idx += 1
            progress = f"[{task_idx}/{total_tasks}]"

            print(f"  {progress} 查询: \"{query[:30]}...\" ({win_start}~{win_end})", end="")
            sys.stdout.flush()

            try:
                result = call_with_retry(client, query, REPORT_TYPES, win_start, win_end)
                stats["total_requests"] += 1

                data_list = result.get("data", [])
                new_report = []
                new_foreign = []
                dup_count = 0

                for item in data_list:
                    item_id = item.get("id", "")
                    if item_id in seen_ids:
                        dup_count += 1
                        continue
                    seen_ids.add(item_id)

                    # 添加元数据
                    item["_query"] = query
                    item["_time_window"] = f"{win_start}~{win_end}"
                    item["_download_time"] = datetime.now().isoformat()

                    item_type = item.get("type", "")
                    if item_type == "report":
                        new_report.append(item)
                    elif item_type == "foreign_report":
                        new_foreign.append(item)

                # 保存
                if new_report:
                    save_jsonl(report_file, new_report)
                    stats["total_report"] += len(new_report)
                if new_foreign:
                    save_jsonl(foreign_file, new_foreign)
                    stats["total_foreign_report"] += len(new_foreign)

                stats["total_duplicates_skipped"] += dup_count

                print(f"  → 国内:{len(new_report)} 海外:{len(new_foreign)} 重复:{dup_count} 总计:{len(data_list)}")

            except Exception as e:
                stats["errors"] += 1
                print(f"  ✗ 错误: {str(e)[:80]}")
                traceback.print_exc()

            # 限速：每次请求后等待
            time.sleep(BASE_DELAY)

    elapsed = time.time() - start_time
    stats["elapsed_seconds"] = round(elapsed, 1)
    stats["unique_ids_total"] = len(seen_ids)

    # 保存日志
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"下载完成！")
    print(f"  耗时: {elapsed/60:.1f} 分钟")
    print(f"  国内研报: {stats['total_report']} 条")
    print(f"  海外研报: {stats['total_foreign_report']} 条")
    print(f"  去重跳过: {stats['total_duplicates_skipped']} 条")
    print(f"  错误: {stats['errors']} 次")
    print(f"  文件: {report_file}")
    print(f"        {foreign_file}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
