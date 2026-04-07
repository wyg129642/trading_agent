"""
AlphaPai 数据量精确测量
测量逻辑:
1. 对每个API，用size=1查询不同时间窗口，观察返回的count字段是否随时间变化
2. 逐日查询最近10天的数据量，验证count是否反映真实筛选结果
3. 测试hasMore的含义：对比size=1和size=100时的count值是否一致
4. 用size=大数拉取单日全量数据，用实际返回条数验证count的准确性
"""
import requests
import json
from datetime import datetime, timedelta

BASE_URL = "https://api-test.rabyte.cn/alpha/open-api/v1/data-manager/query"
APP_ID = "wdWQMvEwFTKWZoFE1Qen0iIb"
HEADERS = {'app-agent': APP_ID, 'Content-Type': 'application/json'}

APIS = [
    ("get_wechat_articles_yjh", "公众号文章"),
    ("get_summary_roadshow_info_yjh", "A股纪要"),
    ("get_summary_roadshow_info_us_yjh", "美股纪要"),
    ("get_comment_info_yjh", "点评数据"),
]


def query(api_name, start_time, end_time="", size=1, fields=None):
    payload = json.dumps({
        "apiName": api_name,
        "params": {"start_time": start_time, "end_time": end_time, "size": size},
        "fields": fields or ["id"]  # 只要id字段，减少传输
    })
    try:
        resp = requests.post(BASE_URL, headers=HEADERS, data=payload, timeout=30)
        r = resp.json()
        if r.get("code") == 200000:
            d = r["data"]
            return {
                "count": d.get("count", -1),
                "hasMore": d.get("hasMore"),
                "returned": len(d.get("data", [])),
            }
        else:
            return {"error": r.get("message", str(r.get("code")))}
    except Exception as e:
        return {"error": str(e)}


# ============================================================
print("=" * 80)
print("测试1: hasMore 的含义 — 对比不同 size 参数下的 count")
print("  逻辑: 如果count代表总匹配数, 那么不同size查询应返回相同count")
print("=" * 80)

test_start = "2026-03-08 00:00:00"
test_end = "2026-03-09 00:00:00"

for api_name, label in APIS:
    results = {}
    for size in [1, 10, 50, 200]:
        r = query(api_name, test_start, test_end, size=size)
        results[size] = r
    print(f"\n  {label} (3月8日一整天):")
    for size, r in results.items():
        if "error" in r:
            print(f"    size={size:>3d}: ERROR {r['error']}")
        else:
            print(f"    size={size:>3d}: count={r['count']:>5}, hasMore={str(r['hasMore']):>5}, 实际返回={r['returned']}")


# ============================================================
print("\n" + "=" * 80)
print("测试2: 逐日数据量 — 最近10天每天各API有多少条数据")
print("  逻辑: 对每一天设定精确的 start_time 和 end_time，查看count")
print("=" * 80)

today = datetime(2026, 3, 10)
for api_name, label in APIS:
    print(f"\n  {label}:")
    print(f"    {'日期':12s} | {'count':>6s} | {'hasMore':>7s} | {'实际返回':>6s}")
    print(f"    {'-'*12}-+-{'-'*6}-+-{'-'*7}-+-{'-'*6}")
    for days_ago in range(10, -1, -1):
        day = today - timedelta(days=days_ago)
        day_start = day.strftime("%Y-%m-%d 00:00:00")
        day_end = day.strftime("%Y-%m-%d 23:59:59")
        r = query(api_name, day_start, day_end, size=1)
        if "error" in r:
            print(f"    {day.strftime('%Y-%m-%d'):12s} | ERROR: {r['error']}")
        else:
            print(f"    {day.strftime('%Y-%m-%d'):12s} | {r['count']:>6d} | {str(r['hasMore']):>7s} | {r['returned']:>6d}")


# ============================================================
print("\n" + "=" * 80)
print("测试3: 验证count准确性 — 拉取单日全量，对比count和实际条数")
print("  逻辑: 选取数据量较小的一天, 用size=500拉取, 看实际返回数是否等于count")
print("=" * 80)

# 选3月8日，分别测试各API
test_day_start = "2026-03-08 00:00:00"
test_day_end = "2026-03-08 23:59:59"

for api_name, label in APIS:
    r = query(api_name, test_day_start, test_day_end, size=500)
    if "error" in r:
        print(f"  {label}: ERROR {r['error']}")
    else:
        match = "MATCH" if r['returned'] == r['count'] else f"MISMATCH (差{r['count'] - r['returned']})"
        print(f"  {label}: count={r['count']}, 实际返回={r['returned']}, hasMore={r['hasMore']} → {match}")


# ============================================================
print("\n" + "=" * 80)
print("测试4: 不传end_time时count的含义")
print("  逻辑: 之前发现不传end_time时, 不同start_time返回相同count, 验证这一点")
print("=" * 80)

for api_name, label in APIS:
    print(f"\n  {label} (不传end_time, 只变start_time):")
    for start in ["2026-03-01 00:00:00", "2026-03-05 00:00:00", "2026-03-09 00:00:00", "2026-03-10 00:00:00"]:
        r = query(api_name, start, end_time="", size=1)
        if "error" in r:
            print(f"    start={start}: ERROR")
        else:
            print(f"    start={start}: count={r['count']:>6d}, hasMore={r['hasMore']}")


# ============================================================
print("\n" + "=" * 80)
print("测试5: start_time的排序方向 — 数据是按时间正序还是倒序返回")
print("  逻辑: 拉取3条数据，看时间字段是递增还是递减")
print("=" * 80)

# 用点评数据(有cmnt_date字段)测试
r_raw = requests.post(BASE_URL, headers=HEADERS, data=json.dumps({
    "apiName": "get_comment_info_yjh",
    "params": {"start_time": "2026-03-08 00:00:00", "end_time": "2026-03-09 00:00:00", "size": 5},
    "fields": ["cmnt_date", "title"]
}), timeout=30).json()

if r_raw.get("code") == 200000:
    items = r_raw["data"]["data"]
    print(f"  点评数据 (3月8日, 前5条):")
    for i, item in enumerate(items):
        print(f"    [{i+1}] {item.get('cmnt_date', '?'):25s} | {(item.get('title') or '?')[:50]}")

# 用纪要数据(有stime字段)测试
r_raw2 = requests.post(BASE_URL, headers=HEADERS, data=json.dumps({
    "apiName": "get_summary_roadshow_info_yjh",
    "params": {"start_time": "2026-03-08 00:00:00", "end_time": "2026-03-09 00:00:00", "size": 5},
    "fields": ["stime", "show_title", "trans_source"]
}), timeout=30).json()

if r_raw2.get("code") == 200000:
    items = r_raw2["data"]["data"]
    print(f"\n  A股纪要 (3月8日, 前5条):")
    for i, item in enumerate(items):
        print(f"    [{i+1}] {item.get('stime', '?'):25s} | [{item.get('trans_source','?')}] {(item.get('show_title') or '?')[:40]}")


print("\n" + "=" * 80)
print("测量完成!")
print("=" * 80)
