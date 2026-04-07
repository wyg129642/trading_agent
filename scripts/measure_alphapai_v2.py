"""
AlphaPai 数据量精确测量 v2
修正: 纪要API不能用fields=["id"]，改用fields=[]返回全量字段

测量逻辑说明:
=============
1. hasMore含义验证: 同一时间窗口，变化size参数，看count是否恒定
   - 如果count恒定 → count=总匹配数, hasMore=是否还有更多未返回
   - 如果count随size变 → count=本次返回数

2. 逐日精确计量: 设定每天00:00:00~23:59:59，查count
   - 验证数据是否只保留最近N天(滚动窗口)

3. count可信度: 对某一天用大size拉取，对比实际返回条数 vs count

4. start_time过滤是否生效: 不传end_time，变化start_time看count变化
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


def query(api_name, start_time, end_time="", size=1):
    """fields=[] 返回所有字段，避免字段名不兼容报错"""
    payload = json.dumps({
        "apiName": api_name,
        "params": {"start_time": start_time, "end_time": end_time, "size": size},
        "fields": []
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
                "data": d.get("data", []),
            }
        else:
            return {"error": r.get("message", str(r))}
    except Exception as e:
        return {"error": str(e)}


print(f"测量时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ============================================================
print("\n" + "=" * 80)
print("测试1: hasMore含义 — 同一查询条件，不同size，count是否恒定")
print("  如果count恒定 → count=总匹配数, hasMore=告诉你还有没拉完的")
print("=" * 80)

# 用3月9日(有数据的日期)，不传end_time
for api_name, label in APIS:
    print(f"\n  {label} (start=3月9日, 无end_time):")
    for size in [1, 5, 50, 200]:
        r = query(api_name, "2026-03-09 00:00:00", "", size)
        if "error" in r:
            print(f"    size={size:>3d}: ERROR: {r['error']}")
        else:
            print(f"    size={size:>3d}: count={r['count']:>5}, hasMore={str(r['hasMore']):>5}, 实际返回={r['returned']}")


# ============================================================
print("\n" + "=" * 80)
print("测试2: 逐日数据量 — 每天设精确start/end，看count")
print("  关键: 这次用 fields=[] 避免字段名报错")
print("=" * 80)

today = datetime(2026, 3, 10)
for api_name, label in APIS:
    print(f"\n  {label}:")
    print(f"    {'日期':12s} | {'count':>6s} | {'hasMore':>7s}")
    print(f"    {'-'*45}")
    for days_ago in range(12, -1, -1):
        day = today - timedelta(days=days_ago)
        day_start = day.strftime("%Y-%m-%d 00:00:00")
        day_end = day.strftime("%Y-%m-%d 23:59:59")
        r = query(api_name, day_start, day_end, size=1)
        if "error" in r:
            print(f"    {day.strftime('%m-%d(%a)'):12s} | ERROR: {r['error'][:30]}")
        else:
            count = r['count']
            bar = '#' * min(count // 20, 40) if count > 0 else ''
            print(f"    {day.strftime('%m-%d(%a)'):12s} | {count:>6d} | {str(r['hasMore']):>7s} | {bar}")


# ============================================================
print("\n" + "=" * 80)
print("测试3: count可信度 — 用大size拉取, 对比实际返回 vs count")
print("  选3月10日(今天)，数据量应较小")
print("=" * 80)

for api_name, label in APIS:
    r = query(api_name, "2026-03-10 00:00:00", "2026-03-10 23:59:59", size=2000)
    if "error" in r:
        print(f"  {label}: ERROR: {r['error']}")
    else:
        diff = r['count'] - r['returned']
        status = "MATCH" if diff == 0 else f"DIFF={diff}"
        print(f"  {label}: count={r['count']:>5}, 实际返回={r['returned']:>5}, hasMore={str(r['hasMore']):>5} → {status}")


# ============================================================
print("\n" + "=" * 80)
print("测试4: start_time过滤验证 — 不传end_time, 变化start_time")
print("  如果count不变 → start_time可能不生效(API返回全量)")
print("=" * 80)

for api_name, label in APIS:
    print(f"\n  {label}:")
    for start in ["2026-02-01 00:00:00", "2026-03-01 00:00:00",
                   "2026-03-09 00:00:00", "2026-03-09 12:00:00",
                   "2026-03-10 00:00:00", "2026-03-10 12:00:00"]:
        r = query(api_name, start, "", size=1)
        if "error" in r:
            print(f"    start={start}: ERROR")
        else:
            print(f"    start={start}: count={r['count']:>6d}")


# ============================================================
print("\n" + "=" * 80)
print("测试5: 数据时间分布 — 拉取实际数据的时间字段，验证排序和时间范围")
print("=" * 80)

# 公众号文章
r = query("get_wechat_articles_yjh", "2026-03-09 00:00:00", "", size=5)
if r.get("data"):
    print(f"\n  公众号文章 (start=3月9日, 前5条):")
    for i, item in enumerate(r["data"]):
        pt = item.get("publish_time", "?")
        st = item.get("spider_time", "?")
        title = (item.get("arc_name") or "?")[:40]
        print(f"    [{i+1}] publish={pt}  spider={st}  | {title}")

# A股纪要
r = query("get_summary_roadshow_info_yjh", "2026-03-09 00:00:00", "", size=5)
if r.get("data"):
    print(f"\n  A股纪要 (start=3月9日, 前5条):")
    for i, item in enumerate(r["data"]):
        stime = item.get("stime", "?")
        src = item.get("trans_source", "?")
        title = (item.get("show_title") or "?")[:40]
        print(f"    [{i+1}] stime={stime}  [{src}]  | {title}")

# 美股纪要
r = query("get_summary_roadshow_info_us_yjh", "2026-03-09 00:00:00", "", size=5)
if r.get("data"):
    print(f"\n  美股纪要 (start=3月9日, 前5条):")
    for i, item in enumerate(r["data"]):
        stime = item.get("stime", "?")
        title = (item.get("show_title") or "?")[:50]
        print(f"    [{i+1}] stime={stime}  | {title}")

# 点评
r = query("get_comment_info_yjh", "2026-03-09 00:00:00", "", size=5)
if r.get("data"):
    print(f"\n  点评数据 (start=3月9日, 前5条):")
    for i, item in enumerate(r["data"]):
        cd = item.get("cmnt_date", "?")
        inst = item.get("inst_cname") or "未知"
        title = (item.get("title") or "?")[:40]
        print(f"    [{i+1}] date={cd}  {inst:8s}  | {title}")


# ============================================================
print("\n" + "=" * 80)
print("测试6: 数据保留窗口 — 找到最早有数据的日期")
print("  从2月1日开始逐周查，找到数据边界")
print("=" * 80)

for api_name, label in APIS:
    print(f"\n  {label}:")
    # 先按周查找大致范围
    found_boundary = False
    for weeks_ago in range(8, -1, -1):
        week_start = today - timedelta(weeks=weeks_ago)
        week_end = week_start + timedelta(days=6)
        r = query(api_name, week_start.strftime("%Y-%m-%d 00:00:00"),
                  week_end.strftime("%Y-%m-%d 23:59:59"), size=1)
        if "error" not in r:
            has_data = "有数据" if r["count"] > 0 else "无数据"
            if r["count"] > 0 and not found_boundary:
                found_boundary = True
                print(f"    {week_start.strftime('%m-%d')}~{week_end.strftime('%m-%d')}: count={r['count']:>5} ← 最早有数据的周")
            elif r["count"] > 0:
                print(f"    {week_start.strftime('%m-%d')}~{week_end.strftime('%m-%d')}: count={r['count']:>5}")
            else:
                print(f"    {week_start.strftime('%m-%d')}~{week_end.strftime('%m-%d')}: 0")


print("\n" + "=" * 80)
print("测量完成!")
print("=" * 80)
