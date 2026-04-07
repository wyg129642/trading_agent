"""
AlphaPai API Deep Exploration - 深入探索AI纪要内容、过滤参数、分页等
"""
import requests
import json
from datetime import datetime, timedelta

BASE_URL = "https://api-test.rabyte.cn/alpha/open-api/v1/data-manager/query"
DOWNLOAD_URL = "https://api-test.rabyte.cn/alpha/open-api/v1/file/download"
APP_ID = "wdWQMvEwFTKWZoFE1Qen0iIb"
HEADERS = {'app-agent': APP_ID, 'Content-Type': 'application/json'}


def query_api(api_name, params=None, fields=None):
    if params is None:
        params = {"start_time": "2026-03-01 00:00:00", "end_time": "", "size": 3}
    payload = json.dumps({"apiName": api_name, "params": params, "fields": fields or []})
    resp = requests.post(BASE_URL, headers=HEADERS, data=payload, timeout=30)
    return resp.json()


def download_file(file_path, file_type="2"):
    payload = json.dumps({"type": file_type, "filePath": file_path})
    resp = requests.post(DOWNLOAD_URL, headers=HEADERS, data=payload, timeout=30)
    return resp


# ====== 1. 探索AI纪要 vs MT纪要的区别 ======
print("=" * 80)
print("1. AI纪要 vs MT(人工)纪要 对比")
print("=" * 80)

result = query_api("get_summary_roadshow_info_yjh",
                    params={"start_time": "2026-03-08 00:00:00", "end_time": "", "size": 10})
if result.get("code") == 200000:
    items = result["data"]["data"]
    for item in items[:10]:
        source = item.get("trans_source", "?")
        title = item.get("show_title", "")[:50]
        wc = item.get("word_count", 0)
        content_path = item.get("content", "")
        print(f"  [{source:2s}] {title:50s} | {wc:>6}字 | {content_path[:60]}")

# ====== 2. 下载AI纪要HTML内容 ======
print("\n" + "=" * 80)
print("2. AI纪要HTML内容示例")
print("=" * 80)

# 找一个AI纪要
for item in items:
    if item.get("trans_source") == "AI":
        ai_content_path = item.get("content", "")
        print(f"  路径: {ai_content_path}")
        resp = download_file(ai_content_path)
        print(f"  大小: {len(resp.content)} bytes")
        text = resp.text
        print(f"  内容前1000字:\n{text[:1000]}")
        break

# ====== 3. 下载MT纪要JSON内容 ======
print("\n" + "=" * 80)
print("3. MT纪要JSON内容结构")
print("=" * 80)

for item in items:
    if item.get("trans_source") == "MT" and item.get("content", "").endswith(".json"):
        mt_content_path = item.get("content", "")
        print(f"  路径: {mt_content_path}")
        resp = download_file(mt_content_path)
        try:
            data = resp.json()
            print(f"  JSON数组长度: {len(data)}")
            print(f"  单条结构示例: {json.dumps(data[0], ensure_ascii=False)}")
            print(f"  字段说明: wp_dl=?, role=角色, bg/ed=时间戳, content=内容, key_word=关键词")
            # 拼接前500字看看
            full_text = "".join([seg.get("content", "") for seg in data[:20]])
            print(f"  前20段拼接:\n  {full_text[:500]}")
        except:
            print(f"  非JSON: {resp.text[:300]}")
        break

# ====== 4. 美股纪要AI辅助JSON ======
print("\n" + "=" * 80)
print("4. 美股纪要 AI辅助JSON (ai_auxiliary_json_s3)")
print("=" * 80)

result_us = query_api("get_summary_roadshow_info_us_yjh",
                       params={"start_time": "2026-03-01 00:00:00", "end_time": "", "size": 20})
if result_us.get("code") == 200000:
    for item in result_us["data"]["data"]:
        ai_json = item.get("ai_auxiliary_json_s3")
        if ai_json:
            print(f"  标题: {item.get('show_title', '')[:60]}")
            print(f"  AI JSON路径: {ai_json}")
            resp = download_file(ai_json)
            print(f"  大小: {len(resp.content)} bytes")
            try:
                ai_data = resp.json()
                print(f"  JSON结构keys: {list(ai_data.keys()) if isinstance(ai_data, dict) else type(ai_data)}")
                print(f"  内容预览: {json.dumps(ai_data, ensure_ascii=False)[:800]}")
            except:
                print(f"  文本: {resp.text[:500]}")
            break

# ====== 5. 探索公众号文章的行业标签和分类 ======
print("\n" + "=" * 80)
print("5. 公众号文章 - 数据特征统计")
print("=" * 80)

result_wx = query_api("get_wechat_articles_yjh",
                       params={"start_time": "2026-03-08 00:00:00", "end_time": "", "size": 50})
if result_wx.get("code") == 200000:
    items_wx = result_wx["data"]["data"]
    authors = {}
    research_types = {}
    has_ind = 0
    has_label = 0
    total_words = 0
    for item in items_wx:
        author = item.get("author", "") or "未知"
        authors[author] = authors.get(author, 0) + 1
        rt = item.get("research_type", "N/A")
        research_types[rt] = research_types.get(rt, 0) + 1
        if item.get("ind_json"):
            has_ind += 1
        if item.get("content_label"):
            has_label += 1
        total_words += item.get("text_count", 0) or 0

    print(f"  样本数: {len(items_wx)}")
    print(f"  平均字数: {total_words // max(len(items_wx), 1)}")
    print(f"  有行业标签: {has_ind}/{len(items_wx)}")
    print(f"  有内容标签: {has_label}/{len(items_wx)}")
    print(f"  研究类型分布: {research_types}")
    print(f"  Top作者:")
    for a, c in sorted(authors.items(), key=lambda x: -x[1])[:10]:
        print(f"    {a}: {c}篇")

# ====== 6. 点评数据 - 数据特征 ======
print("\n" + "=" * 80)
print("6. 点评数据 - 数据特征统计")
print("=" * 80)

result_cm = query_api("get_comment_info_yjh",
                       params={"start_time": "2026-03-08 00:00:00", "end_time": "", "size": 50})
if result_cm.get("code") == 200000:
    items_cm = result_cm["data"]["data"]
    insts = {}
    src_types = {}
    fortune_count = 0
    avg_len = 0
    for item in items_cm:
        inst = item.get("inst_cname", "") or "未知"
        insts[inst] = insts.get(inst, 0) + 1
        st = item.get("src_type", "?")
        src_types[st] = src_types.get(st, 0) + 1
        if item.get("is_new_fortune"):
            fortune_count += 1
        avg_len += len(item.get("content", "") or "")

    print(f"  样本数: {len(items_cm)}")
    print(f"  平均内容长度: {avg_len // max(len(items_cm), 1)} 字符")
    print(f"  新财富分析师占比: {fortune_count}/{len(items_cm)}")
    print(f"  来源类型: {src_types}")
    print(f"  Top机构:")
    for inst, c in sorted(insts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {inst}: {c}条")

# ====== 7. 测试分页 - 用page/offset参数 ======
print("\n" + "=" * 80)
print("7. 分页测试")
print("=" * 80)

# 尝试 offset 参数
for offset in [0, 3]:
    r = query_api("get_comment_info_yjh",
                   params={"start_time": "2026-03-08 00:00:00", "end_time": "", "size": 2, "offset": offset})
    if r.get("code") == 200000:
        items = r["data"]["data"]
        print(f"  offset={offset}: 返回{len(items)}条, 第一条ID={items[0].get('id') if items else 'N/A'}, 标题={items[0].get('title', '')[:40] if items else 'N/A'}")

# 尝试 page 参数
for page in [1, 2]:
    r = query_api("get_comment_info_yjh",
                   params={"start_time": "2026-03-08 00:00:00", "end_time": "", "size": 2, "page": page})
    if r.get("code") == 200000:
        items = r["data"]["data"]
        print(f"  page={page}: 返回{len(items)}条, 第一条ID={items[0].get('id') if items else 'N/A'}, 标题={items[0].get('title', '')[:40] if items else 'N/A'}")

# ====== 8. 测试fields过滤 ======
print("\n" + "=" * 80)
print("8. fields字段过滤测试")
print("=" * 80)

r = query_api("get_comment_info_yjh",
               params={"start_time": "2026-03-08 00:00:00", "end_time": "", "size": 2},
               fields=["title", "psn_name", "inst_cname", "cmnt_date", "content"])
if r.get("code") == 200000:
    items = r["data"]["data"]
    print(f"  指定fields后返回字段: {sorted(items[0].keys()) if items else 'N/A'}")
    print(f"  示例: {json.dumps(items[0], ensure_ascii=False)[:300]}" if items else "")

# 空fields[]返回所有字段
r2 = query_api("get_comment_info_yjh",
                params={"start_time": "2026-03-08 00:00:00", "end_time": "", "size": 1},
                fields=[])
if r2.get("code") == 200000:
    items2 = r2["data"]["data"]
    print(f"  空fields返回字段数: {len(items2[0].keys()) if items2 else 'N/A'}")

# ====== 9. A股纪要行业分布 ======
print("\n" + "=" * 80)
print("9. A股纪要 - 行业与公司分布")
print("=" * 80)

result_rs = query_api("get_summary_roadshow_info_yjh",
                       params={"start_time": "2026-03-01 00:00:00", "end_time": "", "size": 100})
if result_rs.get("code") == 200000:
    items_rs = result_rs["data"]["data"]
    industries = {}
    companies = {}
    sources = {}
    for item in items_rs:
        ind = item.get("ind_json")
        if ind:
            try:
                ind_list = json.loads(ind) if isinstance(ind, str) else ind
                for i in ind_list:
                    name = i.get("name", "?")
                    industries[name] = industries.get(name, 0) + 1
            except:
                pass
        comp = item.get("company", "") or "未知"
        companies[comp] = companies.get(comp, 0) + 1
        src = item.get("trans_source", "?")
        sources[src] = sources.get(src, 0) + 1

    print(f"  样本数: {len(items_rs)}")
    print(f"  来源分布: {sources}")
    print(f"  Top行业:")
    for ind, c in sorted(industries.items(), key=lambda x: -x[1])[:15]:
        print(f"    {ind}: {c}")
    print(f"  Top券商:")
    for comp, c in sorted(companies.items(), key=lambda x: -x[1])[:10]:
        print(f"    {comp}: {c}")

print("\n" + "=" * 80)
print("深度探索完成!")
