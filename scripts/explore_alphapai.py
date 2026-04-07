"""
AlphaPai API Explorer - 探索各接口的数据结构和内容
"""
import requests
import json
import sys
from datetime import datetime, timedelta

BASE_URL = "https://api-test.rabyte.cn/alpha/open-api/v1/data-manager/query"
APP_ID = "wdWQMvEwFTKWZoFE1Qen0iIb"

HEADERS = {
    'app-agent': APP_ID,
    'Content-Type': 'application/json'
}


def query_api(api_name, start_time="2026-03-01 00:00:00", end_time="", size=10, fields=None):
    """通用查询函数"""
    payload = json.dumps({
        "apiName": api_name,
        "params": {
            "start_time": start_time,
            "end_time": end_time,
            "size": size
        },
        "fields": fields or []
    })
    try:
        resp = requests.request("POST", BASE_URL, headers=HEADERS, data=payload, timeout=30)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


def explore_wechat_articles():
    """1. 探索公众号文章接口"""
    print("=" * 80)
    print("1. get_wechat_articles_yjh — 公众号文章")
    print("=" * 80)

    result = query_api("get_wechat_articles_yjh", size=10)
    if result.get("code") == 200000:
        data = result["data"]
        print(f"总数: {data.get('count', 'N/A')}, hasMore: {data.get('hasMore')}")
        print(f"返回条数: {len(data.get('data', []))}")
        for i, item in enumerate(data.get("data", [])[:10]):
            print(f"\n--- 文章 {i+1} ---")
            print(f"  标题(arc_name): {item.get('arc_name')}")
            print(f"  作者(author): {item.get('author')}")
            print(f"  发布时间(publish_time): {item.get('publish_time')}")
            print(f"  抓取时间(spider_time): {item.get('spider_time')}")
            print(f"  字数(text_count): {item.get('text_count')}")
            print(f"  阅读时长(read_duration): {item.get('read_duration')}")
            print(f"  是否原创(is_original): {item.get('is_original')}")
            print(f"  微信URL: {item.get('url', '')[:80]}...")
            print(f"  content_html路径: {item.get('content_html')}")
            print(f"  研究类型(research_type): {item.get('research_type')}")
            print(f"  内容标签(content_label): {item.get('content_label')}")
            # 打印所有字段名
            if i == 0:
                print(f"\n  [所有字段]: {sorted(item.keys())}")
    else:
        print(f"错误: {json.dumps(result, ensure_ascii=False, indent=2)}")
    return result


def explore_roadshow_cn():
    """2. 探索A股公开纪要接口"""
    print("\n" + "=" * 80)
    print("2. get_summary_roadshow_info_yjh — A股公开纪要")
    print("=" * 80)

    result = query_api("get_summary_roadshow_info_yjh", size=10)
    if result.get("code") == 200000:
        data = result["data"]
        print(f"总数: {data.get('count', 'N/A')}, hasMore: {data.get('hasMore')}")
        print(f"返回条数: {len(data.get('data', []))}")
        for i, item in enumerate(data.get("data", [])[:10]):
            print(f"\n--- 纪要 {i+1} ---")
            print(f"  标题(show_title): {item.get('show_title')}")
            print(f"  翻译标题(trans_title): {item.get('trans_title')}")
            print(f"  公司(company): {item.get('company')}")
            print(f"  嘉宾(guest): {item.get('guest')}")
            print(f"  路演标题(roadshow_title): {item.get('roadshow_title')}")
            print(f"  时间(stime): {item.get('stime')}")
            print(f"  字数(word_count): {item.get('word_count')}")
            print(f"  预计阅读(est_reading_time): {item.get('est_reading_time')}")
            print(f"  行业(ind_json): {item.get('ind_json')}")
            print(f"  来源(trans_source): {item.get('trans_source')}")
            print(f"  记录者(recorder): {item.get('recorder')}")
            print(f"  内容路径(content): {item.get('content')}")
            print(f"  是否会议(is_conference): {item.get('is_conference')}")
            print(f"  是否调研(is_investigation): {item.get('is_investigation')}")
            print(f"  是否买方(is_buyside): {item.get('is_buyside')}")
            print(f"  是否高管(is_executive): {item.get('is_executive')}")
            if i == 0:
                print(f"\n  [所有字段]: {sorted(item.keys())}")
    else:
        print(f"错误: {json.dumps(result, ensure_ascii=False, indent=2)}")
    return result


def explore_roadshow_us():
    """3. 探索美股公开纪要接口"""
    print("\n" + "=" * 80)
    print("3. get_summary_roadshow_info_us_yjh — 美股公开纪要")
    print("=" * 80)

    result = query_api("get_summary_roadshow_info_us_yjh", size=10)
    if result.get("code") == 200000:
        data = result["data"]
        print(f"总数: {data.get('count', 'N/A')}, hasMore: {data.get('hasMore')}")
        print(f"返回条数: {len(data.get('data', []))}")
        for i, item in enumerate(data.get("data", [])[:10]):
            print(f"\n--- 美股纪要 {i+1} ---")
            print(f"  标题(show_title): {item.get('show_title')}")
            print(f"  翻译标题(trans_title): {item.get('trans_title')}")
            print(f"  公司(company): {item.get('company')}")
            print(f"  嘉宾(guest): {item.get('guest')}")
            print(f"  来源(rec_source): {item.get('rec_source')}")
            print(f"  季度(quarter_year): {item.get('quarter_year')}")
            print(f"  时间(stime): {item.get('stime')}")
            print(f"  字数(word_count): {item.get('word_count')}")
            print(f"  行业(ind_json): {item.get('ind_json')}")
            print(f"  内容路径(content): {item.get('content')}")
            print(f"  文件类型(files_type): {item.get('files_type')}")
            print(f"  AI辅助(ai_auxiliary_json_s3): {item.get('ai_auxiliary_json_s3')}")
            if i == 0:
                print(f"\n  [所有字段]: {sorted(item.keys())}")
    else:
        print(f"错误: {json.dumps(result, ensure_ascii=False, indent=2)}")
    return result


def explore_comments():
    """4. 探索点评数据接口"""
    print("\n" + "=" * 80)
    print("4. get_comment_info_yjh — 点评数据")
    print("=" * 80)

    result = query_api("get_comment_info_yjh", size=10)
    if result.get("code") == 200000:
        data = result["data"]
        print(f"总数: {data.get('count', 'N/A')}, hasMore: {data.get('hasMore')}")
        print(f"返回条数: {len(data.get('data', []))}")
        for i, item in enumerate(data.get("data", [])[:10]):
            print(f"\n--- 点评 {i+1} ---")
            print(f"  标题(title): {item.get('title')}")
            print(f"  分析师(psn_name): {item.get('psn_name')}")
            print(f"  团队(team_cname): {item.get('team_cname')}")
            print(f"  机构(inst_cname): {item.get('inst_cname')}")
            print(f"  点评日期(cmnt_date): {item.get('cmnt_date')}")
            print(f"  是否新财富(is_new_fortune): {item.get('is_new_fortune')}")
            print(f"  来源类型(src_type): {item.get('src_type')}")
            print(f"  群组(group_name): {item.get('group_name')}")
            print(f"  群组ID(group_id): {item.get('group_id')}")
            content = item.get('content', '') or ''
            print(f"  内容前200字: {content[:200]}...")
            if i == 0:
                print(f"\n  [所有字段]: {sorted(item.keys())}")
    else:
        print(f"错误: {json.dumps(result, ensure_ascii=False, indent=2)}")
    return result


def explore_data_volume():
    """5. 探索各接口的数据量和时间范围"""
    print("\n" + "=" * 80)
    print("5. 数据量和时间范围分析")
    print("=" * 80)

    apis = [
        ("get_wechat_articles_yjh", "公众号文章"),
        ("get_summary_roadshow_info_yjh", "A股公开纪要"),
        ("get_summary_roadshow_info_us_yjh", "美股公开纪要"),
        ("get_comment_info_yjh", "点评数据"),
    ]

    # 查最近一周的数据量
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
    today = datetime.now().strftime("%Y-%m-%d 23:59:59")

    for api_name, label in apis:
        # 全量
        r_all = query_api(api_name, start_time="2024-01-01 00:00:00", size=1)
        # 最近一周
        r_week = query_api(api_name, start_time=week_ago, end_time=today, size=1)
        # 最近一天
        day_ago = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
        r_day = query_api(api_name, start_time=day_ago, end_time=today, size=1)

        total = r_all.get("data", {}).get("count", "?") if r_all.get("code") == 200000 else "ERR"
        weekly = r_week.get("data", {}).get("count", "?") if r_week.get("code") == 200000 else "ERR"
        daily = r_day.get("data", {}).get("count", "?") if r_day.get("code") == 200000 else "ERR"

        print(f"  {label:12s} | 总量: {str(total):>6s} | 近7天: {str(weekly):>5s} | 近1天: {str(daily):>4s}")


def test_file_download():
    """6. 测试文件下载接口"""
    print("\n" + "=" * 80)
    print("6. 测试文件下载接口")
    print("=" * 80)

    # 先从纪要接口拿一个content路径
    result = query_api("get_summary_roadshow_info_yjh", size=1)
    if result.get("code") == 200000:
        items = result["data"].get("data", [])
        if items:
            content_path = items[0].get("content", "")
            print(f"  纪要content路径: {content_path}")

            if content_path:
                download_url = "https://api-test.rabyte.cn/alpha/open-api/v1/file/download"
                payload = json.dumps({
                    "type": "2",
                    "filePath": content_path
                })
                try:
                    resp = requests.post(download_url, headers=HEADERS, data=payload, timeout=30)
                    print(f"  下载状态码: {resp.status_code}")
                    print(f"  Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
                    print(f"  内容大小: {len(resp.content)} bytes")
                    # 尝试解析
                    if 'json' in resp.headers.get('Content-Type', ''):
                        content_data = resp.json()
                        print(f"  JSON内容预览: {json.dumps(content_data, ensure_ascii=False)[:500]}...")
                    else:
                        print(f"  文本内容预览: {resp.text[:500]}...")
                except Exception as e:
                    print(f"  下载失败: {e}")


def explore_wechat_content_detail():
    """7. 探索公众号文章的content_html内容"""
    print("\n" + "=" * 80)
    print("7. 探索公众号文章内容详情")
    print("=" * 80)

    result = query_api("get_wechat_articles_yjh", size=1)
    if result.get("code") == 200000:
        items = result["data"].get("data", [])
        if items:
            item = items[0]
            content_html = item.get("content_html", "")
            print(f"  content_html路径: {content_html}")
            if content_html:
                download_url = "https://api-test.rabyte.cn/alpha/open-api/v1/file/download"
                payload = json.dumps({
                    "type": "2",
                    "filePath": content_html
                })
                try:
                    resp = requests.post(download_url, headers=HEADERS, data=payload, timeout=30)
                    print(f"  下载状态码: {resp.status_code}")
                    print(f"  内容大小: {len(resp.content)} bytes")
                    print(f"  内容预览: {resp.text[:800]}...")
                except Exception as e:
                    print(f"  下载失败: {e}")


if __name__ == "__main__":
    print(f"AlphaPai API Explorer - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base URL: {BASE_URL}")
    print(f"App ID: {APP_ID[:8]}...")
    print()

    r1 = explore_wechat_articles()
    r2 = explore_roadshow_cn()
    r3 = explore_roadshow_us()
    r4 = explore_comments()
    explore_data_volume()
    test_file_download()
    explore_wechat_content_detail()

    print("\n" + "=" * 80)
    print("探索完成!")
