#!/usr/bin/env python3
"""
brm.comein.cn AI纪要爬虫 (MongoDB 存储)

使用方法:
  1. 浏览器登录 brm.comein.cn
  2. F12 → Application → Local Storage → https://brm.comein.cn
  3. 找到 JM_AUTH_INFO, 双击 Value 复制整个值 (base64)
  4. 粘贴到下方 JM_AUTH_INFO 变量 (或通过 --auth / 环境变量 JM_AUTH)
  5. 运行:
       python scraper.py --max 200              # 爬 200 条入库
       python scraper.py                        # 全量爬取
       python scraper.py --watch --interval 300 # 实时模式, 每5分钟拉一次
       python scraper.py --force                # 强制重爬
       python scraper.py --mongo-uri mongodb://user:pass@host:27017

数据存储:
  - MongoDB (默认 mongodb://localhost:27017, db=jinmen)
  - Collection `meetings`: 每条纪要一个 document, _id=roadshowId
  - Collection `account`: 账户级接口数据
"""

import requests
import json
import base64
import hashlib
import time
import sys
import os
import re
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from tqdm import tqdm

# 共享反爬模块 (crawl/antibot.py)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ticker_tag import stamp as _stamp_ticker  # noqa: E402
from antibot import (  # noqa: E402
    AdaptiveThrottle, DailyCap, SessionDead,
    parse_retry_after, is_auth_dead,
    add_antibot_args, throttle_from_args, cap_from_args,
    AccountBudget, SoftCooldown, detect_soft_warning,
    headers_for_platform, log_config_stamp, budget_from_args,
    account_id_for_jinmen, warmup_session,
)

# 模块级 throttle, main() 用 CLI 覆盖. jinmen 之前是 0.3s 硬节流, 改成 3s 基线
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=3.5, jitter=2.0,
                                                burst_size=25,
                                                platform="jinmen")
# 默认值 2026-04-28 从 (3.0, 2.0, 40) 收紧到 (3.5, 2.0, 25) — 历史封控事故后
# 的新基线, 跟 AceCamp 的 (4.0, 2.5, 20) 一同变成"封控过的两个平台"档位.
# 实时档 CLI 覆盖在 crawler_manager.SPECS["jinmen"] (2.5/1.5/30, interval 120s),
# 这里只是非 CLI 路径 (如手动 backfill 不传 antibot 参数时) 的保护性兜底.
_BUDGET: AccountBudget = AccountBudget("jinmen", "default", 0)
_PLATFORM = "jinmen"

# Beijing timezone — all Jinmen platform timestamps (releaseTime epoch ms)
# are wall-clock Beijing. We render `release_time` strings in that TZ so the
# UI shows the same time users see on brm.comein.cn, while `release_time_ms`
# stays as a raw UTC epoch for sorting / cross-platform joins.
_BJ_TZ = timezone(timedelta(hours=8))


def _ms_to_bj_str(ms: int | float) -> str:
    """Convert epoch ms → 'YYYY-MM-DD HH:MM' in Asia/Shanghai (TZ-aware)."""
    return datetime.fromtimestamp(int(ms) / 1000, tz=_BJ_TZ).strftime(
        "%Y-%m-%d %H:%M",
    )

# ==================== 请配置以下内容 ====================

# 从浏览器 localStorage 复制 JM_AUTH_INFO 的值（base64编码字符串）
# 步骤: F12 → Application → Local Storage → https://brm.comein.cn → JM_AUTH_INFO
JM_AUTH_INFO = "eyJ2YWx1ZSI6eyJiZXRhVXNlciI6ZmFsc2UsIm9yZ1R5cGVDb2RlIjoyLCJ1bmFtZSI6IjEzNCoqKjEzMTEiLCJqaWQiOiI0MjEwODM4QGF5MTMxMDI2MjA0MzM5ei93ZWIiLCJ1c2VyTG9naW5JZGVudGl0eSI6eyJhcmVhQ29kZSI6Iis4NiIsImRlcGFydG1lbnRJZCI6MjU2OCwibG9nbyI6Imh0dHBzOi8vcmVzb3VyY2UuY29tZWluLmNuL2NvbWVpbi1maWxlcy9pbWcvZGVmYXVsdC9sb2dvQDJ4LnBuZyIsIm5hbWUiOiLnjovpkrDliJoiLCJvcmdOYW1lIjoi5LiK5rW35aea5rO+5rKz56eB5Yuf5Z+66YeR566h55CG5pyJ6ZmQ5YWs5Y+4Iiwib3V0c2lkZU9yZ0lkIjoiMjUwODkiLCJwaG9uZU51bWJlciI6IjEzNDExNjgxMzExIiwicGljVXJsIjoiaHR0cHM6Ly9pbWFnZS5jb21laW4uY24vd2ViL2JybS9pbWFnZS9kZWZhdWx0X29yZ19sb2dvLnBuZyIsInNob3J0T3JnTmFtZSI6IuWnmuazvuays+WfuumHkSIsInN0YXR1cyI6MX0sImF2YXRhcnVybCI6Imh0dHBzOi8vaW1hZ2UuY29tZWluLmNuL2NvbWVpbi1maWxlcy9pbWcvZGVmYXVsdC9hdmF0YXIuanBnIiwicGhvbmVudW1iZXIiOiIxMzQxMTY4MTMxMSIsImNvbXBsZXRlZCI6MSwib3JnYW5pemF0aW9uSWQiOjI1MDg5LCJ1aWQiOiI0MjEwODM4IiwibG9naW50eXBlIjoiMSIsIndlYnRva2VuIjoiVE9LRU5fMWRlYWRkNTg0ZTk2NDhkOTUzNjIyMTcxZjE5NGQyZWVmN2QzNmE1MWEifSwiZXhwaXJlIjpudWxsLCJpc0NsZWFyIjp0cnVlfQ=="

# ==================== 以下无需修改 ====================

API_BASE = "https://server.comein.cn/comein"

# 主要业务端点
LIST_API = f"{API_BASE}/json_roadshow-list_summary"
SUMMARY_INFO_API = f"{API_BASE}/json_summary_summary-info"
OVERVIEW_API = f"{API_BASE}/json_summary_query-summary-index-list"
CONTENT_LIST_API = f"{API_BASE}/json_summary_summary-content-list"
CHAPTER_SUMMARY_API = f"{API_BASE}/json_summary_list-ai-chapter-summary"
POINTS_API = f"{API_BASE}/json_summary_query-summary-points"

# 账户级接口 (只拉一次)
ACCOUNT_ENDPOINTS = [
    ("agent-group-list",        "json_ai-chat_agent-group-list",              "json"),
    ("thought-list",            "json_ai-chat-thought_list",                  "json"),
    ("thought-switch-status",   "json_ai-chat-thought_global-switch-status",  "json"),
    ("wallet",                  "json_wallet_mywallet",                       "json"),
    ("membership-cards",        "json_membership_identity-cards",             "json"),
    ("user-browse",             "json_common_get-user-browse",                "json"),
]

# 研报端点 (brm.comein.cn/reportManage/index)
REPORT_LIST_API = f"{API_BASE}/json_research_search"
REPORT_DETAIL_API = f"{API_BASE}/json_research_detail"

# 外资研报端点 (brm.comein.cn/foreignResearch). page1 全部 isRealtime=1, 列表本身就是 "实时" feed.
# detail 走 json_oversea-research_preview (researchId 键) —— 这个接口无视 "外资研报未解锁" 的付费墙,
# 对所有 rid 都返回 homeOssPdfUrl (database.comein.cn/original-data/pdf/mndj_report/<md5>.pdf). 之前
# 用的 json_research_detail 是国内端点, 对 oversea rid 会返回数字碰撞的"信达生物 2019 年报告", 丢了
# originalUrl 以及错了整个 title/summary. json_oversea-research_detail 另一条路径有 500 外资研报未解锁.
OVERSEA_REPORT_LIST_API = f"{API_BASE}/json_oversea-research_search"
OVERSEA_REPORT_DETAIL_API = f"{API_BASE}/json_oversea-research_preview"

# MongoDB 配置
MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "jinmen-full")
COL_MEETINGS = "meetings"
COL_REPORTS = "reports"
COL_OVERSEA_REPORTS = "oversea_reports"
COL_ACCOUNT = "account"
COL_STATE = "_state"           # 爬取状态 / checkpoint
COL_OTP_PENDING = "otp_pending"  # aiSummaryAuth=0 且全 stats=0, 等待 OTP 解锁

# 研报 PDF 本地存放目录 (可被 --pdf-dir 或 env JINMEN_PDF_DIR 覆盖).
# 2026-04-17: 迁移到 /home/ygwang/crawl_data/jinmen_pdfs (增量).
# 历史全量 706GB 归档在 /home/ygwang/crawl_data/pdf_full (用户手工管理).
PDF_DIR_DEFAULT = os.environ.get(
    "JINMEN_PDF_DIR",
    "/home/ygwang/crawl_data/jinmen_pdfs",
)

# AES响应解密salt（从前端JS反推得到）
DECRYPT_SALT = "039ed7d839d8915bf01e4f49825fcc6b"


def decrypt_response(resp: requests.Response) -> dict:
    """如果响应头含k字段，则用AES-CBC解密；否则直接当JSON返回.
    解密后顺带过一遍 antibot.detect_soft_warning — 平台返回 "请求过频繁" /
    biz code 限流 / WAF cookie 都会触发同平台全局冷却."""
    k_header = resp.headers.get("k")
    if not k_header:
        body = resp.json()
    else:
        # k头是base64编码的 "<32hex>:<13digits>"
        k_decoded = base64.b64decode(k_header).decode("utf-8").strip()
        # 密钥 = MD5(k_decoded + ":" + salt) 大写hex的UTF-8字节 (32字节, AES-256)
        key_hex = hashlib.md5((k_decoded + ":" + DECRYPT_SALT).encode("utf-8")).hexdigest().upper()
        key = key_hex.encode("utf-8")
        # 响应体: base64解码后, 前16字节是IV, 其余是密文
        raw = base64.b64decode(resp.text)
        iv, ciphertext = raw[:16], raw[16:]
        cipher = AES.new(key, AES.MODE_CBC, iv)
        plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size)
        body = json.loads(plaintext.decode("utf-8"))
    # 软警告检测 (业务层) — body 是已解密的标准 dict
    try:
        reason = detect_soft_warning(resp.status_code, body=body,
                                      cookies=dict(resp.cookies))
        if reason:
            SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=10)
            _THROTTLE.on_warning()
    except Exception:
        pass
    return body


def _raise_for_status_safe(r: requests.Response, endpoint: str = "") -> None:
    """replace-for r.raise_for_status(): 把 HTTP 401/403 升格为 SessionDead.

    jinmen webtoken 被吊销时后端返 401 (有时返 403). 旧路径用 r.raise_for_status()
    抛 HTTPError, 被外层 `except Exception` 吞成普通条目失败, --watch 就会每
    `--interval` 秒硬打同一失效 token 一次, AccountBudget 白白烧, 还延长封禁时间.
    改抛 SessionDead 让 run_once / --watch 显式识别 "要重登了" 并退出, 由
    credential_manager 重新注入 token 后才继续.
    """
    if is_auth_dead(r.status_code):
        raise SessionDead(
            f"jinmen {endpoint or 'API'} HTTP {r.status_code} — "
            f"会话已被吊销 (token 失效 / 账号被拒). 请重新登录更新 JM_AUTH_INFO."
        )
    r.raise_for_status()


def parse_auth(b64_str: str) -> dict:
    """从 JM_AUTH_INFO (base64编码JSON) 中提取 uid 和 webtoken"""
    b64_str = b64_str.strip()
    try:
        decoded = base64.b64decode(b64_str).decode("utf-8")
        data = json.loads(decoded)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"错误: JM_AUTH_INFO 解码失败 - {e}")
        print("请确保复制了 localStorage 中 JM_AUTH_INFO 的完整Value值")
        sys.exit(1)

    value = data.get("value", {})
    uid = value.get("uid", "")
    token = value.get("webtoken") or value.get("token", "")
    org_id = value.get("organizationId", "")

    if not token or not uid:
        print("错误: JM_AUTH_INFO 中缺少 uid 或 webtoken")
        print(f"  可用字段: {list(value.keys())}")
        sys.exit(1)

    print(f"[认证] uid={uid}, token={token[:20]}..., orgId={org_id}")
    return {"uid": str(uid), "token": token, "realm": str(org_id)}


def create_session(auth: dict) -> requests.Session:
    s = requests.Session()
    # 2026-04-22: add trust_env=False — our env has all_proxy=socks5://127.0.0.1:7890
    # (Clash) which requests tries to use for comein.cn OSS downloads. SOCKS deps
    # aren't installed in scraper venv → InvalidSchema → all PDF downloads fail
    # silently. Same pattern as alphapai/gangtise/funda which already opt out.
    s.trust_env = False
    # antibot.headers_for_platform 提供按 process label 稳定 hash 的 UA
    h = headers_for_platform("jinmen")
    # Override referer/origin to brm subdomain (jinmen 后台访问入口)
    h["Referer"] = "https://brm.comein.cn/"
    h["Origin"] = "https://brm.comein.cn"
    h["Content-Type"] = "application/json"
    h.update({
        "uid": auth["uid"],
        "token": auth["token"],
        "web_token": auth["token"],
        "realm": auth.get("realm", ""),
        "os": "brm",
        "c": "pc",
        "b": "4.2.0800",             # app version
        "brandChannel": "windows",
        "webenv": "comein",
        "language": "zh-CN",
        "s": "",
        "uc": "comein-p",
    })
    s.headers.update(h)
    # Warmup: 先 GET brm landing 再 POST 业务 API
    warmup_session(s, "jinmen")
    return s


def headers_for(endpoint_name: str) -> dict:
    """从 json_<mod>_<act> 端点名中拆出 app/mod/act 头"""
    parts = endpoint_name.split("_", 2)
    if len(parts) != 3:
        return {}
    return {"app": parts[0], "mod": parts[1], "act": parts[2]}


def fetch_list(session: requests.Session, page: int = 1, size: int = 40) -> dict:
    """获取纪要列表（单页）"""
    payload = {
        "page": page,
        "size": size,
        "type": 13,
        "sortType": 2,
        "orderType": 2,
        "input": "",
        "options": {
            "needParticiple": False,
            "allowInputEmpty": True,
            "searchScope": 0,
        },
    }
    r = session.post(
        LIST_API,
        json=payload,
        headers=headers_for("json_roadshow-list_summary"),
        timeout=15,
    )
    if r.status_code != 200:
        print(f"[列表API] HTTP {r.status_code}")
        print(f"  响应: {r.text[:500]}")
        _raise_for_status_safe(r, "json_roadshow-list_summary")
    data = decrypt_response(r)

    if str(data.get("code")) != "0":
        print(f"[列表API] 请求失败: code={data.get('code')}, msg={data.get('msg', data.get('errordesc', ''))}")
        sys.exit(1)

    return data


def fetch_summary_info(session: requests.Session, roadshow_id) -> dict:
    """获取纪要详情，提取 aiSummaryId"""
    r = session.post(
        SUMMARY_INFO_API,
        json={"roadshowId": roadshow_id},
        headers=headers_for("json_summary_summary-info"),
        timeout=15,
    )
    _raise_for_status_safe(r, "json_summary_summary-info")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        return {}
    return data.get("data", {})


def fetch_overview(session: requests.Session, ai_summary_id) -> list:
    """获取速览内容"""
    h = headers_for("json_summary_query-summary-index-list")
    h["Content-Type"] = "application/x-www-form-urlencoded"
    r = session.post(
        OVERVIEW_API,
        data={"aiSummaryId": ai_summary_id},
        headers=h,
        timeout=15,
    )
    _raise_for_status_safe(r, "json_summary_query-summary-index-list")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        return []
    return data.get("data", [])


def fetch_raw(session: requests.Session, endpoint: str, payload: dict,
              ct: str = "json") -> dict:
    """通用请求: 返回完整响应 {code, data, msg, ...}"""
    h = headers_for(endpoint)
    kwargs = {"headers": h, "timeout": 20}
    if ct == "form":
        h["Content-Type"] = "application/x-www-form-urlencoded"
        kwargs["data"] = payload
    else:
        kwargs["json"] = payload
    r = session.post(f"{API_BASE}/{endpoint}", **kwargs)
    if r.status_code != 200:
        # 401/403 = 会话吊销: 合成 code=401 dict 会被调用方当普通 biz 错误继续处理,
        # watch 循环会拿同一失效 token 硬打每一个条目. 直接抛 SessionDead 让 run 循环退出.
        _raise_for_status_safe(r, endpoint)
        return {"code": str(r.status_code), "data": None, "msg": f"HTTP {r.status_code}"}
    try:
        return decrypt_response(r)
    except SessionDead:
        raise
    except Exception as e:
        return {"code": "parse_err", "data": None, "msg": str(e)}



# ==================== OTP WAF 解锁 (备用, trust 失效时用) ====================
#
# 2026-04-22 逆向自 brm.comein.cn/js/pro-index-a98c9acb.js:
#   h_="json_waf_send-code"    A_="json_waf_verify-code"
#   Iw=async(e,t={})=>await TC.post(h_,e,t)    Sw=... A_
#
# 这套 WAF SMS 验证**真的存在**, 触发条件是服务端给 userId 的"设备信任"
# 标记失效 (多久一次未知 — 可能新 IP / 新 UA / 定期重验). UI 表现是:
# /json_summary_detail-page-auth 返回 aiSummaryAuth=0 + summary_info.recipient
# 带手机号掩码. 浏览器 SPA 看到这个就弹"为保证账户和内容安全..."对话框.
#
# 实证 (2026-04-22 16:30 CST): 用户浏览器刚完成 OTP 之后的 ~几小时内,
# 同一个 webtoken (硬编码在 scraper 的 TOKEN_1deadd584...) 可以直接调
# /json_summary_query-summary-points + /summary-content-list 拿完整正文 —
# 不需要再单独走 WAF verify. 说明"设备信任"是账号级的, 浏览器过一次 OTP
# 之后 scraper 也吃到红利.
#
# 因此日常 scraper 不跑 OTP. 但当 trust 过期时 (现象: 一批突然 401 或
# aiSummaryId 拿到但 points 返 {}), 就需要用这套函数跑一次 OTP 把
# trust 续上. 保留在这儿当备用.
#
# 关键 wire 信息:
#   POST /comein/json_waf_send-code
#     JSON body: {"roadshowId":"<id>", "uuid":"<uuid from summary_info>"}
#     成功: {code:0, data:60}  冷却: 60s
#     频繁: {code:454}
#
#   POST /comein/json_waf_verify-code
#     Content-Type: application/x-www-form-urlencoded   ← 关键, 不能 JSON
#     body: roadshowId=<id>&uuid=<uuid>&code=<6位>
#     成功: {code:0}
#     错码: 20012=错, 20013=过期, 202=body shape 错
#   成功后 trust 写入 Redis, 同 userId 的后续 API 调用就解锁.

def send_otp_sms(session: requests.Session, roadshow_id: str, uuid: str) -> dict:
    """WAF SMS 触发. roadshow_id + uuid 来自 summary_info 响应里锁住条目的字段.
    返回 {code:0, data:<冷却秒数>} 表示已发, {code:454} 表示 60s 冷却未过.
    """
    return fetch_raw(session, "json_waf_send-code",
                     {"roadshowId": str(roadshow_id), "uuid": uuid})


def verify_otp_sms(session: requests.Session, roadshow_id: str, uuid: str,
                   code: str) -> dict:
    """提交 6 位短信验证码. 必须 form 编码 (JSON 返 202)."""
    return fetch_raw(session, "json_waf_verify-code",
                     {"roadshowId": str(roadshow_id), "uuid": uuid,
                      "code": str(code)},
                     ct="form")


# CLI + monitor 里用短名字. 老代码调的就是 send_otp, 不是 send_otp_sms.
send_otp = send_otp_sms
verify_otp = verify_otp_sms


def record_otp_pending(db, rid, title: str, uuid: str, recipient: str,
                       release_time: str = "", release_time_ms=None) -> None:
    """把一条锁在 OTP 后的纪要记到 jinmen.otp_pending. 监控台据此显示按钮.

    幂等: 同一 _id 再写只更新 updated_at, 保留 status."""
    doc = {
        "_id": rid,
        "roadshow_id": rid,
        "title": title or "",
        "uuid": uuid or "",
        "recipient": recipient or "",
        "release_time": release_time or "",
        "release_time_ms": release_time_ms,
        "updated_at": datetime.now(timezone.utc),
    }
    # status=new 只在首次创建时写入; 后续如果被 monitor 推到 sent/verified 不能被覆盖回 new
    db[COL_OTP_PENDING].update_one(
        {"_id": rid},
        {"$set": doc, "$setOnInsert": {"status": "new",
                                        "created_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def clear_otp_pending(db, rid) -> None:
    """解锁成功后从 otp_pending 删除."""
    try:
        db[COL_OTP_PENDING].delete_one({"_id": rid})
    except Exception:
        pass


def unlock_and_refetch(session: requests.Session, db, roadshow_id,
                      code: str) -> dict:
    """提交 OTP 验证码, 成功则 force=True 重跑 dump_meeting, 返回新的 stats.

    返回 dict 字段:
      verified: bool
      error: str (verified=False 时)
      aiSummaryAuth: int (verify 后的权限值)
      content_chars/chapters/indicators/transcript_items: 新跑出来的 stats
      refetch_err: 如果 verify ok 但 refetch 抛异常, 放这里
    """
    # 1) 从 Mongo 找 uuid (scraper 当时 dump 下来的)
    rid_int = int(roadshow_id) if str(roadshow_id).isdigit() else roadshow_id
    doc = db[COL_MEETINGS].find_one(
        {"$or": [{"_id": rid_int}, {"_id": str(roadshow_id)}]},
        {"title": 1, "release_time": 1, "release_time_ms": 1,
         "organization": 1, "list_item": 1,
         "summary_info.uuid": 1, "summary_info.recipient": 1},
    )
    if not doc:
        # 尝试 otp_pending (可能 meetings 里被某种原因删了)
        p = db[COL_OTP_PENDING].find_one(
            {"$or": [{"_id": rid_int}, {"_id": str(roadshow_id)}]})
        if not p:
            return {"verified": False,
                    "error": f"roadshowId={roadshow_id} 不在 meetings 或 otp_pending 里"}
        uuid = p.get("uuid") or ""
    else:
        uuid = (doc.get("summary_info") or {}).get("uuid") or ""
        if not uuid:
            # fallback: 取 otp_pending 里缓存的 uuid
            p = db[COL_OTP_PENDING].find_one(
                {"$or": [{"_id": rid_int}, {"_id": str(roadshow_id)}]},
                {"uuid": 1})
            uuid = (p or {}).get("uuid") or ""
        if not uuid:
            return {"verified": False,
                    "error": "该条目没有 summary_info.uuid — 跑一次 --watch 触发 scraper 再试"}

    # 2) verify
    vr = verify_otp_sms(session, str(roadshow_id), uuid, code)
    vc = str(vr.get("code"))
    if vc != "0":
        msg_map = {"20012": "验证码错误", "20013": "验证码过期",
                   "202": "参数错误 (body shape)", "454": "请求频繁"}
        msg = msg_map.get(vc, vr.get("msg") or f"code={vc}")
        # 同步回 otp_pending.status = failed + last_error
        db[COL_OTP_PENDING].update_one(
            {"_id": rid_int},
            {"$set": {"status": "failed", "last_error": msg,
                      "updated_at": datetime.now(timezone.utc)}},
        )
        return {"verified": False, "error": msg}

    # 3) verify 成功 → force dump_meeting 回填正文
    if doc:
        item = doc.get("list_item") or {}
        item.setdefault("roadshowId", doc["_id"])
        item.setdefault("title", doc.get("title") or "")
        item.setdefault("organizationName", doc.get("organization") or "")
        release_time = doc.get("release_time") or ""
        release_time_ms = doc.get("release_time_ms")
    else:
        # otp_pending fallback
        p = db[COL_OTP_PENDING].find_one(
            {"$or": [{"_id": rid_int}, {"_id": str(roadshow_id)}]}) or {}
        item = {"roadshowId": p.get("_id"),
                "title": p.get("title", ""),
                "organizationName": ""}
        release_time = p.get("release_time") or ""
        release_time_ms = p.get("release_time_ms")

    refetch_err = None
    new_stats = {}
    auth_after = None
    try:
        res = dump_meeting(session, item, release_time, db,
                          force=True, release_time_ms=release_time_ms)
        new_stats = {k: res.get(k, 0) for k in
                    ("速览字数", "章节", "指标", "对话条数")}
        # 顺便读回 aiSummaryAuth (detail_auth 会被 dump_meeting 刷新)
        fresh = db[COL_MEETINGS].find_one(
            {"$or": [{"_id": rid_int}, {"_id": str(roadshow_id)}]},
            {"detail_auth.aiSummaryAuth": 1})
        auth_after = ((fresh or {}).get("detail_auth") or {}).get("aiSummaryAuth")
    except SessionDead:
        # OTP 流手工触发, 遇到 401 直接把信号抛给 CLI 层让用户去重登, 不能
        # 当成普通 refetch_err 糊过去 — 否则 --otp-verify 汇报 "已验证" 其实没回填.
        raise
    except Exception as e:
        refetch_err = f"{type(e).__name__}: {e}"

    # 4) 清理 pending
    if sum(int(new_stats.get(k, 0) or 0) for k in
           ("速览字数", "章节", "指标", "对话条数")) > 0:
        clear_otp_pending(db, rid_int)
    else:
        db[COL_OTP_PENDING].update_one(
            {"_id": rid_int},
            {"$set": {"status": "verified_but_empty",
                      "last_error": refetch_err or "平台未生成 AI 内容",
                      "updated_at": datetime.now(timezone.utc)}},
        )

    return {
        "verified": True,
        "aiSummaryAuth": auth_after,
        "content_chars": int(new_stats.get("速览字数", 0) or 0),
        "chapters": int(new_stats.get("章节", 0) or 0),
        "indicators": int(new_stats.get("指标", 0) or 0),
        "transcript_items": int(new_stats.get("对话条数", 0) or 0),
        "refetch_err": refetch_err,
    }


def fetch_content_list(session: requests.Session, summary_id) -> list:
    """获取原文对话逐条 (需要 summaryId)"""
    r = session.post(
        CONTENT_LIST_API,
        json={"summaryId": summary_id},
        headers=headers_for("json_summary_summary-content-list"),
        timeout=20,
    )
    _raise_for_status_safe(r, "json_summary_summary-content-list")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        return []
    return data.get("data", []) or []


def fetch_chapter_summary(session: requests.Session, ai_summary_id) -> list:
    """获取AI章节概要 (需要 aiSummaryId) —— 对应前端'章节'或'对话'标签"""
    r = session.post(
        CHAPTER_SUMMARY_API,
        json={"aiSummaryId": ai_summary_id},
        headers=headers_for("json_summary_list-ai-chapter-summary"),
        timeout=15,
    )
    _raise_for_status_safe(r, "json_summary_list-ai-chapter-summary")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        return []
    return data.get("data", []) or []


def fetch_points(session: requests.Session, ai_summary_id) -> str:
    """获取速览 (需要 aiSummaryId) —— 对应前端'速览'标签, markdown 格式"""
    h = headers_for("json_summary_query-summary-points")
    h["Content-Type"] = "application/x-www-form-urlencoded"
    r = session.post(
        POINTS_API,
        data={"aiSummaryId": ai_summary_id},
        headers=h,
        timeout=15,
    )
    _raise_for_status_safe(r, "json_summary_query-summary-points")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        return ""
    d = data.get("data") or {}
    return (d.get("content") or "").strip() if isinstance(d, dict) else ""


def _fmt_ts(ms) -> str:
    """毫秒时长 → HH:MM:SS"""
    try:
        s = int(ms) // 1000
        h, s = divmod(s, 3600)
        m, s = divmod(s, 60)
        return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"
    except (TypeError, ValueError):
        return ""


def format_transcript(items: list) -> str:
    """原文对话 → [时间戳] 发言人: 内容"""
    lines = []
    for it in items:
        if it.get("isDel"):
            continue
        spk = it.get("speakerName", "") or "?"
        t = _fmt_ts(it.get("startSpeakTime"))
        content = (it.get("content") or "").strip()
        if not content:
            continue
        lines.append(f"[{t}] {spk}: {content}" if t else f"{spk}: {content}")
    return "\n".join(lines)


def format_chapter_summary(items: list) -> str:
    """AI章节速览 → 【标题】(时间段) \\n 内容"""
    parts = []
    for it in items:
        title = (it.get("title") or "").strip()
        content = (it.get("content") or "").strip()
        t1, t2 = _fmt_ts(it.get("startTime")), _fmt_ts(it.get("endTime"))
        header = f"【{title}】" if title else ""
        if t1 or t2:
            header += f" ({t1}-{t2})"
        block = (header + "\n" + content).strip()
        if block:
            parts.append(block)
    return "\n\n".join(parts)


def _safe_filename(s: str, max_len: int = 80) -> str:
    s = re.sub(r'[\\/:*?"<>|\r\n\t]', '_', s).strip()
    return s[:max_len]


def format_overview(items: list) -> str:
    """将速览条目列表格式化为可读文本"""
    if not items:
        return ""
    parts = []
    for item in items:
        name = item.get("indicatorName", "")
        text = item.get("originalText", "")
        value = item.get("value", "")
        date = item.get("date", "")
        subject = item.get("subjectName", "")

        line = ""
        if name:
            line += f"[{name}]"
        if subject:
            line += f" {subject}"
        if value:
            line += f" {value}"
        if date:
            line += f" ({date})"
        if text:
            line += f" {text}" if line else text
        if line:
            parts.append(line.strip())
    return "\n".join(parts)


def dump_account(session: requests.Session, db) -> None:
    """账户级接口, 写入 account collection"""
    print("[账户] 抓取账户级接口...")
    col = db[COL_ACCOUNT]
    now = datetime.now(timezone.utc)
    for name, ep, ct in ACCOUNT_ENDPOINTS:
        resp = fetch_raw(session, ep, {}, ct)
        col.replace_one(
            {"_id": name},
            {"_id": name, "endpoint": ep, "response": resp, "updated_at": now},
            upsert=True,
        )
        code = resp.get("code")
        tag = "✓" if code == "0" else f"code={code}"
        print(f"  [{tag}] {name}")


def dump_meeting(session: requests.Session, item: dict, release_time: str,
                 db, force: bool = False, release_time_ms: int | None = None) -> dict:
    """抓取单个会议, 写入 meetings collection. _id = roadshowId"""
    rid = item.get("roadshowId") or item.get("id")
    title = item.get("title", "")
    org = item.get("organizationName", "")
    col = db[COL_MEETINGS]

    # 去重. 平台生成 AI 内容是分阶段的:
    #   1) transcript (对话条数) — 会议结束立即上传
    #   2) chapters (章节) — ~15-30 min
    #   3) points (速览) + indicators (指标) — ~30-60 min
    # 如果首次抓取在阶段 1/2 之间, existing 会有 transcript 但缺速览/指标.
    # 旧逻辑: `total_content==0 and age>3h` 放行 — 但 transcript 不为 0
    # 就永远不会重抓, #专场路演等只抓到半成品. 新逻辑增加第二条放行:
    # 速览+指标都还是 0 且发布已 >30min, 说明 AI 摘要应已生成但我们没拿到 —
    # 直到 24h 兜底 (超过这个时长平台要不就是没生成, 重抓也补不回).
    if not force:
        existing = col.find_one({"_id": rid}, {"_id": 1, "title": 1, "stats": 1,
                                                "release_time_ms": 1})
        if existing:
            stats = existing.get("stats") or {}
            total_content = sum(int(stats.get(k, 0) or 0) for k in
                                ("速览字数", "章节", "指标", "对话条数"))
            ai_summary_empty = all(int(stats.get(k, 0) or 0) == 0
                                    for k in ("速览字数", "指标"))
            age_ms = (int(time.time() * 1000) - release_time_ms
                      if release_time_ms else 0)
            is_stale_empty = release_time_ms and (
                (total_content == 0 and age_ms > 3 * 3600 * 1000) or
                (ai_summary_empty and 30 * 60 * 1000 < age_ms < 24 * 3600 * 1000)
            )
            if not is_stale_empty:
                return {
                    "roadshowId": rid, "标题": title, "时间": release_time, "机构": org,
                    "状态": "已跳过", **(existing.get("stats") or {"速览字数": 0, "章节": 0, "指标": 0, "对话条数": 0}),
                }
            # else fall through and re-scrape — platform may now have content

    # 1) 详情
    info_resp = fetch_raw(session, "json_summary_summary-info", {"roadshowId": rid}, "json")
    info = info_resp.get("data") or {}
    ai_summary = info.get("aiSummary") or {}
    meet_summary = info.get("meetSummary") or {}
    ai_id = ai_summary.get("aiSummaryId")
    sid = meet_summary.get("summaryId") or ai_summary.get("summaryId")

    # 2) 权限
    auth_resp = fetch_raw(session, "json_summary_detail-page-auth", {"roadshowId": rid}, "json")
    auth_data = auth_resp.get("data") or {}

    stats = {"速览字数": 0, "章节": 0, "指标": 0, "对话条数": 0}
    points_text = chapter_text = indicator_text = transcript = ""
    chapters = indicators = content_items = []

    if ai_id:
        pts_resp = fetch_raw(session, "json_summary_query-summary-points", {"aiSummaryId": ai_id}, "form")
        d = pts_resp.get("data") or {}
        if isinstance(d, dict):
            points_text = (d.get("content") or "").strip()
            stats["速览字数"] = len(points_text)

        chap_resp = fetch_raw(session, "json_summary_list-ai-chapter-summary", {"aiSummaryId": ai_id}, "json")
        chapters = chap_resp.get("data") or []
        chapter_text = format_chapter_summary(chapters)
        stats["章节"] = len(chapters)

        ind_resp = fetch_raw(session, "json_summary_query-summary-index-list", {"aiSummaryId": ai_id}, "form")
        indicators = ind_resp.get("data") or []
        indicator_text = format_overview(indicators)
        stats["指标"] = len(indicators)

    if sid:
        cnt_resp = fetch_raw(session, "json_summary_summary-content-list", {"summaryId": sid}, "json")
        content_items = cnt_resp.get("data") or []
        transcript = format_transcript(content_items)
        stats["对话条数"] = len([ln for ln in transcript.split("\n") if ln.strip()])

    # 构造 document
    creators = item.get("creatorNames", []) or []
    guests = item.get("guestNames", []) or []
    industry_list = [t.get("name", "") for t in (item.get("industryTagVoList") or [])]
    stocks = [{"name": s.get("name"), "code": s.get("code"),
               "fullCode": s.get("fullCode"), "market": s.get("market")}
              for s in (item.get("stockInfoVos") or [])]
    themes = [t.get("name", "") for t in (item.get("themeTagVoList") or [])]

    doc = {
        "_id": rid,
        "roadshowId": rid,
        "summaryId": sid,
        "aiSummaryId": ai_id,
        "eid": item.get("eid"),
        "rid": meet_summary.get("rid") or item.get("rid"),
        "title": title,
        "release_time": release_time,
        "release_time_ms": release_time_ms,
        "organization": org,
        "industry": industry_list,
        "stocks": stocks,
        "themes": themes,
        "auth_tag": item.get("authTag"),
        "content_types": item.get("contentTypeTagNames") or [],
        "featured_tag": item.get("featuredTag"),
        "speaker_tag": item.get("speakerTag"),
        "creators": creators,
        "guests": guests,
        "present_url": item.get("presentUrl"),
        "web_url": item.get("presentUrl") or "",  # 统一别名, 跨平台 /items API 用它
        "list_item": item,               # 列表项原始数据
        "summary_info": info,             # 详情原始数据 (含 aiSummary/meetSummary)
        "detail_auth": auth_data,         # 权限
        # 可读文本
        "points_md": points_text,
        "chapter_summary_md": chapter_text,
        "indicators_md": indicator_text,
        "transcript_md": transcript,
        # 原始结构化
        "chapters": chapters,
        "indicators": indicators,
        "content_items": content_items,
        # 统计 + 元
        "stats": stats,
        "crawled_at": datetime.now(timezone.utc),
    }

    _stamp_ticker(doc, "jinmen", col)
    col.replace_one({"_id": rid}, doc, upsert=True)

    # OTP 侦测: 所有 stats=0 + aiSummaryAuth=0 + summary_info.uuid 存在
    # → 平台锁在 WAF OTP 后. 记到 otp_pending, 监控台挂按钮让人解锁.
    # aiSummaryAuth 的字段可能是 data.aiSummaryAuth / data.auth / 直接 auth_data.aiSummaryAuth
    total = sum(int(stats.get(k, 0) or 0) for k in
                ("速览字数", "章节", "指标", "对话条数"))
    if total == 0:
        ai_auth = auth_data.get("aiSummaryAuth") if isinstance(auth_data, dict) else None
        # 实际字段位置: info.uuid + info.recipient (顶层, 不在 aiSummary 下)
        uuid_val = info.get("uuid") or (info.get("aiSummary") or {}).get("uuid") or ""
        recipient = info.get("recipient") or (info.get("aiSummary") or {}).get("recipient") or ""
        if ai_auth == 0 and uuid_val:
            try:
                record_otp_pending(db, rid, title, uuid_val, recipient,
                                  release_time=release_time,
                                  release_time_ms=release_time_ms)
            except Exception:
                pass
    else:
        # 有内容了 → 可能早先被 OTP 卡住, 现在自然解锁. 顺手清理 pending.
        try:
            clear_otp_pending(db, rid)
        except Exception:
            pass

    return {
        "roadshowId": rid, "标题": title, "时间": release_time, "机构": org,
        "状态": "新增", **stats,
    }


def load_state(db) -> dict:
    """加载爬取状态 (checkpoint)"""
    return db[COL_STATE].find_one({"_id": "crawler"}) or {}


def save_state(db, **kwargs) -> None:
    """更新爬取状态"""
    kwargs["updated_at"] = datetime.now(timezone.utc)
    db[COL_STATE].update_one({"_id": "crawler"}, {"$set": kwargs}, upsert=True)


def _item_stime_ms(it: dict) -> int | None:
    """从一条列表项提取发布时间 (毫秒时间戳)."""
    s = it.get("stime") or it.get("releaseTime")
    if s and str(s).isdigit():
        try:
            return int(s)
        except Exception:
            return None
    sut = it.get("summaryUpdateTime", "")
    if sut:
        try:
            return int(datetime.strptime(str(sut)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=_BJ_TZ).timestamp() * 1000)
        except Exception:
            return None
    return None


def fetch_items_paginated(session, max_items=None, page_size=40, max_pages=None,
                          stop_at_roadshow_id=None, stop_before_ms=None):
    """分页拉取纪要列表.
    max_items=None: 翻到尽头
    stop_at_roadshow_id: 遇到该 roadshowId 即停止 (增量模式)
    stop_before_ms: 条目 stime < 该毫秒则停 (用于 --since-hours)
    """
    all_items = []
    page = 1
    while True:
        if max_pages and page > max_pages:
            break
        try:
            ld = fetch_list(session, page=page, size=page_size)
        except SessionDead:
            raise
        except Exception as e:
            tqdm.write(f"  [page {page}] 获取失败: {e}")
            break
        items = ld.get("rows") or ld.get("data") or []
        if not items:
            break

        hit_known = False
        hit_old = False
        new_this_page = 0
        for it in items:
            # 列表不保证严格时间降序, 遇到 hit_known / hit_old 仍扫完本页,
            # 交给下游 dump 逐条 dedup. 仅用作"是否翻下一页"的提示.
            if stop_at_roadshow_id and it.get("roadshowId") == stop_at_roadshow_id:
                hit_known = True
                continue
            if stop_before_ms is not None:
                ts = _item_stime_ms(it)
                if ts is not None and ts < stop_before_ms:
                    hit_old = True
                    continue
            all_items.append(it)
            new_this_page += 1
            if max_items and len(all_items) >= max_items:
                return all_items[:max_items]

        has_more = (ld.get("extra") or {}).get("hasMore")
        tqdm.write(f"  [page {page}] +{new_this_page} (累计 {len(all_items)})  "
                   f"hit_known={hit_known} hit_old={hit_old} hasMore={has_more}")
        if hit_known or hit_old:
            break
        if not has_more:
            break
        if max_items and len(all_items) >= max_items:
            all_items = all_items[:max_items]
            break
        page += 1
        _THROTTLE.sleep_before_next()
    return all_items


def count_today(session, db, date_str: str = None, save_to_db: bool = True) -> dict:
    """统计指定日期(默认今天)平台上有多少条纪要, 并对比本地已入库情况.
    靠 stime 毫秒戳 + 列表按时间倒序的特性, 翻到目标日期前一天即可停止.
    """
    # 平台 stime 是 UTC epoch ms,但显示是 Asia/Shanghai 壁钟,
    # --today 必须用 BJ TZ 对齐,不然在非 BJ 服务器上错位 8 小时.
    if date_str:
        day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
    else:
        day = datetime.now(_BJ_TZ)
    day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start.replace(hour=23, minute=59, second=59, microsecond=999000)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)
    target = day_start.strftime("%Y-%m-%d")

    print(f"[统计] 扫描平台 {target} 的纪要...")
    items_today = []
    page = 1
    while True:
        try:
            ld = fetch_list(session, page=page, size=40)
        except SessionDead:
            raise
        except Exception as e:
            print(f"  [page {page}] 失败: {e}")
            break
        items = ld.get("rows") or ld.get("data") or []
        if not items:
            break

        stop = False
        for it in items:
            stime_raw = it.get("stime")
            try:
                st = int(stime_raw) if stime_raw else 0
            except (TypeError, ValueError):
                st = 0
            if st == 0:
                continue
            if st < start_ms:
                stop = True
                break
            if st <= end_ms:
                items_today.append(it)

        print(f"  [page {page}] 扫 {len(items)} 条, 今日累计 {len(items_today)}, stop={stop}")
        if stop:
            break
        if not (ld.get("extra") or {}).get("hasMore"):
            break
        page += 1
        _THROTTLE.sleep_before_next()

    # 已入库对比
    today_ids = [it.get("roadshowId") for it in items_today if it.get("roadshowId")]
    in_db = db[COL_MEETINGS].count_documents({"_id": {"$in": today_ids}}) if today_ids else 0

    # 分布统计
    from collections import Counter
    org_count = Counter()
    ind_count = Counter()
    tag_count = Counter()
    for it in items_today:
        org_count[it.get("organizationName") or "未知"] += 1
        for t in (it.get("industryTagVoList") or []):
            ind_count[t.get("name", "未知")] += 1
        tag_count[it.get("authTag") or "未知"] += 1

    stats = {
        "date": target,
        "total_on_platform": len(items_today),
        "in_db": in_db,
        "not_in_db": len(items_today) - in_db,
        "by_organization_top10": org_count.most_common(10),
        "by_industry_top10": ind_count.most_common(10),
        "by_tag": dict(tag_count),
        "pages_scanned": page,
        "scanned_at": datetime.now(timezone.utc),
    }

    # 打印
    print(f"\n{'='*55}")
    print(f"📅 {target} 平台纪要统计")
    print(f"{'='*55}")
    print(f"  平台总数:      {stats['total_on_platform']}")
    print(f"  本地已入库:    {stats['in_db']}")
    print(f"  待入库:        {stats['not_in_db']}")
    print(f"\n  按机构 Top10:")
    for org, n in stats["by_organization_top10"]:
        print(f"    {org[:30].ljust(30)}  {n}")
    print(f"\n  按行业 Top10:")
    for ind, n in stats["by_industry_top10"]:
        print(f"    {ind[:20].ljust(20)}  {n}")
    print(f"\n  按标签:")
    for tag, n in sorted(stats["by_tag"].items(), key=lambda x: -x[1]):
        print(f"    {tag[:20].ljust(20)}  {n}")
    print(f"{'='*55}\n")

    # 持久化 (按日期 _id, 便于历史追溯)
    if save_to_db:
        doc = {**stats, "_id": f"daily_{target}"}
        # 把 Counter tuple 转 list (MongoDB 不支持 tuple)
        doc["by_organization_top10"] = [[o, n] for o, n in stats["by_organization_top10"]]
        doc["by_industry_top10"] = [[i, n] for i, n in stats["by_industry_top10"]]
        db[COL_STATE].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        print(f"已保存到 {COL_STATE} collection (_id={doc['_id']})\n")

    return stats


# ==================== 研报爬取 ====================

def fetch_report_list(session: requests.Session, page: int = 1, size: int = 40) -> dict:
    """研报列表 /json_research_search (brm.comein.cn/reportManage/index).

    Response: {code, total, data: [...], extra: {hasMore, total, pagestart, more}}
    - 列表按 releaseTime 降序
    - item 含 id / reportId / title / organizationName / releaseTime / ossUrl / pdfNum / summaryPoint
    """
    r = session.post(
        REPORT_LIST_API,
        json={"page": page, "size": size},
        headers=headers_for("json_research_search"),
        timeout=20,
    )
    _raise_for_status_safe(r, "json_research_search")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        print(f"[研报列表] code={data.get('code')} msg={data.get('msg') or data.get('errordesc')}")
    return data


def fetch_report_detail(session: requests.Session, rid) -> dict:
    """研报详情 /json_research_detail?id=<id>.

    比列表多了完整 summary (核心观点完整版) 以及 originalUrl (PDF)。
    """
    r = session.post(
        REPORT_DETAIL_API,
        json={"id": rid},
        headers=headers_for("json_research_detail"),
        timeout=20,
    )
    _raise_for_status_safe(r, "json_research_detail")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        return {}
    return data.get("data") or {}


def _pdf_filename(report_id: str, rid, title: str) -> str:
    base = report_id or (title if title else f"report_{rid}")
    return _safe_filename(base, max_len=120) + ".pdf"


def _pdf_dest_path(pdf_dir: Path, release_ms, report_id: str, rid, title: str) -> Path:
    try:
        ym = datetime.fromtimestamp(int(release_ms) / 1000, tz=_BJ_TZ).strftime("%Y-%m") if release_ms else "unknown"
    except Exception:
        ym = "unknown"
    return Path(pdf_dir) / ym / _pdf_filename(report_id, rid, title)


def download_report_pdf(session: requests.Session, url: str, dest: Path,
                        timeout: float = 60.0) -> tuple:
    """Download a research-report attachment. Originally PDF-only; now also
    accepts .doc / .docx / .xlsx because jinmen's oversea feed occasionally
    serves Word research docs (e.g. Citi's 韩国经济 2026-04-22 is a .doc).

    Magic-byte whitelist per extension:
      * .pdf  → `%PDF`
      * .doc  → CFB `D0 CF 11 E0 A1 B1 1A E1`  (MS Compound Binary)
      * .docx/.xlsx/.pptx → `PK\x03\x04` (ZIP container)
      * unknown → accept any 200 with non-empty body (opportunistic).

    The caller's `dest` keeps its original `.pdf` suffix for back-compat;
    the actual bytes on disk are whatever the server sent. Downstream
    consumers can look at dest.stat().st_size + a quick magic probe if
    they need to distinguish.
    """
    if not url:
        return 0, "no url"
    if dest.exists() and dest.stat().st_size > 0:
        return dest.stat().st_size, ""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    # Pick expected magic from the URL's extension. URL may have query
    # params (?sig=...) so strip before matching.
    import re as _re
    m = _re.search(r"\.([a-zA-Z0-9]{2,5})(?:$|\?|#)", url)
    ext = (m.group(1).lower() if m else "").rstrip("/")
    magic_ok = {
        "pdf":  lambda b: b.startswith(b"%PDF"),
        "doc":  lambda b: b.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"),
        "docx": lambda b: b.startswith(b"PK\x03\x04"),
        "xlsx": lambda b: b.startswith(b"PK\x03\x04"),
        "pptx": lambda b: b.startswith(b"PK\x03\x04"),
        "xls":  lambda b: b.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"),
    }.get(ext, lambda b: len(b) > 0)  # unknown ext → permissive
    try:
        with session.get(url, stream=True, timeout=timeout) as r:
            if r.status_code != 200:
                return 0, f"HTTP {r.status_code}"
            it = r.iter_content(8192)
            first = next(it, b"")
            if not magic_ok(first):
                prefix = first[:20].hex()
                return 0, f"bad magic for .{ext} (hex={prefix!r})"
            written = 0
            with tmp.open("wb") as f:
                f.write(first); written += len(first)
                for chunk in it:
                    if chunk:
                        f.write(chunk); written += len(chunk)
        tmp.replace(dest)
        return written, ""
    except Exception as e:
        try:
            if tmp.exists(): tmp.unlink()
        except Exception:
            pass
        return 0, f"{type(e).__name__}: {e}"


def dump_report(session: requests.Session, item: dict, db, pdf_dir: Path,
                download_pdf: bool = True, force: bool = False) -> dict:
    """抓取单篇研报, 写入 reports collection. _id = research id (item['id'])."""
    rid = item.get("id")
    col = db[COL_REPORTS]
    title = item.get("title") or ""

    # 去重: 已入库且 PDF 已落盘 (或用户要 skip-pdf) → 跳过
    if not force:
        existing = col.find_one({"_id": rid},
                                 {"_id": 1, "pdf_local_path": 1, "pdf_size_bytes": 1, "stats": 1})
        if existing:
            already_have_pdf = bool(existing.get("pdf_local_path")) and existing.get("pdf_size_bytes", 0) > 0
            if already_have_pdf or not download_pdf:
                return {"状态": "已跳过", "标题": title,
                        **(existing.get("stats") or {"摘要字数": 0, "页数": 0, "pdf_大小": 0})}

    detail = fetch_report_detail(session, rid)

    release_ms = item.get("releaseTime") or detail.get("releaseTime") or 0
    try:
        release_time = _ms_to_bj_str(release_ms) if release_ms else ""
    except Exception:
        release_time = ""

    original_url = detail.get("originalUrl") or item.get("ossUrl") or ""
    report_id = item.get("reportId") or ""

    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    if download_pdf and original_url:
        dest = _pdf_dest_path(pdf_dir, release_ms, report_id, rid, title)
        pdf_size, pdf_err = download_report_pdf(session, original_url, dest)
        if pdf_size > 0:
            pdf_local = str(dest)

    orgs = item.get("organizationList") or detail.get("organizationList") or []
    content_tags = item.get("contentTagNames") or detail.get("contentTagNames") or []
    industry_tags = detail.get("industryTagNames") or []
    companies = detail.get("companyDtoList") or []

    summary_full = (detail.get("summary") or "").strip()
    summary_short = (item.get("summaryPoint") or "").strip()
    summary_md = summary_full if len(summary_full) >= len(summary_short) else summary_short

    doc = {
        "_id": rid,
        "id": rid,
        "report_id": report_id,
        "title": title,
        "release_time": release_time,
        "release_time_ms": int(release_ms) if release_ms else None,
        "organization_name": item.get("organizationName") or (orgs[0].get("name") if orgs else ""),
        "organizations": orgs,
        "type_name": item.get("typeName"),
        "content_tags": content_tags,
        "industry_tags": industry_tags,
        "companies": companies,
        "is_vip": bool(item.get("isVipResearch") or detail.get("isVipResearch")),
        "pdf_num": item.get("pdfNum") or detail.get("pdfNum") or 0,
        "has_image": bool(item.get("hasImage")),
        "origin_source": detail.get("originSource"),
        "summary_point_md": summary_short,
        "summary_md": summary_md,
        "original_url": original_url,                             # 研报原始 PDF URL (S3)
        "link_url": item.get("linkUrl") or "",                    # 移动端 HTML 展示页
        "web_url": item.get("linkUrl") or original_url or "",     # 统一别名, 跨平台用
        "source_url": original_url or "",                         # PDF 直链, 备份
        "pdf_local_path": pdf_local,
        "pdf_size_bytes": pdf_size,
        "pdf_download_error": pdf_err,
        "list_item": item,
        "detail_result": detail,
        "stats": {
            "摘要字数": len(summary_md),
            "页数": item.get("pdfNum") or detail.get("pdfNum") or 0,
            "机构数": len(orgs),
            "公司数": len(companies),
            "pdf_大小": pdf_size,
        },
        "crawled_at": datetime.now(timezone.utc),
    }
    _stamp_ticker(doc, "jinmen", col)
    col.replace_one({"_id": rid}, doc, upsert=True)
    return {"状态": "重爬" if force else "新增", "标题": title,
            "pdf_err": pdf_err, **doc["stats"]}


def fetch_reports_paginated(session, max_items=None, page_size=40,
                            stop_at_id=None, stop_before_ms=None):
    """分页拉研报列表; 同 fetch_items_paginated 语义.

    stop_at_id: 遇到 id 即停 (增量锚点)
    stop_before_ms: releaseTime < 毫秒戳则停 (--since-hours)
    """
    all_items = []
    page = 1
    while True:
        try:
            ld = fetch_report_list(session, page=page, size=page_size)
        except SessionDead:
            raise
        except Exception as e:
            tqdm.write(f"  [page {page}] 研报列表失败: {e}")
            break
        if str(ld.get("code")) != "0":
            break
        items = ld.get("data") or []
        if not items:
            break

        hit_known = hit_old = False
        new_n = 0
        for it in items:
            # 同上: 扫完本页再交给下游 dedup, 防止"未来日期"条目把真正新条目挤后
            if stop_at_id and it.get("id") == stop_at_id:
                hit_known = True
                continue
            if stop_before_ms is not None:
                rt = it.get("releaseTime")
                if isinstance(rt, (int, float)) and rt < stop_before_ms:
                    hit_old = True
                    continue
            all_items.append(it); new_n += 1
            if max_items and len(all_items) >= max_items:
                return all_items[:max_items]

        has_more = (ld.get("extra") or {}).get("hasMore")
        tqdm.write(f"  [page {page}] +{new_n} (累计 {len(all_items)})  "
                   f"hit_known={hit_known} hit_old={hit_old} hasMore={has_more}")
        if hit_known or hit_old:
            break
        if not has_more:
            break
        page += 1
        _THROTTLE.sleep_before_next()
    return all_items


# ==================== 外资研报 (含"实时"feed) ====================
#
# 端点:
#   list   /json_oversea-research_search   body {page, size}
#   detail /json_research_detail            body {id}   (oversea 专用 detail 返 code=202)
#
# 列表 item 字段 (与国内研报 schema 略有差异):
#   id, reportId, releaseDate (ms), title (英文原), titleChn (中译),
#   orgName (英文原), orgNameChn (中译), reportType (公司研究/行业研究/市场动态),
#   languageList, countryList, fullCodeList, stockList, industryNameList,
#   pdfNum, linkUrl (mobile 页), isRealtime (永远 1), transStatus
#
# 写入 jinmen.oversea_reports collection, _id = item['id'] (int).

def fetch_oversea_report_list(session: requests.Session, page: int = 1, size: int = 40,
                               is_realtime: int = 1) -> dict:
    """外资研报列表 /json_oversea-research_search.

    Response: {code, data:[...], extra:{hasMore, total, ...}}
    列表按 releaseDate 降序.

    is_realtime: 1 = "实时" sub-tab (default — JPM/MS/Goldman 的当日翻译)
                 0 = "延时" sub-tab (Deutsche Bank 等隔日/批量翻译版本)
    UI 上 外资研报 tab 还有 "全部" sub-tab, 服务端没对应 enum 值, 但 1+0 联合就是全部.
    """
    body = {"page": page, "size": size}
    if is_realtime in (0, 1):
        body["isRealtime"] = is_realtime
    r = session.post(
        OVERSEA_REPORT_LIST_API,
        json=body,
        headers=headers_for("json_oversea-research_search"),
        timeout=20,
    )
    _raise_for_status_safe(r, "json_oversea-research_search")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        print(f"[外资列表] code={data.get('code')} msg={data.get('msg') or data.get('errordesc')}")
    return data


def fetch_oversea_report_detail(session: requests.Session, rid) -> dict:
    """外资研报详情. 走 json_oversea-research_preview (researchId 键).

    返回字段包含 homeOssPdfUrl, haveAuth, needNbi, title, titleChn, summary,
    stockList 等。PDF URL 是 https://database.comein.cn/original-data/pdf/mndj_report/<md5>.pdf。
    """
    r = session.post(
        OVERSEA_REPORT_DETAIL_API,
        json={"researchId": rid},
        headers=headers_for("json_oversea-research_preview"),
        timeout=20,
    )
    _raise_for_status_safe(r, "json_oversea-research_preview")
    data = decrypt_response(r)
    if str(data.get("code")) != "0":
        return {}
    return data.get("data") or {}


def dump_oversea_report(session: requests.Session, item: dict, db, pdf_dir: Path,
                        download_pdf: bool = True, force: bool = False) -> dict:
    """抓取单篇外资研报, 写入 oversea_reports collection. _id = item['id'].

    2026-04-22: jinmen 把 ``json_oversea-research_search`` 返回的 list item
    瘦身到只含 ``{id}`` — 所有其他字段 (title/titleChn/orgName/releaseDate/
    stockList/...) 挪进了 detail (``json_oversea-research_preview``). 以下
    每个字段都做 ``item → detail`` 的 fallback, 确保新 API shape 下顶层字段
    不再被写空.
    """
    rid = item.get("id")
    col = db[COL_OVERSEA_REPORTS]

    # 先拉 detail — list 瘦身后几乎所有字段都要从这里取.
    detail = fetch_oversea_report_detail(session, rid)

    # 字段 promote: item 优先 (老 API 格式向后兼容), 否则 detail.
    title_en = item.get("title") or detail.get("title") or ""
    title_cn = item.get("titleChn") or detail.get("titleChn") or ""
    title = title_cn or title_en

    # 去重: 已入库且 PDF 已落盘 (或 skip-pdf) → 跳过
    if not force:
        existing = col.find_one({"_id": rid},
                                 {"_id": 1, "pdf_local_path": 1, "pdf_size_bytes": 1, "stats": 1, "title": 1})
        if existing:
            already_have_pdf = bool(existing.get("pdf_local_path")) and existing.get("pdf_size_bytes", 0) > 0
            has_title = bool(existing.get("title"))
            # 只有 title 已填 + PDF 已落盘 才跳过; title 空说明之前被 list-瘦身 bug 坑过, fall-through 重写
            if has_title and (already_have_pdf or not download_pdf):
                return {"状态": "已跳过", "标题": title,
                        **(existing.get("stats") or {"摘要字数": 0, "页数": 0, "pdf_大小": 0})}

    release_ms = item.get("releaseDate") or detail.get("releaseDate") or detail.get("releaseTime") or 0
    try:
        release_time = _ms_to_bj_str(release_ms) if release_ms else ""
    except Exception:
        release_time = ""

    # preview 返回 homeOssPdfUrl (database.comein.cn); 旧 detail 的 originalUrl
    # (comein-files.oss.aliyuncs.com) 作为兜底, 以防老库 detail_result 仍保留.
    original_url = detail.get("homeOssPdfUrl") or detail.get("originalUrl") or ""
    report_id = item.get("reportId") or detail.get("reportId") or ""

    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    if download_pdf and original_url:
        dest = _pdf_dest_path(pdf_dir, release_ms, report_id, rid, title)
        pdf_size, pdf_err = download_report_pdf(session, original_url, dest)
        if pdf_size > 0:
            pdf_local = str(dest)

    summary_full = (detail.get("content") or detail.get("summary") or "").strip()
    summary_short = (item.get("summary") or detail.get("summary") or "").strip()
    summary_md = summary_full if len(summary_full) >= len(summary_short) else summary_short

    doc = {
        "_id": rid,
        "id": rid,
        "report_id": report_id,
        "title": title,
        "title_cn": title_cn,
        "title_en": title_en,
        "release_time": release_time,
        "release_time_ms": int(release_ms) if release_ms else None,
        "organization_name": item.get("orgNameChn") or detail.get("orgNameChn") or item.get("orgName") or detail.get("orgName") or "",
        "organization_name_en": item.get("orgName") or detail.get("orgName") or "",
        "report_type": item.get("reportType") or detail.get("reportType") or "",       # 公司研究 / 行业研究 / 市场动态
        "language_list": item.get("languageList") or detail.get("languageList") or [],
        "country_list": item.get("countryList") or detail.get("countryList") or [],
        "trans_status": item.get("transStatus") if item.get("transStatus") is not None else detail.get("transStatus"),
        "is_realtime": bool(item.get("isRealtime") or detail.get("isRealtime")),
        "stocks": item.get("stockList") or detail.get("stockList") or [],
        "stock_codes": item.get("stockCodeList") or detail.get("stockCodeList") or [],
        "stock_names": item.get("stockNameList") or detail.get("stockNameList") or [],
        "full_codes": item.get("fullCodeList") or detail.get("fullCodeList") or [],
        "industries": item.get("industryNameList") or detail.get("industryNameList") or [],
        "authors": item.get("authorList") or detail.get("authorList") or [],
        "pdf_num": item.get("pdfNum") or detail.get("pdfNum") or 0,
        "has_image": bool(item.get("hasImage") or detail.get("hasImage")),
        "summary_md": summary_md,
        "original_url": original_url,
        "link_url": item.get("linkUrl") or "",
        "web_url": item.get("linkUrl") or original_url or "",
        "source_url": original_url or "",
        "pdf_local_path": pdf_local,
        "pdf_size_bytes": pdf_size,
        "pdf_download_error": pdf_err,
        "list_item": item,
        "detail_result": detail,
        "stats": {
            "摘要字数": len(summary_md),
            "页数": item.get("pdfNum") or 0,
            "pdf_大小": pdf_size,
        },
        "crawled_at": datetime.now(timezone.utc),
    }
    _stamp_ticker(doc, "jinmen", col)
    col.replace_one({"_id": rid}, doc, upsert=True)
    return {"状态": "重爬" if force else "新增", "标题": title,
            "pdf_err": pdf_err, **doc["stats"]}


def fetch_oversea_reports_paginated(session, max_items=None, page_size=40,
                                     stop_at_id=None, stop_before_ms=None):
    """分页拉外资研报列表 — 同时跑 isRealtime=1 (实时) + isRealtime=0 (延时) 两个 sub-tab.

    UI 上 外资研报 有「全部 / 实时 / 延时」三个 sub-tab. 服务端按 isRealtime
    过滤; 不传时默认 isRealtime=1 (only 实时), 漏掉延时那批 (Deutsche Bank 等
    非当日发的批量翻译)。我们把两个 isRealtime 值都跑一遍, 同 _id 由 upsert
    去重 (没有重复 — 一条 item 不会同时是实时+延时).

    跨 sub-tab 共享 stop_at_id (上次 top); 每个 sub-tab 独立 stop_before_ms 比较.
    """
    all_items = []
    seen_ids: set = set()

    for is_realtime in (1, 0):  # 实时优先, 延时其后
        label = "实时" if is_realtime == 1 else "延时"
        page = 1
        sub_added = 0
        while True:
            try:
                ld = fetch_oversea_report_list(session, page=page, size=page_size,
                                                is_realtime=is_realtime)
            except SessionDead:
                raise
            except Exception as e:
                tqdm.write(f"  [外资/{label} page {page}] 列表失败: {e}")
                break
            if str(ld.get("code")) != "0":
                break
            items = ld.get("data") or []
            if not items:
                break

            hit_known = hit_old = False
            new_n = 0
            for it in items:
                if stop_at_id and it.get("id") == stop_at_id:
                    hit_known = True
                    continue
                if stop_before_ms is not None:
                    rt = it.get("releaseDate")
                    if isinstance(rt, (int, float)) and rt < stop_before_ms:
                        hit_old = True
                        continue
                rid = it.get("id")
                if rid in seen_ids:  # cross-subtab dedup safety
                    continue
                seen_ids.add(rid)
                all_items.append(it); new_n += 1; sub_added += 1
                if max_items and len(all_items) >= max_items:
                    return all_items[:max_items]

            has_more = (ld.get("extra") or {}).get("hasMore", True)
            tqdm.write(f"  [外资/{label} page {page}] +{new_n} (累计 {len(all_items)})  "
                       f"hit_known={hit_known} hit_old={hit_old} hasMore={has_more}")
            if hit_known or hit_old:
                break
            if not has_more or len(items) < page_size:
                break
            page += 1
            _THROTTLE.sleep_before_next()
        tqdm.write(f"  [外资/{label}] 子tab完成: 新增 {sub_added}, 累计跨tab {len(all_items)}")

    return all_items


def run_oversea_reports_once(session, db, args):
    """外资研报模式的一轮抓取. checkpoint key = crawler_oversea_reports."""
    state = db[COL_STATE].find_one({"_id": "crawler_oversea_reports"}) or {}
    stop_id = state.get("top_id") if args.resume else None
    if args.resume and stop_id:
        last = state.get("updated_at") or state.get("last_run_end_at")
        print(f"[恢复] 外资研报上次爬到 id={stop_id} (时间 {last})")
    elif args.resume:
        print(f"[恢复] 未找到外资 checkpoint, 按普通模式全量")

    stop_ms = None
    if getattr(args, "since_hours", None) is not None:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        stop_ms = int(cutoff_dt.timestamp() * 1000)
        local_str = cutoff_dt.astimezone().strftime("%Y-%m-%d %H:%M")
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 (cutoff={local_str})")

    pdf_dir = Path(getattr(args, "pdf_dir", PDF_DIR_DEFAULT))
    skip_pdf = getattr(args, "skip_pdf", False)
    print(f"[外资研报] PDF 目录: {pdf_dir}  下载: {'关闭' if skip_pdf else '开启'}")

    print(f"[列表] 抓取外资研报 max={args.max or '全部'} page_size={args.page_size}")
    items = fetch_oversea_reports_paginated(session, max_items=args.max,
                                             page_size=args.page_size,
                                             stop_at_id=stop_id, stop_before_ms=stop_ms)
    print(f"[列表] 共 {len(items)} 条待处理\n")
    if not items:
        print("无新外资研报 (或账号失效)")
        return {"added": 0, "skipped": 0, "failed": 0}

    new_top = items[0].get("id")
    added = skipped = failed = pdf_ok = pdf_fail = 0
    cap = cap_from_args(args)
    now = lambda: datetime.now(timezone.utc)

    pbar = tqdm(items, desc="外资研报", unit="条", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}")
    for item in pbar:
        if cap.exhausted() or _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        rid = item.get("id")
        title = item.get("titleChn") or item.get("title") or ""
        was_skip = False
        try:
            row = dump_oversea_report(session, item, db, pdf_dir=pdf_dir,
                                       download_pdf=not skip_pdf, force=args.force)
            if row["状态"] == "已跳过":
                skipped += 1
                was_skip = True
                tqdm.write(f"  · [{rid}] {title[:60]}  已存在, 跳过")
            else:
                added += 1
                cap.bump(); _BUDGET.bump()
                err = row.get("pdf_err") or ""
                if skip_pdf:
                    pdf_info = "PDF跳过"
                elif row.get("pdf_大小", 0) > 0:
                    pdf_ok += 1
                    pdf_info = f"PDF={row['pdf_大小']:,}B"
                else:
                    pdf_fail += 1
                    pdf_info = f"PDF失败({err[:30]})"
                tqdm.write(f"  ✓ [{rid}] {title[:60]}  摘要{row['摘要字数']}字 页{row['页数']} {pdf_info}")
        except SessionDead:
            raise
        except Exception as e:
            failed += 1
            tqdm.write(f"  ✗ [{rid}] {title[:60]}  ERR: {e}")

        pbar.set_postfix_str(f"新增={added} 跳过={skipped} PDF✓{pdf_ok} PDF✗{pdf_fail} 失败={failed}")

        db[COL_STATE].update_one(
            {"_id": "crawler_oversea_reports"},
            {"$set": {"last_processed_id": rid, "last_processed_at": now(),
                      "in_progress": True, "updated_at": now()}},
            upsert=True,
        )
        if not was_skip:
            _THROTTLE.sleep_before_next()
    pbar.close()

    db[COL_STATE].update_one(
        {"_id": "crawler_oversea_reports"},
        {"$set": {"top_id": new_top, "in_progress": False,
                  "last_run_end_at": now(), "updated_at": now(),
                  "last_run_stats": {"added": added, "skipped": skipped, "failed": failed,
                                      "pdf_ok": pdf_ok, "pdf_fail": pdf_fail}}},
        upsert=True,
    )

    total = db[COL_OVERSEA_REPORTS].estimated_document_count()
    print(f"\n本轮完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}")
    print(f"PDF: 成功 {pdf_ok} / 失败 {pdf_fail}")
    print(f"MongoDB 当前外资研报总数: {total}")
    return {"added": added, "skipped": skipped, "failed": failed,
            "pdf_ok": pdf_ok, "pdf_fail": pdf_fail}


def count_reports_today(session, db, date_str: str = None, save_to_db: bool = True) -> dict:
    """统计某天平台研报条数 + 本地库对比. 列表按 releaseTime 降序, 翻到前一天即停."""
    if date_str:
        day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
    else:
        day = datetime.now(_BJ_TZ)
    day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start.replace(hour=23, minute=59, second=59, microsecond=999000)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)
    target = day_start.strftime("%Y-%m-%d")

    print(f"[统计] 扫描平台 {target} 的研报...")
    items_today = []
    page = 1
    while True:
        try:
            ld = fetch_report_list(session, page=page, size=40)
        except SessionDead:
            raise
        except Exception as e:
            print(f"  [page {page}] 失败: {e}")
            break
        items = ld.get("data") or []
        if not items:
            break
        stop = False
        for it in items:
            rt = it.get("releaseTime")
            try:
                st = int(rt) if rt else 0
            except (TypeError, ValueError):
                st = 0
            if st == 0:
                continue
            if st < start_ms:
                stop = True
                break
            if st <= end_ms:
                items_today.append(it)
        print(f"  [page {page}] 扫 {len(items)} 条, 今日累计 {len(items_today)}, stop={stop}")
        if stop:
            break
        if not (ld.get("extra") or {}).get("hasMore"):
            break
        page += 1
        _THROTTLE.sleep_before_next()

    today_ids = [it.get("id") for it in items_today if it.get("id")]
    in_db = db[COL_REPORTS].count_documents({"_id": {"$in": today_ids}}) if today_ids else 0

    from collections import Counter
    org_count = Counter()
    tag_count = Counter()
    type_count = Counter()
    for it in items_today:
        org_count[it.get("organizationName") or "未知"] += 1
        for t in (it.get("contentTagNames") or []):
            tag_count[t] += 1
        type_count[it.get("typeName") or "未知"] += 1

    stats = {
        "date": target,
        "total_on_platform": len(items_today),
        "in_db": in_db,
        "not_in_db": len(items_today) - in_db,
        "by_organization_top10": org_count.most_common(10),
        "by_content_tag": dict(tag_count),
        "by_type": dict(type_count),
        "pages_scanned": page,
        "scanned_at": datetime.now(timezone.utc),
    }

    print(f"\n{'='*55}")
    print(f"📄 {target} 平台研报统计")
    print(f"{'='*55}")
    print(f"  平台总数:      {stats['total_on_platform']}")
    print(f"  本地已入库:    {stats['in_db']}")
    print(f"  待入库:        {stats['not_in_db']}")
    print(f"\n  按机构 Top10:")
    for org, n in stats["by_organization_top10"]:
        print(f"    {org[:30].ljust(30)}  {n}")
    print(f"\n  按内容标签:")
    for tag, n in sorted(stats["by_content_tag"].items(), key=lambda x: -x[1]):
        print(f"    {tag[:20].ljust(20)}  {n}")
    print(f"\n  按类型:")
    for t, n in sorted(stats["by_type"].items(), key=lambda x: -x[1]):
        print(f"    {t[:20].ljust(20)}  {n}")
    print(f"{'='*55}\n")

    if save_to_db:
        doc = {**stats, "_id": f"daily_reports_{target}"}
        doc["by_organization_top10"] = [[o, n] for o, n in stats["by_organization_top10"]]
        db[COL_STATE].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        print(f"已保存到 {COL_STATE} collection (_id={doc['_id']})\n")
    return stats


def run_reports_once(session, db, args):
    """研报模式的一轮抓取 (对应 brm.comein.cn/reportManage/index)."""
    # 断点恢复
    state = db[COL_STATE].find_one({"_id": "crawler_reports"}) or {}
    stop_id = state.get("top_id") if args.resume else None
    if args.resume and stop_id:
        last = state.get("updated_at") or state.get("last_run_end_at")
        print(f"[恢复] 研报上次爬到 id={stop_id} (时间 {last})")
    elif args.resume:
        print(f"[恢复] 未找到研报 checkpoint, 按普通模式全量")

    stop_ms = None
    if getattr(args, "since_hours", None) is not None:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        stop_ms = int(cutoff_dt.timestamp() * 1000)
        local_str = cutoff_dt.astimezone().strftime("%Y-%m-%d %H:%M")
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 (cutoff={local_str})")

    pdf_dir = Path(getattr(args, "pdf_dir", PDF_DIR_DEFAULT))
    skip_pdf = getattr(args, "skip_pdf", False)
    print(f"[研报] PDF 目录: {pdf_dir}  下载: {'关闭' if skip_pdf else '开启'}")

    print(f"[列表] 抓取研报 max={args.max or '全部'} page_size={args.page_size}")
    items = fetch_reports_paginated(session, max_items=args.max,
                                    page_size=args.page_size,
                                    stop_at_id=stop_id, stop_before_ms=stop_ms)
    print(f"[列表] 共 {len(items)} 条待处理\n")
    if not items:
        print("无新研报 (或账号失效)")
        return {"added": 0, "skipped": 0, "failed": 0}

    new_top = items[0].get("id")
    added = skipped = failed = pdf_ok = pdf_fail = 0
    cap = cap_from_args(args)
    now = lambda: datetime.now(timezone.utc)

    pbar = tqdm(items, desc="研报", unit="条", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}")
    for item in pbar:
        if cap.exhausted() or _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        rid = item.get("id")
        title = item.get("title") or ""
        was_skip = False
        try:
            row = dump_report(session, item, db, pdf_dir=pdf_dir,
                              download_pdf=not skip_pdf, force=args.force)
            if row["状态"] == "已跳过":
                skipped += 1
                was_skip = True
                tqdm.write(f"  · [{rid}] {title[:60]}  已存在, 跳过")
            else:
                added += 1
                cap.bump(); _BUDGET.bump()
                err = row.get("pdf_err") or ""
                if skip_pdf:
                    pdf_info = "PDF跳过"
                elif row.get("pdf_大小", 0) > 0:
                    pdf_ok += 1
                    pdf_info = f"PDF={row['pdf_大小']:,}B"
                else:
                    pdf_fail += 1
                    pdf_info = f"PDF失败({err[:30]})"
                tqdm.write(f"  ✓ [{rid}] {title[:60]}  摘要{row['摘要字数']}字 页{row['页数']} {pdf_info}")
        except SessionDead:
            raise
        except Exception as e:
            failed += 1
            tqdm.write(f"  ✗ [{rid}] {title[:60]}  ERR: {e}")

        pbar.set_postfix_str(f"新增={added} 跳过={skipped} PDF✓{pdf_ok} PDF✗{pdf_fail} 失败={failed}")

        db[COL_STATE].update_one(
            {"_id": "crawler_reports"},
            {"$set": {"last_processed_id": rid, "last_processed_at": now(),
                      "in_progress": True, "updated_at": now()}},
            upsert=True,
        )
        # Existing items hit no remote endpoints (just a DB lookup) — sleeping
        # the full throttle burns ~1h when catching up through a long prefix of
        # already-stored items. Only throttle on actual fetches.
        if not was_skip:
            _THROTTLE.sleep_before_next()
    pbar.close()

    db[COL_STATE].update_one(
        {"_id": "crawler_reports"},
        {"$set": {"top_id": new_top, "in_progress": False,
                  "last_run_end_at": now(), "updated_at": now(),
                  "last_run_stats": {"added": added, "skipped": skipped, "failed": failed,
                                      "pdf_ok": pdf_ok, "pdf_fail": pdf_fail}}},
        upsert=True,
    )

    total = db[COL_REPORTS].estimated_document_count()
    print(f"\n本轮完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}")
    print(f"PDF: 成功 {pdf_ok} / 失败 {pdf_fail}")
    print(f"MongoDB 当前研报总数: {total}")
    return {"added": added, "skipped": skipped, "failed": failed,
            "pdf_ok": pdf_ok, "pdf_fail": pdf_fail}


def run_once_streaming(session, db, args):
    """Streaming backfill: per-page fetch → dump → deep_page checkpoint → next."""
    if db[COL_ACCOUNT].estimated_document_count() == 0 or args.force:
        dump_account(session, db)
    else:
        print("[账户] 已有数据, 跳过")

    state = load_state(db)
    start_page = int(state.get("backfill_deep_page") or 1)
    print(f"[stream] resume_from_page={start_page} page_size={args.page_size}")

    added = skipped = failed = 0
    cap = cap_from_args(args)
    page = start_page
    first_top: Optional[int] = None
    total_seen = 0

    while True:
        if cap.exhausted() or _BUDGET.exhausted():
            print(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停")
            break
        try:
            ld = fetch_list(session, page=page, size=args.page_size)
        except SessionDead:
            raise
        except Exception as e:
            print(f"  [page {page}] 列表获取失败: {e}")
            break
        items = ld.get("rows") or ld.get("data") or []
        if not items:
            print(f"  [page {page}] 空列表, 列表到底 → 重置 backfill_deep_page=1")
            save_state(db, backfill_deep_page=1,
                       backfill_last_run_end_at=datetime.now(timezone.utc))
            break

        if first_top is None:
            first_top = items[0].get("roadshowId")

        page_added = page_skipped = page_failed = 0
        for item in items:
            if cap.exhausted() or _BUDGET.exhausted():
                break
            title = item.get("title", "")
            rid = item.get("roadshowId") or item.get("id")
            release_time = ""
            release_time_ms = None
            stime = item.get("stime") or item.get("releaseTime")
            if stime and str(stime).isdigit():
                try:
                    release_time_ms = int(stime)
                    release_time = _ms_to_bj_str(release_time_ms)
                except (ValueError, OSError):
                    release_time = str(stime)
            was_skip = False
            try:
                row = dump_meeting(session, item, release_time, db,
                                   force=args.force, release_time_ms=release_time_ms)
                if row["状态"] == "已跳过":
                    skipped += 1; page_skipped += 1; was_skip = True
                else:
                    added += 1; page_added += 1
                    cap.bump(); _BUDGET.bump()
                    print(f"  ✓ [{rid}] {title[:60]}")
            except SessionDead:
                raise
            except Exception as e:
                failed += 1; page_failed += 1
                print(f"  ✗ [{rid}] {title[:60]}  ERR: {e}")
            total_seen += 1
            if not was_skip:
                _THROTTLE.sleep_before_next()
            if args.max and total_seen >= args.max:
                break

        save_state(db, backfill_deep_page=page + 1,
                   backfill_last_page_at=datetime.now(timezone.utc),
                   in_progress=True)
        print(f"  [page {page}] +{page_added} ={page_skipped} ✗{page_failed} "
              f"(累计 +{added} ={skipped} ✗{failed})")

        if args.max and total_seen >= args.max:
            break
        has_more = (ld.get("extra") or {}).get("hasMore")
        if has_more is False or len(items) < args.page_size:
            print(f"  [page {page}] 列表到底 → 重置 backfill_deep_page=1")
            save_state(db, backfill_deep_page=1,
                       backfill_last_run_end_at=datetime.now(timezone.utc))
            break
        page += 1
        _THROTTLE.sleep_before_next()

    if first_top is not None and start_page == 1:
        save_state(db, top_roadshow_id=first_top)
    save_state(db, in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})
    print(f"  完成: +{added} ={skipped} ✗{failed}")
    return []


def run_once(session, db, args):
    """一轮抓取"""
    if getattr(args, "stream_backfill", False):
        return run_once_streaming(session, db, args)

    # 账户级接口 (首次或 --force)
    if db[COL_ACCOUNT].estimated_document_count() == 0 or args.force:
        dump_account(session, db)
    else:
        print("[账户] 已有数据, 跳过 (用 --force 可刷新)")

    # 断点恢复: 读取上次的 top_roadshow_id
    state = load_state(db)
    stop_id = state.get("top_roadshow_id") if args.resume else None
    if args.resume and stop_id:
        last = state.get("updated_at")
        print(f"[恢复] 上次爬取到 roadshowId={stop_id} (时间 {last}), 将在遇到该条目时停止")
    elif args.resume:
        print(f"[恢复] 未找到 checkpoint, 按普通模式全量爬")

    stop_ms = None
    if getattr(args, "since_hours", None) is not None:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        stop_ms = int(cutoff_dt.timestamp() * 1000)
        local_str = cutoff_dt.astimezone().strftime("%Y-%m-%d %H:%M")
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 (cutoff={local_str})")

    print(f"\n[列表] 抓取纪要列表 max={args.max or '全部'} page_size={args.page_size}")
    items = fetch_items_paginated(session, max_items=args.max,
                                  page_size=args.page_size,
                                  stop_at_roadshow_id=stop_id,
                                  stop_before_ms=stop_ms)
    print(f"[列表] 共 {len(items)} 条待处理\n")
    if not items:
        print("无新纪要 (或账号失效)")
        return []

    # 记录本轮顶部 id (用于下次 --resume)
    new_top = items[0].get("roadshowId")

    added = skipped = failed = 0
    cap = cap_from_args(args)
    pbar = tqdm(items, desc="抓取", unit="条", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}")
    for item in pbar:
        if cap.exhausted() or _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        title = item.get("title", "")
        rid = item.get("roadshowId") or item.get("id")

        release_time = ""
        release_time_ms = None
        stime = item.get("stime") or item.get("releaseTime")
        if stime and str(stime).isdigit():
            try:
                release_time_ms = int(stime)
                release_time = _ms_to_bj_str(release_time_ms)
            except (ValueError, OSError):
                release_time = str(stime)
        if not release_time:
            sut = item.get("summaryUpdateTime", "")
            if sut:
                release_time = str(sut).rsplit(" ", 1)[0][:16]

        was_skip = False
        try:
            row = dump_meeting(session, item, release_time, db,
                               force=args.force, release_time_ms=release_time_ms)
            if row["状态"] == "已跳过":
                skipped += 1
                was_skip = True
                tqdm.write(f"  · [{rid}] {title[:60]}  已存在, 跳过")
            else:
                added += 1
                cap.bump(); _BUDGET.bump()
                tqdm.write(f"  ✓ [{rid}] {title[:60]}  速览{row['速览字数']}字 章节{row['章节']} 指标{row['指标']} 对话{row['对话条数']}条")
        except SessionDead:
            raise
        except Exception as e:
            failed += 1
            tqdm.write(f"  ✗ [{rid}] {title[:60]}  ERR: {e}")

        pbar.set_postfix_str(f"新增={added} 跳过={skipped} 失败={failed}")

        # 每处理一条更新 checkpoint
        save_state(db, last_processed_roadshow_id=rid,
                   last_processed_at=datetime.now(timezone.utc),
                   in_progress=True)
        # DB dedup hits make no remote call — skip the throttle, matches the
        # optimization already in fetch_reports / oversea_reports loops.
        if not was_skip:
            _THROTTLE.sleep_before_next()
    pbar.close()

    # 本轮完成, 记录顶部 id (增量锚点)
    save_state(db, top_roadshow_id=new_top, in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})

    total = db[COL_MEETINGS].estimated_document_count()
    print(f"\n本轮完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}")
    print(f"MongoDB 当前纪要总数: {total}")
    print(f"Checkpoint 已更新, 下次加 --resume 可增量续爬")
    return {"added": added, "skipped": skipped, "failed": failed}


def parse_args():
    p = argparse.ArgumentParser(description="brm.comein.cn 纪要爬虫 (MongoDB 存储)")
    p.add_argument("--max", type=int, default=None,
                   help="最多爬取条数 (默认: 全部, 一直翻页直到没有更多)")
    p.add_argument("--page-size", type=int, default=40, help="每页大小 (默认 40)")
    p.add_argument("--force", action="store_true",
                   help="强制重爬已存在的会议 (默认跳过)")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 从上次 checkpoint 续爬, 遇到已知 top 停止分页")
    p.add_argument("--stream-backfill", action="store_true",
                   help="流式回填: 每抓完一页立即入库 + 保存 deep_page checkpoint")
    p.add_argument("--show-state", action="store_true",
                   help="显示当前 checkpoint 状态后退出")
    p.add_argument("--reset-state", action="store_true",
                   help="清除 checkpoint 后退出")
    p.add_argument("--today", action="store_true",
                   help="统计今天平台上的纪要条数(及与本地库对比)后退出")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today 指定日期 (默认今天)")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询新纪要. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600 = 10 分钟)")
    p.add_argument("--since-hours", type=float, default=None,
                   help="只抓取过去 N 小时内的纪要 (按 stime). 默认不限制.")
    # 研报模式
    p.add_argument("--reports", action="store_true",
                   help="研报模式: 抓 brm.comein.cn/reportManage/index 并下载 PDF")
    p.add_argument("--oversea-reports", action="store_true",
                   help="外资研报模式: 抓 brm.comein.cn/foreignResearch (实时 feed). 写 oversea_reports collection")
    p.add_argument("--skip-pdf", action="store_true",
                   help="研报模式下跳过 PDF 下载 (只入库元数据)")
    p.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT,
                   help=f"研报 PDF 存放目录 (默认 {PDF_DIR_DEFAULT}, 或 env JINMEN_PDF_DIR)")
    p.add_argument("--clean-reports", action="store_true",
                   help="清空 reports collection 和研报 checkpoint (不删 PDF 文件)")
    p.add_argument("--clean-oversea-reports", action="store_true",
                   help="清空 oversea_reports collection 和外资 checkpoint (不删 PDF)")
    # OTP WAF unlock — bypass aiSummaryAuth=0 gate
    p.add_argument("--otp-send", metavar="ROADSHOW_ID",
                   help="触发 WAF 短信到账号绑定手机 (+86 134****). 有 60s 冷却. "
                        "然后用 --otp-verify <roadshow_id> <code> 提交验证码.")
    p.add_argument("--otp-verify", nargs=2, metavar=("ROADSHOW_ID", "CODE"),
                   help="提交 6 位验证码解锁 + 重新抓该条目的完整内容 (正文 / 章节 / 速览 / 对话), "
                        "更新到 meetings collection.")
    p.add_argument("--list-locked", action="store_true",
                   help="列出所有 aiSummaryAuth=0 + hasAISummary=1 的待解锁纪要 (解锁后可以拿全文).")
    p.add_argument("--refetch-empty", action="store_true",
                   help="回填所有已入库但 stats 全 0 的纪要 — 这些是首次抓取"
                        "时平台还没生成 AI summary 的'空壳'条目. 重新走 detail"
                        "端点抓取正文/章节/对话. 每条 ~3-5s.")
    p.add_argument("--refetch-max", type=int, default=None,
                   help="配合 --refetch-empty 限制本轮最多处理多少条")

    # Token resolution priority: --auth flag > env JM_AUTH > credentials.json (auto_login output)
    # > hardcoded JM_AUTH_INFO. credentials.json wins over the constant once the user logs in
    # via /data-sources screencast — that's the whole point of the new login flow.
    _file_token = ""
    _creds_path = Path(__file__).resolve().parent / "credentials.json"
    if _creds_path.exists():
        try:
            _file_token = (json.loads(_creds_path.read_text(encoding="utf-8")).get("token") or "").strip()
        except Exception:
            _file_token = ""
    p.add_argument("--auth", default=os.environ.get("JM_AUTH", _file_token or JM_AUTH_INFO),
                   help="JM_AUTH_INFO (或 env JM_AUTH / credentials.json)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT,
                   help=f"MongoDB URI (默认 {MONGO_URI_DEFAULT}, 或环境变量 MONGO_URI)")
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT,
                   help=f"MongoDB 数据库名 (默认 {MONGO_DB_DEFAULT}, 或环境变量 MONGO_DB)")
    # 反爬节流 (crawl/antibot.py) — default_cap 2026-04-25 500→0: 实时档不再数量闸
    add_antibot_args(p, default_base=3.0, default_jitter=2.0,
                     default_burst=40, default_cap=0, platform="jinmen")
    return p.parse_args()


def connect_mongo(uri: str, dbname: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({uri}): {e}")
        sys.exit(1)
    db = client[dbname]
    # 索引
    db[COL_MEETINGS].create_index("title")
    db[COL_MEETINGS].create_index("release_time")
    db[COL_MEETINGS].create_index("organization")
    db[COL_MEETINGS].create_index("crawled_at")
    print(f"[Mongo] 已连接 {uri} -> db: {dbname}")
    return db


def main():
    args = parse_args()

    if not args.auth:
        print("错误: 未提供 JM_AUTH_INFO")
        print("请在脚本中配置 JM_AUTH_INFO, 或通过 --auth / 环境变量 JM_AUTH 传入")
        sys.exit(1)

    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform="jinmen")
    # account_id 取自 JM_AUTH_INFO 解出来的 uid (parse_auth 已 logged 出来).
    # 同一 uid 下 meetings / reports / oversea_reports 三个 worker 互斥路由,
    # 给 account_id 追加 category 后缀让每个子模块走独立 1500/24h 桶 —
    # reports 的历史回填不会挤占 meetings 实时增量的预算.
    _auth = parse_auth(args.auth) if isinstance(args.auth, str) else args.auth
    _base_account_id = (
        f"u_{_auth.get('uid', 'unknown')}" if isinstance(_auth, dict) else "u_unknown"
    )
    if args.oversea_reports:
        _category = "oversea_reports"
    elif args.reports:
        _category = "reports"
    else:
        _category = "meetings"
    _account_id = account_id_for_jinmen(_base_account_id, _category)
    _BUDGET = budget_from_args(args, account_id=_account_id, platform="jinmen")
    log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET,
                     extra=f"acct={_account_id} mode={_category}")

    db = connect_mongo(args.mongo_uri, args.mongo_db)
    # 研报集合索引 (首次运行自动建)
    db[COL_REPORTS].create_index("title")
    db[COL_REPORTS].create_index("release_time")
    db[COL_REPORTS].create_index("organization_name")
    db[COL_REPORTS].create_index("crawled_at")
    # 外资研报集合索引
    db[COL_OVERSEA_REPORTS].create_index("title")
    db[COL_OVERSEA_REPORTS].create_index("release_time")
    db[COL_OVERSEA_REPORTS].create_index("organization_name")
    db[COL_OVERSEA_REPORTS].create_index("crawled_at")

    if args.clean_reports:
        nr = db[COL_REPORTS].delete_many({}).deleted_count
        ns = db[COL_STATE].delete_many({"_id": "crawler_reports"}).deleted_count
        nd = db[COL_STATE].delete_many({"_id": {"$regex": "^daily_reports_"}}).deleted_count
        print(f"已清空 reports={nr}, checkpoint={ns}, daily_reports 统计={nd}")
        return

    if args.clean_oversea_reports:
        nr = db[COL_OVERSEA_REPORTS].delete_many({}).deleted_count
        ns = db[COL_STATE].delete_many({"_id": "crawler_oversea_reports"}).deleted_count
        print(f"已清空 oversea_reports={nr}, checkpoint={ns}")
        return

    if args.show_state:
        meet = load_state(db)
        rep = db[COL_STATE].find_one({"_id": "crawler_reports"}) or {}
        ove = db[COL_STATE].find_one({"_id": "crawler_oversea_reports"}) or {}
        def _fmt(s):
            return json.dumps({k: str(v) if isinstance(v, datetime) else v
                               for k, v in s.items()}, ensure_ascii=False, indent=2)
        print("--- 纪要 checkpoint (crawler) ---")
        print(_fmt(meet) if meet else "  无")
        print("\n--- 研报 checkpoint (crawler_reports) ---")
        print(_fmt(rep) if rep else "  无")
        print("\n--- 外资研报 checkpoint (crawler_oversea_reports) ---")
        print(_fmt(ove) if ove else "  无")
        print(f"\nmeetings: {db[COL_MEETINGS].estimated_document_count()}  "
              f"reports: {db[COL_REPORTS].estimated_document_count()}  "
              f"oversea_reports: {db[COL_OVERSEA_REPORTS].estimated_document_count()}  "
              f"account: {db[COL_ACCOUNT].estimated_document_count()}")
        return

    if args.reset_state:
        r = db[COL_STATE].delete_many({})
        print(f"已清除 {r.deleted_count} 条 checkpoint")
        return

    auth = parse_auth(args.auth)
    session = create_session(auth)

    # OTP unlock flow — before routing to list/detail workers.
    if args.list_locked:
        print("=== 待解锁的纪要 (hasAISummary=1 + aiSummaryAuth=0) ===")
        locked = list(db[COL_MEETINGS].find(
            {"list_item.hasAISummary": 1,
             "$or": [{"detail_auth.aiSummaryAuth": 0},
                     {"detail_auth.aiSummaryAuth": {"$exists": False}}]},
            {"_id": 1, "title": 1, "release_time": 1,
             "summary_info.uuid": 1, "summary_info.recipient": 1}
        ).sort("release_time_ms", -1).limit(50))
        if not locked:
            print("  无")
            return
        print(f"共 {len(locked)} 条 (展示前 50):\n")
        for x in locked:
            rid = x["_id"]
            title = (x.get("title") or "")[:55]
            recipient = (x.get("summary_info") or {}).get("recipient") or ""
            print(f"  roadshowId={rid}  [{x.get('release_time','?')}] "
                  f"{recipient}")
            print(f"    {title}")
        total = db[COL_MEETINGS].count_documents(
            {"list_item.hasAISummary": 1,
             "$or": [{"detail_auth.aiSummaryAuth": 0},
                     {"detail_auth.aiSummaryAuth": {"$exists": False}}]})
        print(f"\n全部: {total} 条. 解锁示例:")
        if locked:
            first_id = locked[0]["_id"]
            print(f"  python3 scraper.py --otp-send {first_id}")
            print(f"  (收到短信后)")
            print(f"  python3 scraper.py --otp-verify {first_id} <6位验证码>")
        return

    if args.otp_send:
        rid = args.otp_send
        # Look up uuid from Mongo
        doc = db[COL_MEETINGS].find_one(
            {"$or": [{"_id": int(rid) if rid.isdigit() else rid},
                     {"_id": str(rid)}]},
            {"title": 1, "summary_info.uuid": 1, "summary_info.recipient": 1,
             "list_item.hasAISummary": 1, "detail_auth.aiSummaryAuth": 1})
        if not doc:
            print(f"ERR: roadshowId={rid} 不在 meetings 库里")
            return
        uuid = (doc.get("summary_info") or {}).get("uuid") or ""
        if not uuid:
            print(f"ERR: 该条目没有 summary_info.uuid — 可能 platform 还没处理完 "
                  f"(hasAISummary={doc.get('list_item',{}).get('hasAISummary')})")
            return
        recipient = (doc.get("summary_info") or {}).get("recipient") or "(unknown)"
        print(f"[OTP] 条目: {(doc.get('title') or '')[:60]}")
        print(f"[OTP] 绑定手机: {recipient}")
        print(f"[OTP] 触发 SMS ...")
        r = send_otp(session, str(rid), uuid)
        code = str(r.get("code"))
        if code == "0":
            print(f"[OTP] ✓ 已发送. {r.get('data')}s 冷却. 收到短信后:")
            print(f"      python3 scraper.py --otp-verify {rid} <6位验证码>")
        elif code == "454":
            print(f"[OTP] ⚠ 发送频繁 (60s 冷却). 等一下再试. "
                  f"如刚才已收到短信可直接 --otp-verify.")
        else:
            print(f"[OTP] ✗ 失败 code={code} msg={r.get('msg')}")
        return

    if args.refetch_empty:
        # Find empty-content meetings and re-scrape them via dump_meeting with force=True
        from tqdm import tqdm
        now_ms = int(time.time() * 1000)
        coll = db[COL_MEETINGS]
        # Stats-all-zero OR missing fields + release_time at least 3h ago
        query = {
            "release_time_ms": {"$lt": now_ms - 3 * 3600 * 1000},
            "$or": [
                {"stats": {"$exists": False}},
                {"$and": [
                    {"stats.速览字数": {"$in": [0, None]}},
                    {"stats.章节": {"$in": [0, None]}},
                    {"stats.指标": {"$in": [0, None]}},
                    {"stats.对话条数": {"$in": [0, None]}},
                ]},
            ],
        }
        cursor = coll.find(query, {"_id": 1, "title": 1, "release_time": 1,
                                    "release_time_ms": 1, "list_item": 1,
                                    "organization": 1})
        if args.refetch_max:
            cursor = cursor.limit(args.refetch_max)
        items = list(cursor)
        if not items:
            print("[refetch-empty] 无待补条目")
            return
        print(f"[refetch-empty] 待补 {len(items)} 条空内容纪要 (全 stats=0 + "
              f">3h 前的会议)")

        added = 0; still_empty = 0; failed = 0
        pbar = tqdm(items, desc="refetch", unit="条",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}] {postfix}")
        for doc in pbar:
            rid = doc["_id"]
            title = (doc.get("title") or "")[:40]
            # Reconstruct enough of `item` for dump_meeting (list_item has original API shape)
            item = doc.get("list_item") or {}
            item["roadshowId"] = rid
            item["title"] = doc.get("title") or ""
            item["organizationName"] = doc.get("organization") or ""
            try:
                res = dump_meeting(session, item, doc.get("release_time") or "",
                                    db, force=True,
                                    release_time_ms=doc.get("release_time_ms"))
                # res has stats merged in
                got = int(res.get("速览字数", 0) or 0) + int(res.get("对话条数", 0) or 0)
                if got > 0:
                    added += 1
                    tqdm.write(f"  ✓ [{rid}] {title}  速览 {res.get('速览字数',0)}字 "
                               f"对话 {res.get('对话条数',0)} 章节 {res.get('章节',0)}")
                else:
                    still_empty += 1
                    tqdm.write(f"  · [{rid}] {title}  仍为空 (平台未生成)")
            except SessionDead as sd:
                # 整批停 — refetch-empty 可能跑几百条, 继续硬打只会把几百个
                # 条目都打 401 耗完 budget + 延长封禁. 打印后退出让用户重登.
                pbar.close()
                print(f"\n[refetch-empty] 会话已被吊销, 中止: {sd}")
                print(f"  已处理 ✓{added} 空{still_empty} ✗{failed}")
                return
            except Exception as e:
                failed += 1
                tqdm.write(f"  ✗ [{rid}] {title}  ERR: {e}")
            pbar.set_postfix_str(f"+{added} 空{still_empty} ✗{failed}")
            # Gentle throttle
            time.sleep(1.0 + (hash(str(rid)) & 0xff) / 255.0)
        pbar.close()
        print(f"\n[refetch-empty] 完成: 新补 {added} / 仍空 {still_empty} / 失败 {failed}")
        return

    if args.otp_verify:
        rid, otp_code = args.otp_verify
        print(f"[OTP] 提交验证码 roadshowId={rid} code={otp_code} ...")
        res = unlock_and_refetch(session, db, rid, otp_code)
        if res.get("verified"):
            print(f"[OTP] ✓ 验证通过 aiSummaryAuth={res.get('aiSummaryAuth')}")
            print(f"      正文 {res.get('content_chars', 0)} 字  "
                  f"章节 {res.get('chapters', 0)}  "
                  f"指标 {res.get('indicators', 0)}  "
                  f"对话 {res.get('transcript_items', 0)}")
            print(f"      → 已写入 Mongo. 前端刷新即可看到.")
            if res.get("refetch_err"):
                print(f"      [warn] {res.get('refetch_err')}")
        else:
            print(f"[OTP] ✗ {res.get('error')}")
        return

    if args.today:
        if args.oversea_reports:
            print("[--today] 外资研报暂未实现日统计 (oversea API 不返当日条数), 跳过")
        elif args.reports:
            count_reports_today(session, db, date_str=args.date)
        else:
            count_today(session, db, date_str=args.date)
        return

    # 路由: 纪要 / 研报 / 外资研报
    if args.oversea_reports:
        worker = run_oversea_reports_once
        mode = "外资研报"
    elif args.reports:
        worker = run_reports_once
        mode = "研报"
    else:
        worker = run_once
        mode = "纪要"

    if args.watch:
        print(f"[实时模式/{mode}] 每 {args.interval}s 轮询一次. Ctrl+C 退出.\n")
        round_num = 0
        while True:
            round_num += 1
            print(f"\n{'='*60}\n[轮次 {round_num}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}")
            try:
                worker(session, db, args)
            except KeyboardInterrupt:
                print("\n[实时模式] Ctrl+C 退出")
                break
            except SessionDead as sd:
                # 401/403 = token 被吊销. 继续 --interval 秒后重试只会拿同一失效
                # token 硬打, AccountBudget 烧光 + 延长封禁 + crawler_monitor 里
                # 仍显示进程活着. 直接退出让 credential_manager 重登后再启.
                print(f"\n[实时模式] 会话已被吊销, 立即退出等重登: {sd}")
                print("          /data-sources UI 重新登录更新 JM_AUTH_INFO 后, "
                      "crawler_monitor 会自动重启此 watcher.")
                break
            except Exception as e:
                print(f"[轮次 {round_num}] 异常: {e}")
            try:
                time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\n[实时模式] Ctrl+C 退出")
                break
    else:
        worker(session, db, args)


if __name__ == "__main__":
    main()
