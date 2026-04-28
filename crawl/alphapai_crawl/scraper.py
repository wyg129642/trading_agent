#!/usr/bin/env python3
"""
alphapai-web.rabyte.cn 多分类爬虫 (MongoDB 存储)

抓取四大数据类别：
  roadshow  会议/路演纪要
  comment   券商点评
  report    券商研报
  wechat    社媒（微信公众号）

使用方法:
  1. 浏览器登录 alphapai-web.rabyte.cn
  2. F12 → Application → Local Storage → 复制 USER_AUTH_TOKEN (JWT)
  3. 粘贴到下方 USER_AUTH_TOKEN 变量, 或通过 --auth / 环境变量 JM_AUTH 传入
  4. 启动 MongoDB (默认 mongodb://localhost:27017)
  5. 运行:
       python scraper.py --max 100                      # 各分类各爬 100 条
       python scraper.py --category roadshow --max 200  # 单分类
       python scraper.py                                # 全量爬取所有分类
       python scraper.py --watch --interval 600         # 实时模式 10 分钟一次
       python scraper.py --resume                       # 增量(只抓上次后新增)
       python scraper.py --today                        # 今日统计
       python scraper.py --show-state                   # 查看 checkpoint
"""

import argparse
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from tqdm import tqdm

# 共享反爬模块 (crawl/antibot.py)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from antibot import (  # noqa: E402
    AdaptiveThrottle, DailyCap, SessionDead,
    parse_retry_after, is_auth_dead,
    add_antibot_args, throttle_from_args, cap_from_args,
    AccountBudget, SoftCooldown, detect_soft_warning,
    headers_for_platform, log_config_stamp, budget_from_args,
    account_id_for_alphapai, warmup_session,
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402

# 模块级 throttle, main() 用 CLI 覆盖
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=3.0, jitter=2.0,
                                                burst_size=40,
                                                platform="alphapai")
# 模块级账号预算 (跨进程 24h 滚动窗, Redis 共享)
_BUDGET: AccountBudget = AccountBudget("alphapai", "default", 0)
_PLATFORM = "alphapai"
# 当前 watcher 进程绑定的账号 ID (从 JWT 解出). 多 token 池场景下,
# api_call 检测到 daily-quota 时 mark 这个 ID, watch loop 切到下一个可用 token,
# 全部耗尽则 sleep 到 BJ 第二天 0:02.
_CURRENT_ACCOUNT_ID: str = ""

# ==================== 请配置以下内容 ====================

# 从浏览器 localStorage 复制 USER_AUTH_TOKEN 的值（完整 JWT 字符串）
# 步骤: F12 → Application → Local Storage → https://alphapai-web.rabyte.cn → USER_AUTH_TOKEN
USER_AUTH_TOKEN = (
    "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIxMDA4NzkxMTY2NDkxMDMzNjAwIiwic3ViIjoie1wi"
    "aW50ZXJuYWxcIjpmYWxzZSxcImxvZ2luVGltZVwiOlwiMjAyNi0wNC0xNlQxNToyMTowNi44"
    "NDJcIixcImxvZ2luVHlwZVwiOjIsXCJwbGF0Zm9ybVwiOlwid2ViXCIsXCJ1aWRcIjpcIjEw"
    "MDg3OTExNjY0OTEwMzM2MDBcIn0iLCJpc3MiOiJyYWJ5dGUiLCJpYXQiOjE3NzYzMjQwNjYs"
    "ImV4cCI6MTc3ODkxNjA2Nn0.wZFZuAug6EOLURs_z6eBXJUYi3GzY7sg3rf2gdOH_zo"
)

# ==================== 以下无需修改 ====================

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"


def _load_tokens_from_file() -> list:
    """读 credentials.json 返回 token 列表(支持新旧两种 schema).

    新版 (多账号 token 池):
        {"tokens": [{"token":"...", "label":"main", "updated_at":"..."},
                    {"token":"...", "label":"alt1", ...}],
         "schema_version": 2}

    旧版 (单账号, 向后兼容):
        {"token": "...", "updated_at": "..."}

    返回: [{token, label, updated_at}, ...] 或 [] 表示无 token.
    """
    if not CREDS_FILE.exists():
        return []
    try:
        d = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
        if isinstance(d, dict):
            if isinstance(d.get("tokens"), list):
                return [t for t in d["tokens"]
                        if isinstance(t, dict) and t.get("token")]
            if d.get("token"):
                return [{"token": d["token"].strip(),
                         "label": "main",
                         "updated_at": d.get("updated_at", "")}]
        return []
    except Exception:
        return []


def _load_token_from_file() -> str:
    """向后兼容: 返回 token 池里第一个 token."""
    tokens = _load_tokens_from_file()
    return tokens[0]["token"].strip() if tokens else ""


# -------- Token 池跨进程协调 --------

class TokenExhausted(Exception):
    """当前 token 当日额度耗尽; watch loop 应切换 token 或 sleep 到午夜."""


_TOKEN_EXHAUSTED_KEY = "crawl:tokenpool:alphapai:exhausted_today:{account_id}:{category}"


def _redis_or_none():
    """复用 antibot 的 Redis 客户端 (容错). 失败返 None."""
    try:
        from antibot import _redis_client  # type: ignore
        return _redis_client()
    except Exception:
        try:
            import redis as _r
            host = os.environ.get("REDIS_HOST", "127.0.0.1")
            port = int(os.environ.get("REDIS_PORT", "6379"))
            c = _r.Redis(host=host, port=port, decode_responses=True,
                         socket_timeout=2.0, socket_connect_timeout=2.0)
            c.ping()
            return c
        except Exception:
            return None


def _seconds_until_bj_midnight(extra_seconds: int = 120) -> int:
    """到 BJ 第二天 0 点 + extra 秒. 单账号每日 quota 重置点."""
    BJ = timezone(timedelta(hours=8))
    now = datetime.now(BJ)
    tomorrow = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    return max(60, int((tomorrow - now).total_seconds()) + extra_seconds)


def is_token_exhausted(account_id: str, category: str = "") -> bool:
    """exhaust flag 是 **per-category** 的: 只有 alphapai roadshow 详情端有
    每日 400000 配额, comment / report 列表+详情都没限额. category 为空时
    返 False (相当于"无限额"语义)."""
    if not account_id or not category:
        return False
    r = _redis_or_none()
    if r is None:
        return False
    try:
        return bool(r.exists(
            _TOKEN_EXHAUSTED_KEY.format(account_id=account_id, category=category)))
    except Exception:
        return False


def mark_token_exhausted(account_id: str, category: str = "") -> None:
    """标该 (账号, 类别) 今日额度耗尽; TTL 到 BJ 第二天 0:02. 不传 category
    视为 noop — 防止误标 account-wide 把 comment / report watcher 也卡死."""
    if not account_id or not category:
        return
    r = _redis_or_none()
    if r is None:
        return
    try:
        r.set(_TOKEN_EXHAUSTED_KEY.format(account_id=account_id, category=category),
              "1", ex=_seconds_until_bj_midnight(extra_seconds=120))
    except Exception:
        pass


def pick_available_token(tokens: list, label_hint: str = "",
                         category: str = "") -> dict:
    """从 token list 里挑一个未耗尽的; 用 label_hint hash 让多 watcher 分到不同 token.

    `category` 为空 → 不按类别过滤 (向后兼容); 否则只过滤该类别已 exhaust 的 token.

    返回的 dict 含 token / label / account_id (后者 JWT 解出).
    全部耗尽返 None — 调用方决定 sleep-until-midnight 或退出.
    """
    if not tokens:
        return None
    enriched = []
    for t in tokens:
        tok = (t.get("token") or "").strip()
        if not tok:
            continue
        acc_id = t.get("account_id") or _account_id_from_token(tok)
        enriched.append({**t, "token": tok, "account_id": acc_id})
    if not enriched:
        return None
    if category:
        available = [t for t in enriched
                     if not is_token_exhausted(t["account_id"], category)]
    else:
        available = enriched
    if not available:
        return None
    seed = abs(hash(label_hint or "default"))
    return available[seed % len(available)]


API_BASE = "https://alphapai-web.rabyte.cn/external/alpha/api"
STORAGE_REPORT_BASE = "https://alphapai-storage.rabyte.cn/report/"

# MongoDB 配置 (2026-04-23 迁出至远端 192.168.31.176:35002, 2026-04-26 迁回本机 ta-mongo-crawl :27018)
MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "alphapai-full")
COL_ACCOUNT = "account"
COL_STATE = "_state"  # checkpoint / 当日统计

# 研报 PDF 下载目录 (可被 --pdf-dir 或 env ALPHAPAI_PDF_DIR 覆盖).
# 2026-04-26: 从 SMB 共享盘 /mnt/share/ygwang/alphapai_pdfs 退役,迁回本机
# /home/ygwang/crawl_data/alphapai_pdfs (本地 SSD, 38T 容量, 与其他平台 PDF
# 同盘). env ALPHAPAI_PDF_DIR (.env 已配) 照常覆盖默认.
PDF_DIR_DEFAULT = os.environ.get(
    "ALPHAPAI_PDF_DIR",
    "/home/ygwang/crawl_data/alphapai_pdfs",
)

OK_CODE = 200000  # AlphaPai 成功码 (不是 0)


# -------------------- 通用工具 --------------------

def _hash_id(*parts: Any) -> str:
    """SHA1 哈希作为稳定主键 (适用于 id 是会话级加密 token 的分类)"""
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _html_to_md(s: str) -> str:
    """简易 HTML → Markdown 转换, 保留段落/标题/列表结构.

    Alphapai roadshow 的 detail.aiSummary.content 是含 inline style 的 HTML,
    用正则替换常见块级标签, 剩余标签一律剥离. 不需要完整解析器.
    """
    if not s or not isinstance(s, str):
        return ""
    # 标题
    s = re.sub(r"<h1[^>]*>", "\n# ",   s, flags=re.I)
    s = re.sub(r"</h1>",     "\n\n",   s, flags=re.I)
    s = re.sub(r"<h2[^>]*>", "\n## ",  s, flags=re.I)
    s = re.sub(r"</h2>",     "\n\n",   s, flags=re.I)
    s = re.sub(r"<h3[^>]*>", "\n### ", s, flags=re.I)
    s = re.sub(r"</h3>",     "\n\n",   s, flags=re.I)
    s = re.sub(r"<h4[^>]*>", "\n#### ", s, flags=re.I)
    s = re.sub(r"</h4>",     "\n\n",   s, flags=re.I)
    # 段落 / 换行
    s = re.sub(r"<br\s*/?>",            "\n",  s, flags=re.I)
    s = re.sub(r"</p\s*>",              "\n\n", s, flags=re.I)
    s = re.sub(r"<p\b[^>]*>",           "",     s, flags=re.I)
    # 列表项
    s = re.sub(r"<li[^>]*>",            "\n- ", s, flags=re.I)
    s = re.sub(r"</li\s*>",             "",     s, flags=re.I)
    s = re.sub(r"</?ul[^>]*>|</?ol[^>]*>", "\n", s, flags=re.I)
    # Strong / em 变粗体/斜体
    s = re.sub(r"<(strong|b)\b[^>]*>",  "**",   s, flags=re.I)
    s = re.sub(r"</(strong|b)>",        "**",   s, flags=re.I)
    s = re.sub(r"<(em|i)\b[^>]*>",      "*",    s, flags=re.I)
    s = re.sub(r"</(em|i)>",            "*",    s, flags=re.I)
    # 剩余所有标签 → 删
    s = re.sub(r"<[^>]+>", "", s)
    # HTML entity
    s = html.unescape(s)
    # 压 3+ 空行为 2
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _extract_mt_transcript(mt_content: str) -> str:
    """解析 mtSummary.content (JSON array of per-utterance entries) → Markdown 逐字稿.

    每条形如 {"role": "2", "bg": 64802, "ed": 66524, "content": "审慎..."} —
    role 是说话人 ID, bg/ed 是毫秒时间戳, content 是话语. 我们按时间顺序拼接,
    发言人切换时插入 **[发言人 N]** 标记. 同一 detail 调用里 mtSummary 的信息量
    通常是 aiSummary.content 的 3-6 倍 (真实逐字稿 vs AI 摘要).
    """
    if not isinstance(mt_content, str) or not mt_content.startswith("["):
        return ""
    try:
        arr = json.loads(mt_content)
    except (ValueError, TypeError):
        return ""
    if not isinstance(arr, list):
        return ""
    parts: list[str] = []
    last_role = None
    for u in arr:
        if not isinstance(u, dict):
            continue
        role = u.get("role")
        content = (u.get("content") or "").strip()
        if not content:
            continue
        if role != last_role:
            parts.append(f"\n\n**[发言人 {role}]**\n\n")
            last_role = role
        parts.append(content)
    return " ".join(parts).strip()


def _extract_v3_qa(qa_list: list) -> str:
    """aiSummaryV3.qaList → Markdown Q&A."""
    if not isinstance(qa_list, list):
        return ""
    out = []
    for qa in qa_list:
        if not isinstance(qa, dict):
            continue
        q = (qa.get("q") or "").strip()
        a = (qa.get("a") or "").strip()
        st = qa.get("startTime") or ""
        if not (q or a):
            continue
        head = f"**Q"
        if st:
            head += f" [{st}]"
        head += f"**: {q}"
        out.append(head)
        if a:
            out.append(f"**A**: {a}")
        out.append("")
    return "\n".join(out).strip()


def _extract_v3_bullets(bullets: list) -> str:
    """aiSummaryV3.topicBulletsV2 → Markdown 要点大纲.

    结构: [{title, points: [{content: [{text, isBold}, ...], subPoints: [...], depth}]}]
    """
    if not isinstance(bullets, list):
        return ""

    def render_text_parts(parts):
        if not isinstance(parts, list):
            return ""
        out = []
        for p in parts:
            if not isinstance(p, dict):
                continue
            t = p.get("text") or ""
            if p.get("isBold"):
                out.append(f"**{t}**")
            else:
                out.append(t)
        return "".join(out)

    def render_point(p, depth=0):
        indent = "  " * depth
        line = indent + "- " + render_text_parts(p.get("content"))
        lines = [line]
        for sub in p.get("subPoints") or []:
            if isinstance(sub, dict):
                lines.append(render_point(sub, depth + 1))
        return "\n".join(lines)

    sections = []
    for sec in bullets:
        if not isinstance(sec, dict):
            continue
        title = (sec.get("title") or "").strip()
        sec_lines = []
        if title:
            sec_lines.append(f"## {title}")
        for p in sec.get("points") or []:
            if isinstance(p, dict):
                sec_lines.append(render_point(p))
        if sec_lines:
            sections.append("\n".join(sec_lines))
    return "\n\n".join(sections).strip()


def _extract_roadshow_content(detail: dict) -> tuple[str, str]:
    """从 roadshow detail 提取 (正文, 分段摘要).

    字段优先级 (2026-04-22 起多源叠加, 单次 detail 调用产出最大化):
      1. detail.aiSummary.content          — HTML 结构化 AI 纪要 (3-18k 字)
      2. detail.aiSummaryV3.topicBulletsV2 — 结构化要点大纲 (5-15k 字)
      3. detail.aiSummaryV3.qaList         — 结构化 Q&A (3-6k 字)
      4. detail.mtSummary.content          — 原始逐字稿 JSON (解析后 10-20k 字)
      5. detail.usSummary.content          — 美股/港股英文原稿 (20-140k 字)
      6. detail.summarySegmentList         — 分段摘要 (兜底)
      7. detail.aiSummaryV3.fullTextSummary — 短摘要兜底

    返回 (combined_md, segments_md). combined_md 是所有可用内容拼接, 单条纪要
    实测能从 aiSummary-only 的 18k 扩到 60-160k 字 (无需额外 API 调用).
    """
    if not isinstance(detail, dict):
        return "", ""

    sections: list[str] = []

    # 1. aiSummary.content (主要 AI 纪要 HTML → MD)
    ai_sum = detail.get("aiSummary") or {}
    html_content = ai_sum.get("content") if isinstance(ai_sum, dict) else ""
    ai_md = _html_to_md(html_content) if html_content else ""
    if ai_md:
        sections.append(ai_md)

    # 2. aiSummaryV3.topicBulletsV2 (结构化要点)
    v3 = detail.get("aiSummaryV3") or {}
    bullets_md = ""
    if isinstance(v3, dict):
        bullets_md = _extract_v3_bullets(v3.get("topicBulletsV2") or v3.get("topicBullets") or [])
        if bullets_md:
            sections.append("## 结构化要点\n\n" + bullets_md)

    # 3. aiSummaryV3.qaList (Q&A)
    qa_md = ""
    if isinstance(v3, dict):
        qa_md = _extract_v3_qa(v3.get("qaListV2") or v3.get("qaList") or [])
        if qa_md:
            sections.append("## 问答\n\n" + qa_md)

    # 4. mtSummary.content (原始逐字稿)
    mt_sum = detail.get("mtSummary") or {}
    mt_content = mt_sum.get("content") if isinstance(mt_sum, dict) else ""
    mt_md = _extract_mt_transcript(mt_content) if mt_content else ""
    if mt_md:
        sections.append("## 逐字稿\n\n" + mt_md)

    # 5. usSummary.content (英文原稿)
    us_sum = detail.get("usSummary") or {}
    us_html = us_sum.get("content") if isinstance(us_sum, dict) else ""
    us_md = _html_to_md(us_html) if us_html else ""
    if us_md:
        sections.append("## Original Transcript\n\n" + us_md)

    # 6. summarySegmentList (分段摘要)
    segs = detail.get("summarySegmentList") or []
    seg_parts = []
    if isinstance(segs, list):
        for s in segs:
            if not isinstance(s, dict):
                continue
            t = (s.get("title") or "").strip()
            su = (s.get("summary") or "").strip()
            st = s.get("startTime") or ""
            if t or su:
                header = f"## {t}" if t else "##"
                if st:
                    header += f"  *[{st}]*"
                seg_parts.append(header + ("\n\n" + su if su else ""))
    seg_md = "\n\n".join(seg_parts)
    if seg_md:
        sections.append("## 分段摘要\n\n" + seg_md)

    # 7. 兜底: aiSummaryV3.fullTextSummary (只在主摘要为空时用, 避免重复)
    if not sections and isinstance(v3, dict):
        short = v3.get("fullTextSummary") or ""
        if short:
            sections.append(short)

    combined = "\n\n---\n\n".join(sections)
    return combined, seg_md


def parse_jwt(token: str) -> dict:
    """从 JWT 中提取 uid (不验签, 仅 payload)."""
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        import base64
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        sub = payload.get("sub", "{}")
        if isinstance(sub, str):
            sub = json.loads(sub)
        return {"uid": sub.get("uid", ""), "exp": payload.get("exp"),
                "iat": payload.get("iat")}
    except Exception:
        return {}


def create_session(token: str) -> requests.Session:
    s = requests.Session()
    # 用 antibot.headers_for_platform 取按 process label 稳定 hash 的 UA + 对齐
    # locale + sec-ch-ua hints. 18 个 watcher 各自拿到不同 UA, 不再单一指纹.
    h = headers_for_platform("alphapai")
    h.update({
        "Content-Type": "application/json",
        "Authorization": token,
        "x-from": "web",
        "platform": "web",
    })
    s.headers.update(h)
    # Warmup: 先 GET 一次 landing HTML 再跑 XHR — 真人打开 SPA 必然的顺序,
    # 直接干 API 是硬 bot 指纹. 幂等, 失败不影响后续.
    warmup_session(s, "alphapai")
    return s


def _account_id_from_token(token: str) -> str:
    """JWT 第二段 payload 解出 uid (或回退到 token 前 12 字符 hash).
    AccountBudget 用这个绑账号 — 同一账号多 watcher 共享 24h 配额."""
    import base64 as _b64
    import json as _json
    try:
        parts = (token or "").split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = _json.loads(_b64.urlsafe_b64decode(payload_b64))
            # AlphaPai's `sub` is itself a JSON-string blob with uid inside,
            # so try to peel one more layer.
            sub = payload.get("sub")
            if isinstance(sub, str) and sub.startswith("{"):
                try:
                    inner = _json.loads(sub)
                    if isinstance(inner, dict):
                        for k in ("uid", "userId", "user_id", "id"):
                            v = inner.get(k)
                            if isinstance(v, (str, int)) and str(v):
                                return f"u_{v}"
                except Exception:
                    pass
            for k in ("uid", "userId", "user_id", "id", "sub"):
                v = payload.get(k)
                if isinstance(v, (str, int)) and str(v):
                    return f"u_{v}"
    except Exception:
        pass
    import hashlib as _hl
    return "h_" + _hl.md5((token or "").encode()).hexdigest()[:12]


def api_call(session: requests.Session, method: str, path: str,
             json_body: Optional[dict] = None, retries: int = 2,
             timeout: int = 20) -> dict:
    """调用 AlphaPai API. 失败时返回 {code, message, data} 形式的错误响应.

    - 401/403 → 抛 SessionDead (会话已吊销, 不要重试, 调用方应退出)
    - 429/5xx → 指数退避 + 尊重 Retry-After
    - 网络错误 → 最多 `retries` 次重试
    """
    url = f"{API_BASE}/{path.lstrip('/')}"
    last_exc = None
    # 先检查全平台 SoftCooldown — 如果本平台正在冷却 (e.g. 另一 watcher 刚命中
    # code=400000 / code=7), 立即阻塞直到清除. 不加这一步会导致每轮 watch
    # 触发第一个请求 → 又打到 quota endpoint → 又 trigger cooldown → 永远续命.
    # 之前的设计只在 _THROTTLE.sleep_before_next() 里 wait, 而 api_call 的
    # 第 1 个请求不走 sleep, 所以每轮 watch tick 照样打 quota 端点.
    SoftCooldown.wait_if_active(_PLATFORM)
    for attempt in range(1, retries + 2):
        try:
            r = session.request(method, url, json=json_body, timeout=timeout)
            if is_auth_dead(r.status_code):
                raise SessionDead(f"HTTP {r.status_code} on {path}: {r.text[:200]}")
            if r.status_code == 429 or 500 <= r.status_code < 600:
                # 软警告: 429 / 5xx 触发同平台全局冷却 (10 min), 而不是单请求重试
                if r.status_code == 429:
                    SoftCooldown.trigger(_PLATFORM, reason=f"http_429:{path}",
                                          minutes=10)
                ra = parse_retry_after(r.headers.get("Retry-After"))
                _THROTTLE.on_retry(retry_after_sec=ra, attempt=attempt)
                _THROTTLE.sleep_before_next()
                last_exc = f"HTTP {r.status_code}"
                continue
            if r.status_code != 200:
                return {"code": r.status_code, "message": f"HTTP {r.status_code}",
                        "data": None}
            try:
                body = r.json()
            except ValueError:
                body = None
            # 业务层软警告检测. 口径:
            #   - report detail code=400000 "已达到今日查看上限": 账号级每日配额,
            #     **不触发 SoftCooldown** (否则 roadshow/comment 陪葬且 watch
            #     重启持续续命); 只做本地 on_warning() 轻微减速, 让 dump_one
            #     用 content_truncated=True 标记等明天配额重置.
            #   - roadshow list hasPermission=False / code=7: 同样是日配额, 只本地
            #     on_warning, 交给 scraper 业务层处理.
            #   - 限流关键词 / waf cookie / code=10001/1010 / http 429 5xx: 真
            #     风控/WAF 信号 → SoftCooldown 60 min 跨 watcher 静默.
            reason = detect_soft_warning(r.status_code, body=body,
                                          text_preview=r.text[:400] if r.text else "",
                                          cookies=dict(r.cookies),
                                          platform=_PLATFORM)
            if reason:
                # **每日分类配额** 信号, 不触发平台级冷却 (否则 roadshow/comment
                # 陪葬 + watch 重启持续续命). 所有落到 detect_soft_warning 的
                # 路径都要覆盖:
                #   - reason 含 "400000"  (走 _PLATFORM_SOFT_BIZ_CODES 查表, 现
                #                         已清空, 保留兼容)
                #   - reason == "quota_code_7"  (roadshow hasPermission=False)
                #   - reason 以 "msg:" 开头且命中今日配额中文提示 (keyword 路径)
                is_daily_quota = (
                    "400000" in reason
                    or reason == "quota_code_7"
                    or reason.startswith("msg:已达到今日查看上限")
                    or reason.startswith("msg:请明日再来")
                    or reason.startswith("msg:次数已达上限")
                    or reason.startswith("msg:查看次数已达")
                    or reason.startswith("msg:您已达上限")
                )
                if is_daily_quota:
                    _THROTTLE.on_warning()
                    # 撞到日配额 (400000 / 今日查看上限). alphapai 实测**只有
                    # roadshow detail** 会触发, comment / report 端点没有限额.
                    # 所以 mark 是 **per-category** 的 — 用 path 判定:
                    #   `reading/roadshow/...` → 标 roadshow flag → 仅停 7 个
                    #   roadshow watcher; comment / report watcher 看不到这个
                    #   flag, 继续正常运行.
                    # 万一未来 report / comment 也出现 400000, 在这里加 elif
                    # 标对应类别即可.
                    if _CURRENT_ACCOUNT_ID and "roadshow" in (path or ""):
                        mark_token_exhausted(_CURRENT_ACCOUNT_ID, "roadshow")
                else:
                    SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=10)
                    _THROTTLE.on_warning()
            return body if body is not None else {"code": -1,
                "message": "non-json response", "data": None}
        except SessionDead:
            raise
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            if attempt < retries + 1:
                _THROTTLE.on_retry(attempt=attempt)
                _THROTTLE.sleep_before_next()
    return {"code": -1, "message": f"req_err: {last_exc}", "data": None}


# -------------------- 分类配置 --------------------
#
# 每个分类描述:
#   collection      MongoDB 集合名
#   list_path       列表接口 path (POST)
#   detail_path     详情接口 path 模板 (GET); None = 不抓详情
#   detail_id_key   detail URL 中的 id 参数名 ("id" 或 "commentId")
#   detail_extra    从 list item 取额外参数 (如 wechat 需要 supplierId)
#   time_field      列表项里表示发布时间的字段名 (用于 _id 生成 / today 截断)
#   id_strategy     "raw" = 直接用 item.id ; "hash" = SHA1(title+time)
#   web_url_fmt     用于打印的人类阅读 URL
#   list_extra_body 列表请求 body 默认值 (如 sortType)
# -------------------------------------------------------

CATEGORIES: Dict[str, Dict[str, Any]] = {
    "roadshow": {
        "collection": "roadshows",
        "label": "会议/路演",
        "list_path": "reading/roadshow/summary/list",
        # 2026-04-22 关键发现: `reading/summary/detail` 是 *轻量 metadata* 端点,
        # 不含 content. `reading/roadshow/summary/detail` 才是 *重量* 端点,
        # 返回 aiSummary.content (3-8k 字 AI 纪要) + usSummary.content
        # (英文原会议稿 20k+ 词). **这个端点不走 list 的 100/天 quota gate** —
        # 对 list 返回 hasPermission=False 的条目, detail 照样 200 + 完整内容.
        # 这就是每天 100 条上限的真正旁路.
        "detail_path": "reading/roadshow/summary/detail",
        "detail_id_key": "id",
        "detail_extra": (),  # 仅 id
        "time_field": "date",  # fallbacks below
        "id_strategy": "hash",
        "web_url_fmt": "https://alphapai-web.rabyte.cn/reading/home/meeting/detail?articleId={id}",
        "list_extra_body": {},
    },
    "comment": {
        "collection": "comments",
        "label": "券商点评",
        "list_path": "reading/comment/list",
        "detail_path": "reading/comment/detail",
        "detail_id_key": "commentId",  # 注意: 不是 id !
        "detail_extra": (),
        "time_field": "time",
        "id_strategy": "raw",  # HCMT... 是稳定 ID
        "web_url_fmt": "https://alphapai-web.rabyte.cn/reading/home/comment",  # 列表页, 详情走 url 字段
        "list_extra_body": {},
    },
    "report": {
        "collection": "reports",
        "label": "券商研报",
        # 2026-04-22: 切到 v2 端点. v1 (reading/report/list) 漏外资研报 (JPMorgan /
        # Prudential / 韩国 / 日本 / 澳大利亚 等). v2 (reading/report/list/v2) 是平台
        # UI 当前真实路径, 同等 page 多约 25% 条目, 全部为外资 + 翻译版. 旧的"v2 服务器
        # 500"是当时 reportType 单值未带列表, 现在改成 reportType:[1004] / countryRegionCode:[4]
        # 即可, 但默认无 filter 也照常工作.
        "list_path": "reading/report/list/v2",
        "detail_path": "reading/report/detail",
        "detail_id_key": "id",
        # 必须带 version (2026-04 起平台变化: 无 version 则 content 返回 None,
        # 带上才返回完整"核心观点"文本)
        "detail_extra": ("version",),
        "time_field": "time",
        "id_strategy": "hash",
        "web_url_fmt": "https://alphapai-web.rabyte.cn/reading/home/point/detail?articleId={id}",
        "list_extra_body": {},
    },
    "wechat": {
        "collection": "wechat_articles",
        "label": "社媒/微信",
        "list_path": "reading/social/media/wechat/article/list",
        "detail_path": "reading/social/media/wechat/article/detail",
        "detail_id_key": "id",
        "detail_extra": ("supplierId",),  # wechat 需要 supplierId 参数
        "time_field": "publishDate",
        "id_strategy": "raw",  # RAR... 是稳定 ID
        "web_url_fmt": "https://alphapai-web.rabyte.cn/reading/social-media/detail?articleId={id}&supplierId={supplierId}",
        "list_extra_body": {},
    },
}

# wechat 社媒/公众号爬取 2026-04-24 永久停用 — 信号质量低, 已入库数据保留供查询,
# 不再抓新增. CATEGORIES dict 保留 wechat 条目是为历史数据 API 兼容 (/detail
# 端点仍能按旧 id 查, 归档数据不至于 404). 但 --category all 默认只走
# CATEGORY_ORDER, 从这里剥离后, run_once() 再也不会自动触发 wechat 分类.
# 如果未来恢复 wechat, 把它加回 CATEGORY_ORDER, 并且重新打开 crawler_monitor
# ALL_SCRAPERS 里被注释的那行.
CATEGORY_ORDER = ["roadshow", "comment", "report"]


# Per-category sub-types discovered via CDP on each SPA tab page.  Each entry's
# `body` dict is merged into the list POST body at fetch time.  Keys become the
# value stamped into `_{category}_subcategories` (array) on ingested docs.
SUBTYPES: Dict[str, Dict[str, Dict[str, Any]]] = {
    "roadshow": {
        "ashare": {"label": "A股会议",          "body": {"marketTypeV2": 10}},
        "hk":     {"label": "港股会议",          "body": {"marketTypeV2": 50}},
        "us":     {"label": "美股会议",          "body": {"marketTypeV2": 20}},
        "web":    {"label": "网络资源",          "body": {"marketTypeV2": 30}},
        "ir":     {"label": "投资者关系记录",     "body": {"marketTypeV2": 60}},
        "hot":    {"label": "热门会议",          "body": {"marketTypeV2": 70}, "window_hours": 24},
    },
    "report": {
        "ashare": {"label": "内资报告",          "body": {"marketType": 21, "usReport": False}},
        "us":     {"label": "外资报告",          "body": {"marketType": 30, "usReport": True}},
        "indep":  {"label": "独立研究",          "body": {"marketType": 90, "usReport": False}},
    },
    "comment": {
        "selected": {"label": "干货点评",        "body": {"isSelected": True}},
        "regular":  {"label": "日报周报",        "body": {"isRegular":  True}},
    },
}

# Legacy alias so older callers still resolve (roadshow-only market types).
ROADSHOW_MARKET_TYPES: Dict[str, Dict[str, Any]] = {
    k: {"marketTypeV2": v["body"].get("marketTypeV2"),
        "label": v["label"],
        **({"window_hours": v["window_hours"]} if "window_hours" in v else {})}
    for k, v in SUBTYPES["roadshow"].items()
}


# -------------------- 时间 / id 提取 --------------------

def _extract_time_str(item: dict, primary: str) -> str:
    """从 list item 提取时间字符串. 多个候选字段, 取首个非空."""
    for k in (primary, "time", "publishDate", "roadshowDate", "date", "updateTime"):
        v = item.get(k)
        if v:
            return str(v)
    return ""


def _parse_time_to_dt(time_str: str) -> Optional[datetime]:
    """尝试用多种格式解析 AlphaPai 时间字符串. 返回 naive datetime 或 None."""
    if not time_str:
        return None
    s = str(time_str).replace("T", " ").split(".")[0]  # drop ms
    # naive dt —— 调用方会再显式贴 BJ TZ 转 UTC ms (见 _normalize_time + 883 行附近).
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _normalize_time(item: dict, primary: str) -> str:
    """把发布时间格式化成 'YYYY-MM-DD HH:MM' (用于显示和 _id 生成)."""
    raw = _extract_time_str(item, primary)
    dt = _parse_time_to_dt(raw)
    if dt:
        return dt.strftime("%Y-%m-%d %H:%M")
    return raw[:16] if raw else ""


def make_dedup_id(category_key: str, item: dict, cfg: dict) -> str:
    """生成稳定的 _id."""
    if cfg["id_strategy"] == "raw":
        rid = item.get("id")
        if rid:
            return str(rid)
    # 回退到内容哈希
    title = item.get("title", "")
    time_str = _normalize_time(item, cfg["time_field"])
    return _hash_id(category_key, title, time_str)


# -------------------- 列表 / 详情 --------------------

def fetch_list_page(session, cfg: dict, page: int, size: int,
                    market_type: Optional[str] = None,
                    category_key: Optional[str] = None) -> dict:
    """抓取一页列表. 返回原始响应 (含 code/message/data).

    market_type: 子分类 key. 解析顺序 SUBTYPES[category_key] → body merge.
    hot 附加 24h begin/end 窗口.  category_key 允许 None 以兼容旧调用(走 roadshow 映射).
    """
    body = {"pageNum": page, "pageSize": size, **cfg.get("list_extra_body", {})}
    if market_type:
        cat = category_key or "roadshow"
        sub = (SUBTYPES.get(cat) or {}).get(market_type)
        if sub:
            body.update(sub["body"])
            window_h = sub.get("window_hours")
            if window_h:
                # Upstream 用 Asia/Shanghai 壁钟解释 beginTime/endTime, 显式用 BJ.
                end = datetime.now(timezone(timedelta(hours=8)))
                begin = end - timedelta(hours=window_h)
                body["beginTime"] = begin.strftime("%Y-%m-%d %H:%M:%S")
                body["endTime"] = end.strftime("%Y-%m-%d %H:%M:%S")
    return api_call(session, "POST", cfg["list_path"], json_body=body)


def fetch_detail(session, cfg: dict, item: dict) -> dict:
    """抓取详情. 自动注入 id 与额外参数."""
    if not cfg.get("detail_path"):
        return {}
    qs_parts = [f"{cfg['detail_id_key']}={item.get('id')}"]
    for extra_key in cfg.get("detail_extra", ()):
        v = item.get(extra_key)
        if v is not None:
            qs_parts.append(f"{extra_key}={v}")
    path = cfg["detail_path"] + "?" + "&".join(qs_parts)
    resp = api_call(session, "GET", path)
    if resp.get("code") == OK_CODE:
        return resp.get("data") or {}
    return {"_err": {"code": resp.get("code"), "message": resp.get("message")}}


def fetch_list_paginated(session, cfg: dict, max_items: Optional[int],
                         page_size: int, stop_at_dedup_id: Optional[str] = None,
                         category_key: str = "",
                         stop_before_dt: Optional[datetime] = None,
                         market_type: Optional[str] = None) -> List[dict]:
    """分页抓取列表. 返回 list[item].

    - max_items=None: 抓到 list 长度 < page_size 即停
    - stop_at_dedup_id: 命中已知 dedup id 即停 (增量模式)
    - stop_before_dt: 条目发布时间 < 该时间则停 (用于 --since-hours)
    - market_type: roadshow 子分类 (ashare/hk/us/web/ir/hot), 透传到 fetch_list_page
    """
    all_items: List[dict] = []
    page = 1
    while True:
        resp = fetch_list_page(session, cfg, page, page_size,
                               market_type=market_type,
                               category_key=category_key)
        if resp.get("code") != OK_CODE:
            tqdm.write(f"  [page {page}] 列表失败: code={resp.get('code')} "
                       f"msg={resp.get('message')}")
            break
        data = resp.get("data") or {}
        items = data.get("list") or []
        total = data.get("total")
        if not items:
            tqdm.write(f"  [page {page}] 空列表, 停止")
            break

        new_count = 0
        hit_known = False
        hit_old = False
        # AlphaPai 列表 **不保证严格时间降序** —— e.g. "金信基金｜日刊" 标明天日期
        # 会排在今天发的研报前面. 如果 walker 遇到 hit_known 立即 break, 会漏掉
        # 后面真正 NEW 的条目. 所以这里把整页 items 都收进 all_items, 交给下游
        # dump_one 的 per-item dedup check 决定 upsert 或 skip. 仅用 hit_known
        # 作为"停止继续翻下一页"的提示.
        for it in items:
            if stop_at_dedup_id and make_dedup_id(category_key, it, cfg) == stop_at_dedup_id:
                hit_known = True
                continue  # 记录见过, 继续扫完本页
            if stop_before_dt is not None:
                dt = _parse_time_to_dt(_extract_time_str(it, cfg["time_field"]))
                if dt is not None and dt < stop_before_dt:
                    hit_old = True
                    continue  # 仅当前条太旧, 跳过; 继续扫完本页, 可能有"跳号"新条目
            all_items.append(it)
            new_count += 1
            if max_items and len(all_items) >= max_items:
                break

        tqdm.write(f"  [page {page}] +{new_count}/{len(items)} (累计 {len(all_items)}) "
                   f"total≈{total} hit_known={hit_known} hit_old={hit_old}")

        if hit_known or hit_old:
            break
        if max_items and len(all_items) >= max_items:
            break
        if len(items) < page_size:
            break  # 到尾
        page += 1
        _THROTTLE.sleep_before_next()
    return all_items[:max_items] if max_items else all_items


# -------------------- 研报 PDF 下载 --------------------

_SAFE_FNAME_RE = re.compile(r'[\\/:*?"<>|\x00-\x1f]')

# Linux ext4 文件名上限 = 255 字节 (NOT chars). UTF-8 中文每字 3 字节, 200 字 ≈ 600 字节,
# 远超上限 → OSError(Errno 36). 留 14 字节余量给 ".pdf" + 极端字符宽度.
_FNAME_MAX_BYTES = 240


def _truncate_to_bytes(s: str, max_bytes: int = _FNAME_MAX_BYTES) -> str:
    """按 UTF-8 字节宽度截断字符串, 不会截到半个字符."""
    if not s:
        return s
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    # 二分截到一个完整 UTF-8 边界 (errors='ignore' 砍掉断尾的不完整字节)
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _safe_filename(name: str, max_len: int = 200) -> str:
    """把任意字符串清成合法文件名 (保留中文, 去掉 / : 等).

    既限字符数 (`max_len`) 又限 UTF-8 字节宽度 (`_FNAME_MAX_BYTES`),
    后者防 OSError(Errno 36) 在中文/日文文件名上炸盘.
    """
    cleaned = _SAFE_FNAME_RE.sub("_", name).strip().strip(".")
    cleaned = cleaned[:max_len]
    cleaned = _truncate_to_bytes(cleaned, _FNAME_MAX_BYTES)
    return cleaned or "untitled"


# 把 pdf_error 字符串映射到稳定的 kind, 与 backend/app/api/alphapai_db.py
# 的 _classify_pdf_error 保持一致. 主要用途: dump_one 在 --resume 时跳过
# 永久性失败 (permission_denied) 的 re-fetch, 不浪费 API 配额.
def classify_pdf_error_kind(err: str | None) -> str:
    if not err:
        return "none"
    s = str(err)
    low = s.lower()
    # 2026-04-24: 探测发现 detail/pdf 真实拒绝原因是 code=810002 "今日外资报告
    # 查看上限" (账号 tier=0/day). 不传 originType 时服务端降级返回 10222.
    # 两类同等不可重试.
    if "code=810002" in s or "上限" in s:
        return "quota_exhausted"
    if "code=10222" in s or "无权限查看" in s or "hasPermission=false" in s:
        return "permission_denied"
    if "file name too long" in low or "errno 36" in low:
        return "filename_too_long"
    if any(m in low for m in (
            "chunkedencodingerror", "broken pipe", "connectionerror",
            "ssl", "eof", "timeout", "timed out",
            "connection reset", "remote disconnected")):
        return "transient_network"
    # 2026-04-24: empty_data_type=str 实测 258/286 来自 hasPermission=false 的条目
    # (外资研报付费墙: 服务端降级返回 data="" 而不是 code=10222). 历史 286 条全部
    # 是非付费条目也拿不到 PDF → 等同 permission_denied 不自动重试.
    if "empty_data" in s:
        return "permission_denied"
    if "code=404" in s or "code=400404" in s:
        return "not_found"
    if s.startswith("relpath_err"):
        return "relpath_unknown"
    if s.startswith("download_err"):
        return "download_unknown"
    return "unknown"


def fetch_report_pdf_relpath(session: requests.Session, raw_id: str,
                             version: Any) -> Tuple[Optional[str], Optional[str]]:
    """调用 /reading/report/detail/pdf 拿相对路径字符串.

    返回 (relpath, err_message). relpath 形如
    ``reading-server/2026-04-16/report/<file>.pdf``.
    """
    if not raw_id:
        return None, "no_raw_id"
    qs = f"id={raw_id}"
    if version is not None:
        qs += f"&version={version}"
    resp = api_call(session, "GET", f"reading/report/detail/pdf?{qs}")
    if resp.get("code") != OK_CODE:
        return None, f"code={resp.get('code')} msg={resp.get('message')}"
    data = resp.get("data")
    if isinstance(data, str) and data:
        return data, None
    if isinstance(data, dict):
        for k in ("url", "pdfUrl", "path", "filePath"):
            v = data.get(k)
            if isinstance(v, str) and v:
                return v, None
        return None, f"unexpected_dict: keys={list(data.keys())}"
    return None, f"empty_data_type={type(data).__name__}"


def build_pdf_url(relpath: str, token: str) -> str:
    """把相对路径拼成可下载的签名 URL."""
    encoded = urllib.parse.quote(relpath, safe="")
    return (f"{STORAGE_REPORT_BASE}{encoded}"
            f"?authorization={token}&platform=web")


def download_report_pdf(session: requests.Session, relpath: str, token: str,
                        dest_path: Path, timeout: int = 60,
                        max_retries: int = 4) -> Tuple[int, Optional[str]]:
    """下载 PDF 到本地文件. 返回 (bytes_written, err_message).

    对 SSL EOF / 网络抖动重试 (alphapai-storage 经常在中途关连接).

    2026-04-24 加固 (SMB 共享盘 /mnt/share/ygwang/alphapai_pdfs):
      - 写入时同时捕获前 4 字节, 不再 re-open 读 magic — 避免 CIFS
        actimeo=1/closetimeo=1 短缓存窗口里 .part 暂时不可见的 race
        (今早 5 条 read_back_err: [Errno 2] No such file 都源于此).
      - 显式 flush + os.fsync + 文件关闭后再 rename, 强制 CIFS commit.
      - tmpfile 名带 PID + 随机后缀, 防止两个 watcher 撞同一 dest 互删 .part.
      - rename 用 os.replace, 跨 CIFS 比 Path.rename 更稳 (POSIX semantics).
    """
    import os, secrets
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    url = build_pdf_url(relpath, token)
    # 进程独占的 .part 后缀, 避免多 watcher 撞 dest_path 时互删彼此的临时文件.
    suffix = f".{os.getpid()}.{secrets.token_hex(4)}.part"
    tmp = dest_path.with_suffix(dest_path.suffix + suffix)
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout, stream=True)
        except requests.RequestException as e:
            last_err = f"req_err: {e}"
            _THROTTLE.on_retry(attempt=attempt)
            _THROTTLE.sleep_before_next()
            continue
        if r.status_code != 200:
            r.close()
            last_err = f"http_{r.status_code}"
            if r.status_code in (401, 403):
                return 0, last_err  # auth dead, 别再试
            if r.status_code == 429 or 500 <= r.status_code < 600:
                _THROTTLE.on_retry(retry_after_sec=parse_retry_after(
                    r.headers.get("Retry-After")), attempt=attempt)
                _THROTTLE.sleep_before_next()
                continue
            return 0, last_err
        ctype = (r.headers.get("Content-Type") or "").lower()
        total = 0
        head = b""
        try:
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    if not head:
                        head = chunk[:4]  # capture magic during write — no re-read needed
                    f.write(chunk)
                    total += len(chunk)
                # CIFS 安全: flush 用户态缓冲 → fsync 推到远端 → 关闭. 不依赖
                # 后续 open() 来读回, 因为 CIFS 短缓存可能还没让新 inode 可见.
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    pass  # fsync 不支持的 FS (罕见, 不致命)
        except (requests.RequestException, IOError) as e:
            r.close()
            try: tmp.unlink(missing_ok=True)
            except Exception: pass
            last_err = f"stream_err: {type(e).__name__}: {e}"
            _THROTTLE.on_retry(attempt=attempt)
            _THROTTLE.sleep_before_next()
            continue
        r.close()
        if head != b"%PDF":
            try: tmp.unlink(missing_ok=True)
            except Exception: pass
            return 0, f"not_pdf ctype={ctype} head={head!r}"
        try:
            os.replace(tmp, dest_path)  # POSIX-atomic rename, 比 Path.rename 在 CIFS 上更稳
        except OSError as e:
            try: tmp.unlink(missing_ok=True)
            except Exception: pass
            return 0, f"rename_err: {e}"
        return total, None
    return 0, last_err or "exhausted retries"


def _pdf_dest_path(pdf_dir: Path, publish_time: str, relpath: str,
                   title: str) -> Path:
    """生成本地 PDF 落盘路径.

    ``pdf_dir/<YYYY-MM>/<sanitized-basename>`` — 基名优先用相对路径的 basename
    (服务端已经 sanitize 过), 否则回退到标题.
    """
    # YYYY-MM 子目录 (Asia/Shanghai, 和 release_time 对齐)
    if publish_time and len(publish_time) >= 7:
        sub = publish_time[:7]
    else:
        sub = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m")
    fname = Path(relpath).name if relpath else ""
    if not fname or not fname.lower().endswith(".pdf"):
        fname = _safe_filename(title) + ".pdf"
    fname = _safe_filename(fname)
    return pdf_dir / sub / fname


def enrich_report_doc(session, doc: dict, item: dict, pdf_dir: Path,
                      token: str, download: bool = True) -> None:
    """给研报 doc 附加 pdf_rel_path / pdf_local_path / pdf_size / pdf_error.

    成功时清掉旧的 pdf_error / pdf_error_kind, 失败时同时写入 kind 标记 ——
    后者让 dump_one 的 --resume re-fetch 决策能跳过 permission_denied 这种
    永远不会成功的错误, 不浪费 API 配额.
    """
    if not item.get("pdfFlag"):
        doc["pdf_flag"] = False
        return
    doc["pdf_flag"] = True
    # 2026-04-24: list 里已显式标 hasPermission=False (外资研报 / 独立研究付费墙)
    # 的条目, 直接标 permission_denied 而不打 detail/pdf 端点 —— 实测这些条目
    # 服务端返回 data="" 而不是 code=10222, scraper 原先分类成 empty_data_type=str
    # 的 "not_found", 触发每次 --resume 都重新打 API 浪费配额. 现在短路跳过.
    if item.get("hasPermission") is False:
        doc["pdf_error"] = "relpath_err: hasPermission=false (list 无权限)"
        doc["pdf_error_kind"] = "permission_denied"
        return
    raw_id = item.get("id")
    version = item.get("version") or item.get("originalVersion") or 1
    relpath, err = fetch_report_pdf_relpath(session, raw_id, version)
    if err or not relpath:
        msg = f"relpath_err: {err}"
        doc["pdf_error"] = msg
        doc["pdf_error_kind"] = classify_pdf_error_kind(msg)
        return
    doc["pdf_rel_path"] = relpath
    if not download:
        return
    dest = _pdf_dest_path(pdf_dir, doc.get("publish_time", ""), relpath,
                          doc.get("title", ""))
    if dest.exists() and dest.stat().st_size > 0:
        doc["pdf_local_path"] = str(dest)
        doc["pdf_size"] = dest.stat().st_size
        doc["pdf_error"] = ""
        doc["pdf_error_kind"] = "none"
        return
    size, derr = download_report_pdf(session, relpath, token, dest)
    if derr:
        msg = f"download_err: {derr}"
        doc["pdf_error"] = msg
        doc["pdf_error_kind"] = classify_pdf_error_kind(msg)
        return
    doc["pdf_local_path"] = str(dest)
    doc["pdf_size"] = size
    doc["pdf_error"] = ""
    doc["pdf_error_kind"] = "none"


# -------------------- Truncated retry (每轮 watch 末尾, 跨天自动补齐) --------------------

def retry_truncated_roadshows(session, db, max_retry: int = 500,
                              consec_quota_break: int = 5,
                              min_gain_chars: int = 50,
                              tokens_in_pool: list = None,
                              pool_label_hint: str = "") -> dict:
    """每轮 watch 末尾扫 content_truncated=True 的 roadshow doc, 调 detail 接口
    尝试补全. 替代独立 bypass_backfill.py.

    - cur_len 大的优先(更可能藏 rich aiSummary, 命中价值高).
    - 命中 400000 时主动 mark + 池里切下一个 token, 热替换 session.headers
      继续同一轮 retry. 全部 token 耗尽后 break.
    - 500020 短期 throttle → cool 90s.
    - min_gain_chars=50: 任何明显增长都算 hit, 不再卡 200 字门槛.

    Returns: {"scanned","refilled","quota_blocked","throttled","skipped",
              "rotations"}
    """
    global _CURRENT_ACCOUNT_ID
    stats = {"scanned": 0, "refilled": 0, "quota_blocked": 0,
             "throttled": 0, "skipped": 0, "rotations": 0}
    if max_retry <= 0:
        return stats
    # 撞过 400000 的账号别再进 retry-truncated — 否则单 token 场景下会再扫
    # max_retry 个候选才靠 consec_quota_break 退出, 浪费请求 + 风控加深.
    # retry_truncated_roadshows 永远是 roadshow 场景, 直接传 "roadshow".
    if _CURRENT_ACCOUNT_ID and is_token_exhausted(_CURRENT_ACCOUNT_ID, "roadshow"):
        print(f"[retry-truncated] 账号 {_CURRENT_ACCOUNT_ID} roadshow 配额已耗尽, 跳过本轮")
        return stats
    if "roadshows" not in db.list_collection_names():
        return stats
    col = db["roadshows"]
    cands = list(col.aggregate([
        {"$match": {"content_truncated": True,
                    "raw_id": {"$exists": True, "$ne": None}}},
        {"$addFields": {"_cur_len": {"$strLenCP": {"$ifNull": ["$content", ""]}}}},
        {"$sort": {"_cur_len": -1, "crawled_at": -1}},
        {"$limit": max_retry},
        {"$project": {"_id": 1, "raw_id": 1, "title": 1, "_cur_len": 1}},
    ]))
    if not cands:
        return stats
    print(f"[retry-truncated] roadshow candidates: {len(cands)}")
    consec_quota = 0

    def _try_rotate_token() -> bool:
        """池里挑下一个未耗尽的 token, 热替换 session.headers.
        成功返 True, 没下一个返 False."""
        if not tokens_in_pool:
            return False
        # 排除当前已耗尽的 (roadshow 类别)
        next_t = pick_available_token(tokens_in_pool, label_hint=pool_label_hint,
                                       category="roadshow")
        if next_t is None:
            return False
        if next_t["account_id"] == _CURRENT_ACCOUNT_ID:
            return False  # 没新 token
        session.headers["Authorization"] = next_t["token"]
        new_acc = next_t["account_id"]
        print(f"  [rotate] {_CURRENT_ACCOUNT_ID} (耗尽) → "
              f"{next_t.get('label','?')} ({new_acc})")
        _CURRENT_ACCOUNT_ID = new_acc
        stats["rotations"] += 1
        return True

    for d in cands:
        rid = d.get("raw_id")
        cur = d.get("_cur_len", 0)
        try:
            r = api_call(session, "GET",
                         f"reading/roadshow/summary/detail?id={rid}") or {}
        except SessionDead:
            raise
        except Exception:
            stats["skipped"] += 1
            continue
        stats["scanned"] += 1
        code = r.get("code")
        if code == OK_CODE:
            consec_quota = 0
            detail = r.get("data") or {}
            main_md, seg_md = _extract_roadshow_content(detail)
            new_len = len(main_md or "")
            if new_len > cur + min_gain_chars:
                update = {
                    "content": main_md,
                    "content_truncated": False,
                    "detail": detail,
                    "_retry_truncated_at": datetime.now(timezone.utc),
                }
                if seg_md:
                    update["segments_md"] = seg_md
                col.update_one({"_id": d["_id"]}, {"$set": update})
                stats["refilled"] += 1
                if stats["refilled"] <= 3 or stats["refilled"] % 10 == 0:
                    title = (d.get("title") or "")[:50]
                    print(f"  ✓ +{new_len-cur:>5}  {title}")
            else:
                stats["skipped"] += 1
        elif code == 400000:
            stats["quota_blocked"] += 1
            consec_quota += 1
            # 优先尝试切到池里下一个未耗尽的 token, 切成功就 reset consec_quota 继续
            if _try_rotate_token():
                consec_quota = 0
                continue
            # 切不动 → 池里全死, 走 consec break 逻辑
            if consec_quota >= consec_quota_break:
                print(f"[retry-truncated] {consec_quota}× 连续 400000 且池内"
                      f"无可用 token, 本轮结束 (refilled={stats['refilled']}, "
                      f"rotations={stats['rotations']})")
                break
        elif code == 500020:
            stats["throttled"] += 1
            time.sleep(90)
        else:
            stats["skipped"] += 1
    print(f"[retry-truncated] 扫 {stats['scanned']}, 补 {stats['refilled']}, "
          f"额度阻塞 {stats['quota_blocked']}, throttled {stats['throttled']}, "
          f"切 token {stats['rotations']} 次")
    return stats


# -------------------- Mongo 存储 --------------------

def dump_one(session, db, category_key: str, cfg: dict, item: dict,
             force: bool = False, pdf_dir: Optional[Path] = None,
             download_pdf: bool = True, token: str = "",
             list_only: bool = False,
             market_type: Optional[str] = None) -> str:
    """写入单条到对应集合. 返回 'added' / 'skipped' / 'updated' / 'failed'.

    list_only=True: 只存 list 里的元数据字段, 跳过 detail 抓取. 极速扫全部
    list, 详情后台批量补. list item 本身已含 title/time/organization/url 等,
    搜索检索完全够用.
    """
    col = db[cfg["collection"]]
    dedup_id = make_dedup_id(category_key, item, cfg)

    if not force:
        ex = col.find_one({"_id": dedup_id},
                          {"_id": 1, "content": 1, "pdf_size": 1, "pdf_error": 1,
                           "pdf_error_kind": 1, "pdf_flag": 1,
                           "content_truncated": 1, "hasPermission": 1})
        if ex:
            # 研报: 已入库但 PDF 下载失败过 → 重试 (除了永久无权限).
            # 老数据没有 pdf_error_kind 字段, 在这里现场分类一次, 让历史的
            # 21k 条 code=10222 doc 也能被识别为 permission_denied 并跳过,
            # 不然 --resume 每次都重新打 detail/pdf 接口浪费配额.
            ex_kind = ex.get("pdf_error_kind") or classify_pdf_error_kind(
                ex.get("pdf_error") or "")
            if category_key == "report" and ex.get("pdf_flag") \
                    and not ex.get("pdf_size") and pdf_dir is not None \
                    and download_pdf \
                    and ex_kind not in ("permission_denied", "quota_exhausted"):
                pass  # fall through to re-fetch
            # 研报: 之前 detail 因"已达到今日查看上限"返回 400000, 标了
            # content_truncated=True → 下次再扫时尝试重拉, 跨天配额重置后可拿到内容
            elif category_key == "report" and ex.get("content_truncated"):
                pass  # fall through to re-fetch
            # 纪要/路演: 被平台日额度截断 (hasPermission=False, code=7 "用户访问
            # 纪要次数已达上限") → 下次再扫时尝试重拉, 如果新的一天额度回来了就能拿到全文
            elif category_key == "roadshow" and ex.get("content_truncated"):
                pass  # fall through to re-fetch
            else:
                return "skipped", {
                    "content_len": len(ex.get("content") or ""),
                    "pdf_size": ex.get("pdf_size", 0),
                    "pdf_error": ex.get("pdf_error", ""),
                    "pdf_flag": ex.get("pdf_flag", False),
                }

    # list_only: 跳过 detail 抓取, 节省 3s/条 throttle. 后续 backfill 可用
    # `--force` 配合补 detail.
    if list_only:
        detail = {}
    else:
        detail = fetch_detail(session, cfg, item)
    title = item.get("title", "")
    time_norm = _normalize_time(item, cfg["time_field"])

    # 网页 URL (用于人类阅读)
    try:
        web_url = cfg["web_url_fmt"].format(**{
            "id": item.get("id", ""),
            "supplierId": item.get("supplierId", ""),
        })
    except KeyError:
        web_url = ""

    # 回测对齐: publish_time 是北京时间字符串 (平台原生), 这里反算成 UTC epoch ms
    # 存到 release_time_ms 字段. 回测/跨平台 join 统一用这个 ms 字段 (TZ-free).
    pt_dt = _parse_time_to_dt(time_norm)  # naive, 约定是北京时间
    release_time_ms = None
    if pt_dt is not None:
        # 标为 Asia/Shanghai 然后 → UTC epoch
        from datetime import timezone as _tz, timedelta as _td
        BJ = _tz(_td(hours=8))
        release_time_ms = int(pt_dt.replace(tzinfo=BJ).timestamp() * 1000)
        # AlphaPai occasionally pre-dates reports with tomorrow's date
        # (e.g. morning-report scheduled for next trading day). Cap at "now"
        # so the feed doesn't show a future date.
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if release_time_ms > now_ms:
            release_time_ms = now_ms
            time_norm = datetime.now(tz=_tz(_td(hours=8))).strftime("%Y-%m-%d %H:%M")

    doc = {
        "_id": dedup_id,
        "category": category_key,
        "title": title,
        "publish_time": time_norm,
        "release_time_ms": release_time_ms,
        "raw_id": item.get("id"),
        "list_item": item,
        "detail": detail,
        "web_url": web_url,
        "crawled_at": datetime.now(timezone.utc),
    }
    # 提取常用字段方便 mongo 查询
    for k in ("supplierId", "publishInstitution", "institution", "stock",
              "industry", "analyst", "analysts", "url", "content",
              "accountName", "accountId"):
        v = item.get(k)
        if v not in (None, "", []):
            doc[k] = v

    # 研报: detail.content 是"展开"后的全量核心观点, 覆盖 list 的 180 字截断版
    if isinstance(detail, dict):
        full = detail.get("content")
        if isinstance(full, str) and len(full) > len(doc.get("content") or ""):
            doc["content"] = full
        # 同时暴露常用字段
        for k in ("htmlContent", "summaryCnHtml", "summaryEnHtml", "pageNum",
                  "reportType", "hasPermission"):
            v = detail.get(k)
            if v not in (None, "", []):
                doc.setdefault(k, v)

    # Roadshow: 会议纪要正文在 detail.aiSummary.content (HTML) — list 只有 103 字预览
    if category_key == "roadshow" and isinstance(detail, dict):
        main_md, seg_md = _extract_roadshow_content(detail)
        if main_md and len(main_md) > len(doc.get("content") or ""):
            doc["content"] = main_md
        if seg_md:
            doc["segments_md"] = seg_md

    # Roadshow 额度截断标记. 平台对"查看纪要"有两层每日上限:
    #   (1) list 端 hasPermission=False + noPermissionReason.code=7 (~220 字预览)
    #   (2) detail 端自身耗尽 → fetch_detail 包成 _err.code=400000 (~134 字 list content)
    # 任一发生都标 content_truncated=True, dedup-skip 允许下次重抓,
    # watch loop 末尾 retry_truncated_roadshows 跨天补齐.
    if category_key == "roadshow":
        cur_has_perm = doc.get("hasPermission")
        if cur_has_perm is None:
            cur_has_perm = item.get("hasPermission")
        if isinstance(detail, dict):
            no_perm = detail.get("noPermissionReason") or {}
            err = detail.get("_err") or {}
            is_quota_blocked = (
                (cur_has_perm is False
                 and isinstance(no_perm, dict)
                 and no_perm.get("code") == 7)
                or err.get("code") == 400000
            )
            doc["content_truncated"] = bool(is_quota_blocked)
            if err.get("code") == 400000:
                doc["quota_msg"] = err.get("message") or ""
        else:
            doc["content_truncated"] = bool(cur_has_perm is False)

    # 研报也有每日查看上限 (detail 返回 code=400000 "已达到今日查看上限,请明日
    # 再来"). 标 content_truncated=True, 明天配额恢复后 dedup-skip 会允许 re-fetch.
    # 2026-04-22 实测: 平台日历 927 vs list/v2 855 vs 其中 15 条 detail 已耗尽.
    if category_key == "report":
        err = (detail or {}).get("_err") or {}
        is_quota_blocked = err.get("code") == 400000
        if is_quota_blocked:
            doc["content_truncated"] = True
            doc["quota_msg"] = err.get("message") or ""
        else:
            # 成功取到内容, 清掉旧标记
            if (doc.get("content") or "").strip():
                doc["content_truncated"] = False

    # 研报 PDF 下载
    if category_key == "report" and pdf_dir is not None:
        enrich_report_doc(session, doc, item, pdf_dir, token,
                          download=download_pdf)

    # Sub-category tags for the category's SPA tabs (roadshow/report/comment).
    # Stored as an ARRAY `_{category}_subcategories` — a doc can appear in multiple
    # tabs (e.g. a report tagged [ashare, indep]).  Read prior array, union with
    # current subcategory, write back so replace_one can't drop tags.
    if category_key in SUBTYPES:
        sub_field = f"_{category_key}_subcategories"
        legacy_field = f"_{category_key}_subcategory"
        prev = col.find_one({"_id": dedup_id}, {sub_field: 1, legacy_field: 1})
        existing: set[str] = set()
        if prev:
            ex_arr = prev.get(sub_field)
            if isinstance(ex_arr, list):
                existing.update(str(x) for x in ex_arr if x)
            ex_one = prev.get(legacy_field)
            if isinstance(ex_one, str) and ex_one:
                existing.add(ex_one)
        if market_type:
            existing.add(market_type)
        if existing:
            doc[sub_field] = sorted(existing)

    _stamp_ticker(doc, "alphapai", col)
    col.replace_one({"_id": dedup_id}, doc, upsert=True)
    return "added", {
        "content_len": len(doc.get("content") or ""),
        "pdf_size": doc.get("pdf_size", 0),
        "pdf_error": doc.get("pdf_error", ""),
        "pdf_flag": doc.get("pdf_flag", False),
    }


# -------------------- Checkpoint --------------------

def state_doc_id(category_key: str, market_type: Optional[str] = None) -> str:
    """Per-(category, market_type) checkpoint id.

    Default (no market_type) keeps the legacy `crawler_{category}` key for
    backward compatibility with existing watchers.  Subtype watchers key
    off `crawler_{category}__{market_type}` so each can advance its
    top_dedup_id independently — a burst on A股会议 no longer skips
    港股会议 / 美股会议 items.
    """
    if market_type:
        return f"crawler_{category_key}__{market_type}"
    return f"crawler_{category_key}"


def load_state(db, category_key: str, market_type: Optional[str] = None) -> dict:
    return db[COL_STATE].find_one({"_id": state_doc_id(category_key, market_type)}) or {}


def save_state(db, category_key: str, market_type: Optional[str] = None, **kwargs) -> None:
    kwargs["updated_at"] = datetime.now(timezone.utc)
    db[COL_STATE].update_one(
        {"_id": state_doc_id(category_key, market_type)},
        {"$set": kwargs},
        upsert=True,
    )


# -------------------- 一轮抓取 --------------------

def run_category_streaming(session, db, category_key: str, args) -> dict:
    """Streaming backfill: fetch one page → dump items → save checkpoint → next page.

    DB writes start on page 1 instead of after the whole list is collected.
    On restart, resumes from `backfill_deep_page` in the crawler state doc.
    When the list is exhausted, `backfill_deep_page` is reset to 1.
    """
    cfg = CATEGORIES[category_key]
    mt_arg = getattr(args, "market_type", None)
    market_type = mt_arg if mt_arg and mt_arg in (SUBTYPES.get(category_key) or {}) else None
    sub_label = f"/{SUBTYPES[category_key][market_type]['label']}" if market_type else ""
    print(f"\n{'─' * 60}")
    print(f"[STREAM {cfg['label']}{sub_label}] collection={cfg['collection']}")
    print(f"{'─' * 60}")

    state = load_state(db, category_key, market_type) or {}
    start_page = int(state.get("backfill_deep_page") or 1)
    print(f"[stream] resume_from_page={start_page} (0 / empty = start from 1)")
    print(f"[列表] max={args.max or '全部'} page_size={args.page_size}")

    pdf_dir = Path(args.pdf_dir) if category_key == "report" else None
    token = getattr(args, "auth", "") or ""
    cap = cap_from_args(args)
    added = skipped = failed = 0
    page = start_page
    total_seen = 0
    first_top_dedup_id: Optional[str] = None

    while True:
        if cap.exhausted():
            print(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        if _BUDGET.exhausted():
            print(f"  [antibot] 账号 24h 预算耗尽 ({_BUDGET.daily_limit}), 本轮停 (防封号)")
            break
        resp = fetch_list_page(session, cfg, page, args.page_size,
                               market_type=market_type, category_key=category_key)
        if resp.get("code") != OK_CODE:
            print(f"  [page {page}] 列表失败: code={resp.get('code')} msg={resp.get('message')}")
            break
        data = resp.get("data") or {}
        items = data.get("list") or []
        total = data.get("total")
        if not items:
            print(f"  [page {page}] 空列表, 列表到底 → 重置 backfill_deep_page=1")
            save_state(db, category_key, market_type, backfill_deep_page=1,
                       backfill_last_run_end_at=datetime.now(timezone.utc))
            break

        # Track top-of-list for the first page we see (used as top_dedup_id anchor)
        if first_top_dedup_id is None:
            first_top_dedup_id = make_dedup_id(category_key, items[0], cfg)

        page_added = page_skipped = page_failed = 0
        for it in items:
            if cap.exhausted() or _BUDGET.exhausted():
                break
            title = (it.get("title") or "")[:60]
            time_str = _normalize_time(it, cfg["time_field"])
            was_skip = False
            try:
                status, info = dump_one(session, db, category_key, cfg, it,
                                        force=args.force, pdf_dir=pdf_dir,
                                        download_pdf=not args.skip_pdf, token=token,
                                        list_only=getattr(args, "list_only", False),
                                        market_type=market_type)
                # list-only 模式不发网络请求, 同样不用 throttle per item
                if getattr(args, "list_only", False):
                    was_skip = True  # 借用 was_skip 抑制下方 throttle
                if status == "skipped":
                    skipped += 1; page_skipped += 1; was_skip = True
                else:
                    added += 1; page_added += 1
                    cap.bump(); _BUDGET.bump()
                    print(f"  ✓ {time_str} [{category_key}] {title}")
            except SessionDead:
                raise
            except Exception as e:
                failed += 1; page_failed += 1
                print(f"  ✗ {time_str} [{category_key}] {title}  ERR: {e}")
            total_seen += 1
            if not was_skip:
                _THROTTLE.sleep_before_next()
            if args.max and total_seen >= args.max:
                break

        # Persist deep-page checkpoint after every dumped page (断点续传核心)
        save_state(db, category_key, market_type,
                   backfill_deep_page=page + 1,
                   backfill_last_page_at=datetime.now(timezone.utc),
                   in_progress=True)
        print(f"  [page {page}] +{page_added} /={page_skipped} ✗{page_failed} "
              f"(累计本轮 +{added} ={skipped} ✗{failed}) total≈{total}")

        # Stop conditions
        if args.max and total_seen >= args.max:
            print(f"  [列表] 达到 max={args.max}, 本轮停")
            break
        if len(items) < args.page_size:
            print(f"  [page {page}] partial ({len(items)} < {args.page_size}), 列表到底 → 重置 backfill_deep_page=1")
            save_state(db, category_key, market_type, backfill_deep_page=1,
                       backfill_last_run_end_at=datetime.now(timezone.utc))
            break
        page += 1
        _THROTTLE.sleep_before_next()

    if first_top_dedup_id is not None and start_page == 1:
        save_state(db, category_key, market_type, top_dedup_id=first_top_dedup_id)
    save_state(db, category_key, market_type, in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})

    total_in_db = db[cfg["collection"]].estimated_document_count()
    print(f"  完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}")
    print(f"  当前 {cfg['collection']} 总数: {total_in_db}")
    return {"added": added, "skipped": skipped, "failed": failed}


def run_category(session, db, category_key: str, args) -> dict:
    """对单个分类跑一轮. 返回统计."""
    if getattr(args, "stream_backfill", False):
        return run_category_streaming(session, db, category_key, args)

    cfg = CATEGORIES[category_key]
    # Per-category sub-types (roadshow / report / comment).  Only applies when
    # the passed --market-type key is defined under SUBTYPES[category_key].
    mt_arg = getattr(args, "market_type", None)
    market_type = mt_arg if mt_arg and mt_arg in (SUBTYPES.get(category_key) or {}) else None
    label = cfg["label"]
    if market_type:
        label = f"{cfg['label']}/{SUBTYPES[category_key][market_type]['label']}"
    print(f"\n{'─' * 60}")
    print(f"[{label}] collection={cfg['collection']}")
    print(f"{'─' * 60}")

    state = load_state(db, category_key, market_type)
    stop_id = state.get("top_dedup_id") if args.resume else None
    if args.resume and stop_id:
        last = state.get("updated_at")
        print(f"[恢复] 上次顶部 id={stop_id[:24]}.. 时间={last} → 增量到此停")
    elif args.resume:
        print("[恢复] 未找到 checkpoint, 全量爬")

    stop_dt = None
    if getattr(args, "since_hours", None) is not None:
        # `_parse_time_to_dt` returns NAIVE (Asia/Shanghai wall-clock). Keep
        # stop_dt naive to avoid "offset-naive vs offset-aware" TypeError inside
        # `fetch_list_paginated`'s `dt < stop_before_dt` comparison.
        stop_dt = (datetime.now(timezone(timedelta(hours=8)))
                   - timedelta(hours=args.since_hours)).replace(tzinfo=None)
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 (cutoff={stop_dt:%Y-%m-%d %H:%M})")

    # --sweep-today + --date YYYY-MM-DD 只对 report 生效. 实测 wechat/comment/roadshow
    # 的 list API 忽略 startDate/endDate body 字段 (2026-04-23 测: 设置 2025-10-23 返回
    # 的仍是当天最新条目). 所以日期 sweep 只对 report 有用.
    if getattr(args, "sweep_today", False) and category_key == "report":
        stop_id = None
        stop_dt = None
        print("[sweep-today] report 分类: 禁用 top-dedup / 时间窗早停")

    print(f"[列表] max={args.max or '全部'} page_size={args.page_size}")
    items = fetch_list_paginated(session, cfg,
                                 max_items=args.max,
                                 page_size=args.page_size,
                                 stop_at_dedup_id=stop_id,
                                 category_key=category_key,
                                 stop_before_dt=stop_dt,
                                 market_type=market_type)
    print(f"[列表] 共 {len(items)} 条待处理")
    if not items:
        print("无新内容 (或 token 失效)")
        return {"added": 0, "skipped": 0, "failed": 0}

    new_top_id = make_dedup_id(category_key, items[0], cfg)

    pdf_dir = Path(args.pdf_dir) if category_key == "report" else None
    token = getattr(args, "auth", "") or ""

    added = skipped = failed = 0
    cap = cap_from_args(args)
    pbar = tqdm(items, desc=cfg["label"], unit="条", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")
    for it in pbar:
        if cap.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        if _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 账号 24h 预算耗尽 ({_BUDGET.daily_limit}), 本轮停 (防封号)")
            break
        title = (it.get("title") or "")[:60]
        time_str = _normalize_time(it, cfg["time_field"])
        was_skip = False
        try:
            status, info = dump_one(session, db, category_key, cfg, it,
                                    force=args.force, pdf_dir=pdf_dir,
                                    download_pdf=not args.skip_pdf, token=token,
                                    list_only=getattr(args, "list_only", False),
                                    market_type=market_type)
            if getattr(args, "list_only", False):
                was_skip = True  # list-only: 无网络, 不 throttle per item
            # 详细信息后缀
            parts = []
            cl = info.get("content_len", 0)
            if cl:
                parts.append(f"content={cl}字")
            if category_key == "report" and info.get("pdf_flag"):
                if info.get("pdf_size"):
                    parts.append(f"PDF={info['pdf_size']:,}B")
                elif info.get("pdf_error"):
                    parts.append(f"PDF失败({info['pdf_error'][:40]})")
                else:
                    parts.append("PDF跳过")
            suffix = "  " + "  ".join(parts) if parts else ""
            if status == "skipped":
                skipped += 1
                was_skip = True
                tqdm.write(f"  · {time_str} [{category_key}] {title}  已存在{suffix}")
            else:
                added += 1
                cap.bump(); _BUDGET.bump()
                tqdm.write(f"  ✓ {time_str} [{category_key}] {title}{suffix}")
        except SessionDead:
            raise
        except Exception as e:
            failed += 1
            tqdm.write(f"  ✗ {time_str} [{category_key}] {title}  ERR: {e}")

        pbar.set_postfix_str(f"+{added} ={skipped} ✗{failed}")
        save_state(db, category_key, market_type,
                   last_dedup_id=make_dedup_id(category_key, it, cfg),
                   last_processed_at=datetime.now(timezone.utc),
                   in_progress=True)
        # Pure DB dedup hits made no network call — skip the 3-5s throttle.
        # Why: top-of-list re-scan after watcher catch-up burns hours otherwise.
        if not was_skip:
            _THROTTLE.sleep_before_next()
    pbar.close()

    save_state(db, category_key, market_type,
               top_dedup_id=new_top_id,
               in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})

    total = db[cfg["collection"]].estimated_document_count()
    print(f"  完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}")
    print(f"  当前 {cfg['collection']} 总数: {total}")
    return {"added": added, "skipped": skipped, "failed": failed}


def run_once(session, db, args) -> Dict[str, dict]:
    """对所有(或指定)分类跑一轮.

    注意: SessionDead 不做吞噬 — 401/403 = 会话吊销, 让它冒泡到 main/watch loop
    退出让运维重新登录. 旧版本 `except Exception` 把 SessionDead 也包了,
    结果 `--watch` 每 30s 重试同一个 dead 端点, 单轮打几十分钟 HTTP 401, 同时
    账号预算被耗 (见 2026-04-24 日志复盘).
    """
    cats = CATEGORY_ORDER if args.category == "all" else [args.category]
    summary: Dict[str, dict] = {}
    for c in cats:
        # 类别级日配额硬闸: 该 (账号, 类别) 已 exhausted_today → 直接跳过
        # run_category 不打 list / 不打 detail / 不入库. 用 watch loop 顶部
        # 检查作为兜底 — 这里再加一道, 避免单进程跑 `--category all` 时
        # roadshow 撞上限了还继续打 comment/report (反过来也成立).
        if _CURRENT_ACCOUNT_ID and is_token_exhausted(_CURRENT_ACCOUNT_ID, c):
            print(f"\n[{c}] 账号 {_CURRENT_ACCOUNT_ID} 该类别今日额度耗尽, 跳过本轮")
            summary[c] = {"added": 0, "skipped": 0, "failed": 0, "exhausted": True}
            continue
        try:
            summary[c] = run_category(session, db, c, args)
        except (KeyboardInterrupt, SessionDead):
            raise
        except Exception as e:
            tqdm.write(f"\n[{c}] 分类异常: {e}")
            summary[c] = {"added": 0, "skipped": 0, "failed": -1, "error": str(e)}
    print(f"\n{'═' * 60}")
    print(f"本轮汇总: " + "  ".join(
        f"{c}+{s.get('added', 0)}/={s.get('skipped', 0)}/✗{s.get('failed', 0)}"
        for c, s in summary.items()
    ))
    print(f"{'═' * 60}")
    return summary


# -------------------- 当日统计 --------------------

_BJ_TZ = timezone(timedelta(hours=8))


def count_today(session, db, args) -> dict:
    """对各分类统计指定日期(默认今天) 平台条数, 与本地库对比, 存 _state.
    用 Asia/Shanghai 对齐 — 平台 release_time 是 BJ 壁钟."""
    if args.date:
        target = args.date
        day_start = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
    else:
        day_start = datetime.now(_BJ_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        target = day_start.strftime("%Y-%m-%d")

    print(f"\n[统计] 扫描各分类 {target} 平台条数...")
    cats = CATEGORY_ORDER if args.category == "all" else [args.category]
    overall: Dict[str, dict] = {"date": target}
    for c in cats:
        cfg = CATEGORIES[c]
        items_today: List[dict] = []
        page = 1
        stop = False
        scanned = 0
        while not stop:
            resp = fetch_list_page(session, cfg, page, args.page_size)
            if resp.get("code") != OK_CODE:
                print(f"  [{c}] page {page} 失败: code={resp.get('code')}")
                break
            items = (resp.get("data") or {}).get("list") or []
            scanned += len(items)
            if not items:
                break
            for it in items:
                dt = _parse_time_to_dt(_extract_time_str(it, cfg["time_field"]))
                if dt is None:
                    continue
                if dt < day_start:
                    stop = True
                    break
                if dt.date() == day_start.date():
                    items_today.append(it)
            if len(items) < args.page_size:
                break
            page += 1
            _THROTTLE.sleep_before_next()

        # 对比本地库
        ids = [make_dedup_id(c, it, cfg) for it in items_today]
        in_db = db[cfg["collection"]].count_documents(
            {"_id": {"$in": ids}}) if ids else 0

        cat_stat = {
            "platform_count": len(items_today),
            "in_db": in_db,
            "missing": len(items_today) - in_db,
            "scanned_pages": page,
            "scanned_items": scanned,
        }
        overall[c] = cat_stat
        print(f"  {cfg['label']:>12s}: 平台 {cat_stat['platform_count']:>4d}  "
              f"已入库 {cat_stat['in_db']:>4d}  待入库 {cat_stat['missing']:>4d}  "
              f"(扫描 {scanned} 条 / {page} 页)")

    overall["scanned_at"] = datetime.now(timezone.utc)
    db[COL_STATE].replace_one(
        {"_id": f"daily_{target}"},
        {"_id": f"daily_{target}", **overall},
        upsert=True,
    )
    print(f"\n已保存到 {COL_STATE} (_id=daily_{target})")
    return overall


# -------------------- account / 元数据 --------------------

ACCOUNT_ENDPOINTS = [
    ("report-type-list",     "GET",  "reading/report/type/list"),
    ("report-list-selector", "GET",  "reading/report/list/selector"),
    ("wechat-info",          "GET",  "reading/wechat/home/wechat/info"),
    ("stock-follow-groups",  "GET",  "reading/stock/follow/group/list"),
    ("share-permissions",    "GET",  "reading/share/permissions/query"),
]


def dump_account(session, db) -> None:
    print("\n[账户] 抓取账户级 / 元数据接口...")
    col = db[COL_ACCOUNT]
    now = datetime.now(timezone.utc)
    for name, method, path in ACCOUNT_ENDPOINTS:
        resp = api_call(session, method, path)
        col.replace_one(
            {"_id": name},
            {"_id": name, "endpoint": path, "method": method,
             "response": resp, "updated_at": now},
            upsert=True,
        )
        code = resp.get("code")
        tag = "✓" if code == OK_CODE else f"code={code}"
        print(f"  [{tag}] {name}")


# -------------------- CLI --------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="alphapai-web.rabyte.cn 多分类爬虫 (MongoDB 存储)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--category", choices=["all", *CATEGORY_ORDER], default="all",
                   help=f"指定分类 (默认 all). 可选: {', '.join(CATEGORY_ORDER)}")
    _all_subtype_keys = sorted({k for sub in SUBTYPES.values() for k in sub.keys()})
    p.add_argument("--market-type", choices=_all_subtype_keys,
                   default=None,
                   help="子分类 key — 按 --category 在 SUBTYPES 里查表. "
                        "roadshow: ashare/hk/us/web/ir/hot. "
                        "report: ashare/us/indep. "
                        "comment: selected/regular. "
                        "不填走默认视图.")
    p.add_argument("--max", type=int, default=None,
                   help="最多爬 N 条 (单分类). 默认翻页直到 list < page-size")
    p.add_argument("--page-size", type=int, default=40,
                   help="每页大小 (默认 40)")
    p.add_argument("--force", action="store_true",
                   help="强制重爬已入库的内容 (默认跳过)")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 遇到上次已爬过的 top 即停止分页")
    p.add_argument("--stream-backfill", action="store_true",
                   help="流式回填: 每抓完一页立即入库 + 保存 deep_page checkpoint, "
                        "下次启动从 checkpoint 续翻. 让 DB 从 page 1 开始就有写入可见, "
                        "空页时重置 deep_page 回到 1.")
    p.add_argument("--list-only", action="store_true",
                   help="极速扫: 只存 list 元数据不抓 detail (每条省 3s throttle). "
                        "后续用 --force 或专门 job 批量补 detail.")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600)")
    p.add_argument("--retry-truncated-max", type=int, default=0,
                   help="watch 每轮末尾尝试 retry 的 content_truncated=True roadshow "
                        "上限. 0=禁用 (默认, 2026-04-28 后改). 之前每日 100-150 次配额"
                        "几乎全花在补老 truncated, 新发反而拿不到 detail; 现在配额"
                        "完全留给当日 list-incremental.")
    p.add_argument("--since-hours", type=float, default=None,
                   help="只抓取过去 N 小时内发布的内容 (按 time_field). "
                        "默认不限制.")
    p.add_argument("--show-state", action="store_true",
                   help="打印 checkpoint 后退出")
    p.add_argument("--reset-state", action="store_true",
                   help="清除 checkpoint 后退出")
    p.add_argument("--today", action="store_true",
                   help="统计今日各分类平台条数对比本地库, 结果存 _state")
    p.add_argument("--sweep-today", action="store_true",
                   help="研报分类改用 startDate=endDate=今天 的每日扫描模式 "
                        "(而非全局 top-pagination). 能保证今日所有可见研报"
                        "在一轮内全部覆盖, 不会因 top pagination 跟不上速度漏抓. "
                        "watch 模式下每轮重算日期以正确处理跨天.")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today 指定日期 (默认今天)")
    p.add_argument("--auth",
                   default=_load_token_from_file() or os.environ.get("JM_AUTH") or USER_AUTH_TOKEN,
                   help="USER_AUTH_TOKEN (优先级: credentials.json > env JM_AUTH > 脚本内 USER_AUTH_TOKEN)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT,
                   help=f"MongoDB URI (默认 {MONGO_URI_DEFAULT})")
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT,
                   help=f"MongoDB 数据库名 (默认 {MONGO_DB_DEFAULT})")
    p.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT,
                   help=f"研报 PDF 下载目录 (默认 {PDF_DIR_DEFAULT})")
    p.add_argument("--skip-pdf", action="store_true",
                   help="只记录 pdf_rel_path 不下载 PDF 文件")
    p.add_argument("--clean-reports", action="store_true",
                   help="删除 reports 集合+crawler_report checkpoint 后退出 "
                        "(为全量重爬准备)")
    p.add_argument("--fix-reports-content", action="store_true",
                   help="只回补历史研报的 content (核心观点) 字段. "
                        "对 content 为空且 list_item.id/version 可用的条目, 重新调 "
                        "/reading/report/detail?id=...&version=... 拉取文本后 upsert. "
                        "不下 PDF, 不刷新列表, 适合修复 2026-04 平台变化前入库的数据.")
    # 反爬节流 (crawl/antibot.py) — platform 字符串供 SoftCooldown / AccountBudget 用
    # 2026-04-25 default_cap 500 → 0: 实时档不再靠数量闸防跑飞, 见 antibot.py 顶部 §5/§6.
    add_antibot_args(p, default_base=3.0, default_jitter=2.0,
                     default_burst=40, default_cap=0, platform="alphapai")
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
    for cfg in CATEGORIES.values():
        col = db[cfg["collection"]]
        col.create_index("title")
        col.create_index("publish_time")
        col.create_index("category")
        col.create_index("crawled_at")
    print(f"[Mongo] 已连接 {uri} -> db: {dbname}")
    return db


def show_state(db) -> None:
    print("=" * 60)
    print("Checkpoint")
    print("=" * 60)
    for c in CATEGORY_ORDER:
        s = load_state(db, c)
        if not s:
            print(f"  {c:>10s}: (无)")
            continue
        print(f"  {c:>10s}: top={str(s.get('top_dedup_id'))[:16]}.. "
              f"updated_at={s.get('updated_at')} "
              f"last_run={s.get('last_run_stats')}")
    print()
    print("Collection 总数:")
    for c, cfg in CATEGORIES.items():
        n = db[cfg["collection"]].estimated_document_count()
        print(f"  {c:>10s} ({cfg['collection']}): {n}")


def _setup_globals_for_token(args, token: str) -> str:
    """重置 _THROTTLE / _BUDGET / _CURRENT_ACCOUNT_ID 给指定 token. 返回 account_id."""
    global _THROTTLE, _BUDGET, _CURRENT_ACCOUNT_ID
    _THROTTLE = throttle_from_args(args, platform="alphapai")
    _account_id_base = _account_id_from_token(token or "")
    _CURRENT_ACCOUNT_ID = _account_id_base
    cat_for_budget = (args.category if args.category and args.category != "all"
                      else "all")
    _account_id = account_id_for_alphapai(_account_id_base, cat_for_budget)
    _BUDGET = budget_from_args(args, account_id=_account_id, platform="alphapai")
    log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET,
                     extra=f"acct={_account_id}")
    return _account_id_base


def main():
    args = parse_args()

    # 多账号 token 池 (默认行为):
    #   - credentials.json 是新 schema {"tokens": [{...}, ...]} → 多账号
    #   - 旧 schema {"token": "..."} → 池长度 1 (向后兼容)
    #   - args.auth 显式传入 → 用显式 token, 不进 pool 切换
    tokens_in_pool = _load_tokens_from_file()
    explicit_auth = bool(args.auth) and (
        not tokens_in_pool
        or args.auth.strip() != tokens_in_pool[0]["token"].strip()
    )
    if not args.auth and not tokens_in_pool:
        print("错误: credentials.json 无 token, --auth / JM_AUTH / "
              "USER_AUTH_TOKEN 都没设. 退出.")
        sys.exit(1)
    pool_label_hint = " ".join(sys.argv[1:])[:80]
    pool_size = max(1, len(tokens_in_pool))
    if pool_size > 1 and not explicit_auth:
        print(f"[token-pool] 检测到 {pool_size} 个 alphapai 账号, 启用轮换")

    db = connect_mongo(args.mongo_uri, args.mongo_db)

    if args.show_state:
        show_state(db)
        return

    if args.reset_state:
        n = db[COL_STATE].delete_many(
            {"_id": {"$regex": "^crawler_"}}).deleted_count
        print(f"已清除 {n} 条 crawler checkpoint (daily_* 统计保留)")
        return

    if args.clean_reports:
        col = db[CATEGORIES["report"]["collection"]]
        n_docs = col.estimated_document_count()
        col.drop()
        n_state = db[COL_STATE].delete_one(
            {"_id": state_doc_id("report")}).deleted_count
        print(f"已清除 reports 集合 ({n_docs} 条) 与 crawler_report checkpoint "
              f"({n_state} 条). 本地 PDF 文件保留在 --pdf-dir, 可手动清理: "
              f"rm -r {args.pdf_dir}")
        return

    if args.fix_reports_content:
        session = create_session(args.auth)
        col = db[CATEGORIES["report"]["collection"]]
        query = {"$or": [{"content": {"$exists": False}},
                          {"content": ""}, {"content": None}]}
        total = col.count_documents(query)
        print(f"[fix] 待修复 content 为空的研报: {total}")
        fixed = skipped = failed = 0
        for doc in col.find(query, {"_id": 1, "title": 1, "list_item": 1}):
            item = doc.get("list_item") or {}
            raw_id = item.get("id")
            version = item.get("version") or item.get("originalVersion") or 20
            if not raw_id:
                skipped += 1
                continue
            try:
                detail = fetch_detail(session, CATEGORIES["report"], item)
            except SessionDead:
                print("  [fix] 会话失效, 请刷新 token 后重跑")
                break
            except Exception as e:
                failed += 1
                tqdm.write(f"  ✗ [{doc['_id'][:12]}] {(doc.get('title') or '')[:50]}  ERR: {e}")
                continue
            new_content = detail.get("content") if isinstance(detail, dict) else None
            if not new_content:
                skipped += 1
                tqdm.write(f"  · [{doc['_id'][:12]}] {(doc.get('title') or '')[:50]}  content 依旧为空 (raw_id 可能过期)")
                _THROTTLE.sleep_before_next()
                continue
            col.update_one({"_id": doc["_id"]},
                           {"$set": {"content": new_content,
                                     "detail": detail,
                                     "fix_content_at": datetime.now(timezone.utc)}})
            fixed += 1
            tqdm.write(f"  ✓ [{doc['_id'][:12]}] {(doc.get('title') or '')[:50]}  content={len(new_content)}字")
            _THROTTLE.sleep_before_next()
        print(f"\n[fix] 完成: 修复 {fixed} / 跳过 {skipped} / 失败 {failed}")
        return

    # ========= Token 池 outer loop =========
    # 每次循环挑可用 token → 跑 watch / 一次性命令 → 命中 daily quota 时 break
    # 内层 → outer 重选下一个可用 token. 全部耗尽时 sleep 到 BJ 第二天 0:02.
    # 单 token 场景下 outer 也只走 1 次 (耗尽后 sleep).
    while True:
        # 1. 挑 token
        if explicit_auth:
            chosen_token = args.auth
            chosen_label = "explicit"
        else:
            tokens_now = _load_tokens_from_file() or tokens_in_pool
            # 按本进程的 args.category 过滤 — roadshow watcher 看 roadshow flag,
            # comment / report watcher 不会被 roadshow 的耗尽 flag 卡住.
            picked = pick_available_token(tokens_now, label_hint=pool_label_hint,
                                           category=getattr(args, "category", "") or "")
            if picked is None:
                wait = _seconds_until_bj_midnight(extra_seconds=120)
                cat_lbl = getattr(args, "category", "") or "?"
                print(f"\n[token-pool] {len(tokens_now)} 个 alphapai 账号"
                      f"在 {cat_lbl} 类别下全部今日额度耗尽, sleep {wait}s 到 "
                      f"BJ 第二天 0:02 后重选 (期间可在 credentials.json 追加"
                      f"新账号自动接管)")
                try:
                    time.sleep(wait)
                except KeyboardInterrupt:
                    print("\nCtrl+C 退出"); return
                continue
            chosen_token = picked["token"]
            chosen_label = picked.get("label", "?")

        _setup_globals_for_token(args, chosen_token)
        info = parse_jwt(chosen_token)
        if info:
            exp = info.get("exp")
            exp_str = datetime.fromtimestamp(
                exp, tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M") \
                if exp else "?"
            print(f"[认证] uid={info.get('uid')} 过期={exp_str} pool_label={chosen_label}")

        session = create_session(chosen_token)

        if args.today:
            count_today(session, db, args)
            return

        if db[COL_ACCOUNT].estimated_document_count() == 0 or args.force:
            dump_account(session, db)

        if not args.watch:
            if args.sweep_today:
                date_str = args.date or datetime.now(_BJ_TZ).strftime("%Y-%m-%d")
                CATEGORIES["report"]["list_extra_body"] = {
                    "startDate": date_str, "endDate": date_str,
                }
                print(f"[sweep-today] report startDate={date_str} endDate={date_str}")
            run_once(session, db, args)
            return

        # ===== watch loop =====
        print(f"\n[实时模式] 每 {args.interval}s 轮询. Ctrl+C 退出.")
        round_num = 0
        rotate_needed = False
        while True:
            round_num += 1
            print(f"\n{'═' * 60}\n[轮次 {round_num}] "
                  f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'═' * 60}")
            # 顶部 short-circuit: 撞过限额的账号在下一轮 run_once 之前就 break,
            # 否则会先打一轮 list 端点 (浪费 + 可能脏数据) 再检查 flag.
            # 用本 watcher 的 args.category 查 — comment / report 看不到 roadshow flag.
            _cat = getattr(args, "category", "") or ""
            if not explicit_auth and _CURRENT_ACCOUNT_ID and _cat and \
                    is_token_exhausted(_CURRENT_ACCOUNT_ID, _cat):
                pool_n = len(_load_tokens_from_file() or tokens_in_pool)
                if pool_n > 1:
                    print(f"\n[token-pool] {_cat} 类别: 当前账号 {_CURRENT_ACCOUNT_ID} "
                          f"已 exhausted, outer 重选 (池有 {pool_n} 个)")
                else:
                    print(f"\n[token-pool] {_cat} 类别: 仅 1 个 alphapai 账号且已 "
                          f"exhausted, outer sleep 到午夜后再试 "
                          f"(跳过 run_once / retry-truncated)")
                rotate_needed = True
                break
            if args.sweep_today:
                date_str = args.date or datetime.now(_BJ_TZ).strftime("%Y-%m-%d")
                CATEGORIES["report"]["list_extra_body"] = {
                    "startDate": date_str, "endDate": date_str,
                }
                print(f"[sweep-today] report startDate={date_str} endDate={date_str}")
            try:
                run_once(session, db, args)
            except KeyboardInterrupt:
                print("\n[实时模式] Ctrl+C 退出"); return
            except SessionDead as e:
                print(f"\n[实时模式] 会话已吊销, 立即退出等重登: {e}")
                return
            except Exception as e:
                print(f"[轮次 {round_num}] 异常: {e}")

            # token 池: 当前 token 当日 quota 耗尽 → break 内层让 outer 切下一个.
            # 同样 per-category — roadshow 撞 quota 不会停 comment / report.
            _cat = getattr(args, "category", "") or ""
            if not explicit_auth and _CURRENT_ACCOUNT_ID and _cat and \
                    is_token_exhausted(_CURRENT_ACCOUNT_ID, _cat):
                pool_n = len(_load_tokens_from_file() or tokens_in_pool)
                if pool_n > 1:
                    print(f"\n[token-pool] {_cat} 类别: 当前账号 {_CURRENT_ACCOUNT_ID} "
                          f"今日额度耗尽, outer 重选 (池有 {pool_n} 个)")
                else:
                    print(f"\n[token-pool] {_cat} 类别: 仅 1 个 alphapai 账号且今日"
                          f"耗尽, outer sleep 到午夜后再试")
                rotate_needed = True
                break

            # retry-truncated 末尾扫描 (跨天额度回来时自动补齐 + token 切换时也能及时清单)
            if args.retry_truncated_max > 0:
                try:
                    retry_truncated_roadshows(
                        session, db, max_retry=args.retry_truncated_max)
                except SessionDead as e:
                    print(f"\n[实时模式] retry-truncated 期间会话吊销: {e}")
                    return
                except Exception as e:
                    print(f"[retry-truncated] 异常: {e}")

            try:
                time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\n[实时模式] Ctrl+C 退出"); return

        if rotate_needed:
            continue  # outer-while: 重新挑 token (可能 sleep 到午夜)
        return


if __name__ == "__main__":
    main()
