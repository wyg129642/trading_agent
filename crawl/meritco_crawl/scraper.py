#!/usr/bin/env python3
"""
research.meritco-group.com 论坛爬虫 (MongoDB 存储)

参考 crawl/jinmen/scraper.py 的总体结构, 按 meritco_crawl/promts.md 要求重写:
  - 用 Python + MongoDB
  - 全部命令行参数 (--max/--page-size/--force/--watch/--interval/--auth/
                  --mongo-uri/--mongo-db/--resume/--show-state/--reset-state/
                  --today/--date/--type)

使用步骤:
  1. 浏览器登录 https://research.meritco-group.com/forum
  2. F12 → Network → 任一 XHR (如 /matrix-search/forum/select/list)
  3. Request Headers 找 `token: <32hex>`, 复制值
  4. 写入 credentials.json:  {"token": "<32hex>", "user_agent": "..."}
     或通过 --auth / 环境变量 MERITCO_AUTH 传入

数据存储:
  - MongoDB (默认 mongodb://localhost:27017, db=meritco)
  - Collection `forum`: 每条论坛条目一个 document, _id=id (forumId)
  - Collection `account`: 账户级接口数据
  - Collection `_state`: checkpoint / 日统计

typeId 说明 (matrix-search/forum/select/list 的 `type` 字段):
  1 = 活动 / 活动预告
  2 = 专业内容 (纪要+研报+其他, 默认)
  3 = 久谦自研
"""

import argparse
import base64
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA
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
    warmup_session,
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402

# 模块级 throttle, main() 用 CLI 覆盖
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=3.0, jitter=2.0,
                                                burst_size=40,
                                                platform="meritco")
_BUDGET: AccountBudget = AccountBudget("meritco", "default", 0)
_PLATFORM = "meritco"

# ==================== 配置 ====================

ROOT = Path(__file__).parent.resolve()
CREDS_FILE = ROOT / "credentials.json"
API_BASE = "https://research.meritco-group.com"

LIST_API = "/matrix-search/forum/select/list"
DETAIL_API = "/matrix-search/forum/select/id"

# type=2 (专业内容) 时前端附带的 7 个 platform 标签
DEFAULT_PLATFORM_ARR = [
    "专业内容-纪要-国内市场-专家访谈",
    "专业内容-纪要-国内市场-业绩交流",
    "专业内容-纪要-国内市场-券商路演",
    "专业内容-纪要-海外市场",
    "专业内容-研报-国内市场",
    "专业内容-研报-海外市场",
    "专业内容-其他报告",
]

# 账户级接口 (只拉一次, --force 刷新)
# 注: method 参照实际前端调用 — user-info 是 POST (不是 GET), industries 是 GET (不是 POST)
ACCOUNT_ENDPOINTS = [
    ("user-info",         "/meritco-user/research/info/get",            "post"),
    ("follow-list",       "/meritco-chatgpt/research/user/follow/list", "post"),
    ("industries",        "/matrix-search/forum/company/industries",    "get"),
    ("calendar",          "/matrix-search/forum/calendar",              "post"),
]

MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "jiuqian-full")
COL_FORUM = "forum"
COL_ACCOUNT = "account"
COL_STATE = "_state"

# PDF 下载目录 (与其他平台 2026-04-17 统一路径规范一致,
# 可被 env MERITCO_PDF_DIR 或 --pdf-dir 覆盖)
PDF_DIR_DEFAULT = os.environ.get(
    "MERITCO_PDF_DIR",
    str(Path("/home/ygwang/crawl_data/meritco_pdfs")),
)

# PDF 下载端点 (从 forumPDF 页面逆向出来): body = {"pdfOSSUrlEncoded": <enc>}
# 无 5 天时间窗口限制; 5 天预览规则仅是前端 UI 下载按钮显示层的限制, 后端 API
# 对任意时间的文档直接返 PDF 流 (带水印).
PDF_DOWNLOAD_API = "/matrix-search/forum/pdfDownloadWatermark"


# ==================== X-My-Header RSA 签名 ====================
#
# 2026-04 起, /matrix-search/forum/select/list 和 /matrix-search/forum/select/id
# 被前端 be3b 模块加入到 X-My-Header 签名名单. 缺少此签名时后端统一返回
# code=500 message="参数错误", 极易误判为 body 字段缺失.
#
# 规则:
#   list   : X-My-Header = base64(RSA-PKCS1v1.5(pubKey, token + keyword + page))
#   detail : X-My-Header = base64(RSA-PKCS1v1.5(pubKey, token + forumId))
# 公钥从前端 JS 的 be3b 模块直接硬编码抠出来.

_RSA_PUBLIC_KEY_PEM = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0q3O3srLBw1roKRa8D8D
CUb5yy1uCZJV0WN20h7ePPj3QlUsJNKsIyuxptsV8ql2aBKjcm+tjLx8s+463m8P
MTdqoJdFaabH+dxa3/0tSMZbyWFCnm0OLzGT4PhVXxTq9MNjjIh5DZFhX5NSPtQU
8acmj2551vhzNpwnHqf6hgwVZdCUASNqqp5kOA81DYekT5soFtlZMp/StpXUHa0S
xck1rFkpwjyk0YAXwAnsTdycJovwsnbX0jwFmLqNYW3qtJYKJr5yOHRgMaNojmR/
TliA4DbroIMnChJs+5G4EFUInE6H6eTmi3CxJARDTY39MLjT8ZQGmLXdComHLCEo
LwIDAQAB
-----END PUBLIC KEY-----"""

_rsa_cipher: "PKCS1_v1_5.PKCS115_Cipher | None" = None


def _rsa_encrypt(plain: str) -> str:
    global _rsa_cipher
    if _rsa_cipher is None:
        _rsa_cipher = PKCS1_v1_5.new(RSA.import_key(_RSA_PUBLIC_KEY_PEM))
    return base64.b64encode(_rsa_cipher.encrypt(plain.encode("utf-8"))).decode("ascii")


def sign_list_header(token: str, keyword: str, page: int) -> str:
    return _rsa_encrypt(str(token) + str(keyword or "") + str(page))


def sign_detail_header(token: str, forum_id) -> str:
    return _rsa_encrypt(str(token) + str(forum_id))


# ==================== HTTP 客户端 ====================

class AuthExpired(Exception):
    pass


@dataclass
class HttpConfig:
    page_size: int = 40
    delay: float = 1.5
    max_retries: int = 5
    backoff: float = 2.0
    timeout: float = 30.0


def load_creds_from_file() -> tuple[str, str]:
    if not CREDS_FILE.exists():
        return "", ""
    try:
        c = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
        return (c.get("token") or "").strip(), c.get("user_agent") or ""
    except Exception:
        return "", ""


def default_user_agent() -> str:
    """Backwards-compatible default UA. Real production code path goes through
    headers_for_platform("meritco") in create_client → process-stable rotation."""
    from antibot import pick_user_agent as _pua
    return _pua()


def create_client(token: str, user_agent: str, timeout: float = 30.0) -> httpx.Client:
    h = headers_for_platform("meritco")
    if user_agent:
        h["User-Agent"] = user_agent
    h.update({
        "accept": h.pop("Accept", "application/json, text/plain, */*"),
        "accept-language": h.pop("Accept-Language",
                                  "zh-CN,zh;q=0.9,en;q=0.6"),
        "content-type": "application/json;charset=UTF-8",
        "origin": API_BASE,
        "referer": API_BASE + "/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "token": token,
        "x-user-type": "default",
    })
    # Lower-case some headers httpx is happy with both cases — meritco's
    # X-My-Header signing checks lowercase, so normalize here.
    c = httpx.Client(
        base_url=API_BASE,
        timeout=timeout,
        http2=False,
        trust_env=False,
        headers=h,
    )
    # Warmup: landing HTML 先访问一次, 停 2-5s, 再发 XHR (参见 antibot.warmup_session).
    warmup_session(c, "meritco")
    return c


def _raise_auth_or_http(resp: httpx.Response) -> None:
    if is_auth_dead(resp.status_code, resp.text[:200]):
        raise AuthExpired(f"HTTP {resp.status_code}: token expired or invalid")
    resp.raise_for_status()


def with_retry(fn, cfg: HttpConfig, label: str, *args, **kwargs):
    """429/5xx → 指数退避 + 尊重 Retry-After; 401/403 → 立刻抛 AuthExpired 不重试.

    退避通过 `_THROTTLE.on_retry()` 注入, 下次 `sleep_before_next()` 用。
    429 同时触发同平台全局 SoftCooldown (45 min), 让所有 watcher 一起退场.
    """
    for attempt in range(1, cfg.max_retries + 1):
        try:
            result = fn(*args, **kwargs)
            # 业务层软警告 — meritco 大多数返回 dict, 检测 code/msg 信号
            if isinstance(result, dict):
                reason = detect_soft_warning(200, body=result)
                if reason:
                    SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=10)
                    _THROTTLE.on_warning()
            return result
        except AuthExpired:
            raise
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or status >= 500:
                if status == 429:
                    SoftCooldown.trigger(_PLATFORM, reason=f"http_429:{label}",
                                          minutes=10)
                ra = parse_retry_after(e.response.headers.get("Retry-After"))
                tqdm.write(f"  {label}: HTTP {status}, retry {attempt}/{cfg.max_retries}"
                           + (f" (Retry-After={ra:.0f}s)" if ra else ""))
                _THROTTLE.on_retry(retry_after_sec=ra, attempt=attempt)
                _THROTTLE.sleep_before_next()
            else:
                raise
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError, httpx.RemoteProtocolError) as e:
            tqdm.write(f"  {label}: {type(e).__name__}, retry {attempt}/{cfg.max_retries}")
            _THROTTLE.on_retry(attempt=attempt)
            _THROTTLE.sleep_before_next()
    raise RuntimeError(f"{label}: exhausted {cfg.max_retries} retries")


# ==================== API 封装 ====================

def _get_token(client: httpx.Client) -> str:
    """从 client.headers 提取当前 token (用于每次请求时重新签 X-My-Header)."""
    return client.headers.get("token") or ""


def fetch_list(client: httpx.Client, page: int, page_size: int,
               forum_type: int, trace_id: str, keyword: str = "") -> dict:
    platform_arr = DEFAULT_PLATFORM_ARR if forum_type == 2 else []
    # Body 结构严格对照前端 chunk-0b9eee54 的 getForumList — 多缺一个字段后端就 500 "参数错误"
    body = {
        "forumId": None,
        "page": page,
        "pageSize": page_size,
        "module": "CLASSIC_ALL_SEARCH",
        "contentTag": "",
        "traceId": trace_id,
        "publishTime": "",
        "codeIndustryId": "",
        "totalPage": "",
        "sortColumn": "articleDate",
        "source": "",
        "reportTag": "全部标签",
        "platformArr": platform_arr,
        "outCat1": "",
        "orgNameList": [],
        "outCat2": "",
        "keyword": keyword,
        "type": forum_type,
        "industryList": [],
        "companyUniqueKeyForInfoCenter": "",
        "expertType": "",
        "meetingStartTime": "",
        "meetingEndTime": "",
        "queryHotListFlag": False,
        "sort": 2,
        "platform": "RESEARCH_PC",
    }
    xmh = sign_list_header(_get_token(client), keyword, page)
    resp = client.post(LIST_API, json=body, headers={"x-my-header": xmh})
    _raise_auth_or_http(resp)
    return resp.json()


def fetch_detail(client: httpx.Client, forum_id: int | str) -> dict:
    xmh = sign_detail_header(_get_token(client), forum_id)
    resp = client.post(
        f"{DETAIL_API}?forumId={forum_id}",
        json={"platform": "RESEARCH_PC"},
        headers={"x-my-header": xmh},
    )
    _raise_auth_or_http(resp)
    return resp.json()


def fetch_account_endpoint(client: httpx.Client, path: str, method: str) -> dict:
    try:
        if method == "get":
            resp = client.get(path)
        else:
            resp = client.post(path, json={"platform": "RESEARCH_PC"})
        _raise_auth_or_http(resp)
        return resp.json()
    except AuthExpired:
        raise
    except Exception as e:
        return {"code": "err", "message": f"{type(e).__name__}: {e}"}


# ==================== 文本抽取 ====================

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"\s+\n|\n\s+")

try:
    from markdownify import markdownify as _markdownify
    _HAS_MARKDOWNIFY = True
except ImportError:
    _HAS_MARKDOWNIFY = False


def html_to_text(html: str) -> str:
    """Convert Meritco HTML fragments to Markdown, preserving heading / list
    hierarchy so downstream readers (kb_search body, LLM prompts, frontend render)
    see the same structure the website displays.

    The editor on research.meritco-group.com emits <h2>/<h3>/<ul>/<li>/<ol>/<strong>
    etc.; flattening those to plain text loses the analyst's outline. We use
    `markdownify` (ATX headings, dashes for bullets) which is a dependable
    HTML→Markdown converter. Falls back to a simpler tag-strip pass if the lib
    is unavailable — kept as a legacy safety net.
    """
    if not html or not isinstance(html, str):
        return ""

    if _HAS_MARKDOWNIFY:
        try:
            md = _markdownify(html, heading_style="ATX", bullets="-",
                              strip=["script", "style"])
            md = re.sub(r"\n{3,}", "\n\n", md).strip()
            return md
        except Exception:
            pass  # fall through to legacy path

    # Legacy fallback (no markdownify installed).
    t = html.replace("</p>", "\n").replace("</P>", "\n")
    t = t.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    t = _HTML_TAG_RE.sub("", t)
    for a, b in (("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"),
                 ("&gt;", ">"), ("&quot;", '"'), ("&#39;", "'")):
        t = t.replace(a, b)
    lines = [ln.strip() for ln in t.split("\n")]
    return "\n".join(ln for ln in lines if ln).strip()


def pick_time(item: dict) -> str:
    """Pick the closest "publish time" from the item; return 'YYYY-MM-DD HH:MM'.

    Priority: createTime > meetingTime > recommendTime > operationTime.
    operationTime is LAST because Meritco stamps it on any later re-edit / bulk
    migration — e.g. on 2026-04-02 they retouched every SNOW research post,
    rewriting operationTime to 2026-04-02 20:56 while the real publish date
    stayed in createTime (2026-02-26 23:42). The old priority led to every
    migrated post showing a fake April timestamp.
    """
    for k in ("createTime", "meetingTime", "recommendTime", "operationTime"):
        v = item.get(k)
        if v:
            s = str(v).strip()
            if s and s.lower() != "none":
                return s[:16]
    return ""


_BJ_TZ = timezone(timedelta(hours=8))


def time_to_ms(s: str) -> int:
    """'YYYY-MM-DD HH:MM[:SS]' (Asia/Shanghai 壁钟) → UTC epoch ms.
    TZ-aware so 输出稳定,不受服务器 TZ 影响. 失败返 0."""
    if not s:
        return 0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=_BJ_TZ).timestamp() * 1000)
        except ValueError:
            continue
    return 0


# ==================== MongoDB ====================

def connect_mongo(uri: str, dbname: str):
    cli = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        cli.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({uri}): {e}", file=sys.stderr)
        sys.exit(1)
    db = cli[dbname]
    db[COL_FORUM].create_index("title")
    db[COL_FORUM].create_index("release_time")
    db[COL_FORUM].create_index("type")
    db[COL_FORUM].create_index("industry")
    db[COL_FORUM].create_index("crawled_at")
    print(f"[Mongo] 已连接 {uri} -> db: {dbname}")
    return db


def load_state(db, forum_type: int) -> dict:
    return db[COL_STATE].find_one({"_id": f"crawler_type{forum_type}"}) or {}


def save_state(db, forum_type: int, **kwargs) -> None:
    kwargs["updated_at"] = datetime.now(timezone.utc)
    db[COL_STATE].update_one(
        {"_id": f"crawler_type{forum_type}"},
        {"$set": kwargs},
        upsert=True,
    )


def reset_all_state(db) -> int:
    r = db[COL_STATE].delete_many({})
    return r.deleted_count


# ==================== 账户抓取 ====================

def dump_account(client: httpx.Client, db, force: bool) -> None:
    col = db[COL_ACCOUNT]
    if not force and col.estimated_document_count() > 0:
        print("[账户] 已有数据, 跳过 (用 --force 可刷新)")
        return
    print("[账户] 抓取账户级接口...")
    now = datetime.now(timezone.utc)
    for name, path, method in ACCOUNT_ENDPOINTS:
        data = fetch_account_endpoint(client, path, method)
        col.replace_one(
            {"_id": name},
            {"_id": name, "path": path, "method": method,
             "response": data, "updated_at": now},
            upsert=True,
        )
        code = data.get("code") if isinstance(data, dict) else None
        tag = "✓" if code == 200 else f"code={code}"
        print(f"  [{tag}] {name}")


# ==================== PDF 下载 ====================

_INVALID_FNAME_RE = re.compile(r"[\\/:*?\"<>|\x00-\x1f]")


def _safe_filename(name: str, max_bytes: int = 200) -> str:
    """清洗文件名 + 按 UTF-8 bytes 限长 (Linux 单文件名 255 bytes 上限).

    汉字每个占 3 bytes, 所以 max_bytes=200 大约 60-65 汉字. 预留 50+ bytes
    给前缀 (`<forum_id>_`) 和其他前缀/路径开销.
    """
    s = _INVALID_FNAME_RE.sub("_", name or "").strip()
    s = re.sub(r"\s+", " ", s)
    if len(s.encode("utf-8")) <= max_bytes:
        return s or "unnamed.pdf"

    # 超长: 保留尾部扩展名, 对主名按 bytes 截断 (避免把汉字截一半)
    stem, dot, ext = s.rpartition(".")
    if dot and 1 <= len(ext) <= 8:
        ext_bytes = len(("." + ext).encode("utf-8"))
        stem_bytes_budget = max_bytes - ext_bytes
    else:
        stem = s
        ext = ""
        stem_bytes_budget = max_bytes

    # 按 bytes 逐字符累加, 不超预算
    b = stem.encode("utf-8")
    if len(b) > stem_bytes_budget:
        # 截 bytes 后用 errors="ignore" 防止把多字节字符切一半成无效 UTF-8
        truncated = b[:stem_bytes_budget].decode("utf-8", errors="ignore")
        stem = truncated
    result = f"{stem}.{ext}" if ext else stem
    return result or "unnamed.pdf"


def _pdf_dest_path(pdf_dir: Path, release_time: str, forum_id, name: str) -> Path:
    """pdf_dir/<YYYY-MM>/<forum_id>_<safe_name>.pdf"""
    sub = release_time[:7] if release_time and len(release_time) >= 7 else datetime.now(_BJ_TZ).strftime("%Y-%m")
    fname = _safe_filename(name)
    if not fname.lower().endswith(".pdf"):
        fname += ".pdf"
    # 前缀 forum_id 避免跨 doc 同名冲突
    return pdf_dir / sub / f"{forum_id}_{fname}"


def parse_pdf_url_field(raw: str | list | None) -> list[dict]:
    """pdf_url 字段有三种形态:
       - 空字符串 / "[]" / null      -> 返回 []
       - JSON 字符串 [{uid,name,url,...}]  -> parse 后直接返回
       - 已经是 list / dict          -> 规范化返回
    """
    if raw in (None, "", "[]"):
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict) and x.get("url")]
    if isinstance(raw, dict):
        return [raw] if raw.get("url") else []
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict) and x.get("url")]
        if isinstance(parsed, dict) and parsed.get("url"):
            return [parsed]
    return []


def fetch_pdf_bytes(client: httpx.Client, oss_url_encoded: str,
                    timeout: float = 60.0) -> tuple[bytes, dict]:
    """POST /matrix-search/forum/pdfDownloadWatermark — 响应是 application/pdf 流.

    成功: (pdf_bytes, {"status":200, "content_type":..., "size":...})
    失败: (b"", {"status":..., "content_type":..., "size":..., "error":...})
    网络异常: 直接抛出, 由调用方捕获.
    """
    resp = client.post(PDF_DOWNLOAD_API, json={"pdfOSSUrlEncoded": oss_url_encoded},
                       timeout=timeout)
    ct = resp.headers.get("content-type", "")
    meta = {"status": resp.status_code, "content_type": ct, "size": len(resp.content)}
    if resp.status_code != 200:
        meta["error"] = f"HTTP {resp.status_code}"
        return b"", meta
    if not resp.content.startswith(b"%PDF-"):
        # 后端失败时返 JSON (如 {"code":500,...}), 不是 PDF
        body_preview = resp.content[:300]
        try:
            j = resp.json()
            meta["error"] = f"non-pdf: code={j.get('code')} msg={j.get('message') or j.get('innerMessage')}"
        except Exception:
            meta["error"] = f"non-pdf: {body_preview!r}"
        return b"", meta
    return resp.content, meta


def download_attachments(client: httpx.Client, attachments: list[dict],
                         forum_id, release_time: str, title: str,
                         pdf_dir: Path, force: bool = False,
                         skip_download: bool = False) -> list[dict]:
    """对每个附件尝试下载, 返回增强字段的 attachments 列表.

    每个 entry 会带上:
      - 原始 uid / name / size / type / url
      - pdf_rel_path     (相对 pdf_dir, 便于跨机器)
      - pdf_local_path   (本机绝对路径)
      - pdf_size_bytes   (实际落盘字节, 0 表示失败)
      - pdf_download_error  (失败原因; 成功无此字段)
      - pdf_downloaded_at   (ISO 时间)
    """
    results: list[dict] = []
    for att in attachments:
        entry = dict(att)  # 拷贝原始
        oss_enc = att.get("url") or ""
        name = att.get("name") or ""
        if not oss_enc or not name:
            entry["pdf_download_error"] = "missing url or name"
            results.append(entry)
            continue

        dest = _pdf_dest_path(pdf_dir, release_time or "", forum_id, name)
        rel = dest.relative_to(pdf_dir).as_posix()
        entry["pdf_rel_path"] = rel
        entry["pdf_local_path"] = str(dest)

        # 已存在且非 force: 用磁盘上的大小
        if dest.exists() and dest.stat().st_size > 0 and not force:
            entry["pdf_size_bytes"] = dest.stat().st_size
            results.append(entry)
            continue

        if skip_download:
            # 只记录路径, 不拉文件
            results.append(entry)
            continue

        try:
            content, meta = fetch_pdf_bytes(client, oss_enc)
        except Exception as e:
            entry["pdf_download_error"] = f"{type(e).__name__}: {e}"
            entry["pdf_size_bytes"] = 0
            results.append(entry)
            continue

        if content:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
            entry["pdf_size_bytes"] = len(content)
            entry["pdf_downloaded_at"] = datetime.now(timezone.utc).isoformat()
        else:
            entry["pdf_size_bytes"] = 0
            entry["pdf_download_error"] = meta.get("error") or "empty response"
        results.append(entry)
    return results


# ==================== 单条入库 ====================

def build_doc(list_item: dict, detail_result: dict | None,
              forum_type: int, release_time: str) -> dict:
    """把列表条目 + 详情组装成 MongoDB document."""
    fid = list_item.get("id")
    # 详情以 detail_result 里的字段为准 (含 insight/content/keywordArr 等)
    merged = dict(list_item)
    if detail_result:
        merged.update(detail_result)

    authors = merged.get("authorVOList") or []
    experts = merged.get("forumExpertDetailList") or []
    related = merged.get("relatedTargets") or []

    content_html = merged.get("content") or ""
    insight_html = merged.get("insight") or ""
    summary_md = merged.get("summary") or ""
    topic_md = merged.get("topic") or ""
    background = merged.get("background") or ""
    expert_content = merged.get("expertContent") or ""

    content_text = html_to_text(content_html)
    insight_text = html_to_text(insight_html)

    # Meritco returns the boilerplate "本次活动无纪要产生" when an event produced
    # no formal transcript (but often still has a rich `insight` summary). Storing
    # that 9-char marker as content_md poisons kb_service's first-non-empty body
    # picker, which then shows "本次活动无纪要产生" instead of the real insight.
    # Normalize it to empty here so downstream picks insight_md / summary_md.
    if content_text.strip() in {"本次活动无纪要产生", "本次活动暂无纪要"}:
        content_text = ""

    stats = {
        "正文字数": len(content_text),
        "速览字数": len(insight_text),
        "摘要字数": len(summary_md or ""),
        "专家数": len(experts) if isinstance(experts, list) else 0,
        "关联标的": len(related) if isinstance(related, list) else 0,
    }

    doc = {
        "_id": fid,
        "id": fid,
        "forum_type": forum_type,
        "title": merged.get("title", ""),
        "release_time": release_time,
        "release_time_ms": time_to_ms(release_time),
        "web_url": f"https://research.meritco-group.com/forum?forumId={fid}",
        "meeting_time": merged.get("meetingTime") or "",
        "create_time": merged.get("createTime") or "",
        "operation_time": merged.get("operationTime") or "",
        "industry": merged.get("industry") or "",
        "type": merged.get("type") or forum_type,
        "language": merged.get("language"),
        "author": merged.get("author") or "",
        "operator": merged.get("operator") or "",
        "expert_information": merged.get("expertInformation") or "",
        "expert_type_name": merged.get("expertTypeName") or "",
        "report_type_name": merged.get("reportTypeName") or "",
        "related_targets": related,
        "authors": authors,
        "experts": experts,
        "keyword_arr": merged.get("keywordArr") or [],
        "keyword_black_arr": merged.get("keywordBlackArr") or [],
        "hot_flag": bool(merged.get("hotFlag")),
        "is_top": int(merged.get("isTop") or 0),
        "hc_conf_id": merged.get("hcConfId"),
        "hc_conf_number": merged.get("hcConfNumber"),
        "meeting_link": merged.get("meetingLink") or "",
        "pdf_url": merged.get("pdfUrl") or "",
        # 可读文本
        "summary_md": summary_md,
        "topic_md": topic_md,
        "background_md": background,
        "expert_content_md": expert_content,
        "insight_md": insight_text,
        "content_md": content_text,
        # 原始结构化
        "list_item": list_item,
        "detail_result": detail_result,
        # 统计 + 元
        "stats": stats,
        "crawled_at": datetime.now(timezone.utc),
    }
    return doc


def dump_forum_item(client: httpx.Client, cfg: HttpConfig, item: dict,
                    forum_type: int, db, force: bool = False,
                    pdf_dir: Path | None = None, skip_pdf: bool = False,
                    force_pdf: bool = False) -> dict:
    """抓 detail, 组装 document, 下载 PDF 附件, 写入 forum collection."""
    fid = item.get("id")
    title = item.get("title") or ""
    release_time = pick_time(item)
    col = db[COL_FORUM]

    if not force:
        existing = col.find_one({"_id": fid},
                                {"_id": 1, "stats": 1, "pdf_attachments": 1, "pdf_url": 1,
                                 "content_md": 1, "title": 1})
        if existing:
            # 已存在 — 若 force_pdf 或之前 PDF 下载失败过, fall-through 重跑;
            # 否则直接 skip.
            atts = existing.get("pdf_attachments") or []
            has_pending_pdf = (
                pdf_dir is not None
                and not skip_pdf
                and parse_pdf_url_field(existing.get("pdf_url"))
                and (force_pdf or not atts or any(
                    (not a.get("pdf_size_bytes")) or a.get("pdf_download_error")
                    for a in atts
                ))
            )
            # Meritco-specific recovery passes:
            # (a) STAGED CONTENT — type 2/3 docs saved with empty content_md usually
            #     had articlePermission=false or transcript was still uploading;
            #     re-fetch on each list pass so they auto-heal as soon as the server
            #     publishes the full content.
            # (b) TITLE DRIFT — Meritco sometimes renames/replaces a forum id between
            #     list scans; detect the platform-side title change from the list
            #     response and re-fetch so we don't keep serving the stale placeholder.
            stale_empty_content = (
                forum_type in (2, 3)
                and not (existing.get("content_md") or "").strip()
            )
            list_title = (item.get("title") or "").strip()
            db_title = (existing.get("title") or "").strip()
            title_drift = bool(list_title and db_title and list_title != db_title)

            if not (has_pending_pdf or stale_empty_content or title_drift):
                return {"id": fid, "标题": title, "时间": release_time,
                        "状态": "已跳过",
                        **(existing.get("stats") or {})}

    detail_result = None
    try:
        data = with_retry(fetch_detail, cfg, f"detail {fid}", client, fid)
        if isinstance(data, dict) and data.get("code") == 200:
            detail_result = data.get("result") or None
        else:
            tqdm.write(f"  · detail {fid} code={data.get('code')} msg={data.get('message')!r}")
    except AuthExpired:
        raise
    except Exception as e:
        tqdm.write(f"  · detail {fid} 失败: {type(e).__name__}: {e}")

    doc = build_doc(item, detail_result, forum_type, release_time)

    # PDF 下载
    if pdf_dir is not None:
        attachments = parse_pdf_url_field(doc.get("pdf_url"))
        if attachments:
            results = download_attachments(
                client, attachments, fid, release_time, title,
                pdf_dir, force=force_pdf, skip_download=skip_pdf,
            )
            doc["pdf_attachments"] = results
            # 跨平台一致的单 PDF 字段 (取第一个附件, 绝大多数 meritco doc 只有 1 个)
            first = results[0]
            if first.get("pdf_rel_path"):
                doc["pdf_rel_path"] = first["pdf_rel_path"]
            if first.get("pdf_local_path"):
                doc["pdf_local_path"] = first["pdf_local_path"]
            if first.get("pdf_size_bytes") is not None:
                doc["pdf_size_bytes"] = first.get("pdf_size_bytes", 0)
            if first.get("pdf_download_error"):
                doc["pdf_download_error"] = first["pdf_download_error"]

    _stamp_ticker(doc, "meritco", col)
    col.replace_one({"_id": fid}, doc, upsert=True)

    result = {"id": fid, "标题": title, "时间": release_time,
              "状态": "新增", **doc["stats"]}
    atts = doc.get("pdf_attachments") or []
    if atts:
        ok = sum(1 for a in atts if a.get("pdf_size_bytes"))
        result["PDF"] = f"{ok}/{len(atts)}"
    return result


# ==================== 分页 ====================

def fetch_items_paginated(client, cfg: HttpConfig, forum_type: int,
                          max_items: int | None = None,
                          stop_at_id: int | None = None,
                          stop_before_ms: int | None = None):
    """翻页抓列表. 三种停止条件:
       - max_items: 累计 >= 后停
       - stop_at_id: 命中已知 top id 后停 (--resume)
       - stop_before_ms: 条目 operationTime < 该毫秒数则停 (用于 --today 统计)
    """
    all_items: list[dict] = []
    trace_id = f"{int(time.time()*1000)}{random.randint(100000, 999999):06x}"
    page = 1
    total = None

    while True:
        try:
            data = with_retry(fetch_list, cfg, f"list p{page}",
                              client, page, cfg.page_size, forum_type, trace_id)
        except AuthExpired:
            raise
        except Exception as e:
            tqdm.write(f"  [page {page}] 获取失败: {e}")
            break

        if data.get("code") != 200:
            msg = data.get("message") or ""
            hint = ""
            # 业务码 500 + "参数错误" 在 meritco 后端经常是权限/订阅失效的伪装
            if data.get("code") == 500 and "参数错误" in msg:
                hint = "  (后端 '参数错误' 常见成因: X-My-Header RSA 签名缺失或错误 — " \
                       "前端 2026-04 后对 forum/select/list 也强制签名. " \
                       "若账号/token 正常, 检查 sign_list_header() 公钥是否跟当前前端一致.)"
            tqdm.write(f"  [page {page}] code={data.get('code')} msg={msg!r}, 终止分页{hint}")
            break

        result = data.get("result") or {}
        items = result.get("forumList") or []
        total = result.get("total") or total

        if not items:
            tqdm.write(f"  [page {page}] 空页, 分页结束 (累计 {len(all_items)})")
            break

        stopped = False
        new_this_page = 0
        for it in items:
            # 列表不保证严格时间降序, hit_known/hit_old 扫完本页再翻页. 下游 dump
            # 有 per-item dedup 保证不重复入库.
            if stop_at_id is not None and it.get("id") == stop_at_id:
                stopped = True
                continue
            if stop_before_ms is not None:
                ts = time_to_ms(pick_time(it))
                if ts and ts < stop_before_ms:
                    stopped = True
                    continue
            all_items.append(it)
            new_this_page += 1
            if max_items and len(all_items) >= max_items:
                stopped = True
                break  # max 是硬上限, 真停

        tqdm.write(f"  [page {page}] +{new_this_page} (累计 {len(all_items)}/{total or '?'})  stopped={stopped}")

        if stopped:
            break
        if len(items) < cfg.page_size:
            tqdm.write(f"  [page {page}] partial ({len(items)} < {cfg.page_size}), 分页结束")
            break
        if total and len(all_items) >= total:
            break
        page += 1
        _THROTTLE.sleep_before_next()

    return all_items


# ==================== --today 统计 ====================

def count_today(client, db, forum_type: int, cfg: HttpConfig,
                date_str: str | None = None, save: bool = True) -> dict:
    from collections import Counter

    # Beijing-local calendar day: 平台 release_time 是 Asia/Shanghai 壁钟,
    # 所以 --today 必须用 BJ TZ 对齐.
    day = (datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
           if date_str else datetime.now(_BJ_TZ))
    day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start.replace(hour=23, minute=59, second=59, microsecond=999000)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)
    target = day_start.strftime("%Y-%m-%d")

    print(f"[统计] 扫描平台 {target} 的内容 (type={forum_type})...")
    items = fetch_items_paginated(client, cfg, forum_type,
                                  stop_before_ms=start_ms)
    today_items = [it for it in items
                   if start_ms <= time_to_ms(pick_time(it)) <= end_ms]

    ids = [it.get("id") for it in today_items if it.get("id") is not None]
    in_db = db[COL_FORUM].count_documents({"_id": {"$in": ids}}) if ids else 0

    ind_count = Counter()
    type_count = Counter()
    author_count = Counter()
    for it in today_items:
        ind_count[it.get("industry") or "未知"] += 1
        type_count[str(it.get("type") or "?")] += 1
        author_count[it.get("author") or "未知"] += 1

    stats = {
        "date": target,
        "forum_type": forum_type,
        "total_on_platform": len(today_items),
        "in_db": in_db,
        "not_in_db": len(today_items) - in_db,
        "by_industry_top10": ind_count.most_common(10),
        "by_type": dict(type_count),
        "by_author_top10": author_count.most_common(10),
        "scanned_at": datetime.now(timezone.utc),
    }

    print(f"\n{'='*55}")
    print(f"📅 {target} (type={forum_type}) 平台统计")
    print(f"{'='*55}")
    print(f"  平台总数:      {stats['total_on_platform']}")
    print(f"  本地已入库:    {stats['in_db']}")
    print(f"  待入库:        {stats['not_in_db']}")
    print(f"\n  按行业 Top10:")
    for ind, n in stats["by_industry_top10"]:
        print(f"    {ind[:20].ljust(20)}  {n}")
    print(f"\n  按作者 Top10:")
    for a, n in stats["by_author_top10"]:
        print(f"    {a[:20].ljust(20)}  {n}")
    print(f"\n  按 type:")
    for t, n in sorted(stats["by_type"].items(), key=lambda x: -x[1]):
        print(f"    type={t:<4}  {n}")
    print(f"{'='*55}\n")

    if save:
        doc = {**stats, "_id": f"daily_type{forum_type}_{target}"}
        doc["by_industry_top10"] = [[i, n] for i, n in stats["by_industry_top10"]]
        doc["by_author_top10"] = [[a, n] for a, n in stats["by_author_top10"]]
        db[COL_STATE].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        print(f"已保存到 {COL_STATE} collection (_id={doc['_id']})\n")
    return stats


# ==================== 补齐陈旧/空内容 ====================

def heal_stale_docs(client, cfg: HttpConfig, db, forum_type: int,
                    since_days: int = 7, max_items: int = 40,
                    pdf_dir: Path | None = None,
                    skip_pdf: bool = True) -> dict:
    """Re-fetch recent type 2/3 docs whose content_md never got populated.

    Meritco frequently publishes a post with articlePermission=False or with the
    transcript still uploading — at that moment the list response carries only
    summary/topic and the detail response returns content=null. `--resume` then
    early-stops on the known top id so the half-baked doc is never re-examined.
    This pass complements that by directly targeting `content_md == ""` docs
    from the last `since_days` and re-fetching them. Safe to run every pass;
    returns quickly once the platform stabilises.
    """
    if forum_type not in (2, 3):
        return {"checked": 0, "updated": 0}

    col = db[COL_FORUM]
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=since_days)).timestamp() * 1000)
    cursor = col.find({
        "$or": [{"forum_type": forum_type}, {"type": forum_type}],
        "release_time_ms": {"$gte": cutoff_ms},
        "$or": [{"content_md": ""}, {"content_md": None}],
    }, {"_id": 1, "title": 1, "content_md": 1}).sort("release_time_ms", -1).limit(max_items)

    targets = [d["_id"] for d in cursor]
    if not targets:
        return {"checked": 0, "updated": 0}

    print(f"[heal] type={forum_type} 发现 {len(targets)} 个近 {since_days}d 内 content_md 为空的旧 doc — 重取中...")
    updated = 0
    for fid in targets:
        try:
            data = with_retry(fetch_detail, cfg, f"heal-detail {fid}", client, fid)
            if data.get("code") != 200:
                continue
            result = data.get("result") or {}
            new_content = html_to_text(result.get("content") or "")
            # Skip if server still returns nothing OR the "no transcript" boilerplate.
            # build_doc normalizes the boilerplate to empty, so re-fetching only
            # rewrites the same empty value back — wasteful.
            if not new_content or new_content.strip() in {"本次活动无纪要产生", "本次活动暂无纪要"}:
                continue
            item = {
                "id": fid,
                "title": result.get("title") or "",
                "meetingTime": result.get("meetingTime"),
                "createTime": result.get("createTime"),
                "operationTime": result.get("operationTime"),
                "type": result.get("type") or forum_type,
            }
            # force=True so dump_forum_item rewrites the doc even though _id exists.
            dump_forum_item(client, cfg, item, forum_type, db,
                            force=True, pdf_dir=pdf_dir,
                            skip_pdf=skip_pdf, force_pdf=False)
            updated += 1
            print(f"  [heal] {fid} content_md 0 → {len(new_content)} chars")
        except AuthExpired:
            raise
        except Exception as e:
            print(f"  [heal] {fid} ERROR: {type(e).__name__}: {e}")
        _THROTTLE.sleep_before_next()

    return {"checked": len(targets), "updated": updated}


# ==================== 一轮抓取 ====================

def run_once_streaming(client, db, cfg: HttpConfig, args) -> dict:
    """Meritco streaming: per-page fetch → dump → deep_page checkpoint → next."""
    dump_account(client, db, force=args.force)
    state = load_state(db, args.type) or {}
    start_page = int(state.get("backfill_deep_page") or 1)
    print(f"[stream] type={args.type} resume_from_page={start_page} page_size={cfg.page_size}")

    pdf_dir: Path | None = None
    if getattr(args, "pdf_dir", None):
        pdf_dir = Path(args.pdf_dir).expanduser().resolve()
        pdf_dir.mkdir(parents=True, exist_ok=True)

    cap = cap_from_args(args)
    added = skipped = failed = 0
    trace_id = f"{int(time.time()*1000)}{random.randint(100000, 999999):06x}"
    page = start_page
    first_top = None
    total_seen = 0

    while True:
        if cap.exhausted() or _BUDGET.exhausted():
            print(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停")
            break
        try:
            data = with_retry(fetch_list, cfg, f"list p{page}",
                              client, page, cfg.page_size, args.type, trace_id)
        except AuthExpired:
            raise
        except Exception as e:
            print(f"  [page {page}] 获取失败: {e}")
            break

        if data.get("code") != 200:
            print(f"  [page {page}] code={data.get('code')} msg={data.get('message')!r}, 终止")
            break

        result = data.get("result") or {}
        items = result.get("forumList") or []
        total = result.get("total")
        if not items:
            print(f"  [page {page}] 空页, 列表到底 → 重置 backfill_deep_page=1")
            save_state(db, args.type, backfill_deep_page=1,
                       backfill_last_run_end_at=datetime.now(timezone.utc))
            break

        if first_top is None:
            first_top = items[0].get("id")

        page_added = page_skipped = page_failed = 0
        for item in items:
            if cap.exhausted() or _BUDGET.exhausted():
                break
            fid = item.get("id"); title = (item.get("title") or "")[:60]
            was_skip = False
            try:
                row = dump_forum_item(client, cfg, item, args.type, db,
                                      force=args.force, pdf_dir=pdf_dir,
                                      skip_pdf=args.skip_pdf, force_pdf=args.force_pdf)
                if row["状态"] == "已跳过":
                    skipped += 1; page_skipped += 1; was_skip = True
                else:
                    added += 1; page_added += 1
                    cap.bump(); _BUDGET.bump()
                    print(f"  ✓ [{fid}] {title}")
            except AuthExpired:
                raise
            except Exception as e:
                failed += 1; page_failed += 1
                print(f"  ✗ [{fid}] {title}  ERR: {type(e).__name__}: {e}")
            total_seen += 1
            if not was_skip:
                _THROTTLE.sleep_before_next()
            if args.max and total_seen >= args.max:
                break

        save_state(db, args.type, backfill_deep_page=page + 1,
                   backfill_last_page_at=datetime.now(timezone.utc),
                   in_progress=True)
        print(f"  [page {page}] +{page_added} ={page_skipped} ✗{page_failed} "
              f"(累计 +{added} ={skipped} ✗{failed}) total≈{total}")

        if args.max and total_seen >= args.max:
            break
        if len(items) < cfg.page_size:
            print(f"  [page {page}] partial, 列表到底 → 重置 backfill_deep_page=1")
            save_state(db, args.type, backfill_deep_page=1,
                       backfill_last_run_end_at=datetime.now(timezone.utc))
            break
        page += 1
        _THROTTLE.sleep_before_next()

    if first_top is not None and start_page == 1:
        save_state(db, args.type, top_id=first_top)
    save_state(db, args.type, in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})
    print(f"  完成: +{added} ={skipped} ✗{failed}")
    return {"added": added, "skipped": skipped, "failed": failed}


def run_once(client, db, cfg: HttpConfig, args) -> dict:
    if getattr(args, "stream_backfill", False):
        return run_once_streaming(client, db, cfg, args)
    dump_account(client, db, force=args.force)

    state = load_state(db, args.type)
    stop_id = state.get("top_id") if args.resume else None
    if args.resume and stop_id:
        print(f"[恢复] 上次 top id={stop_id} (更新于 {state.get('updated_at')}), 将在遇到此条时停止")
    elif args.resume:
        print("[恢复] 未找到 checkpoint, 按全量爬")

    stop_ms = None
    if getattr(args, "since_hours", None) is not None:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        stop_ms = int(cutoff_dt.timestamp() * 1000)
        local_str = cutoff_dt.astimezone().strftime("%Y-%m-%d %H:%M")
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 (cutoff={local_str})")

    # PDF 目录 (若开启)
    pdf_dir: Path | None = None
    if getattr(args, "pdf_dir", None):
        pdf_dir = Path(args.pdf_dir).expanduser().resolve()
        pdf_dir.mkdir(parents=True, exist_ok=True)
        print(f"[PDF] 启用附件下载: dir={pdf_dir}  skip={args.skip_pdf}  force={args.force_pdf}")

    print(f"\n[列表] type={args.type}  max={args.max or '全部'}  pageSize={args.page_size}")
    items = fetch_items_paginated(client, cfg, args.type,
                                  max_items=args.max,
                                  stop_at_id=stop_id,
                                  stop_before_ms=stop_ms)
    print(f"[列表] 共 {len(items)} 条待处理\n")
    if not items:
        print("无新内容 (或账号失效)")
        return {"added": 0, "skipped": 0, "failed": 0}

    new_top = items[0].get("id")

    added = skipped = failed = 0
    cap = cap_from_args(args)
    pbar = tqdm(items, desc="抓取", unit="条", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")
    for item in pbar:
        if cap.exhausted() or _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        fid = item.get("id")
        title = (item.get("title") or "")[:60]
        was_skip = False
        try:
            row = dump_forum_item(client, cfg, item, args.type, db,
                                  force=args.force, pdf_dir=pdf_dir,
                                  skip_pdf=args.skip_pdf, force_pdf=args.force_pdf)
            if row["状态"] == "已跳过":
                skipped += 1
                was_skip = True
                tqdm.write(f"  · [{fid}] {title}  已存在")
            else:
                added += 1
                cap.bump(); _BUDGET.bump()
                pdf_tag = f"  PDF={row['PDF']}" if "PDF" in row else ""
                tqdm.write(
                    f"  ✓ [{fid}] {title}  "
                    f"正文{row.get('正文字数', 0)}字 "
                    f"速览{row.get('速览字数', 0)}字 "
                    f"摘要{row.get('摘要字数', 0)}字"
                    f"{pdf_tag}"
                )
        except AuthExpired:
            raise
        except Exception as e:
            failed += 1
            tqdm.write(f"  ✗ [{fid}] {title}  ERR: {type(e).__name__}: {e}")

        pbar.set_postfix_str(f"新增={added} 跳过={skipped} 失败={failed}")
        save_state(db, args.type,
                   last_processed_id=fid,
                   last_processed_at=datetime.now(timezone.utc),
                   in_progress=True)
        # DB dedup hits made no network call — skip the throttle to avoid
        # burning hours re-scanning the top of an already-indexed list.
        if not was_skip:
            _THROTTLE.sleep_before_next()
    pbar.close()

    save_state(db, args.type,
               top_id=new_top,
               in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})

    total = db[COL_FORUM].count_documents({"forum_type": args.type})
    print(f"\n本轮完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}")
    print(f"MongoDB 当前 type={args.type} 条目数: {total}")
    print(f"Checkpoint 已更新, 下次加 --resume 可增量续爬")

    # Staged-content recovery: catch docs whose detail was permission-gated or
    # transcript-pending at first fetch. Bounded to last 7d / 40 items so it stays
    # cheap on every tick but heals the 24-72h staging window.
    if args.type in (2, 3):
        try:
            heal_stats = heal_stale_docs(client, cfg, db, args.type,
                                         since_days=7, max_items=40,
                                         pdf_dir=pdf_dir, skip_pdf=args.skip_pdf)
            if heal_stats["updated"]:
                print(f"[heal] 补齐 {heal_stats['updated']}/{heal_stats['checked']} 个 content_md 空的旧 doc")
        except AuthExpired:
            raise
        except Exception as e:
            print(f"[heal] 非致命错误, 跳过: {type(e).__name__}: {e}")

    return {"added": added, "skipped": skipped, "failed": failed}


# ==================== CLI ====================

def parse_args():
    token_env = os.environ.get("MERITCO_AUTH", "")
    default_token, _ = load_creds_from_file()

    p = argparse.ArgumentParser(description="research.meritco-group.com 论坛爬虫 (MongoDB 存储)")
    p.add_argument("--type", type=str, default="2",
                   help="forumType (默认 2=专业内容; 1=活动, 3=久谦自研; 可逗号分隔多个, 如 '2,3')")
    p.add_argument("--max", type=int, default=None,
                   help="最多爬取条数 (默认: 全部, 一直翻到空)")
    p.add_argument("--page-size", type=int, default=40, help="每页大小 (默认 40)")
    p.add_argument("--force", action="store_true",
                   help="强制重爬已入库的条目 (默认跳过)")
    p.add_argument("--stream-backfill", action="store_true",
                   help="流式回填: 每抓完一页立即入库 + 保存 backfill_deep_page checkpoint")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 从上次 checkpoint 续爬, 遇到已知 top 停止分页")
    p.add_argument("--show-state", action="store_true",
                   help="显示当前 checkpoint 后退出")
    p.add_argument("--reset-state", action="store_true",
                   help="清除所有 checkpoint 后退出")
    p.add_argument("--today", action="store_true",
                   help="统计今天 (或 --date) 平台内容数并与本地对比, 存入 _state")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today 指定日期 (默认今天)")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600)")
    p.add_argument("--since-hours", type=float, default=None,
                   help="只抓取过去 N 小时内更新 (按 operationTime). "
                        "默认不限制 (由 --resume / 全量决定)")
    p.add_argument("--delay", type=float, default=1.5,
                   help="请求间延迟秒数 (默认 1.5)")
    p.add_argument("--max-retries", type=int, default=5)
    p.add_argument("--auth", default=token_env or default_token,
                   help="token (或通过环境变量 MERITCO_AUTH / credentials.json 传入)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT,
                   help=f"MongoDB URI (默认 {MONGO_URI_DEFAULT}, 或 env MONGO_URI)")
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT,
                   help=f"MongoDB 数据库名 (默认 {MONGO_DB_DEFAULT}, 或 env MONGO_DB)")
    # PDF 附件
    p.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT,
                   help=f"PDF 下载目录 (默认 {PDF_DIR_DEFAULT}, "
                        f"env MERITCO_PDF_DIR 可覆盖; 设为空字符串 '' 禁用)")
    p.add_argument("--skip-pdf", action="store_true",
                   help="只记录附件元数据, 不下载 PDF 文件")
    p.add_argument("--force-pdf", action="store_true",
                   help="强制重下本地已存在的 PDF")
    p.add_argument("--pdf-only", action="store_true",
                   help="backfill 模式: 不抓列表, 只扫已入库文档, 补齐缺失 PDF")
    # 反爬节流 (crawl/antibot.py) — default_cap 2026-04-25 500→0: 实时档不再数量闸
    add_antibot_args(p, default_base=3.0, default_jitter=2.0,
                     default_burst=40, default_cap=0, platform="meritco")
    return p.parse_args()


def run_pdf_backfill(client: httpx.Client, db, args) -> dict:
    """扫描已入库文档里 pdf_url 非空的条目, 尝试下载每个附件到本地.

    跳过规则 (幂等):
      - 本地文件已存在且 size > 0, 且对应 attachment 记录 pdf_size_bytes > 0  -> 跳过
      - --force-pdf 强制重下
    触发 antibot 节流 + daily-cap 保护.
    """
    if not args.pdf_dir:
        print("错误: --pdf-only 需要 --pdf-dir 非空", file=sys.stderr)
        return {"processed": 0, "downloaded": 0, "failed": 0}
    pdf_dir = Path(args.pdf_dir).expanduser().resolve()
    pdf_dir.mkdir(parents=True, exist_ok=True)
    print(f"[backfill] PDF 目录: {pdf_dir}")

    col = db[COL_FORUM]
    # pdf_url 非空 (非 "", 非 "[]", 非 null) 的所有文档
    cursor = col.find(
        {"pdf_url": {"$regex": "\"url\":"}},
        {"_id": 1, "pdf_url": 1, "release_time": 1, "title": 1,
         "forum_type": 1, "pdf_attachments": 1},
    ).sort("release_time_ms", -1)
    docs = list(cursor)
    print(f"[backfill] 待处理文档: {len(docs)}")
    if not docs:
        return {"processed": 0, "downloaded": 0, "failed": 0}

    processed = downloaded = failed = 0
    cap = cap_from_args(args)
    pbar = tqdm(docs, desc="PDF backfill", unit="doc", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")
    try:
        for doc in pbar:
            if cap.exhausted() or _BUDGET.exhausted():
                tqdm.write(f"[antibot] daily-cap={cap.max_items} 到, 本轮停")
                break

            fid = doc["_id"]
            title = (doc.get("title") or "")[:50]
            release_time = doc.get("release_time") or ""
            attachments = parse_pdf_url_field(doc.get("pdf_url"))
            if not attachments:
                continue

            # 已全下 + 非 force: 快速跳过 (不占 cap)
            existing_atts = doc.get("pdf_attachments") or []
            if (not args.force_pdf and existing_atts
                    and len(existing_atts) == len(attachments)
                    and all(a.get("pdf_size_bytes") for a in existing_atts)):
                tqdm.write(f"  · [{fid}] {title}  已下载 (skip)")
                continue

            try:
                results = download_attachments(
                    client, attachments, fid, release_time, title,
                    pdf_dir, force=args.force_pdf, skip_download=args.skip_pdf,
                )
            except AuthExpired:
                raise
            except Exception as e:
                failed += 1
                tqdm.write(f"  ✗ [{fid}] {title}  {type(e).__name__}: {e}")
                processed += 1
                continue

            ok = sum(1 for r in results if r.get("pdf_size_bytes"))
            sizes = sum(r.get("pdf_size_bytes", 0) for r in results)
            if ok == len(results):
                downloaded += 1
                cap.bump(); _BUDGET.bump()
                tqdm.write(
                    f"  ✓ [{fid}] {title}  {ok}/{len(results)} PDFs, {sizes/1024:.0f} KB"
                )
            elif ok > 0:
                downloaded += 1
                failed += 1
                cap.bump(); _BUDGET.bump()
                errs = "; ".join(r.get("pdf_download_error", "") for r in results if r.get("pdf_download_error"))
                tqdm.write(
                    f"  ! [{fid}] {title}  部分成功 {ok}/{len(results)}  err={errs[:80]}"
                )
            else:
                failed += 1
                errs = "; ".join(r.get("pdf_download_error", "") for r in results if r.get("pdf_download_error"))
                tqdm.write(f"  ✗ [{fid}] {title}  全失败  err={errs[:120]}")

            # 更新 MongoDB
            update = {
                "pdf_attachments": results,
            }
            first = results[0] if results else None
            if first:
                if first.get("pdf_rel_path"):
                    update["pdf_rel_path"] = first["pdf_rel_path"]
                if first.get("pdf_local_path"):
                    update["pdf_local_path"] = first["pdf_local_path"]
                if first.get("pdf_size_bytes") is not None:
                    update["pdf_size_bytes"] = first.get("pdf_size_bytes", 0)
                if first.get("pdf_download_error"):
                    update["pdf_download_error"] = first["pdf_download_error"]
                else:
                    update["pdf_download_error"] = ""  # 清除残留失败标记
            col.update_one({"_id": fid}, {"$set": update})

            processed += 1
            pbar.set_postfix_str(f"下载={downloaded} 失败={failed}")
            _THROTTLE.sleep_before_next()
    finally:
        pbar.close()

    print(f"\nbackfill 完成: 处理 {processed} / 下载成功 {downloaded} / 失败 {failed}")
    return {"processed": processed, "downloaded": downloaded, "failed": failed}


def check_token(client: httpx.Client) -> dict:
    """快速探活: 调 user-info 看 token 还活不活, 并提取 expireDate / meritcoId."""
    try:
        resp = client.post("/meritco-user/research/info/get",
                           json={"platform": "RESEARCH_PC"})
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    if resp.status_code != 200:
        return {"ok": False, "error": f"HTTP {resp.status_code}"}
    data = resp.json()
    if data.get("code") != 200:
        return {"ok": False, "error": f"code={data.get('code')} msg={data.get('message')}"}
    info = ((data.get("result") or {}).get("userInfo") or {})
    r = data.get("result") or {}
    return {
        "ok": True,
        "meritcoId": r.get("meritcoId") or info.get("meritcoId"),
        "email": info.get("email"),
        "company": info.get("company"),
        "expireDate": r.get("expireDate"),
        "visit": r.get("visit"),
    }


def main() -> int:
    args = parse_args()

    types = [int(t.strip()) for t in str(args.type).split(",") if t.strip()]
    if not types:
        print("错误: --type 不能为空", file=sys.stderr)
        return 1

    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform="meritco")
    # account_id 取 token 头 12 字符 hash (meritco JWT-style 但解析比较繁琐)
    import hashlib as _hl
    _account_id = "h_" + _hl.md5((args.auth or "").encode()).hexdigest()[:12]
    _BUDGET = budget_from_args(args, account_id=_account_id, platform="meritco")
    log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET,
                     extra=f"acct={_account_id}")

    db = connect_mongo(args.mongo_uri, args.mongo_db)

    if args.show_state:
        for t in types:
            args.type = t
            doc = load_state(db, t)
            if not doc:
                print(f"(type={t}) 无 checkpoint")
            else:
                print(f"--- type={t} checkpoint ---")
                print(json.dumps({k: str(v) if isinstance(v, datetime) else v
                                  for k, v in doc.items()},
                                 ensure_ascii=False, indent=2))
            total = db[COL_FORUM].count_documents({"forum_type": t})
            print(f"\ntype={t} 已入库: {total}  account: {db[COL_ACCOUNT].estimated_document_count()}")
        # token 健康检查 (对排查"参数错误"很有用)
        if args.auth:
            _, ua = load_creds_from_file()
            client = create_client(args.auth, ua or default_user_agent())
            info = check_token(client)
            client.close()
            if info["ok"]:
                print(f"\n[token] ✓ uid={info['meritcoId']} "
                      f"email={info['email']} company={info['company']} "
                      f"expireDate={info['expireDate']} visit={info['visit']} "
                      f"(expireDate 不代表 role 到期, 真正到期日在 userMenus.roles[].roleExpireTimeStr)")
            else:
                print(f"\n[token] ✗ 失效: {info['error']}")
        return 0

    if args.reset_state:
        n = reset_all_state(db)
        print(f"已清除 {n} 条 checkpoint / daily 统计")
        return 0

    if not args.auth:
        print("错误: 未提供 token. 请写入 credentials.json 或通过 --auth / env MERITCO_AUTH 传入", file=sys.stderr)
        return 1

    _, ua_from_file = load_creds_from_file()
    cfg = HttpConfig(page_size=args.page_size, delay=args.delay, max_retries=args.max_retries)
    client = create_client(args.auth, ua_from_file or default_user_agent(),
                           timeout=cfg.timeout)

    try:
        if args.today:
            for t in types:
                count_today(client, db, t, cfg, date_str=args.date)
            return 0

        if args.pdf_only:
            try:
                run_pdf_backfill(client, db, args)
            except AuthExpired as e:
                print(f"\n!!! TOKEN EXPIRED: {e}")
                print("!!! 更新 credentials.json 中的 token 后重跑 --pdf-only")
                return 2
            return 0

        if args.watch:
            print(f"[实时模式] types={types}  每 {args.interval}s 轮询一次. Ctrl+C 退出.\n")
            round_num = 0
            while True:
                round_num += 1
                print(f"\n{'='*60}\n[轮次 {round_num}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}")
                for t in types:
                    args.type = t
                    print(f"\n----- [轮次 {round_num}] type={t} -----")
                    try:
                        run_once(client, db, cfg, args)
                    except AuthExpired as e:
                        print(f"\n!!! TOKEN EXPIRED: {e}")
                        print("!!! 更新 credentials.json 中的 token 后重启")
                        return 2
                    except KeyboardInterrupt:
                        print("\n[实时模式] Ctrl+C 退出")
                        return 0
                    except Exception as e:
                        print(f"[轮次 {round_num} type={t}] 异常: {type(e).__name__}: {e}")
                try:
                    time.sleep(args.interval)
                except KeyboardInterrupt:
                    print("\n[实时模式] Ctrl+C 退出")
                    break
        else:
            for t in types:
                args.type = t
                try:
                    run_once(client, db, cfg, args)
                except AuthExpired as e:
                    print(f"\n!!! TOKEN EXPIRED: {e}")
                    print("!!! 更新 credentials.json 中的 token 后重跑 (可加 --resume 续爬)")
                    return 2
    finally:
        client.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
