"""Shared anti-bot / rate-limiting primitives for crawl/*/scraper.py.

设计目标 — 在不降低可用性的前提下,把爬取行为改造得更像真人浏览, 同时
确保实时档(低延迟) 与回填档(夜间安全) 都有保险, 避免任何单次故障升级到封号:

1. **节奏抖动: Gaussian + long-tail 阅读停留 + idle 切 tab 停留**
   均匀抖动是机器特征 (后端方差检测一抓一个准). Gaussian 拟合人类反应时间分布,
   再以 5% 概率叠加 5-30s long-tail ("读完一条停一下"), 3% 概率叠加 60-180s
   idle window ("切了个 tab 离开一会儿"). 三层让请求间隔的高阶矩跟真人对齐.
2. **突发冷却**: 每 N 条请求后静止 30~60s (模拟用户"读一下再继续")
3. **指数退避**: 429/5xx 上逐步加长, 尊重 `Retry-After`
4. **会话死亡快速失败**: 401/403 = 会话被吊销, 立即抛 `SessionDead` 让调用方提示用户重登, 不要重试
5. **DailyCap** (legacy, 实时档默认禁用 2026-04-25): 单进程上限. 实时档不再
   靠"每轮 N 条"这种量闸防跑飞 — WAF 关心的是节奏和指纹, 不是 24h 总数.
   backfill 脚本仍会自带一个保守值做单进程兜底, 实时档 watcher 默认 0.
6. **AccountBudget** (rt 主桶默认禁用 2026-04-25): 跨进程账号配额, Redis backed.
   旧版每平台 1500~20000 作为 rt 硬封顶, 实际效果是**撞顶就漏抓增量**
   (alphapai report 单日 881 条撞 3000 就是这么来的), 反爬价值≈0.
   现在: rt 默认不启用 (0); bg (backfill) 桶保留, 用作 "realtime floor 让位"
   的比较基准 (backfill 在 rt 用量 >= 70% 时暂停). floor 参考值见
   `_DEFAULT_ACCOUNT_BUDGET`, 已不作为 rt 硬闸.
7. **SoftCooldown (核心)**: 软警告全局冷却. 任何 watcher 触发警告 (软 429 / 配额
   截断 / captcha cookie / 风控关键词) → 同平台所有 watcher 静默 30~60min,
   不等到 401/403 才退. Redis backed, 跨进程立即生效. 实时档去掉数量闸后,
   这一层 + 指纹 + 节奏是主要防线.
8. **时段倍增**: 23:00-07:00 CST × 2.5, 周末 × 1.8, 12:00-13:30 × 1.3.
   24/7 平摊节奏一看就是 cron, 工时形态拉低识别率.
9. **进程级 UA 池 + Chrome 126 现代 header**: 18 个 watcher 共享 UA 是教科书级
   bot signature, pool 按 process label 稳定 hash 映射到 5-8 个 Chrome 122-126
   Win/Mac UA. `headers_for_platform` 一并配齐 `Priority: u=1, i` 和完整
   `sec-ch-ua-*` 指纹 (arch/bitness/full-version-list/model/platform-version),
   跟真实 Chrome 126 的 XHR 指纹对齐.
10. **会话 warmup (新 2026-04-25)**: `warmup_session(session, platform)` 在
    scraper create_session 里调一次, 先 GET 一次 landing HTML 再做 XHR, 模拟
    真人打开 SPA 的顺序. 幂等, 失败不影响调用方.

使用方式 (每个 scraper 在顶部):

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from antibot import (
        AdaptiveThrottle, DailyCap, SessionDead, parse_retry_after,
        AccountBudget, SoftCooldown, pick_user_agent, headers_for_platform,
    )

    throttle = AdaptiveThrottle(base_delay=3.0, jitter=2.0, burst_size=40,
                                 platform="alphapai", account_id=ACCOUNT_ID)
    cap = DailyCap(500)
    budget = AccountBudget("alphapai", ACCOUNT_ID, daily_limit=1500)

    for item in items:
        if cap.exhausted() or budget.exhausted():
            break
        try:
            process(item)
        except SessionDead as e:
            print(f"会话已被吊销: {e}. 请更新凭证.")
            return
        cap.bump(); budget.bump()
        throttle.sleep_before_next()    # 自动 + 时段倍增 + 软冷却 wait

所有参数都可以被 CLI flag 覆盖, 每个 scraper 暴露:

    --throttle-base N     (秒, 基础间隔)
    --throttle-jitter N   (秒, 高斯标准差近似)
    --burst-size N        (多少条请求后冷却一次)
    --daily-cap N         (单轮最多抓多少条, 防止封号)
    --account-budget N    (24h 跨进程账号上限, 0=禁用)
    --no-time-of-day      (禁用工时倍增, 默认开启)
    --no-soft-cooldown    (禁用软警告全局冷却, 调试用)
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import os
import random
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

# Redis is optional: 没装/没起就退化为单进程模式 (本机字典假装跨进程).
# 这样新加的代码在开发机/CI 上不会强制依赖 Redis.
try:
    import redis as _redis  # type: ignore
    _REDIS_AVAILABLE = True
except Exception:
    _REDIS_AVAILABLE = False
    _redis = None  # type: ignore


# ============================================================================
# 共享 Redis 客户端 (账号预算 + 软冷却共用)
# ============================================================================

_REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
_REDIS_CLIENT = None


def _get_redis():
    """Lazy singleton Redis client. Returns None if Redis is unavailable —
    callers must handle the None case (degrade to single-process semantics)."""
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    if not _REDIS_AVAILABLE:
        return None
    try:
        c = _redis.Redis.from_url(  # type: ignore
            _REDIS_URL, socket_timeout=2, socket_connect_timeout=2,
            decode_responses=True,
        )
        c.ping()
        _REDIS_CLIENT = c
        return c
    except Exception:
        return None


# In-memory fallback for AccountBudget / SoftCooldown when Redis is down.
# Single-process only — fine for `--show-state` / unit tests / dev.
_MEM_BUDGET: dict[str, list[float]] = {}     # key -> list of unix ts in last 24h
_MEM_COOLDOWN: dict[str, float] = {}         # key -> unix ts when cooldown ends


# ============================================================================
# 异常
# ============================================================================

class SessionDead(Exception):
    """Session has been revoked server-side (401/403 on previously-working session).

    Scraper should **abort immediately** and ask the user to refresh cookies/tokens.
    Do NOT retry — continued hammering on a dead session just extends the ban.
    """


# ============================================================================
# 用户代理池 (进程级稳定映射)
# ============================================================================

# Chrome 122-126 across Win10/Win11/macOS Sonoma — 都是 2025 年仍非常常见的 UA,
# 不会因为版本太老或太新被聚类成"一群机器人". 每个 process label 通过
# stable hash 选定一个, 重启不会跳变 (账号-UA 持久绑定看起来更像真人单设备).
_UA_POOL = [
    # Win 11 Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # macOS Sonoma Chrome (m1/m2)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    # Win 10 Edge (small share, adds variety)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.97",
]


def _process_label() -> str:
    """A stable label for the current scraper process: prefer
    CRAWLER_PROCESS_LABEL (set by crawler_monitor), fallback to argv[1:] joined.
    Same scraper invocation always gets the same label across restarts."""
    explicit = os.environ.get("CRAWLER_PROCESS_LABEL", "").strip()
    if explicit:
        return explicit
    # argv[0] is scraper.py; tail captures --category/--type/--reports etc.
    # Skip --auth/credential strings to avoid token leakage in label.
    parts = []
    skip = False
    for a in sys.argv[1:]:
        if skip:
            skip = False
            continue
        if a in ("--auth", "--mongo-uri"):
            skip = True
            continue
        parts.append(a)
    base = os.path.basename(os.path.dirname(os.path.abspath(sys.argv[0])) or ".")
    return f"{base}|{'_'.join(parts)}"[:120]


def pick_user_agent(label: Optional[str] = None) -> str:
    """Return a Chrome UA stably mapped from a process label.

    Same label → same UA across restarts (so platform-side fingerprint stays
    consistent for that "user"). Different watchers under the same scraper
    end up with different UAs because their argv tails differ.
    """
    lbl = label or _process_label()
    h = int(hashlib.md5(lbl.encode()).hexdigest(), 16)
    return _UA_POOL[h % len(_UA_POOL)]


# Per-platform `Accept-Language` / `Sec-CH-UA-Platform` / referer aligned with
# the platform's expected user base. Mismatch is a strong WAF signal.
_PLATFORM_HEADERS = {
    # Chinese platforms
    # alphapai referer: SPA 登录落在 /reading/home/, XHR 从这里发起. 用根 "/"
    # 相当于告诉后端 "我刚访问主域就发 API", 明显 bot 指纹. 改成 landing 更真实.
    "alphapai":    {"accept_language": "zh-CN,zh;q=0.9,en;q=0.6",
                    "sec_ch_ua_platform": '"Windows"',
                    "referer": "https://alphapai-web.rabyte.cn/reading/home/"},
    # jinmen referer: 所有 XHR 从 brm.comein.cn (登录后台 SPA) 发出, 不是
    # www.comein.cn (C 端主站, 基本不触碰 /comein/* 业务 API). 用 www 等于
    # 告诉后端 "我从游客主页直接调私有 API", 明显 bot 指纹. scraper.create_session
    # 已显式覆盖成 brm, 这里同步默认值以防调 headers_for_platform("jinmen") 不覆盖.
    "jinmen":      {"accept_language": "zh-CN,zh;q=0.9,en;q=0.6",
                    "sec_ch_ua_platform": '"Windows"',
                    "referer": "https://brm.comein.cn/"},
    "meritco":     {"accept_language": "zh-CN,zh;q=0.9,en;q=0.6",
                    "sec_ch_ua_platform": '"Windows"',
                    "referer": "https://www.meritco-group.com/"},
    "gangtise":    {"accept_language": "zh-CN,zh;q=0.9,en;q=0.6",
                    "sec_ch_ua_platform": '"Windows"',
                    "referer": "https://open.gangtise.com/research/"},
    # acecamp 真实域是 acecamptech.com (scraper 用 WEB_BASE=https://www.acecamptech.com
    # 覆盖过). 旧默认 ace-camp.com 是无关域名 — 2026-04-25 纠正, 这样
    # warmup_session / 默认 Referer 都打到正确 landing.
    "acecamp":     {"accept_language": "zh-CN,zh;q=0.9,en;q=0.6",
                    "sec_ch_ua_platform": '"Windows"',
                    "referer": "https://www.acecamptech.com/"},
    # alphaengine referer: API host 是 www.alphaengine.top, 所有 XHR 从同域
    # /#/summary-center SPA 发起. 旧默认 app.alphaengine.com.cn 是老版本或不同环境
    # 的域, 跟 API_BASE 不匹配 → WAF 看到 Referer/Origin 跨域会扣分. scraper
    # create_session 已显式覆盖 (所以实时线路安全), 这里同步默认值以防
    # headers_for_platform("alphaengine") 不覆盖的调用路径失真.
    "alphaengine": {"accept_language": "zh-CN,zh;q=0.9,en;q=0.6",
                    "sec_ch_ua_platform": '"Windows"',
                    "referer": "https://www.alphaengine.top/"},
    "thirdbridge": {"accept_language": "en-US,en;q=0.9,zh-CN;q=0.6",
                    "sec_ch_ua_platform": '"Windows"',
                    "referer": "https://www.thirdbridge.com/"},
    # English / US platforms
    "funda":          {"accept_language": "en-US,en;q=0.9",
                       "sec_ch_ua_platform": '"Windows"',
                       "referer": "https://funda.ai/"},
    "sentimentrader": {"accept_language": "en-US,en;q=0.9",
                       "sec_ch_ua_platform": '"Windows"',
                       "referer": "https://sentimentrader.com/"},
    "semianalysis":   {"accept_language": "en-US,en;q=0.9",
                       "sec_ch_ua_platform": '"macOS"',
                       "referer": "https://newsletter.semianalysis.com/archive"},
    "the_information": {"accept_language": "en-US,en;q=0.9",
                        "sec_ch_ua_platform": '"Windows"',
                        "referer": "https://www.theinformation.com/"},
}


# Chrome major → 真实 build full version. Sec-CH-UA-Full-Version-List 是 Chrome 86+
# 从 `sec-ch-ua` 派生出的 "详细版本" 扩展, 光给 major 会露馅 — 现代 Chrome 默认都发.
_CHROME_FULL_VERSIONS = {
    "126": "126.0.6478.127",
    "125": "125.0.6422.142",
    "124": "124.0.6367.207",
    "123": "123.0.6312.122",
    "122": "122.0.6261.129",
}


def headers_for_platform(platform: str, label: Optional[str] = None) -> dict:
    """Build a baseline header dict for a platform: UA + locale + full Chrome 126
    client-hint fingerprint. Caller layers Authorization / Content-Type on top.

    2026-04-25 (v2.2): 补齐 Chrome 126 默认头 — `Priority: u=1, i` (RFC 9218 hint),
    全套 `sec-ch-ua-arch/bitness/full-version-list/model/platform-version`. 缺这些
    在现代 Chrome UA 下是硬指纹 (Akamai/Datadome/Cloudflare Turnstile 都会查).

    不加 zstd 到 Accept-Encoding — requests/httpx 不原生支持 zstd 解压,
    response 返回 zstd 压缩字节 scraper 会解析失败. `gzip, deflate, br` 对 Chrome
    122~125 完全合理 (125 才默认加 zstd, 历史 UA 普遍禁用 zstd).
    """
    cfg = _PLATFORM_HEADERS.get(platform, _PLATFORM_HEADERS["alphapai"])
    ua = pick_user_agent(label)
    chrome_ver = "126"
    for marker in ("Chrome/126", "Chrome/125", "Chrome/124", "Chrome/123", "Chrome/122"):
        if marker in ua:
            chrome_ver = marker.split("/")[1]
            break
    full_ver = _CHROME_FULL_VERSIONS.get(chrome_ver, f"{chrome_ver}.0.0.0")
    sec_ch_ua = (f'"Chromium";v="{chrome_ver}", '
                 f'"Not.A/Brand";v="24", '
                 f'"Google Chrome";v="{chrome_ver}"')
    sec_ch_ua_full = (f'"Chromium";v="{full_ver}", '
                      f'"Not.A/Brand";v="24.0.0.0", '
                      f'"Google Chrome";v="{full_ver}"')
    is_mac = cfg["sec_ch_ua_platform"] == '"macOS"'
    # UA pool 里 mac 都是 Intel ("Intel Mac OS X"), 没有 arm64. Win 也是 x86.
    arch = '"x86"'
    bitness = '"64"'
    platform_ver = '"14.5.0"' if is_mac else '"15.0.0"'
    return {
        "User-Agent": ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": cfg["accept_language"],
        # HTTP/2 priority hint (RFC 9218). Chrome 126+ always sends for XHR.
        "Priority": "u=1, i",
        # Client Hints - full Chrome 126 set
        "sec-ch-ua": sec_ch_ua,
        "sec-ch-ua-arch": arch,
        "sec-ch-ua-bitness": bitness,
        "sec-ch-ua-full-version-list": sec_ch_ua_full,
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": cfg["sec_ch_ua_platform"],
        "sec-ch-ua-platform-version": platform_ver,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Referer": cfg["referer"],
        "Origin": cfg["referer"].rstrip("/"),
    }


def warmup_session(session, platform: str,
                   pause_min: float = 2.0, pause_max: float = 5.0,
                   verbose: bool = True) -> bool:
    """首次建连时先访问 landing 页 — 真人打开 SPA 必然先请求 HTML 再 XHR,
    直接干 XHR 是硬指纹. 幂等 (对同一 session 多次调用只执行一次).

    Works for both requests.Session and httpx.Client (都支持 .get 和 setattr).

    返回 True 表示 warmup 成功或已 warmed,False 表示失败 (不影响调用方继续).
    失败不抛异常 — landing 可能被 CDN 重定向 / 返回 HTML 登录页, 这不算错误.
    """
    if getattr(session, '_antibot_warmed', False):
        return True
    cfg = _PLATFORM_HEADERS.get(platform)
    if not cfg:
        try:
            session._antibot_warmed = True
        except Exception:
            pass
        return False
    landing = (cfg.get('referer') or '').rstrip('/')
    if not landing:
        try:
            session._antibot_warmed = True
        except Exception:
            pass
        return False
    # Navigate-style headers — 真人打开主页走的是 HTML GET, 不是 XHR.
    # Sec-Fetch-* 切到 navigate; Accept 切到 HTML-first.
    warmup_headers = {
        "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                   "image/avif,image/webp,image/apng,*/*;q=0.8"),
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        r = session.get(landing, headers=warmup_headers, timeout=8,
                        allow_redirects=True)
        status = getattr(r, 'status_code', 0)
        size = len(getattr(r, 'content', b'') or b'')
        ncookies = 0
        try:
            ncookies = len(dict(r.cookies))
        except Exception:
            pass
        if verbose:
            print(f"  [warmup] {platform} GET {landing} -> {status} "
                  f"({size}B, cookies+{ncookies})", flush=True)
        time.sleep(random.uniform(pause_min, pause_max))
        try:
            session._antibot_warmed = True
        except Exception:
            pass
        return True
    except Exception as e:
        if verbose:
            print(f"  [warmup] {platform} failed: {type(e).__name__}: {e} "
                  f"(ignore, continue)", flush=True)
        try:
            session._antibot_warmed = True
        except Exception:
            pass
        return False


# ============================================================================
# 时段倍增 (工时形态)
# ============================================================================

def time_of_day_multiplier(now: Optional[_dt.datetime] = None) -> float:
    """How much to multiply throttle by, based on local CST time.

    Idea: real analysts are most active 09:00-22:00 weekdays. Bots that crawl
    24/7 at constant rate are trivially detected by hour-of-day request density.
    This shifts our traffic shape toward "office worker who occasionally checks
    things at night" — still functional but less mechanical.
    """
    n = now or _dt.datetime.now()
    hour = n.hour
    weekday = n.weekday()  # 0=Mon .. 6=Sun
    mult = 1.0
    # 深夜 / 凌晨: 拉长 2.5x — 真人极少活动
    if hour < 7 or hour >= 23:
        mult *= 2.5
    # 午休: 轻微放慢
    elif 12 <= hour < 14:
        mult *= 1.3
    # 周末: 整体放慢
    if weekday >= 5:
        mult *= 1.8
    return mult


# ============================================================================
# 软冷却 (跨进程, Redis backed)
# ============================================================================

class SoftCooldown:
    """Cross-process per-platform soft cooldown.

    一个 watcher 触发软警告 (config quota / soft 429 / captcha cookie /
    风控关键词) → 该平台所有 watcher 全局静默 N min. 比硬等到 401/403
    再退场早一步, 大概率能在被吊销前救回会话.

    用法 (在 api_call 里):
        # 检测到软警告
        if response_signals_soft_warning(r):
            SoftCooldown.trigger("alphapai", reason="quota_code_7", minutes=45)

        # 在 sleep_before_next 顶部:
        SoftCooldown.wait_if_active("alphapai")
    """

    @staticmethod
    def _key(platform: str) -> str:
        return f"crawl:soft_cooldown:{platform}"

    @staticmethod
    def trigger(platform: str, reason: str = "", minutes: float = 45.0,
                verbose: bool = True) -> None:
        """Set / extend the cooldown flag for a platform.

        Existing cooldown is **extended** (not reset) if longer remaining than
        the new one — multiple warnings shouldn't shorten safety window.
        """
        ends_at = time.time() + max(60.0, minutes * 60.0)
        r = _get_redis()
        key = SoftCooldown._key(platform)
        if r is not None:
            try:
                cur = r.get(key)
                if cur and float(cur) > ends_at:
                    return  # existing flag is longer; keep it
                r.setex(key, int(minutes * 60) + 10, str(ends_at))
            except Exception:
                _MEM_COOLDOWN[key] = max(_MEM_COOLDOWN.get(key, 0.0), ends_at)
        else:
            _MEM_COOLDOWN[key] = max(_MEM_COOLDOWN.get(key, 0.0), ends_at)
        if verbose:
            print(f"  [soft-cooldown] {platform} 静默 {minutes:.0f}min "
                  f"(原因: {reason or 'unspecified'})", flush=True)

    @staticmethod
    def remaining(platform: str) -> float:
        """Seconds until cooldown ends. 0 = no active cooldown."""
        key = SoftCooldown._key(platform)
        r = _get_redis()
        ends_at = 0.0
        if r is not None:
            try:
                v = r.get(key)
                if v:
                    ends_at = float(v)
            except Exception:
                ends_at = _MEM_COOLDOWN.get(key, 0.0)
        else:
            ends_at = _MEM_COOLDOWN.get(key, 0.0)
        return max(0.0, ends_at - time.time())

    @staticmethod
    def wait_if_active(platform: str, verbose: bool = True,
                       max_chunk: float = 60.0) -> None:
        """Block until cooldown clears. Wakes every `max_chunk` seconds so a
        manual flag-clear in Redis is picked up quickly."""
        first = True
        while True:
            rem = SoftCooldown.remaining(platform)
            if rem <= 0:
                return
            if first and verbose:
                print(f"  [soft-cooldown] {platform} 还需静默 {rem:.0f}s", flush=True)
                first = False
            time.sleep(min(max_chunk, rem))

    @staticmethod
    def clear(platform: str) -> None:
        """Manual override (admin)."""
        key = SoftCooldown._key(platform)
        r = _get_redis()
        if r is not None:
            try:
                r.delete(key)
            except Exception:
                pass
        _MEM_COOLDOWN.pop(key, None)


# Soft-warning detector — pulled out so each scraper's api_call can call it
# uniformly. Callers pass status_code + parsed body (dict) + raw text snippet.
_SOFT_WARNING_BODY_KEYWORDS = (
    "请求过于频繁", "操作过于频繁", "访问过于频繁", "稍后再试",
    "次数已达上限", "查看次数已达", "您已达上限", "rate limit",
    "too many requests", "captcha", "verify you are human",
    "访问受限", "暂时限制", "限流",
    # alphapai 报告 detail 日配额耗尽 (code=400000) 的人话提示
    "已达到今日查看上限", "请明日再来",
)


# Per-platform business-code soft-warning table. Codes listed here map to
# `quota_code_<code>` reason → SoftCooldown.trigger with the 30 min ("quota")
# branch in callers. 只放**真正需要跨进程同步冷却**的信号 (WAF/captcha/
# 服务端限流). **每日分类配额**不要进这个表 —— 例如 alphapai code=400000
# (report detail 每天 100 条查看上限) 曾经放这里, 导致:
#   (1) 命中时全平台 SoftCooldown 30min, roadshow/comment 陪葬
#   (2) 每次 watch_report 重启第一轮再命中, cooldown 被不断续命
#   (3) 整个 alphapai 实际被打停一整天
# 正确做法是 scraper 层按分类自行降级 (dump_one 标 content_truncated=True
# 等次日配额重置), 不劳烦 SoftCooldown.
_PLATFORM_SOFT_BIZ_CODES: dict[str, dict[int, str]] = {
    "alphapai": {
        # intentionally empty — 每日配额 (400000) 不进表, 见上方注释.
    },
}


def detect_soft_warning(status_code: int,
                        body: Optional[dict] = None,
                        text_preview: str = "",
                        cookies: Optional[dict] = None,
                        platform: Optional[str] = None) -> Optional[str]:
    """Returns a short reason string if response indicates we're being warned
    (but not yet banned). Returns None if response looks normal.

    `platform` enables the per-platform biz-code table (see
    `_PLATFORM_SOFT_BIZ_CODES`) — e.g. alphapai's report detail day-cap
    (code=400000) is only a soft warning on alphapai, not a cross-platform
    signal.

    Callers should:
        reason = detect_soft_warning(r.status_code, body=parsed,
                                     text_preview=r.text[:500],
                                     platform="alphapai")
        if reason: SoftCooldown.trigger(PLATFORM, reason=reason)
    """
    # Soft 429 (some platforms return 429 instead of 401 for rate limit)
    if status_code == 429:
        return "http_429"
    # 503 / 522 / 502 cluster — often WAF challenge pages
    if status_code in (502, 503, 522, 503):
        return f"http_{status_code}"
    # Body-level signals
    if isinstance(body, dict):
        # AlphaPai / common Chinese platform pattern
        if body.get("hasPermission") is False:
            np = body.get("noPermissionReason") or {}
            if isinstance(np, dict) and np.get("code") in (7, "7"):
                return "quota_code_7"
        # Generic biz code patterns
        biz_code = body.get("code") or body.get("biz_code") or body.get("retCode")
        if biz_code in (10001, "10001", "rate_limited", 1010):
            return f"biz_code_{biz_code}"
        # Platform-specific biz codes (e.g. alphapai 400000 day-cap)
        if platform and biz_code is not None:
            try:
                code_int = int(biz_code)
            except (TypeError, ValueError):
                code_int = None
            if code_int is not None:
                pf_tbl = _PLATFORM_SOFT_BIZ_CODES.get(platform) or {}
                tag = pf_tbl.get(code_int)
                if tag:
                    # include "quota" in the reason so callers' "quota" → 30 min
                    # branch picks it up uniformly.
                    return f"quota_{tag}_{code_int}"
        msg = (body.get("message") or body.get("msg") or "")
        if isinstance(msg, str):
            for kw in _SOFT_WARNING_BODY_KEYWORDS:
                if kw in msg:
                    return f"msg:{kw[:20]}"
    # Captcha / WAF cookies (Datadome, Akamai, perimeter)
    if cookies:
        for ck in ("datadome", "_pxvid", "_abck", "ak_bmsc", "captcha", "geetest"):
            if any(ck in k.lower() for k in cookies.keys()):
                return f"waf_cookie:{ck}"
    # Text-level keywords (challenge pages)
    if text_preview:
        low = text_preview[:400].lower()
        for kw in _SOFT_WARNING_BODY_KEYWORDS:
            if kw.lower() in low:
                return f"text:{kw[:20]}"
    return None


# ============================================================================
# 账号预算 (跨进程, 24h 滚动窗)
# ============================================================================

class AccountBudget:
    """Cross-process per-account 24h rolling budget.

    `DailyCap` 是单进程的; 4 个 alphapai watcher 同时跑就 = 单账号 4×500=2000/天.
    AccountBudget 用 Redis sorted set 记录最近 24h 的请求时间戳, 多 watcher
    共享同一账号配额.

    role 区分主桶 (rt = realtime, 默认) 和后台桶 (bg = backfill):
      - rt 桶: 完全独立 24h 配额
      - bg 桶: 独立配额 + **realtime_floor** 让位规则 — 当主桶用量 >= floor%
        (默认 70%) 时, bg.exhausted() 返回 True, 让 backfill 暂停优先放 realtime.

    实例化:
        budget_rt = AccountBudget("alphapai", "u_123", 3000)               # 主桶
        budget_bg = AccountBudget("alphapai", "u_123", 1500, role="bg",
                                   realtime_floor_pct=70)                  # 后台桶

    主循环:
        if budget.exhausted():
            print("账号配额已耗尽, 停"); break
        process(item)
        budget.bump()
    """

    WINDOW_SECONDS = 86400

    def __init__(self, platform: str, account_id: Optional[str] = None,
                 daily_limit: int = 0, role: str = "rt",
                 realtime_floor_pct: int = 70):
        self.platform = platform
        self.account_id = account_id or "default"
        self.daily_limit = max(0, int(daily_limit))
        self.role = role  # "rt" (realtime, 默认主桶) or "bg" (backfill 后台桶)
        self.realtime_floor_pct = max(0, min(100, realtime_floor_pct))
        # role suffix only for non-rt buckets so existing keys stay intact
        suffix = "" if role == "rt" else f":{role}"
        self._key = f"crawl:budget:{platform}:{self.account_id}{suffix}"
        # rt key sibling — bg buckets read it to enforce the floor
        self._rt_key = f"crawl:budget:{platform}:{self.account_id}"
        # In-memory fallback list of timestamps
        self._mem_buf = _MEM_BUDGET.setdefault(self._key, [])

    def _trim(self) -> None:
        cutoff = time.time() - self.WINDOW_SECONDS
        r = _get_redis()
        if r is not None:
            try:
                r.zremrangebyscore(self._key, 0, cutoff)
                return
            except Exception:
                pass
        # Memory fallback
        while self._mem_buf and self._mem_buf[0] < cutoff:
            self._mem_buf.pop(0)

    def count_24h(self) -> int:
        if not self.daily_limit:
            return 0
        self._trim()
        r = _get_redis()
        if r is not None:
            try:
                return int(r.zcard(self._key))
            except Exception:
                pass
        return len(self._mem_buf)

    def _rt_count(self) -> int:
        """Sibling rt-bucket usage count, used by bg buckets for floor logic.
        Returns 0 if rt bucket is unset / Redis is down."""
        r = _get_redis()
        cutoff = time.time() - self.WINDOW_SECONDS
        if r is not None:
            try:
                r.zremrangebyscore(self._rt_key, 0, cutoff)
                return int(r.zcard(self._rt_key))
            except Exception:
                pass
        # Memory fallback shares the rt list under the rt key
        rt_buf = _MEM_BUDGET.get(self._rt_key, [])
        while rt_buf and rt_buf[0] < cutoff:
            rt_buf.pop(0)
        return len(rt_buf)

    def bump(self, n: int = 1) -> None:
        if not self.daily_limit:
            return
        now = time.time()
        r = _get_redis()
        if r is not None:
            try:
                pipe = r.pipeline()
                for i in range(n):
                    # zset score=ts, member=ts+rand to avoid duplicates
                    pipe.zadd(self._key, {f"{now}:{random.random()}": now})
                pipe.expire(self._key, self.WINDOW_SECONDS + 60)
                pipe.execute()
                return
            except Exception:
                pass
        for _ in range(n):
            self._mem_buf.append(now)

    def exhausted(self) -> bool:
        if not self.daily_limit:
            return False
        # bg bucket: realtime_floor takes priority. If rt bucket has used
        # ≥ floor% of *its own* limit, bg yields. We don't know rt's exact
        # daily_limit from here, so use platform default.
        if self.role == "bg" and self.realtime_floor_pct > 0:
            rt_limit = _DEFAULT_ACCOUNT_BUDGET.get(self.platform, 0)
            if rt_limit:
                rt_used_pct = (self._rt_count() * 100) / rt_limit
                if rt_used_pct >= self.realtime_floor_pct:
                    return True
        return self.count_24h() >= self.daily_limit

    def remaining(self) -> Optional[int]:
        if not self.daily_limit:
            return None
        return max(0, self.daily_limit - self.count_24h())

    def status(self) -> dict:
        """Diagnostic snapshot for log_config_stamp and dashboards."""
        return {
            "role": self.role,
            "used_24h": self.count_24h() if self.daily_limit else 0,
            "limit": self.daily_limit,
            "rt_sibling_used": self._rt_count() if self.role == "bg" else None,
            "floor_pct": self.realtime_floor_pct if self.role == "bg" else None,
        }


# ============================================================================
# Backfill 专属保护层 (BackfillWindow / BackfillSession / BackfillCheckpointBackoff
#                     / BackfillLock + 平台默认表)
# ============================================================================

# Per-platform allowed backfill windows. Each entry is a tuple describing
# CST-local hours-of-day where backfill may run. Windows can wrap midnight
# (e.g. (22, 8) = 22:00-08:00 next day). A `None` entry means "any time".
#
# Rationale: realtime is bounded by content velocity (~hundreds/day per
# platform), but backfill traffic shape is sustained at the cap rate for
# many hours — exactly the cron pattern WAFs cluster on. Forcing backfill
# to weekday-night + weekend-only windows reshapes our hour-of-day request
# density toward "user who occasionally catches up after work" rather than
# "always-on harvester".
_PLATFORM_BACKFILL_WINDOW = {
    # CN platforms: weekday 22:00 ~ 08:00 next morning + weekends all-day
    "alphapai":    (22, 8),
    "jinmen":      (22, 8),
    "meritco":     (22, 8),
    "gangtise":    (22, 8),
    "acecamp":     (22, 8),
    "alphaengine": (22, 8),
    # third_bridge most strict — narrower window
    "thirdbridge": (23, 7),
    # US platforms — funda subscribers are global; use any-time windows
    "funda":       None,
    "sentimentrader": None,
    "semianalysis": None,
}


def _in_backfill_window(platform: str, now: Optional[_dt.datetime] = None) -> tuple[bool, float]:
    """Returns (allowed_now, seconds_until_next_window).

    If allowed=True, second value is seconds remaining in current window.
    If allowed=False, second value is seconds to wait until the next window opens.
    """
    cfg = _PLATFORM_BACKFILL_WINDOW.get(platform, (22, 8))
    n = now or _dt.datetime.now()
    weekday = n.weekday()  # 0=Mon, 6=Sun
    # cfg=None means "any time"
    if cfg is None:
        return True, 86400.0  # arbitrary large remaining
    start_h, end_h = cfg
    # Weekend (Sat=5, Sun=6) — always allowed for CN platforms
    if weekday >= 5:
        return True, 86400.0
    hr = n.hour + n.minute / 60.0 + n.second / 3600.0
    # Window wraps midnight (e.g. start=22, end=8): allowed if hr>=start OR hr<end
    if start_h > end_h:
        if hr >= start_h or hr < end_h:
            # In window. Compute remaining: if hr>=start, until end_h next day; else until end_h today
            if hr >= start_h:
                remaining_h = (24 - hr) + end_h
            else:
                remaining_h = end_h - hr
            return True, remaining_h * 3600
        # Outside window. Wait until start_h today
        wait_h = start_h - hr
        return False, wait_h * 3600
    # Window non-wrap (e.g. start=2, end=6 — dawn only)
    if start_h <= hr < end_h:
        return True, (end_h - hr) * 3600
    if hr < start_h:
        return False, (start_h - hr) * 3600
    # hr >= end_h, wait until tomorrow's start
    wait_h = (24 - hr) + start_h
    return False, wait_h * 3600


class BackfillWindow:
    """决定当前时刻是否允许回填. 工时段 → 强制 sleep 到允许窗口.

    用法 (回填脚本主循环顶部):
        BackfillWindow.wait_until_allowed("alphapai")
        # 现在保证在允许的时间段, 才往下抓
    """

    @staticmethod
    def seconds_until_allowed(platform: str) -> float:
        """0 if currently allowed, else seconds to wait."""
        allowed, secs = _in_backfill_window(platform)
        return 0.0 if allowed else secs

    @staticmethod
    def is_allowed(platform: str) -> bool:
        allowed, _ = _in_backfill_window(platform)
        return allowed

    @staticmethod
    def remaining_in_window(platform: str) -> float:
        """If allowed now, how many seconds until window closes (so backfill
        can plan: if window has only 30min left, maybe finish current batch
        and stop instead of starting a new sweep)."""
        allowed, secs = _in_backfill_window(platform)
        return secs if allowed else 0.0

    @staticmethod
    def wait_until_allowed(platform: str, verbose: bool = True,
                           max_chunk: float = 300.0) -> None:
        """Block until current wall-clock is inside allowed window.
        Wakes every `max_chunk` seconds for liveness."""
        first = True
        while True:
            secs = BackfillWindow.seconds_until_allowed(platform)
            if secs <= 0:
                if not first and verbose:
                    print(f"  [backfill-window] {platform} 窗口打开, 继续",
                          flush=True)
                return
            if first and verbose:
                cfg = _PLATFORM_BACKFILL_WINDOW.get(platform, (22, 8))
                if cfg is None:
                    win = "any-time"
                else:
                    win = f"{cfg[0]:02d}:00~{cfg[1]:02d}:00"
                hours = secs / 3600
                print(f"  [backfill-window] {platform} 当前不在允许窗口 "
                      f"({win} CST 工作日 + 周末全天), 等 {hours:.1f}h",
                      flush=True)
                first = False
            time.sleep(min(max_chunk, secs))


# Per-platform backfill defaults — used by add_backfill_args / monitor's
# backfill mode. Callable with `backfill_defaults("alphapai")`.
#
# Values per platform: (base, jitter, burst, daily_cap, bg_budget, pace, mandatory_break_every)
# pace: "fast" (PDF 字节流) / "normal" (常规 list+detail) / "slow" (敏感平台)
# mandatory_break_every: 每抓 N 条强制 5-15 min idle (BackfillSession.step 触发)
_PLATFORM_BACKFILL_DEFAULTS = {
    # platform: (base, jitter, burst, daily_cap, bg_budget, pace, break_every)
    "alphapai":    (4.0, 2.5, 30, 400, 1500, "normal", 50),
    "jinmen":      (4.0, 2.5, 30, 400, 1200, "normal", 50),
    "meritco":     (5.0, 3.0, 25, 300,  600, "slow",   30),
    "thirdbridge": (8.0, 4.0, 20, 100,  150, "slow",   20),
    "funda":       (4.0, 2.5, 30, 400, 1000, "normal", 50),
    "gangtise":    (4.0, 2.5, 30, 400, 1500, "normal", 50),
    # acecamp: 2026-04-24 事故后收紧 — base 3.5→4.5, jitter 2.0→2.5, burst 30→20,
    # daily_cap 400→250. VIP 团队金卡的 detail 端点 quota 非常紧 (~12/次后 10003/10040),
    # 即使 bg 桶也需要更长平均间隔; burst 20 避免一轮 30 条撞进 quota 触发封控.
    # break_every 50→30 让 mandatory 5-15min idle 更频繁切断稳态密度.
    "acecamp":     (4.5, 2.5, 20, 250,  500, "slow",   30),
    "alphaengine": (4.0, 2.5, 30, 400,  750, "normal", 40),
    "semianalysis": (4.0, 2.0, 30, 200,  500, "normal", 40),
}


def backfill_defaults(platform: str) -> dict:
    """Returns the backfill default knobs for a platform as a dict."""
    base, jitter, burst, cap, bg_budget, pace, break_every = (
        _PLATFORM_BACKFILL_DEFAULTS.get(platform,
                                         (4.0, 2.5, 30, 400, 1000, "normal", 50)))
    return {
        "base": base, "jitter": jitter, "burst": burst,
        "daily_cap": cap, "bg_budget": bg_budget,
        "pace": pace, "break_every": break_every,
    }


# Pace presets for BackfillSession.step's mandatory-pause behavior.
# Each value is (min_sec, max_sec) — the random pause length when the
# `break_every` counter trips.
_BF_PACE_PAUSE = {
    "fast":   (60.0, 180.0),     # PDF 字节流: 1-3 min
    "normal": (300.0, 600.0),    # 常规 list+detail: 5-10 min
    "slow":   (600.0, 1500.0),   # 敏感平台: 10-25 min
}


class BackfillSession:
    """Per-process backfill pacer — sits *above* AdaptiveThrottle and adds
    longer mandatory "reading break" pauses every N items.

    AdaptiveThrottle 的 5% long-tail 概率上不保证 — backfill 跑 10 万条可能 5000
    个 long-tail 全在凌晨 3 点 burst. BackfillSession.step() 是确定性触发:
    每 break_every 条强制 5-15 min idle, 切碎稳态密度.

    用法:
        bf = BackfillSession(platform="alphapai", pace="normal", break_every=50)
        for item in items:
            process(item)
            bf.step()                       # 触发 mandatory pause if N reached
            throttle.sleep_before_next()
        bf.page_done()                      # 翻一页 / sweep 切换日期时调用
    """

    def __init__(self, platform: str, pace: str = "normal",
                 break_every: int = 50,
                 page_pause_min: float = 30.0, page_pause_max: float = 90.0,
                 verbose: bool = True):
        self.platform = platform
        self.pace = pace if pace in _BF_PACE_PAUSE else "normal"
        self.break_every = max(1, int(break_every))
        self.page_pause_min = page_pause_min
        self.page_pause_max = page_pause_max
        self.verbose = verbose
        self._count = 0
        self._total = 0

    def step(self) -> None:
        """Called once per processed item. Triggers mandatory pause every
        `break_every` items."""
        self._count += 1
        self._total += 1
        if self._count >= self.break_every:
            lo, hi = _BF_PACE_PAUSE[self.pace]
            pause = random.uniform(lo, hi)
            if self.verbose:
                print(f"  [backfill-session] 阅读休息 {pause:.0f}s "
                      f"(已抓 {self._total} 条, pace={self.pace})", flush=True)
            time.sleep(pause)
            self._count = 0

    def page_done(self) -> None:
        """Called when finishing a list page or switching sweep day. Adds
        30-90s pause to break the per-page request batch into discrete chunks."""
        pause = random.uniform(self.page_pause_min, self.page_pause_max)
        if self.verbose:
            print(f"  [backfill-session] page 切换 {pause:.0f}s 间隔 "
                  f"(累计 {self._total} 条)", flush=True)
        time.sleep(pause)


class BackfillCheckpointBackoff:
    """Slow-start at script boot / after a checkpoint resume.

    backfill 中断后续抓时, 一上来就并发请求很 suspicious — 真人重新打开页面
    会先停一下看上下文. 这个类让 throttle 在前 N 条节奏 ×K, 之后回到正常.

    用法:
        backoff = BackfillCheckpointBackoff(throttle, warm_up=30, factor=3.0)
        backoff.arm()  # 在 main 启动 / 恢复 checkpoint 后立即调用
        # 之后正常用 throttle.sleep_before_next() — 前 30 次自动 ×3
    """

    def __init__(self, throttle, warm_up: int = 30, factor: float = 3.0):
        self.throttle = throttle
        self.warm_up = warm_up
        self.factor = factor

    def arm(self) -> None:
        """Tell the throttle: next `warm_up` requests pace ×factor."""
        # We reuse AdaptiveThrottle._preemptive_remaining + _preemptive_factor
        # — same mechanism as on_warning(), but with our own params.
        self.throttle._preemptive_remaining = max(
            getattr(self.throttle, "_preemptive_remaining", 0), self.warm_up)
        self.throttle._preemptive_factor = max(
            getattr(self.throttle, "_preemptive_factor", 2.0), self.factor)


class BackfillLock:
    """Cross-process per-platform single-instance lock for backfill workers.

    防 13 个 gangtise/backfill_pdfs.py 同时跑这种事故. 用 Redis SET NX EX TTL
    实现. backfill 进程死了 TTL 自动清, heartbeat 续期.

    用法 (backfill 脚本顶部):
        if not BackfillLock.acquire("alphapai", role="pdf"):
            sys.exit("另一个 alphapai PDF backfill 已在跑")
        try:
            while not stopped:
                BackfillLock.heartbeat("alphapai", role="pdf")
                ... 抓一轮 ...
        finally:
            BackfillLock.release("alphapai", role="pdf")
    """

    DEFAULT_TTL_MIN = 30  # 心跳每 5-10 min 一次, TTL 30 min 给两次心跳余量

    @staticmethod
    def _key(platform: str, role: str) -> str:
        return f"crawl:bf_lock:{platform}:{role}"

    @staticmethod
    def acquire(platform: str, role: str = "default",
                ttl_min: int = DEFAULT_TTL_MIN, force: bool = False,
                verbose: bool = True) -> bool:
        """Try to claim the lock. Returns True on success, False if another
        backfill holds it. `force=True` bypasses the check (use only when
        you know the other process is dead)."""
        r = _get_redis()
        key = BackfillLock._key(platform, role)
        token = f"{os.getpid()}:{time.time()}"
        if r is not None:
            try:
                if force:
                    r.set(key, token, ex=int(ttl_min * 60))
                    return True
                got = r.set(key, token, ex=int(ttl_min * 60), nx=True)
                if not got and verbose:
                    holder = r.get(key) or "?"
                    print(f"  [backfill-lock] {platform}:{role} 已被 PID "
                          f"{holder} 占用, 跳过启动", flush=True)
                return bool(got)
            except Exception:
                pass
        # In-memory fallback (single-process only — fine for dev / no Redis)
        mem_key = key
        existing = _MEM_COOLDOWN.get(mem_key, 0.0)  # reuse mem_cooldown dict
        if existing > time.time() and not force:
            if verbose:
                print(f"  [backfill-lock] {platform}:{role} 内存锁占用中",
                      flush=True)
            return False
        _MEM_COOLDOWN[mem_key] = time.time() + ttl_min * 60
        return True

    @staticmethod
    def heartbeat(platform: str, role: str = "default",
                  ttl_min: int = DEFAULT_TTL_MIN) -> bool:
        """Renew TTL. Call every 5-10 min from main loop."""
        r = _get_redis()
        key = BackfillLock._key(platform, role)
        token = f"{os.getpid()}:{time.time()}"
        if r is not None:
            try:
                r.set(key, token, ex=int(ttl_min * 60))
                return True
            except Exception:
                pass
        _MEM_COOLDOWN[key] = time.time() + ttl_min * 60
        return True

    @staticmethod
    def release(platform: str, role: str = "default") -> None:
        """Explicit release on graceful exit."""
        r = _get_redis()
        key = BackfillLock._key(platform, role)
        if r is not None:
            try:
                r.delete(key)
            except Exception:
                pass
        _MEM_COOLDOWN.pop(key, None)

    @staticmethod
    def held_by(platform: str, role: str = "default") -> Optional[str]:
        """Return the holder token (pid:ts) or None if no lock."""
        r = _get_redis()
        key = BackfillLock._key(platform, role)
        if r is not None:
            try:
                v = r.get(key)
                return v
            except Exception:
                pass
        ttl = _MEM_COOLDOWN.get(key, 0.0)
        return f"mem:{ttl}" if ttl > time.time() else None


# ============================================================================
# 节流核心
# ============================================================================

@dataclass
class AdaptiveThrottle:
    """Pacing that looks less like a bot.

    用法:
        t = AdaptiveThrottle(base_delay=3, jitter=2, burst_size=40,
                             platform="alphapai")
        for item in items:
            ... do work ...
            t.sleep_before_next()                # Gaussian + 时段倍增 + 软冷却 wait
            # 如果刚才的请求 429/5xx:
            t.on_retry(retry_after_sec=30)       # 下次 sleep 变成 30s

    自动规律:
      - 先看 SoftCooldown — 该平台被全局冷却就睡到清除
      - 每 `burst_size` 条请求后, 下一次 sleep 变成 [burst_cooldown_min, burst_cooldown_max] 随机
      - `on_retry` 设置的 backoff 只作用一次, 之后回到正常节奏
      - 正常节奏: max(0.2, gauss(base, jitter/2)) × time_of_day_multiplier
      - 2026-04-25: normal 节奏尾部依次概率触发 (互斥, 避免一次停 3+ min):
          * idle_window_prob (默认 0.03) → 60-180s "切 tab 离开一会儿"
          * elif long_tail_prob (默认 0.05) → 5-30s "读完一条停一下"
      - 出现一次警告 → 后续 N 次请求节奏 ×2 (preemptive_factor)
    """
    base_delay: float = 3.0
    jitter: float = 2.0
    burst_size: int = 40
    burst_cooldown_min: float = 30.0
    burst_cooldown_max: float = 60.0
    backoff_base: float = 2.0
    backoff_max: float = 120.0
    verbose: bool = True
    platform: Optional[str] = None       # 用于查 SoftCooldown
    enable_time_of_day: bool = True
    enable_soft_cooldown: bool = True
    long_tail_prob: float = 0.05         # 5% 概率叠加阅读停留
    long_tail_min: float = 5.0
    long_tail_max: float = 30.0
    # idle_window: 更稀疏但更长的"切 tab 离开"停留. 跟 long_tail 互斥 (避免叠加 3+min).
    # 实时档默认 0.03 (由 crawler_monitor 的 realtime _mode_args 注入), CLI 默认 0.0.
    idle_window_prob: float = 0.0
    idle_window_min: float = 60.0
    idle_window_max: float = 180.0

    _count_since_burst: int = field(default=0, init=False)
    _pending_backoff: float = field(default=0.0, init=False)
    _preemptive_remaining: int = field(default=0, init=False)
    _preemptive_factor: float = field(default=2.0, init=False)

    def sleep_before_next(self) -> None:
        """Sleep the right amount of time before the next request.

        Priority:
          1. Soft cooldown (if active for this platform — block until cleared)
          2. pending backoff (from last on_retry) — one-shot
          3. burst cooldown (every N requests)
          4. normal: gauss(base, jitter/2) × tod × preemptive [+ long-tail 5%]
        """
        # 0. 全平台软冷却 — 同平台任一 watcher 触发就静默
        if self.enable_soft_cooldown and self.platform:
            SoftCooldown.wait_if_active(self.platform, verbose=self.verbose)

        # 1. backoff (来自 on_retry)
        if self._pending_backoff > 0:
            t = self._pending_backoff
            self._pending_backoff = 0.0
            if self.verbose:
                print(f"  [throttle] backoff sleep {t:.1f}s", flush=True)
            time.sleep(t)
            return

        # 2. burst cooldown
        # burst_size <= 0 disables periodic cooldowns — intended for realtime
        # --watch daemons where each tick only fetches a handful of new items
        # and the long `--interval` between ticks already gives the platform
        # plenty of idle time.
        if self.burst_size > 0 and self._count_since_burst >= self.burst_size:
            cd = random.uniform(self.burst_cooldown_min, self.burst_cooldown_max)
            if self.verbose:
                print(f"  [throttle] burst cooldown {cd:.1f}s "
                      f"(过去 {self._count_since_burst} 条)", flush=True)
            time.sleep(cd)
            self._count_since_burst = 0
            return

        # 3. 正常节奏: Gaussian + 时段倍增 + 预防性倍增
        # Gaussian sigma ≈ jitter/2 让 ±2σ 落在 ±jitter 范围 (覆盖 95%).
        # 用 max 钳到 0.2s, 用 base+jitter*2 钳上限 (truncated normal).
        sigma = max(0.05, self.jitter / 2.0)
        delay = random.gauss(self.base_delay, sigma)
        delay = max(0.2, min(self.base_delay + self.jitter * 2, delay))

        # 时段倍增
        if self.enable_time_of_day:
            delay *= time_of_day_multiplier()

        # 预防性倍增 (出现警告后 N 条慢一些)
        if self._preemptive_remaining > 0:
            delay *= self._preemptive_factor
            self._preemptive_remaining -= 1

        # Idle window 和 long-tail 互斥 — 前者更稀疏更长 (60-180s, 模拟切 tab),
        # 后者更常见更短 (5-30s, 模拟读完一条). 两个一起叠加概率太低但万一触发
        # 就是 3+ min 停留, 会拖累实时响应, 因此 elif 互斥.
        if self.idle_window_prob > 0 and random.random() < self.idle_window_prob:
            extra = random.uniform(self.idle_window_min, self.idle_window_max)
            delay += extra
            if self.verbose:
                print(f"  [throttle] idle window +{extra:.0f}s "
                      f"(模拟切 tab 离开)", flush=True)
        elif random.random() < self.long_tail_prob:
            extra = random.uniform(self.long_tail_min, self.long_tail_max)
            delay += extra
            if self.verbose:
                print(f"  [throttle] long-tail read pause +{extra:.1f}s", flush=True)

        time.sleep(delay)
        self._count_since_burst += 1

    def on_retry(self, retry_after_sec: Optional[float] = None, attempt: int = 1) -> None:
        """Register a transient failure (429 / 5xx). Next `sleep_before_next` will use backoff.

        retry_after_sec: if server sent `Retry-After`, pass its parsed seconds here.
        attempt: 1 on first retry, 2 on second, etc. — drives exponential backoff.
        """
        if retry_after_sec:
            self._pending_backoff = min(float(retry_after_sec), self.backoff_max)
        else:
            self._pending_backoff = min(self.backoff_base ** max(1, attempt),
                                         self.backoff_max)
        # 任何 retry 都触发预防性慢速 — 后续 20 条节奏 ×2
        self._preemptive_remaining = max(self._preemptive_remaining, 20)

    def on_warning(self, hits: int = 30) -> None:
        """Mark a soft warning that doesn't deserve full backoff but warrants
        slower follow-up. E.g. partial response / single rate-limit body."""
        self._preemptive_remaining = max(self._preemptive_remaining, hits)

    def reset(self) -> None:
        """Reset internal counters — call between independent rounds (e.g. --watch loops)."""
        self._count_since_burst = 0
        self._pending_backoff = 0.0
        # Note: preemptive_remaining survives across rounds — warnings don't expire on `--watch` tick


def parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    """Parse Retry-After header value (RFC 7231): integer seconds or HTTP-date.

    Returns None if absent / unparseable.
    """
    if not header_value:
        return None
    v = str(header_value).strip()
    if not v:
        return None
    if v.isdigit():
        return float(v)
    # HTTP-date form is rare; default to conservative 60s if present
    return 60.0


def is_auth_dead(status_code: int, body_preview: str = "") -> bool:
    """True if status indicates **permanent** session death (not transient).

    401/403 = dead (even if body says rate limit). Redirect to /401 or /login = dead.
    """
    if status_code in (401, 403):
        return True
    return False


class DailyCap:
    """Hard cap on items per run. Protects against runaway crawls triggering 风控.

    用法:
        cap = DailyCap(500)
        for item in items:
            if cap.exhausted():
                print(f"达到单轮上限 {cap.max_items}, 停")
                break
            process(item)
            cap.bump()

    Note: 这是 *per-process* 单轮闸; 跨进程账号总量请用 AccountBudget.
    """

    def __init__(self, max_items: Optional[int] = None):
        self.max_items = max_items or 0
        self.count = 0

    def bump(self, n: int = 1) -> None:
        self.count += n

    def exhausted(self) -> bool:
        return bool(self.max_items) and self.count >= self.max_items

    def remaining(self) -> Optional[int]:
        if not self.max_items:
            return None
        return max(0, self.max_items - self.count)


# ============================================================================
# CLI 标准化
# ============================================================================

# Per-platform reference values for **estimated realtime peak volume / 24h**.
#
# 2026-04-25: 这个字典曾经是 "rt 24h 硬封顶" (add_antibot_args 的默认 --account-budget),
# 实际价值≈0 — 被 WAF 抓的从来是节奏和指纹, 不是 24h 总数; 撞顶就漏抓增量
# (alphapai report 单日 881 条撞 3000 就是这么来的). 现在保留只因为 bg 桶的
# `realtime_floor_pct` 让位逻辑需要一个参考 (AccountBudget.exhausted 对 bg
# role: 当 rt sibling 用量 >= floor% 时, bg.exhausted=True 暂停 backfill).
# rt 桶本身不再 default-on, 要启用只能 CLI 传 `--account-budget N>0`.
#
# 子模块分桶 (alphapai/jinmen/alphaengine) 保留 — 不同模块的 rt 统计分开算,
# floor 对比更精准. 见下方 account_id_for_* 函数.
#
# 估算依据 (大致日入库 ×2 余量): 仅供 floor 参照, 不必精确.
_DEFAULT_ACCOUNT_BUDGET = {
    "alphapai":    3000,    # 单模块额度 (用子模块 suffix 账号隔离, 见下)
    "jinmen":      1500,    # 单模块额度 (用子模块 suffix 账号隔离, 见下)
    "meritco":     1200,
    "thirdbridge":  300,    # 4⭐ 反爬最难, 配额留小
    "funda":       2500,   # 2026-04-24 上调: 3 个 watcher × 600/天 realtime cap + 500 daily_catchup + 临时 backfill, 旧 2000 会饿死
    "gangtise":    20000,  # 1 个 G_token 同时供 5 进程用; 日产 research 几千 + chief 很多 — 20000/24h 是"budget 永不触顶"保险线, 不漏抓; 封号靠节奏 (base/burst/cooldown) 不靠数量闸
    "acecamp":      800,    # 2026-04-24 事故后下调 1500→800: articles detail quota 极紧
                             # (~12/次 → 10003/10040); bg 桶独立 500/24h 之外再压 rt 桶.
    "alphaengine": 1500,    # 单模块额度 (用子模块 suffix 账号隔离, 见下)
    "sentimentrader": 200,
    "semianalysis":   600,
}


# alphapai 子模块分桶: 同一账号 uid 下, 按 category 独立算 24h 预算.
# scraper 把 category 追加到 account_id (e.g. u_124434...:roadshow), 让
# Redis 下的 sorted set key 天然分离 — roadshow 被打满不影响 comment/report.
def account_id_for_alphapai(base_account_id: str, category: Optional[str]) -> str:
    """Suffix the alphapai account_id with the module category so 3 模块
    各走独立 3000/24h 预算. 传入 None / 空 → 退回 base (兼容老调用)."""
    base = (base_account_id or "default").strip()
    if not category:
        return base
    return f"{base}:{category}"


# jinmen 子模块分桶: 跟 alphapai 同思路. scraper 按 --reports / --oversea-reports
# 路由出三条互斥的 worker, 每个 worker 进程只抓一个 category 的 API, 所以给
# account_id 追加 `:meetings` / `:reports` / `:oversea_reports` 后缀后, Redis
# 下的 budget key 天然分离, 一条线的 backfill 不会吃掉其它两条的实时 quota.
def account_id_for_jinmen(base_account_id: str, category: Optional[str]) -> str:
    """Suffix the jinmen account_id with the module category so 3 模块
    各走独立 1500/24h 预算. category 取 'meetings' / 'reports' /
    'oversea_reports' (跟 COL_* 集合一致). 传入 None / 空 → 退回 base
    (兼容老调用 / --show-state / --today 之类无 worker 路由的路径)."""
    base = (base_account_id or "default").strip()
    if not category:
        return base
    return f"{base}:{category}"


# alphaengine 子模块分桶: 4 条 list watcher (summary / chinaReport /
# foreignReport / news) + 1 条 enrich worker + 1 条 roadshow_events backfill
# 共 6 条业务线. 旧版全部挤在 `u_<uid>` 一个桶里, 4 个 list watcher 各占
# 300-500/天 几乎就把 1500 吃光; 一个类别 foreignReport 撞 REFRESH_LIMIT
# 退避阶段还在数据库桶里占配额, enrich 和 roadshow_events backfill 饿死.
# 拆完后: category 追 `:summary` / `:chinaReport` / `:foreignReport` /
# `:news` 后缀, enrich 用 `:enrich`, roadshow_events 用 `:roadshow_events`,
# pdf backfill 用 `:pdf_backfill`. 每桶独立 1500/24h, 互不干扰.
def account_id_for_alphaengine(base_account_id: str, category: Optional[str]) -> str:
    """Suffix the alphaengine account_id with the worker category so 6 条
    业务线各走独立 1500/24h 预算. category 常见值:
      'summary' / 'chinaReport' / 'foreignReport' / 'news'  (主 list watcher)
      'enrich'                                              (detail bypass worker)
      'roadshow_events'                                     (alphaglobalpage backfill)
      'pdf_backfill'                                        (单独 PDF 回填 worker)
      'all'                                                 (一次性全量跑, 共用桶)
    传入 None / 空 → 退回 base (兼容老调用)."""
    base = (base_account_id or "default").strip()
    if not category:
        return base
    return f"{base}:{category}"


def add_antibot_args(parser, default_base: float = 3.0,
                     default_jitter: float = 2.0,
                     default_burst: int = 40,
                     default_cap: Optional[int] = 0,
                     platform: Optional[str] = None) -> None:
    """Attach the standard --throttle-* / --daily-cap / --burst-size /
    --account-budget / --idle-window-prob / --no-time-of-day / --no-soft-cooldown flags.

    每个 scraper 的 parse_args() 里调用:
        add_antibot_args(p, default_base=3, default_jitter=2,
                         default_burst=40, platform="alphapai")
    然后用 `throttle_from_args(args)` / `cap_from_args(args)` /
    `budget_from_args(args, account_id=...)` 建对应实例.

    2026-04-25 (v2.2) 默认变更:
      - default_cap 500 → 0 (禁用). 实时档不再靠数量闸, 见模块顶部 §5.
      - --account-budget 默认 0 (禁用 rt 主桶), 见模块顶部 §6.
      - 新增 --idle-window-prob: 概率触发的 60-180s "切 tab" 停留; 实时档
        crawler_monitor 注入 0.03, CLI 默认 0.
    """
    g = parser.add_argument_group("反爬 / 节流 (antibot)")
    g.add_argument("--throttle-base", type=float, default=default_base,
                   help=f"基础请求间隔秒数 (默认 {default_base}s)")
    g.add_argument("--throttle-jitter", type=float, default=default_jitter,
                   help=f"间隔抖动幅度 +/- 秒 (默认 {default_jitter}s, "
                        f"现已改为 Gaussian σ ≈ jitter/2)")
    g.add_argument("--burst-size", type=int, default=default_burst,
                   help=f"每 N 条请求后冷却一次 (默认 {default_burst})")
    g.add_argument("--burst-cooldown-min", type=float, default=30.0,
                   help="突发冷却最短秒数 (默认 30)")
    g.add_argument("--burst-cooldown-max", type=float, default=60.0,
                   help="突发冷却最长秒数 (默认 60)")
    g.add_argument("--daily-cap", type=int, default=default_cap,
                   help=f"单轮最多抓 N 条, 0=无限 (默认 {default_cap}, "
                        f"实时档默认 0; backfill 脚本会覆盖)")
    g.add_argument("--account-budget", type=int, default=0,
                   help="24h 跨进程账号总闸 (默认 0=禁用; 紧急限流或 backfill 用; "
                        "实时档不启用, 由 SoftCooldown/节奏/指纹防护)")
    g.add_argument("--idle-window-prob", type=float, default=0.0,
                   help="每请求间 N%% 概率叠加 60-180s idle 停留 (模拟切 tab). "
                        "默认 0.0; 实时档 crawler_monitor 注入 0.03")
    g.add_argument("--no-time-of-day", dest="time_of_day", action="store_false",
                   default=True,
                   help="禁用工时倍增 (调试时偶尔用; 默认开启)")
    g.add_argument("--no-soft-cooldown", dest="soft_cooldown", action="store_false",
                   default=True,
                   help="禁用软警告全局冷却 (默认开启)")
    g.add_argument("--no-long-tail", dest="long_tail", action="store_false",
                   default=True,
                   help="禁用 long-tail 阅读停留 (默认开启)")
    # --account-role 这里也加一份, 让普通 scraper.py 接受 backfill orchestrator
    # 注入的 --account-role bg flag 不报错 (orchestrator-spawned subprocess
    # 复用 scraper.py, 走 bg 桶让位 realtime). 默认 'rt' 不影响行为.
    g.add_argument("--account-role", default="rt", choices=("rt", "bg"),
                   help="账号桶角色 (默认 'rt' 主桶; 'bg' 后台桶 — backfill 时让位 realtime)")
    g.add_argument("--bg-budget", type=int, default=0,
                   help="bg 桶预算 (0=用平台默认; 仅 --account-role bg 生效)")
    g.add_argument("--realtime-floor-pct", type=int, default=70,
                   help="bg 桶让位阈值 — 主桶用量 >= 此 %% 时, bg.exhausted=True")
    # 平台名透传给 throttle_from_args / budget_from_args, 默认通过环境变量绑定
    parser.set_defaults(_antibot_platform=platform)


def throttle_from_args(args, platform: Optional[str] = None) -> AdaptiveThrottle:
    """Build AdaptiveThrottle from parsed args. `platform` is used to look up
    soft-cooldown flag (must match what scrapers pass to SoftCooldown.trigger).
    """
    pf = platform or getattr(args, "_antibot_platform", None)
    return AdaptiveThrottle(
        base_delay=args.throttle_base,
        jitter=args.throttle_jitter,
        burst_size=args.burst_size,
        burst_cooldown_min=args.burst_cooldown_min,
        burst_cooldown_max=args.burst_cooldown_max,
        platform=pf,
        enable_time_of_day=getattr(args, "time_of_day", True),
        enable_soft_cooldown=getattr(args, "soft_cooldown", True),
        long_tail_prob=0.05 if getattr(args, "long_tail", True) else 0.0,
        idle_window_prob=max(0.0, min(1.0, getattr(args, "idle_window_prob", 0.0))),
    )


def cap_from_args(args) -> DailyCap:
    return DailyCap(max_items=args.daily_cap if args.daily_cap else 0)


def budget_from_args(args, account_id: Optional[str] = None,
                     platform: Optional[str] = None,
                     role: Optional[str] = None) -> AccountBudget:
    """Build AccountBudget. account_id should be derived from the credential
    (e.g. user uid / phone hash) so multiple watchers under the same account
    share the same budget bucket.

    role=None: 取 args.account_role (CLI 决定)
    role="rt" 主桶, role="bg" 后台桶 — 后台桶有 realtime_floor 让位规则.
    """
    pf = platform or getattr(args, "_antibot_platform", None) or "unknown"
    # 优先用显式 role, 否则看 CLI --account-role
    actual_role = role or getattr(args, "account_role", "rt")
    if actual_role == "bg":
        # bg 桶预算: 显式 --bg-budget > 平台默认 (_PLATFORM_BACKFILL_DEFAULTS)
        limit = getattr(args, "bg_budget", 0) or 0
        if not limit:
            limit = _PLATFORM_BACKFILL_DEFAULTS.get(pf,
                     (4.0, 2.5, 30, 400, 1000, "normal", 50))[4]
        floor = getattr(args, "realtime_floor_pct", 70)
        return AccountBudget(pf, account_id=account_id, daily_limit=int(limit),
                              role="bg", realtime_floor_pct=int(floor))
    limit = getattr(args, "account_budget", 0) or 0
    return AccountBudget(pf, account_id=account_id, daily_limit=int(limit))


def add_backfill_args(parser, platform: Optional[str] = None) -> None:
    """Attach backfill-specific flags. 跟 add_antibot_args 一起加 (后者管基础节流).

    每个 backfill 脚本在 parse_args() 里:
        add_antibot_args(p, platform="alphapai")
        add_backfill_args(p, platform="alphapai")
    然后:
        bf_session = backfill_session_from_args(args, platform="alphapai")
        bg_budget  = budget_from_args(args, account_id=..., platform=...,
                                       role="bg")
    """
    g = parser.add_argument_group("回填专属保护 (backfill antibot)")
    defaults = backfill_defaults(platform or "alphapai")
    # --account-role / --bg-budget / --realtime-floor-pct 已在 add_antibot_args 里加了 (默认 'rt' / 0 / 70).
    # 这里改默认: backfill 脚本默认 role=bg, bg-budget=平台默认.
    # 如果 add_antibot_args 没被先调用, 才补 (兼容只用 add_backfill_args 的脚本).
    has_role = any(a.dest == "account_role" for a in parser._actions)
    if has_role:
        # 改默认值为 bg
        for a in parser._actions:
            if a.dest == "account_role":
                a.default = "bg"
            elif a.dest == "bg_budget":
                a.default = defaults["bg_budget"]
        parser.set_defaults(account_role="bg", bg_budget=defaults["bg_budget"])
    else:
        g.add_argument("--account-role", default="bg",
                       choices=("rt", "bg"),
                       help="账号桶角色: 'bg'(默认 backfill 后台桶) / 'rt'(主桶)")
        g.add_argument("--bg-budget", type=int, default=defaults["bg_budget"],
                       help=f"backfill 24h 后台桶预算 (默认 {defaults['bg_budget']})")
        g.add_argument("--realtime-floor-pct", type=int, default=70,
                       help="主桶用量超过此 %% 时, 后台桶让位 (默认 70)")
    g.add_argument("--no-backfill-window", dest="backfill_window",
                   action="store_false", default=True,
                   help="禁用强制工时禁跑窗口 (默认开启, 工作日 22:00-08:00 + 周末)")
    g.add_argument("--bf-pace", default=defaults["pace"],
                   choices=("fast", "normal", "slow"),
                   help=f"backfill 阅读停留 pace (默认 {defaults['pace']})")
    g.add_argument("--bf-break-every", type=int, default=defaults["break_every"],
                   help=f"每抓 N 条强制 5-15 min idle (默认 {defaults['break_every']})")
    g.add_argument("--bf-page-pause-min", type=float, default=30.0,
                   help="翻页 / sweep 切换日期时最短停留 (默认 30s)")
    g.add_argument("--bf-page-pause-max", type=float, default=90.0,
                   help="翻页 / sweep 切换日期时最长停留 (默认 90s)")
    g.add_argument("--bf-warm-up", type=int, default=30,
                   help="启动 / checkpoint 恢复后前 N 条节奏 ×3 慢起 (默认 30, 0=禁用)")
    g.add_argument("--bf-lock-role", default="default",
                   help="BackfillLock 子角色 (区分 PDF / list / oversea 等), 默认 'default'")
    g.add_argument("--bf-no-lock", dest="bf_lock", action="store_false",
                   default=True,
                   help="禁用 BackfillLock 单实例锁 (默认开启)")
    g.add_argument("--bf-force-lock", action="store_true",
                   help="强制夺锁 (用在确认前一进程已死的情况下)")


def backfill_session_from_args(args, platform: Optional[str] = None,
                                verbose: bool = True) -> BackfillSession:
    pf = platform or getattr(args, "_antibot_platform", None) or "unknown"
    return BackfillSession(
        platform=pf,
        pace=getattr(args, "bf_pace", "normal"),
        break_every=getattr(args, "bf_break_every", 50),
        page_pause_min=getattr(args, "bf_page_pause_min", 30.0),
        page_pause_max=getattr(args, "bf_page_pause_max", 90.0),
        verbose=verbose,
    )


def acquire_backfill_lock_from_args(args, platform: str) -> bool:
    """Convenience: read --bf-no-lock / --bf-lock-role / --bf-force-lock from
    args, acquire BackfillLock. Returns False if locked and we shouldn't run.
    Caller should sys.exit() if False.
    """
    if not getattr(args, "bf_lock", True):
        return True
    role = getattr(args, "bf_lock_role", "default")
    force = getattr(args, "bf_force_lock", False)
    return BackfillLock.acquire(platform, role=role, force=force)


# ============================================================================
# 调试帮手 — 打印 antibot 配置一行 stamp (scraper 启动时调用一次)
# ============================================================================

def log_config_stamp(throttle: AdaptiveThrottle,
                     cap: Optional[DailyCap] = None,
                     budget: Optional[AccountBudget] = None,
                     bf_session: Optional["BackfillSession"] = None,
                     bf_window_platform: Optional[str] = None,
                     extra: str = "") -> None:
    """Emit a single-line stamp to stdout describing the active antibot config.
    Lets crawler_monitor / log greppers verify scraper booted with the right
    knobs.

    If bf_session is passed, also prints a `[backfill]` line with backfill-
    specific config (window status, pace, break_every, lock holder).
    """
    parts = [
        f"platform={throttle.platform or '?'}",
        f"label={_process_label()}",
        f"base={throttle.base_delay}s",
        f"jitter±{throttle.jitter}s(σ={throttle.jitter/2:.2f})",
        f"burst={throttle.burst_size}",
        f"tod={'on' if throttle.enable_time_of_day else 'off'}",
        f"soft_cd={'on' if throttle.enable_soft_cooldown else 'off'}",
        f"longtail={throttle.long_tail_prob:.2f}",
    ]
    if cap and cap.max_items:
        parts.append(f"daily_cap={cap.max_items}")
    if budget and budget.daily_limit:
        role_tag = budget.role
        parts.append(f"acct_budget={budget.daily_limit}/24h({role_tag})")
        if budget.role == "bg":
            parts.append(f"rt_floor={budget.realtime_floor_pct}%")
    parts.append(f"ua={pick_user_agent()[:40]}...")
    if extra:
        parts.append(extra)
    print(f"[antibot] {' '.join(parts)}", flush=True)

    if bf_session is not None:
        plat = bf_window_platform or bf_session.platform
        cfg = _PLATFORM_BACKFILL_WINDOW.get(plat, (22, 8))
        win_str = "any-time" if cfg is None else f"{cfg[0]:02d}:00~{cfg[1]:02d}:00"
        allowed, secs = _in_backfill_window(plat)
        win_status = ("ALLOWED, " f"{secs/3600:.1f}h 剩余") if allowed else \
                     ("BLOCKED, " f"{secs/3600:.1f}h 后开窗")
        bf_parts = [
            f"platform={plat}",
            f"pace={bf_session.pace}",
            f"break_every={bf_session.break_every}",
            f"page_pause={bf_session.page_pause_min:.0f}-{bf_session.page_pause_max:.0f}s",
            f"window=[{win_str} CST 工作日 + 周末全天] {win_status}",
        ]
        # Lock holder
        holder = BackfillLock.held_by(plat, role=getattr(bf_session, "_lock_role",
                                                          "default"))
        if holder:
            bf_parts.append(f"lock_holder={holder[:24]}")
        print(f"[backfill] {' '.join(bf_parts)}", flush=True)


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 老 API (向后兼容)
    "AdaptiveThrottle",
    "DailyCap",
    "SessionDead",
    "parse_retry_after",
    "is_auth_dead",
    "add_antibot_args",
    "throttle_from_args",
    "cap_from_args",
    # antibot v2 (2026-04-24)
    "AccountBudget",
    "SoftCooldown",
    "detect_soft_warning",
    "pick_user_agent",
    "headers_for_platform",
    "time_of_day_multiplier",
    "budget_from_args",
    "log_config_stamp",
    "account_id_for_alphapai",
    "account_id_for_jinmen",
    "account_id_for_alphaengine",
    # antibot v2.2 (2026-04-25) — realtime 去数量闸 + 浏览器模拟加强
    "warmup_session",
    # backfill v1 (2026-04-24)
    "BackfillWindow",
    "BackfillSession",
    "BackfillCheckpointBackoff",
    "BackfillLock",
    "backfill_defaults",
    "add_backfill_args",
    "backfill_session_from_args",
    "acquire_backfill_lock_from_args",
]
