"""Unified read/write for the 6 crawler platforms' credentials + token-health probe.

Each platform stores its auth artifact in its own `credentials.json` (Jinmen is
the exception — the scraper embeds a base64 blob as a Python constant, so we
also read/write a sibling `credentials.json` and the scraper was patched to
prefer it over the hardcoded default).

The health probe runs `python scraper.py --show-state` as a subprocess so we
reuse every platform's existing auth-validation logic without duplicating it
here. Callers MUST invoke the async entry points from within an asyncio loop.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

# Repo root: backend/app/services/credential_manager.py → up 4 levels.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_CRAWL_DIR = _REPO_ROOT / "crawl"


@dataclass(frozen=True)
class PlatformSpec:
    """Static per-platform metadata for the credential UI."""

    key: str
    display_name: str
    dir_name: str
    token_fields: tuple[str, ...]
    supports_auto_login: bool
    login_hint: str
    # "phone" or "email" — tells the UI which label + input to show
    login_identifier: str = "phone"
    # Whether the auto-login flow requires a password (else SMS-OTP only)
    login_needs_password: bool = False
    # Primary login mode: "password" | "qr" | "sms" — drives the default tab
    # in the UI. Platforms with heavy 2FA are better served by QR-scan.
    login_mode: str = "password"
    # Whether QR-scan (e.g. WeChat scan) is an option for this platform —
    # surfaced as an extra tab in the UI even when login_mode is "password".
    supports_qr_login: bool = False

    @property
    def dir_path(self) -> Path:
        return _CRAWL_DIR / self.dir_name

    @property
    def credentials_path(self) -> Path:
        return self.dir_path / "credentials.json"

    @property
    def saved_login_path(self) -> Path:
        return self.dir_path / "login_saved.json"


PLATFORMS: dict[str, PlatformSpec] = {
    "alphapai": PlatformSpec(
        key="alphapai",
        display_name="AlphaPai (Alpha派)",
        dir_name="alphapai_crawl",
        token_fields=("token",),
        supports_auto_login=True,
        login_hint="手机号 + 密码, 或微信扫码",
        login_needs_password=True,
        supports_qr_login=True,
    ),
    "gangtise": PlatformSpec(
        key="gangtise",
        display_name="Gangtise (岗底斯)",
        dir_name="gangtise",
        token_fields=("token", "uid", "user_key", "tenant_id"),
        supports_auto_login=True,
        login_hint="微信扫码 (最推荐) 或密码",
        login_needs_password=True,
        login_mode="qr",
        supports_qr_login=True,
    ),
    "funda": PlatformSpec(
        key="funda",
        display_name="Funda (funda.ai)",
        dir_name="funda",
        token_fields=("cookie", "api_key"),
        supports_auto_login=True,
        login_hint="邮箱 + 密码 (api_key 需手动填)",
        login_identifier="email",
        login_needs_password=True,
    ),
    "jinmen": PlatformSpec(
        key="jinmen",
        display_name="Jinmen (进门财经)",
        dir_name="jinmen",
        token_fields=("token",),
        supports_auto_login=True,
        login_hint="手机号 + 密码, 或微信扫码",
        login_needs_password=True,
        supports_qr_login=True,
    ),
    "meritco": PlatformSpec(
        key="meritco",
        display_name="Meritco (久谦中台)",
        dir_name="meritco_crawl",
        token_fields=("token", "user_agent"),
        supports_auto_login=True,
        login_hint="扫码登录 (密码路径需多次 2FA 不推荐)",
        login_needs_password=False,
        login_mode="qr",
        supports_qr_login=True,
    ),
    "thirdbridge": PlatformSpec(
        key="thirdbridge",
        display_name="Third Bridge (高临)",
        dir_name="third_bridge",
        token_fields=("cookie", "user_agent"),
        supports_auto_login=True,
        login_hint="邮箱 + 密码 (WAF 可能拦截)",
        login_identifier="email",
        login_needs_password=True,
    ),
    "acecamp": PlatformSpec(
        key="acecamp",
        display_name="AceCamp (本营)",
        dir_name="AceCamp",
        token_fields=("cookie",),
        supports_auto_login=True,
        login_hint="手机号 + 密码, 或微信扫码",
        login_needs_password=True,
        supports_qr_login=True,
    ),
    "alphaengine": PlatformSpec(
        key="alphaengine",
        display_name="AlphaEngine (阿尔法引擎)",
        dir_name="alphaengine",
        token_fields=("token", "refresh_token"),
        supports_auto_login=True,
        login_hint="手机号 + 密码 (首次登录需完成滑块验证). "
                   "获取 token 后 scraper 每 6h 自动调 /auth/refresh 续期, "
                   "无需人工重登 (refresh_token 30 天有效).",
        login_needs_password=True,
        supports_qr_login=False,
    ),
    "sentimentrader": PlatformSpec(
        key="sentimentrader",
        display_name="SentimenTrader (情绪指标)",
        dir_name="sentimentrader",
        # Scraper uses email/password directly; no API token to manage.
        # Session is persisted in playwright_data/storage_state.json.
        token_fields=("email", "password"),
        supports_auto_login=True,
        login_hint="邮箱 + 密码（Playwright 自动登录, session 自持久化）",
        login_identifier="email",
        login_needs_password=True,
    ),
    "semianalysis": PlatformSpec(
        key="semianalysis",
        display_name="SemiAnalysis (Substack)",
        dir_name="semianalysis",
        token_fields=("cookie",),
        supports_auto_login=False,
        login_hint="可选: 浏览器登录 newsletter.semianalysis.com 后复制整串 document.cookie (含 substack.sid=...). 留空即匿名模式.",
        login_needs_password=False,
    ),
    "wechat_mp": PlatformSpec(
        key="wechat_mp",
        display_name="微信公众号 (mp.weixin.qq.com)",
        dir_name="wechat_mp",
        token_fields=("token",),
        supports_auto_login=True,
        login_hint="扫码登录公众号管理员后台 (需已注册公众号的微信号). "
                   "session ~4 天后失效, 撞 401/-6 时自动标 expired.",
        login_needs_password=False,
        login_mode="qr",
        supports_qr_login=True,
    ),
}


@dataclass
class PlatformStatus:
    """Aggregate view of a platform's credential state for the UI."""

    key: str
    display_name: str
    supports_auto_login: bool
    login_hint: str
    has_credentials: bool
    credentials_path: str
    last_refreshed: str | None
    token_fields: dict[str, str]  # field → redacted preview
    # Latest MongoDB document ingestion time across all collections.
    last_data_at: str | None = None
    data_age_hours: float | None = None
    data_total: int | None = None
    login_identifier: str = "phone"
    login_needs_password: bool = False
    login_mode: str = "password"
    supports_qr_login: bool = False
    has_saved_login: bool = False
    saved_identifier: str = ""  # redacted preview
    # Health classification — kept as a string field for forward-compat with
    # platform-specific states. Stable values:
    #   ok          ✓ 健康          token valid, real user attached, full content
    #   expired     ✗ 已过期/失效     HTTP 401/403 or explicit token-rejected error
    #                                → must refresh token / re-login
    #   anonymous   ⚠ 匿名访问       HTTP 200 but session NOT bound to a user
    #                                (visitor cookie or aged-out user session)
    #                                → still gets preview content; re-login for full
    #   ratelimited ⚠ 额度用尽       HTTP 200 but daily quota burnt (e.g. AlphaEngine 450)
    #                                → token fine; resets at platform midnight
    #   degraded    ✗ detail 被封    cookie/users/me 通过, 但最近 N 条入库文档
    #                                content_md 空壳比例 ≥ 阈值 → 平台后台已对
    #                                此账号掐掉正文权限 (典型: AceCamp 账号封控/
    #                                quota 耗尽). list 端点还能返摘要, 所以 dashboard
    #                                会看到"今日入库"虚高 —— 实际正文已拉不到.
    #                                → 必须换账号或等平台恢复; 换 token 没用
    #   unknown     ? 未知           probe inconclusive (network / non-JSON / unfamiliar shape)
    health: str = "unknown"
    health_detail: str = ""
    health_checked_at: str | None = None
    # 当 health=degraded 时填充: 最近 N 条文档中 content_md 空壳的比例,
    # 以及样本大小. UI 需要展示给用户判断是否要换账号.
    content_empty_ratio: float | None = None
    content_sample_size: int | None = None


def _redact(value: str) -> str:
    """Keep first 4 + last 4 chars, mask the middle."""
    if not value:
        return ""
    if len(value) <= 12:
        return "*" * len(value)
    return f"{value[:4]}…{value[-4:]} (len={len(value)})"


def list_platforms() -> list[PlatformSpec]:
    return list(PLATFORMS.values())


def get_platform(key: str) -> PlatformSpec:
    if key not in PLATFORMS:
        raise KeyError(f"Unknown platform: {key}")
    return PLATFORMS[key]


def read_credentials(key: str) -> dict[str, Any]:
    """Return the raw dict stored in credentials.json (or empty dict)."""
    spec = get_platform(key)
    path = spec.credentials_path
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except (json.JSONDecodeError, OSError):
        return {}


def write_credentials(key: str, data: dict[str, Any]) -> Path:
    """Atomic replace: write tmp + rename. Back up the existing file first.

    Directory is guaranteed to exist (every platform ships with one). We never
    append — writes are full overwrites with whatever keys the caller provides.
    """
    spec = get_platform(key)
    path = spec.credentials_path
    path.parent.mkdir(parents=True, exist_ok=True)

    # Keep one rolling backup for paranoia — credentials are hard to re-obtain.
    if path.exists():
        backup = path.with_suffix(".json.bak")
        shutil.copy2(path, backup)

    data = dict(data)
    data.setdefault("updated_at", datetime.utcnow().isoformat() + "Z")

    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)
    return path


# ── Saved login (identifier + password) ─────────────────────────────────
#
# Plaintext next to the scraper. Chmod 0600 on write to be a bit safer.
# Stored separately from credentials.json so that rotating tokens doesn't
# wipe the saved password, and so the gitignore rule is crisper.


def _redact_identifier(s: str) -> str:
    if not s:
        return ""
    if "@" in s:
        name, _, domain = s.partition("@")
        if len(name) <= 2:
            return f"{name[:1]}*@{domain}"
        return f"{name[:2]}***@{domain}"
    # Phone: keep 3 + 4
    if len(s) >= 7:
        return f"{s[:3]}****{s[-4:]}"
    return "*" * len(s)


def read_saved_login(key: str) -> dict[str, Any]:
    spec = get_platform(key)
    p = spec.saved_login_path
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except (json.JSONDecodeError, OSError):
        return {}


def write_saved_login(key: str, identifier: str, password: str) -> Path:
    spec = get_platform(key)
    p = spec.saved_login_path
    p.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "identifier": identifier,
        "password": password,
        "saved_at": datetime.utcnow().isoformat() + "Z",
    }
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(p)
    try:
        os.chmod(p, 0o600)
    except OSError:
        pass
    return p


def delete_saved_login(key: str) -> bool:
    spec = get_platform(key)
    p = spec.saved_login_path
    if p.exists():
        p.unlink()
        return True
    return False


def status_snapshot(key: str) -> PlatformStatus:
    """Build a PlatformStatus without running the health probe."""
    spec = get_platform(key)
    creds = read_credentials(key)
    path = spec.credentials_path

    token_preview: dict[str, str] = {}
    for field_name in spec.token_fields:
        val = creds.get(field_name, "")
        if isinstance(val, str):
            token_preview[field_name] = _redact(val)
        else:
            token_preview[field_name] = str(val)[:40]

    last_refreshed = creds.get("updated_at")
    if last_refreshed is None and path.exists():
        last_refreshed = datetime.utcfromtimestamp(path.stat().st_mtime).isoformat() + "Z"

    saved = read_saved_login(key)
    saved_identifier = _redact_identifier(saved.get("identifier", "")) if saved else ""

    return PlatformStatus(
        key=spec.key,
        display_name=spec.display_name,
        supports_auto_login=spec.supports_auto_login,
        login_hint=spec.login_hint,
        has_credentials=bool(creds),
        credentials_path=str(path),
        last_refreshed=last_refreshed,
        token_fields=token_preview,
        login_identifier=spec.login_identifier,
        login_needs_password=spec.login_needs_password,
        login_mode=spec.login_mode,
        supports_qr_login=spec.supports_qr_login,
        has_saved_login=bool(saved),
        saved_identifier=saved_identifier,
    )


async def probe_health(key: str, timeout: float = 25.0) -> tuple[str, str]:
    """Return (health, detail) where health is "ok" | "expired" | "unknown".

    For platforms with a direct HTTP probe registered in _DIRECT_PROBES, call
    that — it's faster and more accurate than subprocess. Otherwise fall back
    to parsing `scraper.py --show-state` output.
    """
    creds = read_credentials(key)
    probe_fn = _DIRECT_PROBES.get(key)
    if probe_fn is not None:
        # Let the probe itself decide — some platforms (Jinmen) can source
        # auth from a hardcoded constant when credentials.json is empty.
        try:
            return await probe_fn(creds, timeout=min(timeout, 15.0))
        except Exception as exc:
            return "unknown", f"probe error: {exc}"

    spec = get_platform(key)
    if not spec.dir_path.exists():
        return "unknown", f"dir missing: {spec.dir_path}"

    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "scraper.py",
            "--show-state",
            cwd=str(spec.dir_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return "unknown", "probe timed out"
    except FileNotFoundError:
        return "unknown", "python executable not found"
    except Exception as exc:  # pragma: no cover - defensive
        return "unknown", f"probe failed: {exc}"

    text = stdout.decode("utf-8", errors="replace") if stdout else ""
    lowered = text.lower()

    # Any of these mean auth is dead.
    expired_markers = (
        "[token] ✗",
        "401 unauthor",
        "403 forbid",
        "session expired",
        "invalid token",
        "auth failed",
        "token 过期",
        "请重新登录",
    )
    if any(m.lower() in lowered for m in expired_markers):
        return "expired", _tail(text)

    # Explicit healthy marker (gangtise / funda / thirdbridge emit this).
    if "[token] ✓" in text:
        return "ok", _tail(text)

    # Scrapers that don't probe auth — report unknown rather than guessing.
    return "unknown", _tail(text)


def _tail(text: str, max_lines: int = 6) -> str:
    lines = [ln for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines[-max_lines:])


async def status_with_health(key: str) -> PlatformStatus:
    snap = status_snapshot(key)
    # Only short-circuit if there is neither a credentials.json nor a direct
    # probe that knows how to source the token some other way (e.g. Jinmen
    # stores its auth blob as a hardcoded constant inside scraper.py).
    if not snap.has_credentials and key not in _DIRECT_PROBES:
        snap.health = "expired"
        snap.health_detail = "no credentials file"
        return snap
    # Probe auth + data freshness + content quality in parallel.
    # Content-quality probe currently only meaningful for acecamp (detail endpoint
    # quota blocks leave content_md empty while users/me still returns ok).
    # Cheap to expand to other platforms by adding their (db, coll) mapping.
    quality_spec = _CONTENT_QUALITY_PROBE_SPEC.get(key)
    tasks: list[Any] = [probe_health(key), _probe_data_freshness(key)]
    if quality_spec is not None:
        tasks.append(_probe_content_quality(*quality_spec))
    results = await asyncio.gather(*tasks, return_exceptions=False)
    (health, detail) = results[0]
    data_info = results[1]
    quality = results[2] if quality_spec is not None else None

    snap.health = health
    snap.health_detail = detail
    snap.health_checked_at = datetime.utcnow().isoformat() + "Z"
    snap.last_data_at = data_info.get("last_data_at")
    snap.data_age_hours = data_info.get("data_age_hours")
    snap.data_total = data_info.get("data_total")
    if quality and quality.get("probed"):
        snap.content_empty_ratio = quality["empty_ratio"]
        snap.content_sample_size = quality["sample_size"]
    return snap


# Platforms where we also check "recent docs have real content_md":
# value = (db_name, collection_name). Probe result overrides status to
# `degraded` if empty_ratio ≥ 0.7 (logic in the individual _probe_* fns).
# Not all platforms need this — only those where list/detail can diverge
# silently (AceCamp is the canonical case; others 401 hard).
_CONTENT_QUALITY_PROBE_SPEC: dict[str, tuple[str, str]] = {}
def _init_quality_spec() -> None:
    s = _get_settings_for_dbmap()
    _CONTENT_QUALITY_PROBE_SPEC["acecamp"] = (s.acecamp_mongo_db, "articles")


async def status_all() -> list[PlatformStatus]:
    """Probe all platforms in parallel."""
    return list(await asyncio.gather(*(status_with_health(k) for k in PLATFORMS)))


from backend.app.config import get_settings as _get_settings_for_dbmap
_init_quality_spec()

# Platform → (mongo_db_name, [content_collection_names]).
# 2026-04-23 迁移: DB 名改为远端带后缀的; sentimentrader 合并进 funda。
# Non-content collections (_state, account) are excluded.
def _build_data_sources() -> dict[str, tuple[str, tuple[str, ...]]]:
    s = _get_settings_for_dbmap()
    return {
        "alphapai":    (s.alphapai_mongo_db,
                        ("comments", "reports", "roadshows", "wechat_articles")),
        "gangtise":    (s.gangtise_mongo_db,
                        ("summaries", "researches", "chief_opinions")),
        "jinmen":      (s.jinmen_mongo_db, ("meetings", "reports", "oversea_reports")),
        "meritco":     (s.meritco_mongo_db, ("forum",)),
        "thirdbridge": (s.thirdbridge_mongo_db, ("interviews",)),
        "funda":       (s.funda_mongo_db,
                        ("posts", "earnings_reports", "earnings_transcripts", "sentiments")),
        "acecamp":     (s.acecamp_mongo_db, ("articles", "events")),
        "alphaengine": (s.alphaengine_mongo_db,
                        ("summaries", "china_reports", "foreign_reports", "news_items")),
        # 合并: sentimentrader_indicators 在 funda DB 下
        "sentimentrader": (s.sentimentrader_mongo_db,
                          (getattr(s, "sentimentrader_collection", "indicators"),)),
        # 2026-04-24 迁到独立 foreign-website DB (之前 co-host 在 funda)
        "semianalysis": (getattr(s, "semianalysis_mongo_db", "foreign-website"),
                        (getattr(s, "semianalysis_collection", "semianalysis_posts"),)),
        # 微信公众号 (mp.weixin.qq.com) — 2026-04-29 直采
        "wechat_mp":   (getattr(s, "wechat_mp_mongo_db", "wechat-mp"), ("articles",)),
    }


_DATA_SOURCES: dict[str, tuple[str, tuple[str, ...]]] = _build_data_sources()


async def _probe_data_freshness(key: str) -> dict[str, Any]:
    """Read the latest `crawled_at` across all the platform's content
    collections. Runs entirely locally against Mongo — cheap.

    Returns {last_data_at, data_age_hours, data_total}. All fields None on error.
    """
    spec = _DATA_SOURCES.get(key)
    if not spec:
        return {"last_data_at": None, "data_age_hours": None, "data_total": None}
    db_name, coll_names = spec

    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except ImportError:
        return {"last_data_at": None, "data_age_hours": None, "data_total": None}

    client = AsyncIOMotorClient(_get_settings_for_dbmap().alphapai_mongo_uri,
                                 serverSelectionTimeoutMS=1500, tz_aware=True)
    try:
        db = client[db_name]
        existing = set(await db.list_collection_names())
        candidates = [c for c in coll_names if c in existing]
        if not candidates:
            return {"last_data_at": None, "data_age_hours": None, "data_total": 0}

        async def probe_coll(cn: str) -> tuple[str | None, int]:
            coll = db[cn]
            # `crawled_at` is the consistent field across all crawlers.
            doc = await coll.find_one({"crawled_at": {"$exists": True}},
                                       sort=[("crawled_at", -1)],
                                       projection={"crawled_at": 1})
            ts = None
            if doc and doc.get("crawled_at") is not None:
                c = doc["crawled_at"]
                # BSON Date is always UTC. Motor returns a naive datetime, so the
                # bare .isoformat() has no tz marker and the frontend's dayjs
                # will treat it as local — off by 8h in CST. Stamp UTC explicitly.
                if hasattr(c, "isoformat"):
                    ts = c.isoformat()
                    if c.tzinfo is None:
                        ts += "Z"
                else:
                    ts = str(c)
            total = await coll.estimated_document_count()
            return ts, total

        results = await asyncio.gather(*(probe_coll(c) for c in candidates))
        tss = [t for (t, _) in results if t]
        total = sum(n for (_, n) in results)
        if not tss:
            return {"last_data_at": None, "data_age_hours": None, "data_total": total}
        latest_iso = max(tss)
        # Age in hours, naive parse.
        try:
            from datetime import datetime as _dt
            parsed = _dt.fromisoformat(latest_iso.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                age_s = (_dt.utcnow() - parsed).total_seconds()
            else:
                from datetime import timezone as _tz
                age_s = (_dt.now(_tz.utc) - parsed).total_seconds()
            age_hours = round(age_s / 3600, 2)
        except Exception:
            age_hours = None
        return {"last_data_at": latest_iso, "data_age_hours": age_hours, "data_total": total}
    except Exception:
        return {"last_data_at": None, "data_age_hours": None, "data_total": None}
    finally:
        client.close()


async def _probe_content_quality(
    db_name: str,
    coll_name: str,
    sample_size: int = 20,
    min_content_chars: int = 200,
) -> dict[str, Any]:
    """Check the N latest-by-crawled_at docs for content_md emptiness.

    用途: 平台账号若被降级为"仅标题访问"(AceCamp 封控典型症状 —— list 端点能返,
    detail 端点返 10003/10040), scraper 仍能成功入库但 content_md 是空 / 仅摘要.
    users/me 级别的探针看不出问题, 必须看实际爬进来的数据质量.

    Returns::
        {
            "sample_size": int,           # 实际采样的文档数 (可能 < sample_size)
            "empty_count": int,            # content_md 长度 < min_content_chars 的数量
            "empty_ratio": float,          # empty_count / sample_size, 0.0-1.0
            "latest_crawled_at": str|None, # 采样里最新一条的 crawled_at (ISO)
            "probed": bool,                # False = 采不到样本, 结论不可用
        }
    """
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except ImportError:
        return {"probed": False, "sample_size": 0, "empty_count": 0,
                "empty_ratio": 0.0, "latest_crawled_at": None}

    client = AsyncIOMotorClient(_get_settings_for_dbmap().alphapai_mongo_uri,
                                 serverSelectionTimeoutMS=1500, tz_aware=True)
    try:
        cur = client[db_name][coll_name].find(
            {"crawled_at": {"$exists": True}},
            projection={"content_md": 1, "summary_md": 1, "brief_md": 1,
                        "transcribe_md": 1, "crawled_at": 1},
            sort=[("crawled_at", -1)],
        ).limit(sample_size)

        sample: list[dict] = []
        async for d in cur:
            sample.append(d)
        n = len(sample)
        if n == 0:
            return {"probed": False, "sample_size": 0, "empty_count": 0,
                    "empty_ratio": 0.0, "latest_crawled_at": None}

        # "正文" 优先级: content_md > transcribe_md > summary_md > brief_md.
        # 任一长度 ≥ 阈值就算不空壳. AceCamp 被封时只剩 list_item.summary 的
        # ≤ 200 字符 preview, 阈值 200 能把这类筛出.
        empty = 0
        for d in sample:
            best = max(
                len(d.get("content_md") or ""),
                len(d.get("transcribe_md") or ""),
                len(d.get("summary_md") or ""),
                len(d.get("brief_md") or ""),
            )
            if best < min_content_chars:
                empty += 1
        latest = sample[0].get("crawled_at")
        latest_iso = None
        if latest is not None and hasattr(latest, "isoformat"):
            from datetime import timezone as _tz
            latest_iso = latest.isoformat() + ("Z" if latest.tzinfo is None else "")
        return {
            "probed": True,
            "sample_size": n,
            "empty_count": empty,
            "empty_ratio": round(empty / n, 3),
            "latest_crawled_at": latest_iso,
        }
    except Exception:
        return {"probed": False, "sample_size": 0, "empty_count": 0,
                "empty_ratio": 0.0, "latest_crawled_at": None}
    finally:
        client.close()


async def ingestion_daily_series(days: int = 14, tz: str = "Asia/Shanghai") -> dict[str, Any]:
    """Return daily ingestion counts per platform for the last `days` days.

    The user's mental model of "a day" is CST midnight, not UTC — we stored
    `crawled_at` as UTC but bucket with a timezone-aware aggregation.

    Return shape::
        {
            "tz": "Asia/Shanghai",
            "dates": ["2026-04-08", ..., "2026-04-21"],
            "series": {
                "alphapai": [0, 12, 34, ..., 51],
                ...
            },
            "totals_today": {"alphapai": 51, ...},
        }
    """
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except ImportError:
        return {"tz": tz, "dates": [], "series": {}, "totals_today": {}}

    from datetime import datetime as _dt, timedelta as _td, timezone as _tz

    # Build CST date axis. "Today" = CST date of now.
    # zoneinfo is stdlib (3.9+); fall back to fixed UTC+8 if missing.
    try:
        from zoneinfo import ZoneInfo
        tzinfo = ZoneInfo(tz)
    except Exception:
        tzinfo = _tz(_td(hours=8))

    now_local = _dt.now(tzinfo)
    today = now_local.date()
    dates = [(today - _td(days=i)).isoformat() for i in range(days - 1, -1, -1)]
    # Start boundary in UTC for Mongo filter (exclusive lower bound = midnight CST days-1 ago).
    start_local = _dt.combine(today - _td(days=days - 1), _dt.min.time(), tzinfo=tzinfo)
    start_utc = start_local.astimezone(_tz.utc)

    client = AsyncIOMotorClient(_get_settings_for_dbmap().alphapai_mongo_uri,
                                 serverSelectionTimeoutMS=1500, tz_aware=True)

    # One-off bulk backfills tag docs with _canonical_extract_source ending
    # in "_bulk" (e.g. "jinmen_oversea_bulk"). They insert tens of thousands
    # of historical docs in a single session — if we counted them by
    # crawled_at they'd spike the chart and hide the real daily production
    # signal. The realtime watchers don't set this field, so filtering it
    # out leaves exactly the normal ingestion.
    _EXCLUDE_BULK_MATCH = {
        "$match": {
            "$and": [
                {"crawled_at": {"$gte": start_utc}},
                {"$or": [
                    {"_canonical_extract_source": {"$exists": False}},
                    {"_canonical_extract_source": {"$not": {"$regex": "_bulk$"}}},
                ]},
            ],
        }
    }

    # Per-(platform, collection) extra filter applied after the base
    # crawled_at/bulk guard. Used to exclude "metadata-only" docs that never
    # got their full payload scraped — counting them inflates the chart to
    # absurd levels (jinmen oversea_reports is 1.5M metadata / 2.6k with-PDF,
    # a 580× distortion).
    _EXTRA_FILTER: dict[tuple[str, str], dict[str, Any]] = {
        ("jinmen", "oversea_reports"): {
            "pdf_local_path": {"$nin": [None, ""]},
            "pdf_size_bytes": {"$gt": 0},
        },
    }

    # Split realtime vs backfill by crawled_at − release_time_ms:
    #   < 24h  → realtime (watcher caught the doc shortly after platform publish)
    #   ≥ 24h  → backfill (historical doc scraped later by catchup / bulk sweeps)
    #   missing release_time_ms → realtime (e.g. funda.sentiments uses `date` only)
    _BACKFILL_THRESHOLD_MS = 24 * 3600 * 1000

    def _match_stage(extra_filter: dict[str, Any] | None) -> dict[str, Any]:
        """Base crawled_at + bulk guard, optionally AND'd with per-coll extra filter.

        Keeping the base filter as one compiled dict saves every aggregate
        the overhead of re-building.
        """
        ands = [
            {"crawled_at": {"$gte": start_utc}},
            {"$or": [
                {"_canonical_extract_source": {"$exists": False}},
                {"_canonical_extract_source": {"$not": {"$regex": "_bulk$"}}},
            ]},
        ]
        if extra_filter:
            for k, v in extra_filter.items():
                ands.append({k: v})
        return {"$match": {"$and": ands}}

    async def per_platform(key: str, spec: tuple[str, tuple[str, ...]]) -> tuple[str, dict[str, dict[str, int]]]:
        db_name, coll_names = spec
        db = client[db_name]
        existing = set(await db.list_collection_names())
        buckets_rt: dict[str, int] = {d: 0 for d in dates}
        buckets_bf: dict[str, int] = {d: 0 for d in dates}
        base_pipeline_tail = [
            {"$addFields": {
                "_is_backfill": {
                    "$cond": {
                        "if": {"$and": [
                            {"$ne": [{"$type": "$release_time_ms"}, "missing"]},
                            {"$ne": ["$release_time_ms", None]},
                            {"$gt": [
                                {"$subtract": [
                                    {"$toLong": "$crawled_at"},
                                    {"$ifNull": ["$release_time_ms", 0]},
                                ]},
                                _BACKFILL_THRESHOLD_MS,
                            ]},
                        ]},
                        "then": True,
                        "else": False,
                    }
                }
            }},
            {"$group": {
                "_id": {
                    "d": {"$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$crawled_at",
                        "timezone": tz,
                    }},
                    "bf": "$_is_backfill",
                },
                "c": {"$sum": 1},
            }},
        ]
        for cn in coll_names:
            if cn not in existing:
                continue
            # Rebuild pipeline per coll so the $match picks up per-coll
            # extras (e.g. jinmen.oversea_reports requires pdf_local_path).
            coll_pipeline = [
                _match_stage(_EXTRA_FILTER.get((key, cn))),
                *base_pipeline_tail,
            ]
            try:
                async for row in db[cn].aggregate(coll_pipeline):
                    gid = row.get("_id") or {}
                    d = gid.get("d")
                    is_bf = bool(gid.get("bf"))
                    if d not in buckets_rt:
                        continue
                    cnt = int(row.get("c", 0))
                    (buckets_bf if is_bf else buckets_rt)[d] += cnt
            except Exception:
                continue
        return key, {"realtime": buckets_rt, "backfill": buckets_bf}

    try:
        results = await asyncio.gather(
            *(per_platform(k, s) for k, s in _DATA_SOURCES.items()),
            return_exceptions=False,
        )
        series_realtime: dict[str, list[int]] = {}
        series_backfill: dict[str, list[int]] = {}
        series: dict[str, list[int]] = {}  # total (back-compat)
        totals_today: dict[str, int] = {}
        totals_today_realtime: dict[str, int] = {}
        totals_today_backfill: dict[str, int] = {}
        today_key = today.isoformat()
        for key, split in results:
            rt = [split["realtime"][d] for d in dates]
            bf = [split["backfill"][d] for d in dates]
            series_realtime[key] = rt
            series_backfill[key] = bf
            series[key] = [a + b for a, b in zip(rt, bf)]
            totals_today_realtime[key] = split["realtime"].get(today_key, 0)
            totals_today_backfill[key] = split["backfill"].get(today_key, 0)
            totals_today[key] = totals_today_realtime[key] + totals_today_backfill[key]
        return {
            "tz": tz,
            "dates": dates,
            "series": series,
            "series_realtime": series_realtime,
            "series_backfill": series_backfill,
            "totals_today": totals_today,
            "totals_today_realtime": totals_today_realtime,
            "totals_today_backfill": totals_today_backfill,
        }
    finally:
        client.close()


# ── Per-platform direct probes (cheap authenticated API call) ────────────
#
# Register a probe here when the scraper's own `--show-state` does not
# validate the token. Fallback is the subprocess path above.


async def _probe_alphapai(creds: dict, timeout: float = 10.0) -> tuple[str, str]:
    """POST a 1-item list request. AlphaPai returns code=200000 on success,
    code=401000 ("无权访问") when the JWT is invalid/expired.
    """
    import httpx

    token = (creds or {}).get("token", "")
    if not token:
        return "expired", "no token in credentials"

    url = "https://alphapai-web.rabyte.cn/external/alpha/api/reading/comment/list"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://alphapai-web.rabyte.cn/",
        "Origin": "https://alphapai-web.rabyte.cn",
        "x-from": "web",
        "platform": "web",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    # trust_env=False → Clash can't divert alphapai-web CDN (see project memory).
    async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
        resp = await client.post(url, headers=headers, json={"page": 1, "pageSize": 1})

    if resp.status_code in (401, 403):
        return "expired", f"HTTP {resp.status_code}"
    try:
        data = resp.json()
    except ValueError:
        return "unknown", f"non-JSON response: HTTP {resp.status_code}"

    code = data.get("code")
    msg = data.get("msg") or data.get("message") or ""
    # Be tolerant — accept both int and string forms; different AlphaPai
    # endpoints have returned either.
    ok_codes = {200000, "200000", 0, "0", "000000"}
    if code in ok_codes or "success" in msg.lower():
        return "ok", f"API OK (code={code})"
    expired_codes = {401000, "401000", 401001, "401001", 403000, "403000", 401, 403}
    if code in expired_codes or "无权" in msg or "未登录" in msg:
        return "expired", f"code={code} {msg}".strip()
    return "unknown", f"unexpected code={code} {msg}".strip()


async def _probe_gangtise(creds: dict, timeout: float = 10.0) -> tuple[str, str]:
    """GET /application/userCenter/userCenter/api/account — needs Authorization: bearer <G_token>."""
    import httpx

    token = (creds or {}).get("token", "")
    if not token:
        return "expired", "no token"
    url = "https://open.gangtise.com/application/userCenter/userCenter/api/account"
    headers = {
        "Authorization": f"bearer {token}",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://open.gangtise.com/",
    }
    async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code in (401, 403):
        return "expired", f"HTTP {resp.status_code}"
    try:
        data = resp.json()
    except ValueError:
        return "unknown", f"non-JSON response HTTP {resp.status_code}"
    # Gangtise success code: integer 0 OR string "0" OR "000000" (legacy).
    code = data.get("code")
    ok_codes = {0, "0", "000000"}
    if code in ok_codes and (data.get("data") or {}).get("uid"):
        d = data["data"]
        return "ok", f"uid={d.get('uid')} user={d.get('userName','')} company={d.get('companyName','')}".strip()
    # msg "操作成功" is also a success signal even when code format shifts.
    if (data.get("msg") or "").strip() in ("操作成功", "success"):
        d = data.get("data") or {}
        return "ok", f"msg=操作成功 uid={d.get('uid','?')}"
    return "expired", f"code={code} msg={data.get('msg','')}".strip()


async def _probe_funda(creds: dict, timeout: float = 12.0) -> tuple[str, str]:
    """tRPC user.getUserProfile. Session cookie must have `session-token`."""
    import httpx, json, urllib.parse

    cookie = (creds or {}).get("cookie", "")
    if not cookie:
        return "expired", "no cookie"
    inp = {"0": {"json": None, "meta": {"values": ["undefined"], "v": 1}}}
    enc = urllib.parse.quote(json.dumps(inp, separators=(",", ":")))
    url = f"https://funda.ai/api/trpc/user.getUserProfile?batch=1&input={enc}"
    headers = {
        "Cookie": cookie,
        "Accept": "application/json",
        "Referer": "https://funda.ai/reports",
    }
    async with httpx.AsyncClient(timeout=timeout, trust_env=False, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code in (401, 403):
        return "expired", f"HTTP {resp.status_code}"
    try:
        arr = resp.json()
    except ValueError:
        return "unknown", f"non-JSON HTTP {resp.status_code}"
    if isinstance(arr, list) and arr:
        first = arr[0]
        if "error" in first:
            # Distinguish auth errors from transient tRPC errors.
            err_str = str(first.get("error"))[:160].lower()
            if "unauth" in err_str or "forbid" in err_str or "expired" in err_str:
                return "expired", f"tRPC auth error: {err_str[:120]}"
            return "unknown", f"tRPC error (non-auth): {err_str[:120]}"
        data = (first.get("result") or {}).get("data", {}).get("json") or {}
        org_id = data.get("orgId") or (data.get("user") or {}).get("orgId")
        user_id = data.get("id") or (data.get("user") or {}).get("id")
        if org_id or user_id:
            org = data.get("org") or {}
            return "ok", f"orgId={org_id or '?'} userId={user_id or '?'} tier={org.get('tier','?')}"
        # Any successful JSON response means the session-token was accepted;
        # only the payload shape is unexpected.
        return "unknown", f"tRPC response shape unexpected (HTTP 200, cookie may still be valid)"
    return "unknown", f"tRPC response empty (HTTP {resp.status_code})"


async def _probe_thirdbridge(creds: dict, timeout: float = 12.0) -> tuple[str, str]:
    """GET /api/client-users/account-management. Needs the full WAF cookie jar."""
    import httpx

    cookie = (creds or {}).get("cookie", "")
    if not cookie:
        return "expired", "no cookie"
    ua = (creds or {}).get("user_agent") or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    url = "https://forum.thirdbridge.com/api/client-users/account-management"
    headers = {
        "Cookie": cookie,
        "User-Agent": ua,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://forum.thirdbridge.com/en/home/all",
    }
    async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code in (401, 403):
        return "expired", f"HTTP {resp.status_code}"
    # Cloudfront / WAF may redirect to a login page with 200.
    if resp.status_code == 200 and "<html" in resp.text[:300].lower():
        return "expired", "redirected to login page (WAF / session lost)"
    try:
        data = resp.json()
    except ValueError:
        return "unknown", f"non-JSON HTTP {resp.status_code}"
    uuid = data.get("uuid") or data.get("userUuid") or (data.get("user") or {}).get("uuid")
    email = data.get("email") or (data.get("user") or {}).get("email")
    if uuid or email:
        return "ok", f"uuid={uuid or '?'} email={email or '?'} company={data.get('companyName','')}".strip()
    # HTTP 200 + valid JSON but no uuid → probably session extended but empty payload.
    return "unknown", f"HTTP 200 but account fields missing (cookie may still be valid)"


def _decode_acecamp_user_token(cookie: str) -> tuple[str | None, str]:
    """Decode the user_token JWT embedded in the AceCamp cookie string.

    AceCamp cookie carries `user_token=<jwt>` next to the Rails session id.
    The JWT payload (Anthropic-shape: `eyJhbGciOiJIUzI1NiJ9.<b64>.<sig>`)
    has fields `user_id`, `refresh_at`, `expires_in`. Token is HS256 so we
    can't *verify* without the secret, but for "is this token live?" decoding
    + checking expiry is sufficient.

    Returns: (user_id_str | None, error_status).
      error_status: "" on valid, "missing"/"malformed"/"expired" on issue.
    """
    import re, base64, json, time

    if not cookie:
        return None, "missing"
    m = re.search(r"user_token=([^;\s]+)", cookie)
    if not m:
        return None, "missing"
    parts = m.group(1).split(".")
    if len(parts) < 2:
        return None, "malformed"
    try:
        pad = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(pad))
    except Exception:
        return None, "malformed"
    user_id = payload.get("user_id") or payload.get("uid") or payload.get("sub")
    if not user_id:
        return None, "malformed"
    refresh_at = payload.get("refresh_at") or payload.get("iat") or 0
    expires_in = payload.get("expires_in") or payload.get("exp_in") or 0
    if refresh_at and expires_in:
        # JWT 的 expires_in 跟 refresh_at 是绝对秒, refresh_at + expires_in = 过期时刻
        if (float(refresh_at) + float(expires_in)) < time.time():
            return None, "expired"
    return str(user_id), ""


async def _probe_acecamp(creds: dict, timeout: float = 10.0) -> tuple[str, str]:
    """JWT decode + users/me + list 端点 + content 质量, 三层判.

    2026-04-22 → 04-29 → 04-29 (二次修正): 健康判据收敛.
      - users/me data:null = 服务端 Rails session 已销毁, 即"匿名". list 端点
        对匿名仍开放 (返公开摘要), detail 接口对匿名一律 paywall (返 ret=False
        code=10003). 这种状态: scraper 能拉 list 但**永远拿不到 detail 正文**,
        StockHub 卡片"信息不全"的根因.
      - 04-29 一度把 anonymous 当 ok, 因为想区分"users/me 端点变了"和"真匿名".
        实测 (cookie user_id=50522192 + Bearer JWT) 都返 data:null → 端点没变,
        是 cookie session 真的失效. 还原成 anonymous → 监控正确报橙色, 引导
        viewer 重登.
      - JWT exp / 缺失 → expired (user 必须重登, 而非 cookie 局部 refresh).

    返回 (health, detail):
      - "ok": users/me 拿到 user 对象 → cookie 真有效, 正文质量也通过.
      - "anonymous": users/me data:null → 重登捕获新 session cookie.
      - "expired": JWT 不可用 / HTTP 401 / 业务鉴权 401|403|1001.
      - "degraded": users/me ok, 但近 20 条 articles ≥50% 正文 <200 字 → detail
        端点疑似被封控 (quota 10003/10040 / 真付费墙).
      - "unknown": 网络异常等.
    """
    import httpx

    cookie = (creds or {}).get("cookie", "")
    if not cookie:
        return "expired", "no cookie"

    user_id, jwt_err = _decode_acecamp_user_token(cookie)
    if jwt_err == "missing":
        return "anonymous", "cookie 缺 user_token JWT (未登录捕获)"
    if jwt_err == "expired":
        return "expired", "user_token JWT 已过期, 需重新登录"

    headers = {
        "Cookie": cookie,
        "Accept": "application/json",
        "Origin": "https://www.acecamptech.com",
        "Referer": "https://www.acecamptech.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
            resp = await client.get(
                "https://api.acecamptech.com/api/v1/users/me",
                headers=headers,
                params={"get_follows": "true", "with_owner": "true",
                         "with_resume": "true", "version": "2.0"},
            )
    except httpx.HTTPError as e:
        return "unknown", f"网络错误: {type(e).__name__}: {str(e)[:120]}"

    if resp.status_code in (401, 403):
        return "expired", f"HTTP {resp.status_code} (cookie 失效)"
    try:
        data = resp.json()
    except ValueError:
        return "unknown", f"non-JSON HTTP {resp.status_code}"

    user = data.get("data") if isinstance(data, dict) else None
    if isinstance(user, dict) and (user.get("id") or user.get("user_id") or user.get("username")):
        uid = user.get("id") or user.get("user_id")
        name = user.get("username") or user.get("name") or user.get("nick_name") or ""
        quality = await _probe_content_quality(
            db_name=_get_settings_for_dbmap().acecamp_mongo_db,
            coll_name="articles",
            sample_size=20,
            min_content_chars=200,
        )
        base_msg = f"user_id={uid} name={name}".strip()
        if quality.get("probed") and quality["empty_ratio"] >= 0.5:
            return "degraded", (
                f"{base_msg} · 近 {quality['sample_size']} 条 articles 中 "
                f"{quality['empty_count']} 条正文 <200 字 "
                f"({quality['empty_ratio']*100:.0f}%) — detail 端点疑似被封控 "
                f"(quota 10003/10040 / 付费墙)"
            )
        return "ok", base_msg

    if user is None and isinstance(data, dict) and data.get("ret") is True:
        return "anonymous", (
            f"匿名 session · 服务端 Rails session 已失效 (users/me data=null). "
            f"JWT user_id={user_id} 仍存但服务器不认, list 端点仍开放但 detail "
            f"全部 paywall — 请在实时查看里重登捕获新 session cookie."
        )
    code = (data or {}).get("code")
    msg = str((data or {}).get("msg") or "")
    if code in (401, 403, 1001):
        return "expired", f"业务鉴权失败 code={code} {msg[:60]}"
    return "anonymous", f"users/me 异常 code={code} {msg[:80]}"


async def _probe_jinmen(creds: dict, timeout: float = 10.0) -> tuple[str, str]:
    """Jinmen stashes JM_AUTH_INFO (base64 JSON) either in credentials.json
    under "token" or as a hardcoded constant in jinmen/scraper.py. We try
    credentials first, then fall back to regex-scraping the constant.

    Response bodies are AES-encrypted, but HTTP status alone distinguishes
    auth-live vs expired reliably.
    """
    import base64, json as _json, re
    import httpx

    blob = (creds or {}).get("token", "")
    if not blob:
        # Fallback: pull the hardcoded JM_AUTH_INFO out of scraper.py.
        scraper_path = _CRAWL_DIR / "jinmen" / "scraper.py"
        if scraper_path.exists():
            src = scraper_path.read_text(encoding="utf-8")
            m = re.search(r'JM_AUTH_INFO\s*=\s*"([^"]+)"', src)
            if m:
                blob = m.group(1)
    if not blob:
        return "expired", "no JM_AUTH_INFO anywhere"

    try:
        decoded = base64.b64decode(blob).decode("utf-8", errors="replace")
        auth = _json.loads(decoded).get("value") or {}
    except Exception as exc:
        return "unknown", f"base64/JSON decode failed: {exc}"

    uid = auth.get("uid")
    webtoken = auth.get("webtoken") or auth.get("token")
    if not (uid and webtoken):
        return "expired", "auth blob missing uid/webtoken"

    headers = {
        "uid": str(uid),
        "token": webtoken,
        "web_token": webtoken,
        "realm": auth.get("realm", "") or "",
        "os": "brm",
        "c": "pc",
        "b": "4.2.0800",
        "brandChannel": "windows",
        "webenv": "comein",
        "language": "zh-CN",
        "app": "json",
        "mod": "roadshow-list",
        "act": "summary",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://brm.comein.cn/",
        "Origin": "https://brm.comein.cn",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    url = "https://server.comein.cn/comein/json_roadshow-list_summary"
    body = {"page": 1, "size": 1, "type": 13, "sortType": 2, "orderType": 2,
            "input": "", "options": {"needParticiple": False, "allowInputEmpty": True, "searchScope": 0}}

    async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
        resp = await client.post(url, headers=headers, json=body)

    if resp.status_code in (401, 403):
        return "expired", f"HTTP {resp.status_code}"
    if resp.status_code != 200:
        return "unknown", f"HTTP {resp.status_code}"

    # HTTP 200 不等于认证活着: session 活着时 body 是 AES 密文 (有 k header),
    # 挂了时 body 直接回明文 JSON {"code":"500","msg":"用户信息不存在"}.
    k_header = resp.headers.get("k") or resp.headers.get("K")
    payload: dict = {}
    if k_header:
        try:
            import hashlib
            from Crypto.Cipher import AES
            from Crypto.Util.Padding import unpad
            _SALT = "039ed7d839d8915bf01e4f49825fcc6b"
            k_decoded = base64.b64decode(k_header).decode("utf-8").strip()
            key = hashlib.md5((k_decoded + ":" + _SALT).encode("utf-8")).hexdigest().upper().encode("utf-8")
            raw = base64.b64decode(resp.text)
            plaintext = unpad(AES.new(key, AES.MODE_CBC, raw[:16]).decrypt(raw[16:]), AES.block_size)
            payload = _json.loads(plaintext.decode("utf-8"))
        except Exception as exc:
            return "unknown", f"uid={uid} decrypt failed: {exc}"
    else:
        try:
            payload = resp.json()
        except Exception:
            return "unknown", f"uid={uid} HTTP 200 non-json body"

    code = str(payload.get("code") or "").strip()
    msg = (payload.get("msg") or payload.get("errordesc") or payload.get("message") or "")[:60]
    if code in ("0", "200"):
        return "ok", f"uid={uid} code={code}"
    if "用户信息不存在" in msg or "请登录" in msg or "未登录" in msg or code in ("401", "403", "500"):
        return "expired", f"uid={uid} 掉线 需重新登录: code={code} msg={msg}"
    return "unknown", f"uid={uid} unexpected code={code} msg={msg}"


async def _probe_sentimentrader(creds: dict, timeout: float = 10.0) -> tuple[str, str]:
    """Health = can the scraper likely succeed without re-logging-in?

    The scraper doesn't use an API token — it keeps a Playwright storage_state
    (cookies + localStorage) that auto-refreshes on every successful scrape.
    As long as the stored session has been exercised recently (≤14 days),
    the next scrape should reuse it without a password login. Beyond that,
    Playwright will transparently log in again using the credentials file.

    We intentionally do not make a real HTTP request here — the scrape itself
    is the definitive health check, and it's already running daily via cron.
    Doing a Playwright boot for every status refresh would be too slow.
    """
    email = (creds or {}).get("email", "")
    password = (creds or {}).get("password", "")
    if not email or not password:
        return "expired", "缺少 email / password（需要重新填写登录凭据）"

    storage = _CRAWL_DIR / "sentimentrader" / "playwright_data" / "storage_state.json"
    if not storage.exists():
        # Credentials are on file so auto-login will still work on next run —
        # but we haven't had a successful scrape yet, so the session is unknown.
        return "unknown", "尚无浏览器会话，首次运行时将自动登录"

    # `time.time()` and `st_mtime` are both Unix epoch seconds in UTC — safe
    # to subtract directly. `datetime.utcnow().timestamp()` treats the naive
    # datetime as LOCAL time, which introduced an 8-hour skew on CST hosts.
    import time as _time
    age_s = max(0.0, _time.time() - storage.stat().st_mtime)
    age_days = age_s / 86400
    if age_days <= 14:
        return "ok", f"浏览器会话 {age_days:.1f} 天前刷新过，可直接复用"
    return "expired", f"浏览器会话已 {age_days:.0f} 天未刷新，下次运行可能需要重新登录"


async def _probe_alphaengine(creds: dict, timeout: float = 10.0) -> tuple[str, str]:
    """Probe www.alphaengine.top — hit the same streamSearch list endpoint the
    scraper uses (size=1, code=summary). Auth-dead → 401; valid → SSE body with
    `event:update` + `"_final"` payload; expired JWT → HTTP 401 on the whole
    stream open.
    """
    import httpx

    tok = (creds or {}).get("token", "")
    if not tok:
        return "expired", "no token"
    url = "https://www.alphaengine.top/api/v1/kmpsummary/summary/search/streamSearch"
    headers = {
        "Authorization": f"Bearer {tok}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://www.alphaengine.top",
        "Referer": "https://www.alphaengine.top/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "Chrome/124.0 Safari/537.36"
        ),
    }
    body = {"code": "summary", "size": 1, "realtime": False}
    # Allow proxy here (unlike Gangtise); EdgeOne CDN works fine with Clash.
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, headers=headers, json=body)

    if resp.status_code in (401, 403):
        return "expired", f"HTTP {resp.status_code}"
    if resp.status_code != 200:
        return "unknown", f"HTTP {resp.status_code}"
    txt = resp.text
    # Server may return HTTP 200 with biz-level 401 `刷新 token` when the
    # refresh_token chain on our side got invalidated (e.g. someone logged
    # in elsewhere, or we burned the chain by calling /auth/refresh too often
    # without persisting the new refresh_token). This is NOT a quota issue —
    # the user must copy a fresh `token` + `refresh_token` pair from their
    # browser localStorage into credentials.json.
    if '"code":401' in txt and ("刷新 token" in txt or "用户状态发生变更" in txt):
        return "expired", (
            "refresh_token 链已失效 (另一地点登录后旧 token 被顶下线). "
            "请从浏览器 localStorage 复制最新的 token + refresh_token 到 credentials.json"
        )
    if '"_final"' in txt and '"results"' in txt:
        # Try to extract total if present
        import re as _re
        m = _re.search(r'"total"\s*:\s*(\d+)', txt)
        total = m.group(1) if m else "?"
        return "ok", f"streamSearch 响应正常 · total={total}"
    # 200 OK but no _final — distinguish rate-limit (account fine, quota burnt
    # for today) from actual auth trouble. Rate-limit is business-level code
    # 450 with REFRESH_LIMIT sub-code; token itself is still valid. 0点重置,
    # 不需要用户换 token.
    import re as _re
    if '"code":450' in txt or 'REFRESH_LIMIT' in txt or '额度已达上限' in txt or '额度达到上限' in txt:
        m = _re.search(r'"description"\s*:\s*"([^"]{0,200})"', txt) or \
            _re.search(r'"msg"\s*:\s*"([^"]{0,200})"', txt)
        detail = m.group(1) if m else "刷新额度已用尽"
        # Use a distinct "ratelimited" status so the frontend can tint orange
        # instead of red; token reloading wouldn't fix it.
        return "ratelimited", f"每日额度用尽 (0 点重置): {detail}"
    # Generic 200-but-no-final — could be a platform error event we haven't seen.
    m = _re.search(r'"content"\s*:\s*"([^"]{0,200})"', txt)
    detail = m.group(1) if m else txt[:120]
    return "unknown", f"200 OK 但无 _final 事件: {detail}"


async def _probe_wechat_mp(creds: dict, timeout: float = 10.0) -> tuple[str, str]:
    """探活: 用 searchbiz 调一个肯定不会撞结果的关键字, 看 base_resp.ret。
    quota 成本 1 (列入正常使用配额; 平台日上限 ~500, 探活每分钟一次仍可控)。
    """
    import httpx
    import random as _random

    tok = (creds or {}).get("token", "")
    cookies_list = (creds or {}).get("cookies") or []
    if not tok:
        return "expired", "no token"
    if not cookies_list:
        return "expired", "no cookies"

    url = "https://mp.weixin.qq.com/cgi-bin/searchbiz"
    cookies = {c.get("name"): c.get("value")
               for c in cookies_list if c.get("name") and c.get("value")}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://mp.weixin.qq.com/cgi-bin/home?t=home/index&token={tok}&lang=zh_CN",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "*/*",
    }
    params = {
        "action": "search_biz", "token": tok, "lang": "zh_CN",
        "f": "json", "ajax": "1",
        "random": f"{_random.random():.16f}",
        "query": "__probe_no_match__zzz", "begin": "0", "count": "5",
    }
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        resp = await client.get(url, params=params, headers=headers, cookies=cookies)
    if resp.status_code in (302, 401, 403):
        return "expired", f"HTTP {resp.status_code} (重定向到登录页)"
    if resp.status_code != 200:
        return "unknown", f"HTTP {resp.status_code}"
    try:
        body = resp.json()
    except Exception:
        return "unknown", f"非 JSON 响应 (前 80 字符): {resp.text[:80]}"
    base = body.get("base_resp") or {}
    ret = base.get("ret")
    msg = base.get("err_msg") or ""
    if ret == 0:
        total = body.get("total")
        return "ok", f"searchbiz 响应正常 · total={total}"
    if ret == -6 or "login" in msg.lower():
        return "expired", f"会话失效 ret={ret} msg={msg}"
    if ret in (200013, 200002) or "freq" in msg.lower():
        return "ratelimited", f"频率限制 ret={ret} msg={msg}"
    return "unknown", f"未识别 ret={ret} msg={msg}"


_DIRECT_PROBES = {
    "alphapai": _probe_alphapai,
    "gangtise": _probe_gangtise,
    "funda": _probe_funda,
    "thirdbridge": _probe_thirdbridge,
    "acecamp": _probe_acecamp,
    "jinmen": _probe_jinmen,
    "sentimentrader": _probe_sentimentrader,
    "alphaengine": _probe_alphaengine,
    "wechat_mp": _probe_wechat_mp,
}
