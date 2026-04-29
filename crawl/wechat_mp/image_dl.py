"""微信公众号图片本地化下载.

mmbiz.qpic.cn 对 Referer 做严格校验:
  - 缺 Referer 或 Referer 非 mp.weixin.qq.com → 403
  - 实测 `Referer: https://mp.weixin.qq.com/` 永远放行
  - User-Agent 用普通 Chrome 桌面 UA 即可,无需 MicroMessenger

下载策略:
  - 与正文抓取分池节流 (CDN 比主站宽松,base 0.8s + jitter 0.5s)
  - 失败不阻断主流程, 把 download_error 写回 doc.images 那条
  - 文件按 .../<biz>/<sn>/<idx>.<ext> 落盘, ext 由响应 Content-Type 决定
"""

from __future__ import annotations

import os
import re
import sys
import threading
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from antibot import AdaptiveThrottle  # noqa: E402

DEFAULT_ROOT = Path(
    os.environ.get("WECHAT_MP_IMAGE_ROOT")
    or "/home/ygwang/crawl_data/wechat_mp_images"
)

_REFERER = "https://mp.weixin.qq.com/"
_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_IMG_THROTTLE = AdaptiveThrottle(
    base_delay=0.8, jitter=0.5, burst_size=20, platform="wechat_mp_image"
)
_IMG_THROTTLE_LOCK = threading.Lock()  # AdaptiveThrottle 不一定线程安全

_EXT_BY_CT = {
    "image/jpeg": ".jpeg",
    "image/jpg": ".jpeg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
    "image/bmp": ".bmp",
}

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_segment(s: str) -> str:
    s = _SAFE_NAME.sub("_", s.strip())
    return s[:80] or "x"


def _ext_from(ct: str, url: str) -> str:
    ct = (ct or "").split(";", 1)[0].strip().lower()
    if ct in _EXT_BY_CT:
        return _EXT_BY_CT[ct]
    # mmbiz_jpg / mmbiz_png / mmbiz_gif / mmbiz_webp 路径段已经暗示了扩展名
    path = (urlparse(url).path or "").lower()
    for token, ext in (
        ("mmbiz_jpg", ".jpeg"),
        ("mmbiz_png", ".png"),
        ("mmbiz_gif", ".gif"),
        ("mmbiz_webp", ".webp"),
    ):
        if token in path:
            return ext
    return ".bin"


def relative_path(biz: str, sn: str, idx: int, ext: str) -> Path:
    return Path(_safe_segment(biz)) / _safe_segment(sn) / f"{idx}{ext}"


def download_one(
    url: str, biz: str, sn: str, idx: int,
    *, root: Path = DEFAULT_ROOT, timeout: float = 20.0,
    session: Optional[requests.Session] = None,
) -> dict:
    """返回 {"src": url, "local_path": "wechat_mp_images/...", "size_bytes": N,
    "download_error": None or str}.

    local_path 写相对路径(不带前缀斜杠), 镜像 API 拼 root 即可。
    """
    out: dict = {"src": url, "local_path": None, "size_bytes": None,
                 "download_error": None}
    if not url or not url.startswith(("http://", "https://")):
        out["download_error"] = "bad_url"
        return out

    sess = session or requests
    headers = {
        "User-Agent": _UA,
        "Referer": _REFERER,
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    }
    with _IMG_THROTTLE_LOCK:
        _IMG_THROTTLE.wait()
    try:
        r = sess.get(url, headers=headers, timeout=timeout, stream=True)
    except requests.RequestException as e:
        out["download_error"] = f"request_failed:{e.__class__.__name__}"
        return out

    if r.status_code != 200:
        out["download_error"] = f"http_{r.status_code}"
        try:
            r.close()
        except Exception:
            pass
        return out

    ext = _ext_from(r.headers.get("Content-Type", ""), url)
    rel = relative_path(biz, sn, idx, ext)
    abs_path = root / rel
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = abs_path.with_suffix(abs_path.suffix + ".part")
    try:
        n = 0
        with tmp.open("wb") as fh:
            for chunk in r.iter_content(64 * 1024):
                if not chunk:
                    continue
                fh.write(chunk)
                n += len(chunk)
        tmp.replace(abs_path)
        out["local_path"] = str(rel)
        out["size_bytes"] = n
    except Exception as e:
        out["download_error"] = f"write_failed:{e.__class__.__name__}"
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
    finally:
        try:
            r.close()
        except Exception:
            pass
    return out


def download_many(
    images: list[dict], biz: str, sn: str,
    *, root: Path = DEFAULT_ROOT,
    session: Optional[requests.Session] = None,
) -> list[dict]:
    """images 入参形如 [{"src": "https://mmbiz.qpic.cn/..."}], 出参在每条上
    增补 local_path / size_bytes / download_error。idx 用入参顺序。"""
    out = []
    for i, item in enumerate(images):
        src = (item or {}).get("src") or ""
        merged = dict(item or {})
        merged.update(download_one(src, biz, sn, i, root=root, session=session))
        out.append(merged)
    return out
