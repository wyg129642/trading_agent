"""Unified PDF retrieval — local SSD only.

History: until 2026-04-26 PDFs lived on a /mnt/share SMB mount; bytes were
mirrored into per-platform Mongo GridFS buckets at migration time. After
the 2026-04-27 GridFS→SSD extraction (35 710 files, md5-verified bit-perfect)
the local SSD became the single source of truth and the GridFS fallback
was retired. The fs.files / fs.chunks collections were dropped from the 5
crawler DBs (alphapai-full / gangtise-full / jinmen-full / jiuqian-full /
alphaengine), reclaiming ~45 GB Mongo dataDir.

A `pdf_local_path` whose file is missing now 404s immediately instead of
silently degrading to a slow GridFS stream. That makes broken-binary cases
visible (404 in nginx logs) instead of papering over them at 50 KB/s.

The `db` parameter is preserved on the function signature for backwards
compatibility with callers that still pass it; it is not read.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional, Sequence, Union
from urllib.parse import quote as urlquote

from fastapi import HTTPException
from fastapi.responses import FileResponse
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

PdfRootArg = Union[str, os.PathLike, Sequence[Union[str, os.PathLike]]]


def _normalize_roots(pdf_root: PdfRootArg) -> list[Path]:
    """Accept a single path or list of paths and return resolved Path list,
    de-duplicating while preserving caller order.
    """
    if isinstance(pdf_root, (str, os.PathLike)):
        items = [pdf_root]
    else:
        items = list(pdf_root)
    out: list[Path] = []
    seen: set[str] = set()
    for r in items:
        if not r:
            continue
        rp = Path(r).resolve()
        key = str(rp)
        if key not in seen:
            seen.add(key)
            out.append(rp)
    return out


def _filename_for_pdf(rel_path: str, pdf_root: PdfRootArg) -> str:
    """Convert a stored ``pdf_local_path`` into a stable rel-path key
    (``<root_basename>/<sub-path>``).

    Kept after the GridFS retirement because tests + the chat KB ingest
    pipeline still use this to compute a canonical platform-prefixed
    relative path for logging / cross-referencing.

    Input variants we must handle:
      - absolute path: ``/home/ygwang/crawl_data/alphapai_pdfs/2025-11/x.pdf``
      - relative to repo: ``alphapai_pdfs/2025-11/x.pdf``
      - partial inside root: ``2025-11/x.pdf``
    """
    roots = _normalize_roots(pdf_root)
    p = Path(rel_path)
    if p.is_absolute():
        for root in roots:
            try:
                rel = p.relative_to(root.parent)
                return rel.as_posix()
            except ValueError:
                continue
        root_names = {r.name for r in roots}
        for i, part in enumerate(p.parts):
            if part in root_names:
                return Path(*p.parts[i:]).as_posix()
        return p.name
    root_names = [r.name for r in roots]
    if p.parts and p.parts[0] in root_names:
        return p.as_posix()
    if roots:
        return f"{roots[0].name}/{p.as_posix()}"
    return p.as_posix()


async def stream_pdf_or_file(
    *,
    db: AsyncIOMotorDatabase,
    pdf_rel_path: str,
    pdf_root: PdfRootArg,
    download_filename: str,
    download: bool = False,
    media_type: str = "application/pdf",
) -> FileResponse:
    """Serve a PDF straight from local SSD via Range-friendly FileResponse.

    Resolution: pdf_rel_path is checked under every root in ``pdf_root`` (a
    single path or list). Security: the resolved file must live under one
    of the configured roots. Disk miss → 404.

    `db` is unused; kept on the signature for caller backwards compatibility.
    """
    del db  # GridFS retired 2026-04-27 — kept on signature for caller compat.
    roots = _normalize_roots(pdf_root)
    if not roots:
        raise HTTPException(500, "pdf_root misconfigured (empty)")

    p = Path(pdf_rel_path)
    candidates: list[Path] = []
    if p.is_absolute():
        candidates.append(p)
    else:
        for root in roots:
            if p.parts and p.parts[0] == root.name:
                candidates.append(root.parent / p)
            else:
                candidates.append(root / p)

    tried: list[str] = []
    for cand in candidates:
        owns = any(_is_under(cand, root) for root in roots)
        if not owns:
            tried.append(f"{cand} [outside roots]")
            continue
        if cand.is_file():
            headers = _cd_headers(download_filename, download,
                                  size=cand.stat().st_size)
            headers["X-PDF-Source"] = "disk"
            return FileResponse(
                cand,
                media_type=media_type,
                headers=headers,
            )
        tried.append(str(cand))

    logger.warning("PDF missing on disk: rel=%s tried=%s", pdf_rel_path, tried)
    raise HTTPException(
        404,
        f"PDF file missing on disk: {Path(pdf_rel_path).name}",
    )


def _is_under(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def _cd_headers(filename: str, download: bool, size: Optional[int] = None) -> dict:
    """RFC 5987 Content-Disposition (supports non-ASCII filenames via filename*)."""
    safe = "".join(c for c in filename if c.isascii() and c not in "\\/:*?\"<>|\r\n\t").strip()
    if not safe or safe == ".pdf":
        safe = "file.pdf"
    if not safe.endswith(".pdf"):
        safe += ".pdf"
    utf8_name = "".join(c for c in filename if c not in "\\/:*?\"<>|\r\n\t").strip() or safe
    if not utf8_name.endswith(".pdf"):
        utf8_name += ".pdf"
    disp = "attachment" if download else "inline"
    cd = f"{disp}; filename=\"{safe}\"; filename*=UTF-8''{urlquote(utf8_name)}"
    h = {"Content-Disposition": cd, "Cache-Control": "private, max-age=3600"}
    if size is not None:
        h["Content-Length"] = str(size)
    return h
