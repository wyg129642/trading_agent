"""Unified PDF retrieval: local SSD → Mongo GridFS.

After 2026-04-26 the SMB share at ``/mnt/share/`` was retired and every PDF
lives either on the local SSD under ``/home/ygwang/crawl_data/<plat>_pdfs/``
or in the platform's Mongo GridFS bucket. We therefore:

  - Read only from local SSD — never from /mnt/share.
  - Skip the on-disk cache (``~/.cache/trading_agent/pdf_cache``) — the local
    SSD *is* the cache; an extra copy buys nothing.
  - Fall back to GridFS for PDFs whose binary never made it to local disk
    (e.g. files only present in remote Mongo's GridFS pre-migration).

Lookup order (fastest → slowest):
  1. Local SSD under one of the configured platform roots (Range-friendly
     ``FileResponse``).
  2. Mongo GridFS streamed via motor — ~500 kB/s in practice. Slow but
     unavoidable for the small minority of PDFs that exist nowhere else.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import AsyncIterator, Optional, Sequence, Union
from urllib.parse import quote as urlquote

from fastapi import HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorGridFSBucket

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
    """Convert a stored ``pdf_local_path`` / ``pdf_rel_path`` into the GridFS
    filename, which is keyed by ``<root_basename>/<sub-path>`` (e.g.
    ``alphapai_pdfs/2025-11/x.pdf``).

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
        # Configured roots didn't match. Look for any root's basename inside
        # the absolute path's parts and rebuild the rel-path from there —
        # tolerates legacy paths that point at a directory the backend no
        # longer surfaces (the GridFS key still uses the platform basename).
        root_names = {r.name for r in roots}
        for i, part in enumerate(p.parts):
            if part in root_names:
                return Path(*p.parts[i:]).as_posix()
        return p.name
    # Relative input: ensure it starts with a known root basename.
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
) -> StreamingResponse | FileResponse:
    """Return a response that serves a PDF.

    Resolution order: local SSD → GridFS → 404. Local disk returns
    ``FileResponse`` (Range-friendly). GridFS streams via motor.

    ``pdf_root`` may be a single path OR a list of paths.
    ``download_filename`` is the user-facing filename. ``download=True`` sets
    ``Content-Disposition: attachment``; otherwise inline.
    """
    roots = _normalize_roots(pdf_root)
    if not roots:
        raise HTTPException(500, "pdf_root misconfigured (empty)")
    gridfs_name = _filename_for_pdf(pdf_rel_path, roots)

    # 1) Local SSD. Build candidate paths grouped per root: absolute paths
    # are tried as-is; relative ones get joined with each configured root.
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
        # Security: resolved target must live under one of the allowed roots.
        owns = any(_is_under(cand, root) for root in roots)
        if not owns:
            tried.append(f"{cand} [outside roots]")
            continue
        if cand.is_file():
            return FileResponse(
                cand,
                media_type=media_type,
                headers=_cd_headers(download_filename, download,
                                    size=cand.stat().st_size),
            )
        tried.append(str(cand))

    # 2) GridFS — only used when the binary isn't on local SSD yet.
    # NOTE: motor's GridFSBucket.find() yields ``GridOut`` objects, NOT dicts.
    bucket = AsyncIOMotorGridFSBucket(db)
    try:
        cursor = bucket.find({"filename": gridfs_name}, limit=1)
        async for fdoc in cursor:
            file_id = fdoc._id
            file_length = getattr(fdoc, "length", None)
            stream = await bucket.open_download_stream(file_id)
            return StreamingResponse(
                _gridfs_iter(stream),
                media_type=media_type,
                headers=_cd_headers(download_filename, download, size=file_length),
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("GridFS lookup failed for %s: %s", gridfs_name, e)

    logger.warning("PDF not on disk or in GridFS: rel=%s tried=%s",
                   pdf_rel_path, tried)
    raise HTTPException(
        404,
        f"PDF not in GridFS and file missing on disk: {Path(pdf_rel_path).name}",
    )


def _is_under(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


async def _gridfs_iter(stream) -> AsyncIterator[bytes]:
    """Stream GridFS chunks straight to the client. No on-disk cache."""
    try:
        while True:
            chunk = await stream.readchunk()
            if not chunk:
                break
            yield chunk
    finally:
        # motor 3.x exposes GridOut.close as a sync method (returns None).
        try:
            close_fn = getattr(stream, "close", None)
            if callable(close_fn):
                res = close_fn()
                if hasattr(res, "__await__"):
                    await res
        except Exception:
            pass


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
