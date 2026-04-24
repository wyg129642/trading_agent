"""Unified PDF retrieval: Mongo GridFS first, local filesystem fallback.

Each crawler platform writes PDFs to its target Mongo DB's GridFS (filename =
relative path under the platform root, e.g. `alphapai_pdfs/2025-11/x.pdf`).
Backend routers call `stream_pdf_or_file()` to serve them to the frontend.

The fallback lets rollback be trivial: if a platform's GridFS is empty (or
remote is down), we still serve from the original `/home/ygwang/crawl_data/
<platform>_pdfs/` directory as before migration.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import AsyncIterator, Optional
from urllib.parse import quote as urlquote

from fastapi import HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorGridFSBucket

logger = logging.getLogger(__name__)


def _filename_for_pdf(rel_path: str, pdf_root: str) -> str:
    """Convert a stored `pdf_local_path` / `pdf_rel_path` into the GridFS
    filename used by migrate_to_remote_mongo.py.

    The migration stores files with filename = relative path starting with
    the platform root basename, e.g. `alphapai_pdfs/2025-11/x.pdf`.

    Input variants we must handle:
      - absolute path: `/home/ygwang/crawl_data/alphapai_pdfs/2025-11/x.pdf`
      - relative to repo: `alphapai_pdfs/2025-11/x.pdf`
      - partial inside root: `2025-11/x.pdf`
    """
    # Strip any leading pdf_root prefix
    root = Path(pdf_root).resolve()
    root_name = root.name  # e.g. "alphapai_pdfs"
    p = Path(rel_path)
    if p.is_absolute():
        try:
            # produce path relative to root's PARENT so it includes root_name
            rel = p.resolve().relative_to(root.parent)
            return rel.as_posix()
        except ValueError:
            return p.name  # shouldn't happen, fallback filename-only
    # Already relative; ensure it starts with root_name
    parts = p.parts
    if parts and parts[0] == root_name:
        return p.as_posix()
    return f"{root_name}/{p.as_posix()}"


async def stream_pdf_or_file(
    *,
    db: AsyncIOMotorDatabase,
    pdf_rel_path: str,
    pdf_root: str,
    download_filename: str,
    download: bool = False,
    media_type: str = "application/pdf",
) -> StreamingResponse | FileResponse:
    """Return a response that streams a PDF.

    1. Query GridFS in `db` for a file whose filename == migration-shape.
    2. On hit: return a StreamingResponse piping from GridFSBucket.
    3. On miss: fall back to FileResponse from local disk (rollback path).

    `download_filename` is the user-facing filename (title.pdf or similar).
    `download=True` sets Content-Disposition: attachment; else inline.
    """
    gridfs_name = _filename_for_pdf(pdf_rel_path, pdf_root)
    bucket = AsyncIOMotorGridFSBucket(db)

    # 1) Try GridFS
    try:
        cursor = bucket.find({"filename": gridfs_name}, limit=1)
        async for fdoc in cursor:
            stream = await bucket.open_download_stream(fdoc["_id"])

            async def iter_chunks() -> AsyncIterator[bytes]:
                try:
                    while True:
                        chunk = await stream.readchunk()
                        if not chunk:
                            break
                        yield chunk
                finally:
                    await stream.close()

            return StreamingResponse(
                iter_chunks(),
                media_type=media_type,
                headers=_cd_headers(download_filename, download, size=fdoc.get("length")),
            )
    except Exception as e:
        logger.warning("GridFS lookup failed for %s: %s (falling back to disk)", gridfs_name, e)

    # 2) Fallback: local disk (pre-migration path)
    # Security: resolve under pdf_root
    root = Path(pdf_root).resolve()
    p = Path(pdf_rel_path)
    if p.is_absolute():
        target = p.resolve()
    else:
        # Try as-is under root, then prefix-strip
        if p.parts and p.parts[0] == root.name:
            target = (root.parent / p).resolve()
        else:
            target = (root / p).resolve()
    try:
        target.relative_to(root)
    except ValueError:
        logger.warning("PDF path escape attempt: %s (root=%s)", pdf_rel_path, pdf_root)
        raise HTTPException(403, "PDF path outside allowed directory")

    if not target.is_file():
        raise HTTPException(404, f"PDF not in GridFS and file missing on disk: {target.name}")

    return FileResponse(
        target,
        media_type=media_type,
        headers=_cd_headers(download_filename, download, size=target.stat().st_size),
    )


def _cd_headers(filename: str, download: bool, size: Optional[int] = None) -> dict:
    """RFC 5987 Content-Disposition (supports non-ASCII filenames via filename*)."""
    # ASCII fallback
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
