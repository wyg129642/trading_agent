"""REST API for the personal knowledge base.

All endpoints are authenticated and scoped to ``get_current_user().id``.
Users cannot see, modify, or search other users' documents — the service
layer enforces this at every call site; the API layer just plumbs the user
id through.

Endpoint surface (mounted under ``/api/user-kb``):

========================================  =========================================
Method  Path                               Purpose
========================================  =========================================
GET     /ping                              Mongo reachability probe
GET     /stats                             Per-user dashboard summary
POST    /documents                         Upload one file
GET     /documents                         List (paginated, filterable)
GET     /documents/{id}                    Metadata detail
GET     /documents/{id}/content            Full extracted text
GET     /documents/{id}/file               Original file binary (download)
PATCH   /documents/{id}                    Update title/description/tags
POST    /documents/{id}/reparse            Retry parsing
DELETE  /documents/{id}                    Delete (cascades chunks + binary)
POST    /search                            Ad-hoc chunk search (same as the chat tool)
========================================  =========================================
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote as urlquote

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.user import User
from backend.app.models.user_preference import UserPreference
from backend.app.services import (
    kb_folder_service as folder_svc,
    kb_skills_service as skill_svc,
    user_kb_asr_client,
    user_kb_service as svc,
    user_kb_workbook,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Response schemas ───────────────────────────────────────────


class AudioSegmentResponse(BaseModel):
    index: int
    start_ms: int
    end_ms: int
    text: str


class AudioChapterResponse(BaseModel):
    index: int
    title: str
    start_ms: int
    end_ms: int
    start_segment_index: int = 0
    bullets: list[str] = Field(default_factory=list)


class AudioMetaResponse(BaseModel):
    duration_seconds: Optional[float] = None
    language: Optional[str] = None
    segments: list[AudioSegmentResponse] = Field(default_factory=list)
    summary: Optional[str] = None
    summary_generated_at: Optional[str] = None
    # The service sets chapters back to ``None`` when the transcript is
    # edited (to invalidate the cache). Accept both an explicit ``None``
    # and a missing field by defaulting to ``[]``.
    chapters: Optional[list[AudioChapterResponse]] = Field(default_factory=list)
    chapters_generated_at: Optional[str] = None


class DocumentResponse(BaseModel):
    id: str
    user_id: str
    title: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    original_filename: str
    file_extension: str = ""
    content_type: str = ""
    file_size_bytes: int = 0
    upload_status: str
    upload_error: Optional[str] = None
    parse_status: str
    parse_error: Optional[str] = None
    parser_backend: Optional[str] = None
    parse_warnings: list[str] = Field(default_factory=list)
    # Live progress reported by the ASR parser (0–100 + free-form phase
    # string like "transcribing 12/40"). Sync parsers leave these at 0/"".
    parse_progress_percent: int = 0
    parse_phase: str = ""
    extracted_char_count: int = 0
    num_chunks: int = 0
    folder_id: Optional[str] = None
    scope: str = "personal"
    # "file" (default) or "spreadsheet" — spreadsheets are edited via the
    # dedicated /spreadsheet endpoint instead of the parser pipeline.
    doc_type: str = "file"
    created_at: str = ""
    updated_at: str = ""
    parse_started_at: Optional[str] = None
    parse_completed_at: Optional[str] = None
    # Present on audio uploads (mp3/m4a/wav/...) once parsing completes.
    # Drives the dedicated audio player + segmented transcript UI.
    audio: Optional[AudioMetaResponse] = None


class AudioSummaryResponse(BaseModel):
    document_id: str
    summary: str
    generated_at: str
    cached: bool


class AudioChaptersResponse(BaseModel):
    document_id: str
    chapters: list[AudioChapterResponse]
    generated_at: str
    cached: bool


class AudioSegmentEditRequest(BaseModel):
    text: str


class SpreadsheetResponse(BaseModel):
    document_id: str
    title: str
    doc_type: str
    spreadsheet_data: dict


class SpreadsheetUpdateRequest(BaseModel):
    spreadsheet_data: dict


class MarkdownResponse(BaseModel):
    document_id: str
    title: str
    content_md: str


class MarkdownUpdateRequest(BaseModel):
    content_md: str


class MarkdownCreateRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=500)
    original_filename: str = Field(..., min_length=1, max_length=255)
    folder_id: Optional[str] = None
    scope: str = Field("personal", description="personal | public")
    content_md: str = ""
    description: str = ""
    tags: list[str] = Field(default_factory=list)


class SkillResponse(BaseModel):
    id: str
    owner_id: Optional[str] = None
    scope: str
    name: str
    description: str = ""
    icon: str
    target_types: list[str]
    slug: Optional[str] = None
    spec: dict
    is_published: bool
    installs_count: int
    created_at: str
    updated_at: str


class SkillCreateRequest(BaseModel):
    scope: str = Field("personal", description="personal | public")
    name: str = Field(..., min_length=1, max_length=255)
    description: str = ""
    icon: str = "ThunderboltOutlined"
    target_types: list[str] = Field(default_factory=lambda: ["stock", "industry", "general"])
    spec: dict = Field(default_factory=dict)


class SkillUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    target_types: Optional[list[str]] = None
    spec: Optional[dict] = None
    is_published: Optional[bool] = None


class SkillInstallResponse(BaseModel):
    skill_id: str
    folder_id: str
    created_folders: list[str]
    created_documents: list[str]
    skipped_existing: int


class DocumentListResponse(BaseModel):
    items: list[DocumentResponse]
    total: int
    limit: int
    offset: int


class DocumentUpdateRequest(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[list[str]] = None
    # folder_id: pass null to move to "unfiled" at root; omit to keep unchanged.
    # Pydantic can't distinguish omitted-from-JSON vs explicit-null without
    # Field(default=...), so we use model_fields_set at the handler.
    folder_id: Optional[str] = None
    scope: Optional[str] = None


class FolderCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    folder_type: str = Field(..., description="stock | industry | general")
    scope: str = Field("personal", description="personal | public")
    parent_id: Optional[str] = None
    stock_ticker: Optional[str] = None
    stock_market: Optional[str] = None
    stock_name: Optional[str] = None


class FolderUpdateRequest(BaseModel):
    name: Optional[str] = None
    parent_id: Optional[str] = None
    order_index: Optional[int] = None


class FolderResponse(BaseModel):
    id: str
    user_id: Optional[str] = None
    scope: str
    parent_id: Optional[str] = None
    name: str
    folder_type: str
    stock_ticker: Optional[str] = None
    stock_market: Optional[str] = None
    stock_name: Optional[str] = None
    order_index: int = 0
    created_at: str = ""
    updated_at: str = ""


class FolderTreeNodeResponse(BaseModel):
    id: str
    user_id: Optional[str] = None
    scope: str
    parent_id: Optional[str] = None
    name: str
    folder_type: str
    stock_ticker: Optional[str] = None
    stock_market: Optional[str] = None
    stock_name: Optional[str] = None
    order_index: int = 0
    created_at: str = ""
    updated_at: str = ""
    document_count: int = 0
    children: list["FolderTreeNodeResponse"] = Field(default_factory=list)


FolderTreeNodeResponse.model_rebuild()


class TreeResponse(BaseModel):
    scope: str
    folders: list[FolderTreeNodeResponse]
    unfiled_count: int = 0
    can_write: bool = True


class UploadResponse(BaseModel):
    document_id: str
    was_duplicate: bool
    # echo a fresh snapshot so the client can render the row immediately
    document: DocumentResponse


class DocumentContentResponse(BaseModel):
    document_id: str
    content: str
    char_count: int
    truncated: bool


class SearchRequest(BaseModel):
    query: str
    top_k: int = 5
    document_ids: Optional[list[str]] = None
    # "all"  — cross-user (team-shared retrieval, default; matches chat tool)
    # "mine" — restrict to the caller's own uploads
    scope: str = "all"


class SearchHitModel(BaseModel):
    document_id: str
    title: str
    original_filename: str
    chunk_index: int
    text: str
    score: float
    created_at: str
    uploader_user_id: str = ""


class SearchResponse(BaseModel):
    query: str
    hits: list[SearchHitModel]


class StatsResponse(BaseModel):
    total_documents: int = 0
    total_bytes: int = 0
    total_chars: int = 0
    total_chunks: int = 0
    status_pending: int = 0
    status_parsing: int = 0
    status_completed: int = 0
    status_failed: int = 0


class PingResponse(BaseModel):
    ok: bool
    message: str


class AsrPingResponse(BaseModel):
    """Rich ASR health snapshot for the personal-KB banner + status pill.

    Fields beyond ``ok``/``message`` let the frontend distinguish a genuine
    outage from a single tunnel blip, and surface live queue depth + GPU
    state so the user can see why a transcription is slow rather than
    staring at a stuck progress bar.
    """
    ok: bool
    message: str
    classification: str  # ok | loading | transient | unreachable | misconfigured
    latency_ms: Optional[int] = None
    model_loaded: Optional[bool] = None
    model_error: Optional[str] = None
    model_path: Optional[str] = None
    gpu: Optional[bool] = None
    gpu_count: Optional[int] = None
    queue_size: Optional[int] = None
    jobs_in_memory: Optional[int] = None


# ── Ping / stats ──────────────────────────────────────────────


@router.get("/ping", response_model=PingResponse)
async def ping(_user: User = Depends(get_current_user)):
    """Quick Mongo reachability check so the UI can show a warning banner."""
    ok, reason = await svc.ping()
    return PingResponse(ok=ok, message=reason)


@router.get("/asr/ping", response_model=AsrPingResponse)
async def asr_ping(_user: User = Depends(get_current_user)):
    """ASR service reachability check via the supervised SSH tunnel.

    The frontend uses this to drive the personal-KB banner + status pill:
    green when everything's healthy (model loaded, GPU present, queue
    drained), amber when the model is still warming up, red only after the
    retry-hardened probe has failed, so a fleeting tunnel blip doesn't
    blank the page for the rest of the session.
    """
    r = await user_kb_asr_client.probe_detailed()
    return AsrPingResponse(
        ok=r.ok,
        message=r.reason,
        classification=r.classification,
        latency_ms=r.latency_ms,
        model_loaded=r.model_loaded,
        model_error=r.model_error,
        model_path=r.model_path,
        gpu=r.gpu,
        gpu_count=r.gpu_count,
        queue_size=r.queue_size,
        jobs_in_memory=r.jobs_in_memory,
    )


@router.get("/stats", response_model=StatsResponse)
async def stats(user: User = Depends(get_current_user)):
    data = await svc.get_user_stats(str(user.id))
    return StatsResponse(**data)


# ── Upload ─────────────────────────────────────────────────────


@router.post("/documents", response_model=UploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    title: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    tags: Optional[str] = Form(None),  # comma-separated for multipart simplicity
    folder_id: Optional[str] = Form(None),
    scope: Optional[str] = Form(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload a single file. Parsing starts in the background; poll
    ``GET /documents/{id}`` for ``parse_status`` progress.

    Optional ``folder_id`` places the upload inside an existing folder. If a
    public folder is chosen, the caller must be admin/boss. When ``folder_id``
    is provided, ``scope`` is inherited from the folder; otherwise it defaults
    to ``personal``.
    """
    filename = (file.filename or "").strip()
    if not filename:
        raise HTTPException(400, "filename is required")

    data = await file.read()
    await file.close()

    tag_list: list[str] | None = None
    if tags:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]

    resolved_scope = svc.SCOPE_PERSONAL
    folder_id_str: str | None = None
    if folder_id:
        try:
            folder = await folder_svc.can_access_folder(db, folder_id, user)
        except folder_svc.FolderNotFound:
            raise HTTPException(404, "folder not found")
        except folder_svc.FolderPermissionDenied as e:
            raise HTTPException(403, str(e))
        resolved_scope = folder.scope
        folder_id_str = str(folder.id)
        # Writing into a public folder requires admin/boss role.
        if resolved_scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
            raise HTTPException(
                403, "only admin/boss can upload to the public knowledge base",
            )
    elif scope:
        if scope not in (folder_svc.SCOPE_PERSONAL, folder_svc.SCOPE_PUBLIC):
            raise HTTPException(400, "scope must be 'personal' or 'public'")
        if scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
            raise HTTPException(
                403, "only admin/boss can upload to the public knowledge base",
            )
        resolved_scope = scope

    logger.info(
        "user_kb upload body-read done user=%s file=%r bytes=%d scope=%s",
        user.id, filename, len(data), resolved_scope,
    )
    try:
        outcome = await svc.create_document(
            user_id=str(user.id),
            original_filename=filename,
            data=data,
            title=title,
            description=description,
            tags=tag_list,
            folder_id=folder_id_str,
            scope=resolved_scope,
        )
    except ValueError as e:
        logger.warning(
            "user_kb upload 400 user=%s file=%r size=%d: %s",
            user.id, filename, len(data), e,
        )
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.exception("user_kb upload failed for %s", user.id)
        raise HTTPException(500, f"upload failed: {e}")

    if not outcome.was_duplicate:
        svc.schedule_parse(outcome.document_id)

    doc = await svc.get_document(str(user.id), outcome.document_id)
    # For public uploads by non-uploaders we fall back to the permissive read.
    if doc is None:
        doc = await svc.get_accessible_document(str(user.id), outcome.document_id)
    if doc is None:
        # Shouldn't happen — we just wrote it.
        raise HTTPException(500, "document disappeared after insert")

    return UploadResponse(
        document_id=outcome.document_id,
        was_duplicate=outcome.was_duplicate,
        document=DocumentResponse(**doc),
    )


# ── Listing / detail ──────────────────────────────────────────


@router.get("/documents", response_model=DocumentListResponse)
async def list_documents(
    status: Optional[str] = Query(None, description="Filter by parse_status"),
    search: Optional[str] = Query(None, description="Substring on title/filename"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    scope: str = Query("personal", description="personal | public"),
    folder_id: Optional[str] = Query(
        None, description="Restrict to one folder",
    ),
    unfiled: bool = Query(
        False, description="Only docs without a folder (only if folder_id omitted)",
    ),
    user: User = Depends(get_current_user),
):
    if scope not in (svc.SCOPE_PERSONAL, svc.SCOPE_PUBLIC):
        raise HTTPException(400, "scope must be 'personal' or 'public'")
    items, total = await svc.list_documents(
        str(user.id),
        status=status,
        search=search,
        limit=limit,
        offset=offset,
        scope=scope,
        folder_id=folder_id,
        include_unfiled=unfiled and folder_id is None,
    )
    # Strip the per-segment transcript from list responses — a 3-hour
    # meeting has ~180 segments ≈ 100 KB, so 20 rows would be ~2 MB of
    # bandwidth nobody reads until the drawer opens. Detail endpoint
    # returns the full payload.
    trimmed = [_strip_audio_segments(i) for i in items]
    return DocumentListResponse(
        items=[DocumentResponse(**i) for i in trimmed],
        total=total,
        limit=limit,
        offset=offset,
    )


def _strip_audio_segments(doc: dict) -> dict:
    audio = doc.get("audio")
    if not isinstance(audio, dict):
        return doc
    out = {**doc, "audio": {**audio, "segments": []}}
    return out


@router.get("/documents/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: str,
    user: User = Depends(get_current_user),
):
    # Users may read their own personal docs AND any public doc.
    doc = await svc.get_accessible_document(str(user.id), document_id)
    if doc is None:
        raise HTTPException(404, "document not found")
    return DocumentResponse(**doc)


@router.get("/documents/{document_id}/content", response_model=DocumentContentResponse)
async def get_document_content(
    document_id: str,
    max_chars: int = Query(50_000, ge=100, le=500_000),
    user: User = Depends(get_current_user),
):
    doc = await svc.get_accessible_document(str(user.id), document_id)
    if doc is None:
        raise HTTPException(404, "document not found")
    content = await svc.get_accessible_document_content(
        str(user.id), document_id, max_chars=max_chars,
    ) or ""
    total = int(doc.get("extracted_char_count") or len(content))
    return DocumentContentResponse(
        document_id=document_id,
        content=content,
        char_count=total,
        truncated=len(content) < total,
    )


@router.get("/documents/{document_id}/file")
async def download_document_file(
    document_id: str,
    inline: bool = Query(
        False,
        description=(
            "When true, serve with Content-Disposition: inline so the "
            "browser renders the file directly (used by the audio player)."
        ),
    ),
    user: User = Depends(get_current_user),
):
    """Stream back the exact bytes the user uploaded."""
    # First check read access (own personal OR public).
    accessible = await svc.get_accessible_document(str(user.id), document_id)
    if accessible is None:
        raise HTTPException(404, "document not found")
    # Fetch bytes via the uploader's user_id (which we just confirmed the
    # caller has read access to; GridFS is shared anyway).
    owner_uid = accessible.get("user_id") or str(user.id)
    result = await svc.download_file(owner_uid, document_id)
    if result is None:
        raise HTTPException(404, "document not found")
    meta, data = result
    filename = meta.get("original_filename") or f"{document_id}.bin"
    content_type = meta.get("content_type") or "application/octet-stream"
    disposition = "inline" if inline else "attachment"
    # Use RFC 5987 filename* so non-ASCII filenames survive transit.
    headers = {
        "Content-Disposition": (
            f"{disposition}; filename=\"{_ascii_fallback(filename)}\"; "
            f"filename*=UTF-8''{urlquote(filename)}"
        ),
        # Let the browser's media player seek — required for
        # <audio> scrubbing on larger files.
        "Accept-Ranges": "bytes",
    }
    return Response(content=data, media_type=content_type, headers=headers)


def _ascii_fallback(name: str) -> str:
    return "".join(c if 0x20 <= ord(c) < 0x7f and c != '"' else '_' for c in name) or "file"


@router.post(
    "/documents/{document_id}/audio-summary",
    response_model=AudioSummaryResponse,
)
async def generate_audio_summary_endpoint(
    document_id: str,
    force: bool = Query(
        False,
        description="Re-generate even if a cached summary exists.",
    ),
    user: User = Depends(get_current_user),
):
    """Generate (or return cached) AI summary for an audio transcript.

    On first call we run the summarizer LLM and persist the result; repeat
    calls are O(ms) reads unless ``force=true``.
    """
    doc = await svc.get_accessible_document(str(user.id), document_id)
    if doc is None:
        raise HTTPException(404, "document not found")
    if doc.get("parse_status") != "completed":
        raise HTTPException(409, "document has not finished parsing")
    ext = (doc.get("file_extension") or "").lower()
    # Accept either new-style audio docs (with `audio.segments`) or legacy
    # docs whose only marker is the file extension — both can be summarized
    # from their reassembled extracted text.
    audio = doc.get("audio") if isinstance(doc.get("audio"), dict) else {}
    has_audio_marker = bool(audio.get("segments")) or ext in {
        "mp3", "wav", "m4a", "flac", "ogg", "opus", "webm", "aac",
    }
    if not has_audio_marker:
        raise HTTPException(400, "document is not an audio transcript")

    try:
        outcome = await svc.generate_audio_summary(
            str(user.id), document_id, force=force,
        )
    except RuntimeError as e:
        raise HTTPException(502, str(e)) from e
    if outcome is None:
        raise HTTPException(404, "document not found")
    summary, cached = outcome

    # Re-read to pick up the persisted timestamp (authoritative) — cheap.
    refreshed = await svc.get_accessible_document(str(user.id), document_id)
    generated_at = ""
    if refreshed:
        a = refreshed.get("audio") or {}
        generated_at = str(a.get("summary_generated_at") or "")
    return AudioSummaryResponse(
        document_id=document_id,
        summary=summary,
        generated_at=generated_at,
        cached=cached,
    )


@router.post(
    "/documents/{document_id}/audio-chapters",
    response_model=AudioChaptersResponse,
)
async def generate_audio_chapters_endpoint(
    document_id: str,
    force: bool = Query(
        False,
        description="Re-generate even if cached chapters exist.",
    ),
    user: User = Depends(get_current_user),
):
    """Generate (or return cached) per-chapter segmented summary.

    Splits the transcript into 3–8 semantically coherent chapters and
    summarizes each one with a title + 2–5 bullets + seek anchor.
    """
    doc = await svc.get_accessible_document(str(user.id), document_id)
    if doc is None:
        raise HTTPException(404, "document not found")
    if doc.get("parse_status") != "completed":
        raise HTTPException(409, "document has not finished parsing")
    ext = (doc.get("file_extension") or "").lower()
    audio = doc.get("audio") if isinstance(doc.get("audio"), dict) else {}
    has_audio_marker = bool(audio.get("segments")) or ext in {
        "mp3", "wav", "m4a", "flac", "ogg", "opus", "webm", "aac",
    }
    if not has_audio_marker:
        raise HTTPException(400, "document is not an audio transcript")

    try:
        outcome = await svc.generate_audio_chapter_summary(
            str(user.id), document_id, force=force,
        )
    except RuntimeError as e:
        raise HTTPException(502, str(e)) from e
    if outcome is None:
        raise HTTPException(
            400, "document has no transcript segments to summarize",
        )
    chapters, cached = outcome

    refreshed = await svc.get_accessible_document(str(user.id), document_id)
    generated_at = ""
    if refreshed:
        a = refreshed.get("audio") or {}
        generated_at = str(a.get("chapters_generated_at") or "")
    return AudioChaptersResponse(
        document_id=document_id,
        chapters=[AudioChapterResponse(**c) for c in chapters],
        generated_at=generated_at,
        cached=cached,
    )


@router.patch(
    "/documents/{document_id}/audio-segments/{segment_index}",
    response_model=AudioMetaResponse,
)
async def update_audio_segment(
    document_id: str,
    segment_index: int,
    body: AudioSegmentEditRequest,
    user: User = Depends(get_current_user),
):
    """Edit a single transcript segment's text (ASR correction).

    Invalidates the cached summary + chapter-summary because the underlying
    text changed — the next fetch will regenerate.
    """
    doc = await svc.get_accessible_document(str(user.id), document_id)
    if doc is None:
        raise HTTPException(404, "document not found")
    if doc.get("user_id") != str(user.id):
        raise HTTPException(403, "only the uploader can edit transcript text")
    updated = await svc.update_audio_segment_text(
        str(user.id), document_id, segment_index, body.text,
    )
    if updated is None:
        raise HTTPException(404, "segment not found")
    return AudioMetaResponse(**updated)


# ── Update / delete / reparse ─────────────────────────────────


@router.patch("/documents/{document_id}", response_model=DocumentResponse)
async def update_document(
    document_id: str,
    body: DocumentUpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Validate folder_id (if given) and scope transitions before touching Mongo.
    fields_set = body.model_fields_set
    folder_id_arg: Optional[str] | type(...) = ...
    if "folder_id" in fields_set:
        folder_id_arg = body.folder_id
        if folder_id_arg:
            try:
                folder = await folder_svc.can_access_folder(
                    db, folder_id_arg, user,
                )
            except folder_svc.FolderNotFound:
                raise HTTPException(404, "folder not found")
            except folder_svc.FolderPermissionDenied as e:
                raise HTTPException(403, str(e))
            # Writing into public requires admin/boss.
            if folder.scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
                raise HTTPException(
                    403, "only admin/boss can place docs in public folders",
                )

    if body.scope is not None:
        if body.scope not in (folder_svc.SCOPE_PERSONAL, folder_svc.SCOPE_PUBLIC):
            raise HTTPException(400, "scope must be 'personal' or 'public'")
        if body.scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
            raise HTTPException(
                403, "only admin/boss can publish docs",
            )

    # Check the caller is allowed to edit this doc:
    # - own personal/public doc (uploader) → always.
    # - someone else's public doc → only admin/boss.
    target = await svc.get_accessible_document(str(user.id), document_id)
    if target is None:
        raise HTTPException(404, "document not found")
    is_owner = str(target.get("user_id") or "") == str(user.id)
    is_public = target.get("scope") == folder_svc.SCOPE_PUBLIC
    try:
        if is_owner:
            updated = await svc.update_document(
                str(user.id),
                document_id,
                title=body.title,
                description=body.description,
                tags=body.tags,
                folder_id=folder_id_arg,
                scope=body.scope,
            )
        elif is_public and folder_svc._can_write_public(user):
            updated = await svc.update_public_document(
                document_id,
                title=body.title,
                description=body.description,
                tags=body.tags,
                folder_id=folder_id_arg,
                scope=body.scope,
            )
        else:
            raise HTTPException(403, "you cannot modify this document")
    except ValueError as e:
        raise HTTPException(400, str(e))
    if updated is None:
        raise HTTPException(404, "document not found")
    return DocumentResponse(**updated)


@router.delete("/documents/{document_id}")
async def delete_document(
    document_id: str,
    user: User = Depends(get_current_user),
):
    target = await svc.get_accessible_document(str(user.id), document_id)
    if target is None:
        raise HTTPException(404, "document not found")
    is_owner = str(target.get("user_id") or "") == str(user.id)
    is_public = target.get("scope") == folder_svc.SCOPE_PUBLIC
    if is_owner:
        ok = await svc.delete_document(str(user.id), document_id)
    elif is_public and folder_svc._can_write_public(user):
        ok = await svc.delete_public_document(document_id)
    else:
        raise HTTPException(403, "you cannot delete this document")
    if not ok:
        raise HTTPException(404, "document not found")
    return {"ok": True}


# ── Spreadsheets (valuation tables) ────────────────────────────


_SPREADSHEET_KINDS = {svc.DOC_TYPE_SPREADSHEET, svc.DOC_TYPE_WORKBOOK}


@router.get("/documents/{document_id}/spreadsheet", response_model=SpreadsheetResponse)
async def get_spreadsheet(
    document_id: str,
    user: User = Depends(get_current_user),
):
    data = await svc.get_spreadsheet_data(str(user.id), document_id)
    if data is None:
        raise HTTPException(404, "spreadsheet not found")
    # Accept both the legacy ``spreadsheet`` doc_type and the new
    # ``workbook`` doc_type — the read path normalizes either into the
    # multi-sheet workbook shape the frontend consumes.
    if data.get("doc_type") not in _SPREADSHEET_KINDS:
        raise HTTPException(400, "document is not a spreadsheet")
    return SpreadsheetResponse(**data)


@router.patch("/documents/{document_id}/spreadsheet", response_model=SpreadsheetResponse)
async def update_spreadsheet(
    document_id: str,
    body: SpreadsheetUpdateRequest,
    user: User = Depends(get_current_user),
):
    # Read access check doubles as existence check + scope check.
    doc = await svc.get_accessible_document(str(user.id), document_id)
    if doc is None:
        raise HTTPException(404, "spreadsheet not found")
    if doc.get("doc_type") not in _SPREADSHEET_KINDS:
        raise HTTPException(400, "document is not a spreadsheet")
    allow_public = doc.get("scope") == folder_svc.SCOPE_PUBLIC and folder_svc._can_write_public(user)
    try:
        ok = await svc.update_spreadsheet_data(
            str(user.id), document_id, body.spreadsheet_data,
            allow_public_admin=allow_public,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    if not ok:
        raise HTTPException(403, "you cannot edit this spreadsheet")
    data = await svc.get_spreadsheet_data(str(user.id), document_id)
    return SpreadsheetResponse(**data)  # type: ignore[arg-type]


@router.get("/folders/{folder_id}/default-spreadsheet", response_model=SpreadsheetResponse)
async def get_folder_default_spreadsheet(
    folder_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return (and lazily create) the default 估值表 for a stock folder.

    If the folder is a stock folder that was created before the
    auto-seed-on-create logic existed (or whose seed was dropped by a
    transient error), this endpoint seeds it on first access so the user
    always sees an editable sheet.
    """
    try:
        folder = await folder_svc.can_access_folder(db, folder_id, user)
    except folder_svc.FolderNotFound:
        raise HTTPException(404, "folder not found")
    except folder_svc.FolderPermissionDenied as e:
        raise HTTPException(403, str(e))
    if folder.folder_type != folder_svc.FOLDER_STOCK:
        raise HTTPException(400, "only stock folders have a default spreadsheet")

    doc_id = await svc.find_default_spreadsheet_in_folder(
        str(user.id), folder.scope, str(folder.id),
    )
    if doc_id is None:
        # Seed on first access if the folder somehow lacks one.
        if folder.scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
            raise HTTPException(
                404, "no default spreadsheet (ask an admin to create one)",
            )
        stock_name = folder.stock_name or folder.name
        title = f"{stock_name} 估值表" if stock_name else "估值表"
        try:
            doc_id = await svc.create_spreadsheet_document(
                user_id=str(user.id),
                original_filename=f"{folder.stock_ticker or 'stock'}-valuation.sheet",
                title=title,
                folder_id=str(folder.id),
                scope=folder.scope,
                description=f"{stock_name} ({folder.stock_ticker}) 估值表",
            )
        except Exception as e:
            logger.exception("failed to seed spreadsheet on-demand: %s", e)
            raise HTTPException(500, f"could not seed spreadsheet: {e}")

    data = await svc.get_spreadsheet_data(str(user.id), doc_id)
    if data is None:
        raise HTTPException(500, "spreadsheet disappeared after insert")
    return SpreadsheetResponse(**data)


@router.get("/documents/{document_id}/markdown", response_model=MarkdownResponse)
async def get_markdown_document(
    document_id: str,
    user: User = Depends(get_current_user),
):
    data = await svc.get_markdown_content(str(user.id), document_id)
    if data is None:
        raise HTTPException(404, "markdown document not found")
    return MarkdownResponse(**data)


@router.patch("/documents/{document_id}/markdown", response_model=MarkdownResponse)
async def update_markdown_document_endpoint(
    document_id: str,
    body: MarkdownUpdateRequest,
    user: User = Depends(get_current_user),
):
    doc = await svc.get_accessible_document(str(user.id), document_id)
    if doc is None:
        raise HTTPException(404, "document not found")
    if doc.get("doc_type") != svc.DOC_TYPE_MARKDOWN:
        raise HTTPException(400, "document is not markdown")
    is_owner = str(doc.get("user_id") or "") == str(user.id)
    is_public = doc.get("scope") == folder_svc.SCOPE_PUBLIC
    allow_public = is_public and folder_svc._can_write_public(user)
    if not (is_owner or allow_public):
        raise HTTPException(403, "you cannot edit this document")
    try:
        updated = await svc.update_markdown_document(
            str(user.id), document_id, body.content_md,
            allow_public_admin=allow_public,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    if updated is None:
        raise HTTPException(404, "markdown document not found")
    data = await svc.get_markdown_content(str(user.id), document_id)
    if data is None:
        # Shouldn't happen but return a sensible default.
        data = {"document_id": document_id, "title": "", "content_md": body.content_md}
    return MarkdownResponse(**data)


@router.post("/documents/markdown", response_model=DocumentResponse)
async def create_markdown_document_endpoint(
    body: MarkdownCreateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a plain markdown document. Content lives inline — no upload step."""
    resolved_scope = body.scope if body.scope in (svc.SCOPE_PERSONAL, svc.SCOPE_PUBLIC) else svc.SCOPE_PERSONAL
    if body.folder_id:
        try:
            folder = await folder_svc.can_access_folder(db, body.folder_id, user)
        except folder_svc.FolderNotFound:
            raise HTTPException(404, "folder not found")
        except folder_svc.FolderPermissionDenied as e:
            raise HTTPException(403, str(e))
        resolved_scope = folder.scope
        if resolved_scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
            raise HTTPException(403, "only admin/boss can write to public folders")
    elif resolved_scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
        raise HTTPException(403, "only admin/boss can create public documents")

    try:
        doc_id = await svc.create_markdown_document(
            user_id=str(user.id),
            original_filename=body.original_filename,
            title=body.title,
            folder_id=body.folder_id,
            scope=resolved_scope,
            content_md=body.content_md,
            description=body.description,
            tags=body.tags,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    doc = await svc.get_accessible_document(str(user.id), doc_id)
    if doc is None:
        raise HTTPException(500, "document disappeared after insert")
    return DocumentResponse(**doc)


# ── Skills ─────────────────────────────────────────────────────────


@router.get("/skills", response_model=list[SkillResponse])
async def list_skills_endpoint(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    skills = await skill_svc.list_skills(db, user)
    return [SkillResponse(**skill_svc.serialize(s)) for s in skills]


@router.get("/skills/{skill_id}", response_model=SkillResponse)
async def get_skill_endpoint(
    skill_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        skill = await skill_svc.get_skill(db, user, skill_id)
    except skill_svc.SkillNotFound as e:
        raise HTTPException(404, str(e))
    except skill_svc.SkillPermissionDenied as e:
        raise HTTPException(403, str(e))
    return SkillResponse(**skill_svc.serialize(skill))


@router.post("/skills", response_model=SkillResponse)
async def create_skill_endpoint(
    body: SkillCreateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        skill = await skill_svc.create_skill(
            db, user,
            scope=body.scope,
            name=body.name,
            description=body.description,
            icon=body.icon,
            target_types=",".join(body.target_types),
            spec=body.spec,
        )
    except skill_svc.SkillPermissionDenied as e:
        raise HTTPException(403, str(e))
    except skill_svc.SkillValidationError as e:
        raise HTTPException(400, str(e))
    return SkillResponse(**skill_svc.serialize(skill))


@router.patch("/skills/{skill_id}", response_model=SkillResponse)
async def update_skill_endpoint(
    skill_id: str,
    body: SkillUpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        skill = await skill_svc.update_skill(
            db, user, skill_id,
            name=body.name,
            description=body.description,
            icon=body.icon,
            target_types=(",".join(body.target_types)
                          if body.target_types is not None else None),
            spec=body.spec,
            is_published=body.is_published,
        )
    except skill_svc.SkillNotFound as e:
        raise HTTPException(404, str(e))
    except skill_svc.SkillPermissionDenied as e:
        raise HTTPException(403, str(e))
    except skill_svc.SkillValidationError as e:
        raise HTTPException(400, str(e))
    return SkillResponse(**skill_svc.serialize(skill))


@router.delete("/skills/{skill_id}")
async def delete_skill_endpoint(
    skill_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        await skill_svc.delete_skill(db, user, skill_id)
    except skill_svc.SkillNotFound as e:
        raise HTTPException(404, str(e))
    except skill_svc.SkillPermissionDenied as e:
        raise HTTPException(403, str(e))
    return {"ok": True}


@router.post("/folders/{folder_id}/install-skill/{skill_id}",
             response_model=SkillInstallResponse)
async def install_skill_endpoint(
    folder_id: str,
    skill_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Materialize a skill into the given folder (idempotent on filename clashes)."""
    try:
        summary = await skill_svc.install_skill(db, user, skill_id, folder_id)
    except skill_svc.SkillNotFound as e:
        raise HTTPException(404, str(e))
    except skill_svc.SkillPermissionDenied as e:
        raise HTTPException(403, str(e))
    except skill_svc.SkillValidationError as e:
        raise HTTPException(400, str(e))
    return SkillInstallResponse(**summary)


@router.post("/documents/{document_id}/reparse", response_model=DocumentResponse)
async def reparse_document(
    document_id: str,
    user: User = Depends(get_current_user),
):
    # Only the uploader (or an admin/boss for public) can reparse.
    target = await svc.get_accessible_document(str(user.id), document_id)
    if target is None:
        raise HTTPException(404, "document not found")
    is_owner = str(target.get("user_id") or "") == str(user.id)
    is_public = target.get("scope") == folder_svc.SCOPE_PUBLIC
    if not (is_owner or (is_public and folder_svc._can_write_public(user))):
        raise HTTPException(403, "you cannot reparse this document")
    # The service filters by user_id — use the uploader's id for the update.
    owner_uid = str(target.get("user_id") or user.id)
    ok = await svc.reparse_document(owner_uid, document_id)
    if not ok:
        raise HTTPException(404, "document not found")
    doc = await svc.get_accessible_document(str(user.id), document_id)
    return DocumentResponse(**doc)  # type: ignore[arg-type]


# ── Search ─────────────────────────────────────────────────────


@router.post("/search", response_model=SearchResponse)
async def search(
    body: SearchRequest,
    user: User = Depends(get_current_user),
):
    """Ad-hoc text search.

    ``scope="all"`` (default) mirrors the chat-tool behavior: team-wide
    shared retrieval. ``scope="mine"`` restricts to the caller's own
    uploads, useful when a user wants to revisit only their contributions.
    """
    scoped_user_id: str | None = None
    if body.scope == "mine":
        scoped_user_id = str(user.id)
    elif body.scope != "all":
        raise HTTPException(400, "scope must be 'all' or 'mine'")
    hits = await svc.search_chunks(
        body.query,
        user_id=scoped_user_id,
        top_k=body.top_k,
        document_ids=body.document_ids,
    )
    return SearchResponse(
        query=body.query,
        hits=[SearchHitModel(**h.__dict__) for h in hits],
    )


# ── Folders ──────────────────────────────────────────────────────


def _folder_to_response(f) -> FolderResponse:
    d = folder_svc.serialize(f)
    return FolderResponse(**d)


@router.get("/folders", response_model=list[FolderResponse])
async def list_folders_endpoint(
    scope: str = Query("personal", description="personal | public"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Flat listing of all folders visible in this scope."""
    try:
        folders = await folder_svc.list_folders(db, user, scope)
    except folder_svc.FolderValidationError as e:
        raise HTTPException(400, str(e))
    return [_folder_to_response(f) for f in folders]


@router.post("/folders", response_model=FolderResponse)
async def create_folder_endpoint(
    body: FolderCreateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        folder = await folder_svc.create_folder(
            db, user,
            scope=body.scope,
            name=body.name,
            folder_type=body.folder_type,
            parent_id=body.parent_id,
            stock_ticker=body.stock_ticker,
            stock_market=body.stock_market,
            stock_name=body.stock_name,
        )
    except folder_svc.FolderPermissionDenied as e:
        raise HTTPException(403, str(e))
    except folder_svc.FolderNotFound as e:
        raise HTTPException(404, str(e))
    except folder_svc.FolderValidationError as e:
        raise HTTPException(400, str(e))
    await db.commit()

    # Stock folders auto-get a default 估值表 (valuation spreadsheet).
    # We do this after the folder commit so a transient Mongo hiccup can't
    # roll back the Postgres row — the user can still add one manually later.
    if folder.folder_type == folder_svc.FOLDER_STOCK:
        try:
            ticker = folder.stock_ticker or ""
            stock_name = folder.stock_name or folder.name
            title = f"{stock_name} 估值表" if stock_name else "估值表"
            await svc.create_spreadsheet_document(
                user_id=str(user.id),
                original_filename=f"{ticker}-valuation.sheet" if ticker else "valuation.sheet",
                title=title,
                folder_id=str(folder.id),
                scope=folder.scope,
                description=f"{stock_name} ({ticker}) 默认估值表，支持公式与模型编辑",
            )
        except Exception as e:
            # Never fail the folder create if spreadsheet seeding fails —
            # log and let the caller add one manually.
            logger.warning(
                "failed to seed default spreadsheet for stock folder %s: %s",
                folder.id, e,
            )
    return _folder_to_response(folder)


@router.patch("/folders/{folder_id}", response_model=FolderResponse)
async def update_folder_endpoint(
    folder_id: str,
    body: FolderUpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    fields_set = body.model_fields_set
    try:
        folder = await folder_svc.update_folder(
            db, user, folder_id,
            name=body.name,
            parent_id=body.parent_id if "parent_id" in fields_set else ...,
            order_index=body.order_index,
        )
    except folder_svc.FolderPermissionDenied as e:
        raise HTTPException(403, str(e))
    except folder_svc.FolderNotFound as e:
        raise HTTPException(404, str(e))
    except folder_svc.FolderValidationError as e:
        raise HTTPException(400, str(e))
    await db.commit()
    return _folder_to_response(folder)


# ── Bulk import portfolio holdings as stock folders ─────────────


class ImportHoldingsResponse(BaseModel):
    parent_folder_id: str
    created: int
    skipped_existing: int
    failed: int
    total_holdings: int


_HOLDINGS_PARENT_NAME = "持仓股票"

# Subfolders each stock folder gets when the workspace sub-taxonomy is
# seeded. Kept to a single minimal bucket (专家访谈) on user request;
# anything else (研报 / 公司公告 / ...) can be added manually per folder.
# The seeder is idempotent and respects user-deleted folders, so shrinking
# this list does NOT resurrect any stocks' existing subfolders.
_STOCK_SUBFOLDERS: list[str] = [
    "专家访谈",
]

# Subfolders earlier versions of the seeder created but we no longer want.
# The per-tree cleanup sweep removes these if and only if they are empty
# (no documents, no user-created children) — so users with data in e.g. a
# 研报 subfolder keep that folder; users who never touched them get a
# cleaner workspace on the next fetch.
_STOCK_LEGACY_AUTO_SUBFOLDERS: frozenset[str] = frozenset({
    "研报", "公司公告", "公司交流", "调研", "轮播", "模型", "其他",
})

_STOCK_KEYDRIVER_TEMPLATE = (
    "# {stock_name} 关键驱动\n\n"
    "## 核心假设\n- \n\n"
    "## 风险因素\n- \n\n"
    "## 一句话观点\n> \n"
)

_STOCK_NOTES_TEMPLATE = (
    "# {stock_name} 研究笔记\n\n"
    "> 股票代码：{ticker}\n"
    "> 最近更新：{today}\n\n"
    "## {today}\n\n"
)

# Map raw portfolio market labels to the coarser grouping we present in the
# workspace. A-share exchanges (主板/创业板/科创板) are rolled up to "A股"
# to match the 持仓概览 market filter. Unknown labels fall through to "其他"
# so the sync never creates an empty / unlabeled market folder.
_MARKET_GROUPS: dict[str, str] = {
    "美股": "美股",
    "港股": "港股",
    "主板": "A股",
    "创业板": "A股",
    "科创板": "A股",
    "北交所": "A股",
    "A股": "A股",
    "韩股": "韩股",
    "日股": "日股",
    "澳股": "澳股",
}


def _normalize_market(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return "其他"
    return _MARKET_GROUPS.get(raw, raw)


@dataclass
class _HoldingsSyncResult:
    parent_folder_id: str
    created_markets: int
    created_stocks: int
    migrated_stocks: int    # moved from flat root into their market subfolder
    removed_stocks: int
    removed_markets: int
    preserved_stocks: int   # kept despite being delisted because user has data
    total_holdings: int


async def _cleanup_legacy_stock_subfolders(
    user: User, db: AsyncSession, stock_folder,
    *, existing_folders: list[Any] | None = None,
) -> int:
    """Remove empty auto-seeded legacy subfolders (研报 / 公司公告 / ...) from
    a stock folder.

    "Empty" = no documents AND no user-created subfolders. Non-empty folders
    are preserved so a user who manually added research doesn't lose it.
    Returns the number of folders removed. Never raises; logs on failure.
    """
    removed = 0
    try:
        if existing_folders is None:
            existing_folders = await folder_svc.list_folders(
                db, user, stock_folder.scope,
            )
        kids = [
            f for f in existing_folders
            if str(f.parent_id) == str(stock_folder.id)
            and f.name in _STOCK_LEGACY_AUTO_SUBFOLDERS
        ]
        if not kids:
            return 0
        # Count descendants + docs in one pass.
        id_to_children: dict[str, int] = {}
        for f in existing_folders:
            pid = str(f.parent_id) if f.parent_id else ""
            id_to_children[pid] = id_to_children.get(pid, 0) + 1
        doc_counts = await svc.get_folder_document_counts(
            str(user.id), stock_folder.scope,
        )
        for f in kids:
            if id_to_children.get(str(f.id), 0) > 0:
                continue  # has subfolders — user data, preserve
            if doc_counts.get(str(f.id), 0) > 0:
                continue  # has documents — preserve
            try:
                deleted_ids = await folder_svc.delete_folder(
                    db, user, str(f.id),
                )
                await db.commit()
                # Empty folder cascade is a no-op for docs, but run it in
                # case of races (new doc inserted between the count and
                # the delete).
                await svc.delete_documents_by_folder_ids(
                    [str(x) for x in deleted_ids],
                )
                removed += 1
            except Exception as e:
                logger.info(
                    "subtaxonomy cleanup: skipped removing '%s' in %s: %s",
                    f.name, stock_folder.name, e,
                )
    except Exception as e:
        logger.warning(
            "subtaxonomy cleanup failed for %s: %s", stock_folder.name, e,
        )
    return removed


async def _ensure_stock_subtaxonomy(
    user: User, db: AsyncSession,
    stock_folder,
    *, existing_folders: list[Any] | None = None,
) -> tuple[int, int]:
    """Idempotently ensure a stock folder has its standard subfolders +
    key-driver.md + notes.md.

    Returns ``(folders_created, files_created)``. Never raises — failures
    are logged and swallowed so a transient Mongo hiccup can't break the
    caller's bigger reconciliation pass.
    """
    created_folders = 0
    created_files = 0
    stock_name = stock_folder.stock_name or stock_folder.name
    ticker = stock_folder.stock_ticker or ""
    try:
        # Cache existing children. If the caller gave us the full folder list,
        # use it to avoid a round trip; otherwise re-query.
        if existing_folders is None:
            existing_folders = await folder_svc.list_folders(
                db, user, stock_folder.scope,
            )
        kids_by_name: dict[str, Any] = {
            f.name: f for f in existing_folders
            if str(f.parent_id) == str(stock_folder.id)
        }
        # 1. Subfolders
        for sub_name in _STOCK_SUBFOLDERS:
            if sub_name in kids_by_name:
                continue
            try:
                await folder_svc.create_folder(
                    db, user,
                    scope=stock_folder.scope,
                    name=sub_name,
                    folder_type=folder_svc.FOLDER_GENERAL,
                    parent_id=str(stock_folder.id),
                )
                await db.commit()
                created_folders += 1
            except Exception as e:
                logger.info(
                    "subtaxonomy: skipped subfolder '%s' under %s: %s",
                    sub_name, stock_name, e,
                )

        # 2. Markdown seeds — only create if a doc with the same filename
        #    doesn't already exist in this folder. This lets us run the seed
        #    multiple times safely after a user edits/deletes/renames.
        target_files = {
            "key-driver.md": _STOCK_KEYDRIVER_TEMPLATE.format(
                stock_name=stock_name,
            ),
            "notes.md": _STOCK_NOTES_TEMPLATE.format(
                stock_name=stock_name,
                ticker=ticker,
                today=datetime.now(timezone.utc).date().isoformat(),
            ),
        }
        # Short-circuit: do one aggregation to find existing names instead
        # of calling Mongo for each file.
        existing = set()
        try:
            match = svc._scope_match_filter(
                str(user.id), stock_folder.scope,
            )
            match["folder_id"] = str(stock_folder.id)
            cursor = svc._docs().find(
                match, {"original_filename": 1},
            )
            async for row in cursor:
                existing.add(str(row.get("original_filename") or ""))
        except Exception as e:
            logger.info(
                "subtaxonomy: skipping seed-existence check for %s: %s",
                stock_name, e,
            )
        for fname, content in target_files.items():
            if fname in existing:
                continue
            try:
                await svc.create_markdown_document(
                    user_id=str(user.id),
                    original_filename=fname,
                    title=f"{stock_name} · {fname}",
                    folder_id=str(stock_folder.id),
                    scope=stock_folder.scope,
                    content_md=content,
                    description="由工作台模板自动生成，可直接编辑",
                )
                created_files += 1
            except Exception as e:
                logger.info(
                    "subtaxonomy: skipped seed %s for %s: %s",
                    fname, stock_name, e,
                )
    except Exception as e:
        logger.warning(
            "subtaxonomy seed for %s failed globally: %s", stock_name, e,
        )
    return created_folders, created_files


async def _perform_holdings_sync(
    user: User, db: AsyncSession,
    *, scope: str = "personal",
    parent_name: str = _HOLDINGS_PARENT_NAME,
    allow_create_parent: bool = True,
    seed_subtaxonomy: bool = True,
) -> _HoldingsSyncResult | None:
    """Reconcile the `持仓股票` tree with ``config/portfolio_sources.yaml``.

    Structure written:

        持仓股票 (general)
          ├─ 美股 (general)
          │    ├─ 英特尔 (stock, INTC, sheet)
          │    └─ ...
          ├─ 港股 (general)
          ├─ A股 (general)            ← 主板/创业板/科创板 rolled up here
          └─ ...

    Two-way reconciliation:

    * **Add** — a ticker in the portfolio that has no stock folder anywhere
      under the parent → create the market subfolder (if absent) and the
      stock folder, seed its default 估值表.
    * **Remove** — a stock folder whose ticker is no longer in the portfolio
      is deleted ONLY when it's "clean": it has exactly its auto-seeded
      default spreadsheet (at most) and no user subfolders. Otherwise we
      preserve the folder — the user may have uploaded research into it
      since the stock was added.
    * Market subfolders that end up empty after the stock cleanup are also
      deleted so the tree doesn't accumulate orphan buckets.

    Returns ``None`` iff the caller shouldn't sync — specifically, the
    ``持仓股票`` parent folder is missing and ``allow_create_parent`` is
    False (i.e. the user explicitly deleted it and we're respecting that).

    Idempotent: running twice with no portfolio changes is a no-op (one
    folder-list query).
    """
    # Load + dedup holdings.
    from backend.app.api.sources import _load_portfolio_yaml
    raw_holdings = _load_portfolio_yaml()
    by_ticker: dict[str, dict] = {}
    for s in raw_holdings:
        tk = (s.get("stock_ticker") or "").strip()
        if not tk or tk in by_ticker:
            continue
        by_ticker[tk] = s

    # Current folder tree, flat — we'll do the diff in memory.
    existing = await folder_svc.list_folders(db, user, scope)
    parent = next(
        (f for f in existing if f.parent_id is None and f.name == parent_name),
        None,
    )
    if parent is None:
        if not allow_create_parent:
            return None
        parent = await folder_svc.create_folder(
            db, user,
            scope=scope,
            name=parent_name,
            folder_type=folder_svc.FOLDER_GENERAL,
            parent_id=None,
        )
        await db.commit()
        # Re-list so our in-memory view includes the new parent.
        existing = await folder_svc.list_folders(db, user, scope)

    parent_id = str(parent.id)
    # Index children of the parent (market folders) + grandchildren (stocks).
    market_folders: dict[str, Any] = {}   # display_name → KbFolder
    stock_folders: dict[str, Any] = {}    # ticker.upper() → KbFolder
    by_id = {str(f.id): f for f in existing}
    for f in existing:
        if str(f.parent_id) == parent_id:
            market_folders[f.name] = f
        # Walk up to see if ancestor is the 持仓股票 parent.
        cur = f
        depth = 0
        while cur is not None and depth < 6:
            if str(cur.parent_id) == parent_id:
                # cur is a market folder → f is nested somewhere under it.
                break
            if cur.parent_id is None:
                cur = None
                break
            cur = by_id.get(str(cur.parent_id))
            depth += 1
        if cur is None:
            continue  # f isn't under the holdings parent at all
        # f is under some market folder — if it's a stock folder, index by ticker.
        if f.folder_type == folder_svc.FOLDER_STOCK and f.stock_ticker:
            stock_folders[f.stock_ticker.upper()] = f

    # ── 1. Ensure every market we'll need exists ─────────────────
    wanted_markets: dict[str, list[dict]] = {}
    for h in by_ticker.values():
        m = _normalize_market(h.get("stock_market") or "")
        wanted_markets.setdefault(m, []).append(h)

    created_markets = 0
    for m_name in sorted(wanted_markets.keys()):
        if m_name in market_folders:
            continue
        try:
            mf = await folder_svc.create_folder(
                db, user,
                scope=scope,
                name=m_name,
                folder_type=folder_svc.FOLDER_GENERAL,
                parent_id=parent_id,
            )
            await db.commit()
            market_folders[m_name] = mf
            created_markets += 1
        except folder_svc.FolderValidationError as e:
            logger.info("holdings-sync: can't create market '%s': %s", m_name, e)

    # ── 2. Ensure each holding has a stock folder under its market ────
    # Migration case: if a stock folder exists but sits as a *direct* child
    # of the 持仓股票 parent (the pre-market-grouping flat layout), move it
    # under the correct market subfolder. We only migrate from the root —
    # stocks the user manually moved under another folder stay put.
    created_stocks = 0
    migrated_stocks = 0
    desired_tickers: set[str] = set()
    for m_name, holdings in wanted_markets.items():
        mf = market_folders.get(m_name)
        if mf is None:
            continue  # creation failed above; skip rather than orphan
        for h in holdings:
            ticker = (h.get("stock_ticker") or "").strip()
            if not ticker:
                continue
            desired_tickers.add(ticker.upper())
            existing_sf = stock_folders.get(ticker.upper())
            if existing_sf is not None:
                # Already exists — migrate it only if it's still sitting at
                # the flat root (direct child of 持仓股票). Stocks already
                # nested under any market folder are left where they are.
                if str(existing_sf.parent_id) == parent_id:
                    try:
                        await folder_svc.update_folder(
                            db, user, str(existing_sf.id),
                            parent_id=str(mf.id),
                        )
                        await db.commit()
                        migrated_stocks += 1
                    except Exception as e:
                        logger.warning(
                            "holdings-sync: failed to migrate %s into market '%s': %s",
                            ticker, m_name, e,
                        )
                continue
            name = (h.get("stock_name") or ticker).strip()
            try:
                sub = await folder_svc.create_folder(
                    db, user,
                    scope=scope,
                    name=name,
                    folder_type=folder_svc.FOLDER_STOCK,
                    parent_id=str(mf.id),
                    stock_ticker=ticker,
                    stock_market=(h.get("stock_market") or "").strip(),
                    stock_name=name,
                )
                await db.commit()
                stock_folders[ticker.upper()] = sub
                created_stocks += 1
            except folder_svc.FolderValidationError as e:
                logger.info(
                    "holdings-sync: skip %s (%s) under %s — %s",
                    name, ticker, m_name, e,
                )
                continue
            # Seed default spreadsheet.
            try:
                await svc.create_spreadsheet_document(
                    user_id=str(user.id),
                    original_filename=f"{ticker}-valuation.sheet",
                    title=f"{name} 估值表",
                    folder_id=str(sub.id),
                    scope=scope,
                    description=f"{name} ({ticker}) 默认估值表",
                )
            except Exception as e:
                logger.warning(
                    "holdings-sync: folder ok but spreadsheet failed for %s: %s",
                    ticker, e,
                )
            # Seed subtaxonomy (subfolders + key-driver.md + notes.md) for
            # this brand-new stock folder. Fresh folders always get the full
            # skeleton regardless of ``seed_subtaxonomy`` — the flag only
            # gates the broader "backfill existing folders" pass below.
            try:
                await _ensure_stock_subtaxonomy(user, db, sub)
            except Exception:
                pass  # _ensure_stock_subtaxonomy already logs on failure

    # ── 3. Remove stock folders for tickers no longer in the portfolio ──
    # "Clean" = holds ≤ 1 document (only the auto-seeded sheet) AND has no
    # subfolders. Otherwise preserve — the user has added research.
    removed_stocks = 0
    preserved_stocks = 0
    stale_tickers = [t for t in stock_folders.keys() if t not in desired_tickers]
    if stale_tickers:
        # Batch-collect child counts per folder (subfolders in memory,
        # doc counts via one Mongo aggregation).
        id_to_children: dict[str, int] = {}
        for f in existing:
            pid = str(f.parent_id) if f.parent_id else ""
            id_to_children[pid] = id_to_children.get(pid, 0) + 1
        doc_counts = await svc.get_folder_document_counts(
            str(user.id), scope,
        )
        for t in stale_tickers:
            f = stock_folders[t]
            n_subfolders = id_to_children.get(str(f.id), 0)
            n_docs = doc_counts.get(str(f.id), 0)
            if n_subfolders > 0 or n_docs > 1:
                preserved_stocks += 1
                logger.info(
                    "holdings-sync: preserving delisted %s (%s) — has %d subfolders, %d docs",
                    f.name, f.stock_ticker, n_subfolders, n_docs,
                )
                continue
            # Safe to delete: at most the seeded spreadsheet, nothing user-made.
            try:
                deleted_ids = await folder_svc.delete_folder(
                    db, user, str(f.id),
                )
                await db.commit()
                await svc.delete_documents_by_folder_ids(
                    [str(x) for x in deleted_ids],
                )
                stock_folders.pop(t, None)
                removed_stocks += 1
            except Exception as e:
                logger.warning(
                    "holdings-sync: failed to remove stale %s: %s", t, e,
                )

    # ── 4. Remove market folders that no longer have any stock children ──
    removed_markets = 0
    if removed_stocks > 0:
        # Refresh view to see the actual child counts after deletes.
        existing_after = await folder_svc.list_folders(db, user, scope)
        id_to_children.clear()
        for f in existing_after:
            pid = str(f.parent_id) if f.parent_id else ""
            id_to_children[pid] = id_to_children.get(pid, 0) + 1
        for m_name, mf in list(market_folders.items()):
            if id_to_children.get(str(mf.id), 0) > 0:
                continue
            # Only delete market folder if it was auto-created by us (name
            # matches a known market group). If the user renamed it we
            # leave it alone.
            if m_name not in _MARKET_GROUPS.values() and m_name != "其他":
                continue
            try:
                deleted_ids = await folder_svc.delete_folder(
                    db, user, str(mf.id),
                )
                await db.commit()
                await svc.delete_documents_by_folder_ids(
                    [str(x) for x in deleted_ids],
                )
                market_folders.pop(m_name, None)
                removed_markets += 1
            except Exception as e:
                logger.warning(
                    "holdings-sync: failed to remove empty market '%s': %s",
                    m_name, e,
                )

    # ── 5. Sub-taxonomy backfill for existing stock folders (gated) ─────
    # When ``seed_subtaxonomy`` is True (the first ``/tree`` fetch per user),
    # ensure each pre-existing stock folder has the minimal skeleton
    # (专家访谈 + key-driver.md + notes.md). New folders created above
    # already got this via the per-folder call.
    if seed_subtaxonomy and stock_folders:
        try:
            existing_now = await folder_svc.list_folders(db, user, scope)
        except Exception:
            existing_now = []
        for f in list(stock_folders.values()):
            try:
                await _ensure_stock_subtaxonomy(
                    user, db, f, existing_folders=existing_now,
                )
            except Exception:
                pass

    # ── 6. Cleanup pass: remove EMPTY legacy auto-seeded subfolders ──
    # 研报 / 公司公告 / 公司交流 / 调研 / 轮播 / 模型 / 其他 were seeded by
    # earlier versions of this service; the current version only seeds
    # 专家访谈. This pass removes the leftovers if they have no user data.
    # Runs on every sync (not gated) so users see stale empties disappear
    # as soon as the backend is updated, without explicit action.
    if stock_folders:
        try:
            existing_now = await folder_svc.list_folders(db, user, scope)
        except Exception:
            existing_now = []
        for f in list(stock_folders.values()):
            try:
                await _cleanup_legacy_stock_subfolders(
                    user, db, f, existing_folders=existing_now,
                )
            except Exception:
                pass

    return _HoldingsSyncResult(
        parent_folder_id=parent_id,
        created_markets=created_markets,
        created_stocks=created_stocks,
        migrated_stocks=migrated_stocks,
        removed_stocks=removed_stocks,
        removed_markets=removed_markets,
        preserved_stocks=preserved_stocks,
        total_holdings=len(by_ticker),
    )


@router.post("/folders/import-holdings", response_model=ImportHoldingsResponse)
async def import_portfolio_holdings(
    scope: str = Query("personal", description="personal | public"),
    parent_name: str = Query(
        _HOLDINGS_PARENT_NAME, description="Root folder name",
    ),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Explicit re-sync of the 持仓股票 tree.

    Runs the same idempotent reconciliation that fires automatically on
    each personal-scope tree fetch — useful as a manual trigger after a
    portfolio YAML edit, or for tests. Adds new holdings, removes delisted
    ones (if no user data is in them), and reorganizes into the market
    grouping described in :func:`_perform_holdings_sync`.
    """
    if scope not in (folder_svc.SCOPE_PERSONAL, folder_svc.SCOPE_PUBLIC):
        raise HTTPException(400, "scope must be 'personal' or 'public'")
    try:
        folder_svc.assert_can_write(user, scope)
    except folder_svc.FolderPermissionDenied as e:
        raise HTTPException(403, str(e))
    try:
        result = await _perform_holdings_sync(
            user, db, scope=scope, parent_name=parent_name,
            allow_create_parent=True,
        )
    except folder_svc.FolderValidationError as e:
        raise HTTPException(400, str(e))
    # Keep the response shape stable so existing clients don't break, but
    # surface the new stats in the failed counter slot for diagnostics.
    assert result is not None  # allow_create_parent=True can't return None
    return ImportHoldingsResponse(
        parent_folder_id=result.parent_folder_id,
        created=result.created_stocks,
        skipped_existing=result.total_holdings - result.created_stocks,
        failed=result.removed_stocks + result.preserved_stocks,
        total_holdings=result.total_holdings,
    )


class EnsureStockChildrenResponse(BaseModel):
    folder_id: str
    folders_created: int
    files_created: int


@router.post("/folders/{folder_id}/ensure-stock-children",
             response_model=EnsureStockChildrenResponse)
async def ensure_stock_children_endpoint(
    folder_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Idempotently seed the stock-folder sub-taxonomy (subfolders + .md)."""
    try:
        folder = await folder_svc.can_access_folder(db, folder_id, user)
    except folder_svc.FolderNotFound:
        raise HTTPException(404, "folder not found")
    except folder_svc.FolderPermissionDenied as e:
        raise HTTPException(403, str(e))
    if folder.folder_type != folder_svc.FOLDER_STOCK:
        raise HTTPException(400, "only stock folders have sub-taxonomy")
    if folder.scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
        raise HTTPException(403, "only admin/boss can seed public stock folders")
    folders_created, files_created = await _ensure_stock_subtaxonomy(
        user, db, folder,
    )
    return EnsureStockChildrenResponse(
        folder_id=str(folder.id),
        folders_created=folders_created,
        files_created=files_created,
    )


@router.delete("/folders/{folder_id}")
async def delete_folder_endpoint(
    folder_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a folder and cascade-delete all descendants + their documents.

    Confirm on the client — this is destructive. Response includes the number
    of documents removed from MongoDB so the UI can show an accurate toast.
    """
    try:
        deleted_folder_ids = await folder_svc.delete_folder(db, user, folder_id)
    except folder_svc.FolderPermissionDenied as e:
        raise HTTPException(403, str(e))
    except folder_svc.FolderNotFound as e:
        raise HTTPException(404, str(e))
    except folder_svc.FolderValidationError as e:
        raise HTTPException(400, str(e))
    await db.commit()
    # Cascade into Mongo. Folder IDs are UUIDs; docs store them as strings.
    deleted_docs = await svc.delete_documents_by_folder_ids(
        [str(i) for i in deleted_folder_ids],
    )
    return {
        "ok": True,
        "deleted_folders": len(deleted_folder_ids),
        "deleted_documents": deleted_docs,
    }


# ── Workspace tree (folders + documents) ────────────────────────


async def _maybe_auto_sync_holdings(
    user: User, db: AsyncSession,
) -> None:
    """Reconcile the 持仓股票 tree with the portfolio YAML on each fetch.

    The sync is cheap in the steady state (one folder-list query, no
    writes) but also incremental: it adds new holdings and removes
    delisted ones (only if the stock folder has no user-uploaded data).
    See :func:`_perform_holdings_sync` for the reconciliation rules.

    To respect a user who explicitly deleted the 持仓股票 parent, we pass
    ``allow_create_parent`` based on the per-user flag:

      * First fetch (``kb_holdings_initialized_at`` is NULL) → create the
        parent and populate it. Set the timestamp.
      * Later fetches with the parent present → reconcile in place.
      * Later fetches after the user deleted the parent → skip.

    Failures are swallowed and logged so a transient portfolio-config or
    Mongo issue never breaks a tree fetch.
    """
    from sqlalchemy import select
    pref = await db.scalar(
        select(UserPreference).where(UserPreference.user_id == user.id),
    )
    if pref is None:
        pref = UserPreference(user_id=user.id)
        db.add(pref)
        await db.flush()

    first_time = pref.kb_holdings_initialized_at is None
    # Sub-taxonomy (key-driver.md / notes.md / 研报 / 公司公告 / ...) is a
    # separate one-shot seed. We fire it on the first ``/tree`` fetch after
    # the feature shipped — *regardless* of whether this is the first-ever
    # holdings import — so legacy users get the skeleton backfilled once.
    # After the flag is set, later fetches leave existing folders alone
    # (they only seed NEW stock folders as they're added).
    first_subtaxonomy = pref.kb_workspace_subfolders_initialized_at is None
    try:
        result = await _perform_holdings_sync(
            user, db, scope="personal",
            # On first visit, always create the parent. After that, respect
            # the user's choice if they deleted it — the parent won't come
            # back, but if they still have it, we'll keep it in sync.
            allow_create_parent=first_time,
            seed_subtaxonomy=first_subtaxonomy,
        )
    except Exception as e:
        logger.warning(
            "auto holdings sync failed for user=%s: %s", user.id, e,
        )
        return

    if result is None:
        # User deleted the parent and we're respecting that. No-op.
        return

    if first_time:
        pref.kb_holdings_initialized_at = datetime.now(timezone.utc)
    if first_subtaxonomy:
        pref.kb_workspace_subfolders_initialized_at = datetime.now(timezone.utc)
    if first_time or first_subtaxonomy:
        await db.commit()

    if (result.created_stocks or result.removed_stocks
            or result.created_markets or result.removed_markets
            or result.migrated_stocks):
        logger.info(
            "holdings-sync user=%s: +%d stocks, migrated=%d, -%d stocks, "
            "+%d markets, -%d markets, preserved=%d",
            user.id,
            result.created_stocks, result.migrated_stocks,
            result.removed_stocks, result.created_markets,
            result.removed_markets, result.preserved_stocks,
        )


@router.get("/tree", response_model=TreeResponse)
async def workspace_tree(
    scope: str = Query("personal", description="personal | public"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """VS Code-style workspace: folders with per-folder doc counts.

    First personal-scope fetch per user triggers the auto-import of
    portfolio holdings into the "持仓股票" folder (see
    :func:`_maybe_auto_import_holdings`).

    The frontend fetches documents inside a folder on-demand via
    ``GET /documents?folder_id=...`` to avoid shipping every file's full
    metadata in one round trip.
    """
    if scope not in (svc.SCOPE_PERSONAL, svc.SCOPE_PUBLIC):
        raise HTTPException(400, "scope must be 'personal' or 'public'")

    if scope == svc.SCOPE_PERSONAL:
        await _maybe_auto_sync_holdings(user, db)

    try:
        folders = await folder_svc.list_folders(db, user, scope)
    except folder_svc.FolderValidationError as e:
        raise HTTPException(400, str(e))

    # Per-folder doc counts in one aggregation; drop those that aren't ours.
    counts = await svc.get_folder_document_counts(str(user.id), scope)

    # Walk the tree, attach counts, compute children recursively.
    nodes = folder_svc.build_tree(folders)
    def _annotate(n: dict) -> dict:
        n["document_count"] = counts.get(n["id"], 0)
        n["children"] = [_annotate(c) for c in n.get("children", [])]
        return n
    annotated = [_annotate(n) for n in nodes]
    unfiled = counts.get("", 0)  # docs without a folder_id
    can_write = True
    if scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
        can_write = False
    return TreeResponse(
        scope=scope,
        folders=[FolderTreeNodeResponse(**n) for n in annotated],
        unfiled_count=unfiled,
        can_write=can_write,
    )
