"""Personal knowledge base service — per-user document storage + retrieval.

Layered over one MongoDB database (``ti-user-knowledge-base`` on the shared
ops cluster ``192.168.31.176:35002`` by default; override via
``USER_KB_MONGO_URI`` / ``USER_KB_MONGO_DB``):

* ``documents``  — one row per uploaded file. Holds metadata, upload status,
                   parse status, extracted text (for small-file convenience)
                   and user-editable fields (title/description/tags).
* ``chunks``     — text chunks produced by the parser; indexed by
                   ``(user_id, text)`` so BM25-ish ``$text`` retrieval can be
                   scoped to the current user efficiently.
* ``fs.files`` / ``fs.chunks`` — GridFS; stores the original file binary so
                   users can download what they uploaded.

Every document is scoped by ``user_id`` (UUID string). Retrieval tools and
API routes **must** pass the user ID so one account can never read another
account's documents.

Parsing runs as an asyncio task spawned from the upload endpoint (or from the
startup recovery sweep for docs whose parse was interrupted by a restart).
The parser is CPU-bound (especially the JVM path for PDFs), so it runs under
``asyncio.to_thread`` to keep the event loop responsive.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta as _timedelta, timezone
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
    AsyncIOMotorGridFSBucket,
)
from pymongo import ASCENDING, DESCENDING, TEXT
from pymongo.errors import OperationFailure

from backend.app.config import get_settings
from backend.app.services import (
    user_kb_asr_client,
    user_kb_embedder,
    user_kb_parser,
    user_kb_tokenizer,
    user_kb_vector,
    user_kb_workbook,
)

logger = logging.getLogger(__name__)


# ── Status enums (stored as strings in Mongo for human debuggability) ──


class UploadStatus:
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"


class ParseStatus:
    PENDING = "pending"
    PARSING = "parsing"
    COMPLETED = "completed"
    FAILED = "failed"


# ── ContextVar: used by chat tool dispatcher to scope searches ─────


# The chat tool dispatcher (chat_llm.py) does not pass user context through
# its 3-arg signature. We follow the same pattern as `chat_debug.get_current_trace_id()`
# and use a ContextVar set at the chat endpoint layer. Tools read it back.
_current_user_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "user_kb_current_user_id", default=""
)


def set_current_user_id(user_id: str) -> contextvars.Token:
    """Set the current user id for tool dispatch. Returns a reset token."""
    return _current_user_id.set(user_id or "")


def get_current_user_id() -> str:
    """Return the current user id bound by the chat request."""
    return _current_user_id.get()


def reset_current_user_id(token: contextvars.Token) -> None:
    _current_user_id.reset(token)


# ── MongoDB client wiring ──────────────────────────────────────


# One Motor client *per event loop*. Motor clients bind to the loop they're
# first awaited on; caching a single instance across loops (e.g. uvicorn's
# main loop vs. the TestClient's anyio portal loop vs. a pytest-asyncio
# per-test loop) produces cryptic "Future attached to a different loop"
# errors. Keying the cache on loop id makes this fail-safe.
_mongo_clients_by_loop: dict[int, AsyncIOMotorClient] = {}
_indexes_by_loop: set[int] = set()


def _mongo_client() -> AsyncIOMotorClient:
    """Return the Motor client bound to the *current* event loop.

    Creating a Motor client is cheap (it only opens sockets on first use),
    and each loop gets at most one — so in production with a single uvicorn
    worker we end up with exactly one pool. Always called from async code —
    if there's no running loop this is a programming error and we want to
    surface it loudly.
    """
    loop = asyncio.get_running_loop()
    loop_id = id(loop)
    client = _mongo_clients_by_loop.get(loop_id)
    if client is not None:
        return client
    settings = get_settings()
    client = AsyncIOMotorClient(
        settings.user_kb_mongo_uri,
        tz_aware=True,
        # Short-ish socket timeouts — user-kb operations are all sub-second;
        # if Mongo is unreachable we want to fail fast rather than stall the
        # chat request for minutes.
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )
    _mongo_clients_by_loop[loop_id] = client
    return client


def _clear_mongo_client_cache() -> None:
    """Drop all cached Motor clients and per-loop state. Intended for test teardown."""
    for c in _mongo_clients_by_loop.values():
        try:
            c.close()
        except Exception:
            pass
    _mongo_clients_by_loop.clear()
    _indexes_by_loop.clear()
    _parse_semaphores.clear()


def _db() -> AsyncIOMotorDatabase:
    settings = get_settings()
    return _mongo_client()[settings.user_kb_mongo_db]


def _docs() -> AsyncIOMotorCollection:
    # Staging shares the remote `ti-user-knowledge-base` DB with prod
    # (u_spider cannot create new DBs). Per-env isolation is done via a
    # `stg_` collection prefix — see Settings.user_kb_docs_collection.
    return _db()[get_settings().user_kb_docs_collection]


def _chunks() -> AsyncIOMotorCollection:
    return _db()[get_settings().user_kb_chunks_collection]


def _gridfs() -> AsyncIOMotorGridFSBucket:
    # GridFS materializes as `{bucket}.files` + `{bucket}.chunks`, so the
    # bucket prefix keeps binary uploads isolated too (`fs.*` for prod,
    # `stg_fs.*` for staging).
    return AsyncIOMotorGridFSBucket(_db(), bucket_name=get_settings().user_kb_gridfs_bucket)


# ── Index setup ────────────────────────────────────────────────


async def ensure_indexes() -> None:
    """Create Mongo indexes idempotently. Safe to call repeatedly.

    Remembers completion *per event loop* — so if a test process creates a
    fresh loop, we re-verify indexes against that loop's Motor client. The
    actual createIndex calls are themselves idempotent at the Mongo level.
    """
    try:
        loop_id = id(asyncio.get_running_loop())
    except RuntimeError:
        loop_id = 0
    if loop_id in _indexes_by_loop:
        return
    docs = _docs()
    chunks = _chunks()

    # Per-user document listing ordered by newest first.
    await docs.create_index(
        [("user_id", ASCENDING), ("created_at", DESCENDING)],
        name="user_id_created_at",
    )
    # Stale-parse recovery — finds rows stuck in parsing/pending status by
    # parse_started_at. Compound on (parse_status, parse_started_at).
    await docs.create_index(
        [("parse_status", ASCENDING), ("parse_started_at", ASCENDING)],
        name="parse_status_started_at",
    )
    # Dedup within a user's own KB (same hash).
    await docs.create_index(
        [("user_id", ASCENDING), ("content_hash", ASCENDING)],
        name="user_id_content_hash",
    )
    # Chunk lookups are always scoped by user_id and document_id.
    await chunks.create_index(
        [("user_id", ASCENDING), ("document_id", ASCENDING), ("chunk_index", ASCENDING)],
        name="user_doc_chunk",
    )

    # Text index on the jieba-tokenized ``tokens`` field. The raw ``text``
    # field stays untouched for retrieval / display, but ``tokens`` is what
    # MongoDB's ``$text`` actually matches against — it has whitespace-split
    # Chinese words thanks to ``user_kb_tokenizer.tokenize``. Without this
    # jieba preprocessing, pure-CJK queries like "接口说明" match nothing
    # because MongoDB's default tokenizer sees the whole Chinese span as a
    # single atomic token.
    #
    # We use a plain (non-compound) text index: MongoDB's planner demands
    # equality filters on every non-text prefix of a compound text index,
    # which would break the cross-user search path the chat tool relies on.
    # Per-user scoping is applied *after* the text match when requested.
    target_name = "chunk_tokens_idx"
    target_keys = [("tokens", TEXT)]
    existing_text_idx = None
    existing_is_compatible = False
    async for idx in chunks.list_indexes():
        if idx.get("weights"):  # only text indexes carry weights
            existing_text_idx = idx["name"]
            # Compatible = our target name AND indexes only `tokens`.
            if (
                idx["name"] == target_name
                and set(idx.get("weights", {}).keys()) == {"tokens"}
            ):
                existing_is_compatible = True
            break
    if existing_text_idx and not existing_is_compatible:
        try:
            await chunks.drop_index(existing_text_idx)
            existing_text_idx = None
        except OperationFailure:
            logger.warning("could not drop stale text index %s", existing_text_idx)

    if not existing_is_compatible:
        try:
            await chunks.create_index(
                target_keys,
                name=target_name,
                default_language="none",  # skip English stemmer — tokens field
                                          # is already jieba-segmented
            )
        except OperationFailure as e:
            # Tolerate benign "index already exists" / option-conflict races.
            if "IndexOptionsConflict" in str(e) or "Index with name" in str(e):
                await chunks.drop_index(target_name)
                await chunks.create_index(
                    target_keys,
                    name=target_name,
                    default_language="none",
                )
            else:
                raise
    _indexes_by_loop.add(loop_id)


async def ping() -> tuple[bool, str]:
    """Probe Mongo connectivity. Returns (ok, reason)."""
    try:
        await _mongo_client().admin.command("ping")
        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ── Helpers ────────────────────────────────────────────────────


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _oid(doc_id: str) -> ObjectId:
    try:
        return ObjectId(doc_id)
    except Exception as e:
        raise ValueError(f"invalid document id: {doc_id}") from e


_HEAVY_DOC_FIELDS = {"extracted_text", "spreadsheet_data", "content_md"}


def _jsonable(v: Any) -> Any:
    """Recursively coerce Mongo-native values (ObjectId, datetime) to JSON-safe
    primitives. Needed for nested docs like ``audio.summary_generated_at``
    where the top-level scan alone would leave a datetime inside an inner dict
    and break Pydantic validation on the API layer."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_jsonable(x) for x in v]
    return v


def _serialize_doc(doc: dict) -> dict:
    """Turn a Mongo doc into a JSON-serializable dict for API responses.

    Strips large fields (``extracted_text``, ``spreadsheet_data``) that have
    dedicated content endpoints, and converts ObjectIds / datetimes to
    strings (recursively — nested values like ``audio.summary_generated_at``
    matter for the audio viewer). The ``doc_type`` field rides through so
    the UI can pick the right icon / editor.
    """
    out: dict[str, Any] = {}
    for k, v in doc.items():
        if k in _HEAVY_DOC_FIELDS:
            continue
        out[k] = _jsonable(v)
    out["id"] = str(doc["_id"])
    out.pop("_id", None)
    # Default doc_type for legacy rows without the field.
    out.setdefault("doc_type", DOC_TYPE_FILE)
    return out


# ── Upload ─────────────────────────────────────────────────────


@dataclass
class UploadOutcome:
    document_id: str
    was_duplicate: bool


async def count_user_documents(user_id: str) -> int:
    return await _docs().count_documents({"user_id": user_id})


# ── Spreadsheet documents (valuation tables) ─────────────────
#
# Stock folders auto-get a default spreadsheet doc. It's a regular user_kb
# document from the storage perspective (lives in the ``documents`` Mongo
# collection, is visible in the folder's file list), but:
#   * doc_type="spreadsheet" — UI uses this to route to the grid editor.
#   * parse_* fields are set to "completed" on creation so the parser
#     pipeline skips it (no file bytes to extract).
#   * ``spreadsheet_data`` holds the grid as a JSON blob (rows, cols, cells).

DOC_TYPE_FILE = "file"
DOC_TYPE_SPREADSHEET = "spreadsheet"
# Multi-sheet workbook — successor of DOC_TYPE_SPREADSHEET. Old rows with
# `doc_type="spreadsheet"` remain readable; the GET path normalizes them
# into the multi-sheet shape so the frontend always sees `sheets: [...]`.
DOC_TYPE_WORKBOOK = "workbook"
DOC_TYPE_MARKDOWN = "markdown"
# All spreadsheet-kind doc_types — used by queries that don't care about
# flat-vs-multi-sheet distinction (e.g. "find the default 估值表 in a
# stock folder").
_SPREADSHEET_DOC_TYPES = [DOC_TYPE_SPREADSHEET, DOC_TYPE_WORKBOOK]

# Scope constants used by both spreadsheet seeding and legacy listing paths.
# Declared here (above the spreadsheet functions that use them as defaults)
# instead of further down alongside the folder-aware queries — Python
# evaluates function default values at def-time, so SCOPE_PERSONAL must
# exist before ``create_spreadsheet_document`` is defined.
SCOPE_PERSONAL = "personal"
SCOPE_PUBLIC = "public"

# 5-year DCF scaffold — canonical definition now lives in ``user_kb_workbook``
# (multi-sheet shape). The alias below is kept so any caller that imported
# this symbol keeps working. New code should use workbook helpers directly.
DEFAULT_VALUATION_TEMPLATE: dict[str, Any] = user_kb_workbook.default_valuation_workbook()


async def create_spreadsheet_document(
    *,
    user_id: str,
    original_filename: str = "valuation.sheet",
    title: str,
    folder_id: str | None,
    scope: str = SCOPE_PERSONAL,
    spreadsheet_data: dict | None = None,
    description: str = "",
) -> str:
    """Insert a workbook doc directly — no GridFS, no parse pipeline.

    The stored doc uses ``doc_type="workbook"`` (multi-sheet shape). Old
    callers that expect ``doc_type="spreadsheet"`` keep working because the
    GET path normalizes both.

    Returns the new document id. The UI surfaces these under the "估值表"
    tab in stock folders, editing the grid in-place via GET/PATCH
    ``/documents/{id}/spreadsheet``.
    """
    await ensure_indexes()
    now = _now()
    raw = spreadsheet_data if spreadsheet_data is not None else DEFAULT_VALUATION_TEMPLATE
    # Always persist in the canonical multi-sheet shape so future reads are
    # free of migration cost. ``validate_for_write`` accepts either.
    try:
        data = user_kb_workbook.validate_for_write(raw)
    except user_kb_workbook.WorkbookValidationError:
        # Fallback — don't block seeding on a malformed custom template.
        data = user_kb_workbook.default_valuation_workbook()
    row = {
        "user_id": user_id,
        "original_filename": original_filename,
        "file_extension": "sheet",
        "content_type": "application/x-kb-workbook",
        "file_size_bytes": 0,
        "content_hash": "",  # not deduped
        "gridfs_file_id": None,
        "doc_type": DOC_TYPE_WORKBOOK,
        "spreadsheet_data": data,
        # Mark upload+parse complete so polling stops immediately.
        "upload_status": UploadStatus.COMPLETED,
        "upload_error": None,
        "parse_status": ParseStatus.COMPLETED,
        "parse_error": None,
        "parse_started_at": now,
        "parse_completed_at": now,
        "parser_backend": "workbook",
        "parse_warnings": [],
        "parse_progress_percent": 100,
        "parse_phase": "done",
        "extracted_text": "",
        "extracted_char_count": 0,
        "num_chunks": 0,
        "title": title.strip() or "估值表",
        "description": description or "",
        "tags": [],
        "folder_id": folder_id,
        "scope": scope,
        "created_at": now,
        "updated_at": now,
    }
    result = await _docs().insert_one(row)
    return str(result.inserted_id)


async def create_markdown_document(
    *,
    user_id: str,
    original_filename: str,
    title: str,
    folder_id: str | None,
    scope: str = SCOPE_PERSONAL,
    content_md: str = "",
    description: str = "",
    tags: list[str] | None = None,
) -> str:
    """Insert a markdown doc. Content lives inline in ``content_md`` (≤ 1 MB).

    No GridFS and no parse pipeline — markdown is plain text, already
    searchable via the chunk indexer. The document is chunk-indexed on
    create so the shared RAG path surfaces it immediately.
    """
    await ensure_indexes()
    now = _now()
    if not original_filename or not original_filename.strip():
        raise ValueError("filename is required")
    text = (content_md or "").strip("﻿")
    # Cap inline size to keep individual Mongo docs bounded.
    max_bytes = 1_024 * 1_024
    if len(text.encode("utf-8")) > max_bytes:
        raise ValueError(f"markdown content exceeds {max_bytes} bytes")
    row = {
        "user_id": user_id,
        "original_filename": original_filename.strip(),
        "file_extension": "md",
        "content_type": "text/markdown; charset=utf-8",
        "file_size_bytes": len(text.encode("utf-8")),
        "content_hash": "",
        "gridfs_file_id": None,
        "doc_type": DOC_TYPE_MARKDOWN,
        "content_md": text,
        "upload_status": UploadStatus.COMPLETED,
        "upload_error": None,
        "parse_status": ParseStatus.COMPLETED,
        "parse_error": None,
        "parse_started_at": now,
        "parse_completed_at": now,
        "parser_backend": "markdown-inline",
        "parse_warnings": [],
        "parse_progress_percent": 100,
        "parse_phase": "done",
        "extracted_text": text[: _INLINE_TEXT_MAX_CHARS],
        "extracted_char_count": len(text),
        "num_chunks": 0,
        "title": (title or "").strip() or original_filename.strip(),
        "description": (description or "").strip()[:5000],
        "tags": list(tags or []),
        "folder_id": folder_id,
        "scope": scope,
        "created_at": now,
        "updated_at": now,
    }
    result = await _docs().insert_one(row)
    doc_id = str(result.inserted_id)
    # Chunk + tokenize for BM25 / RAG. Fail-open: a chunker hiccup here
    # shouldn't rollback the insert — the doc still works for direct open.
    try:
        await _reindex_markdown_chunks(user_id, doc_id, text)
    except Exception as e:
        logger.warning("markdown chunking failed for %s: %s", doc_id, e)
    return doc_id


async def _reindex_markdown_chunks(user_id: str, doc_id: str, text: str) -> None:
    """Replace this doc's chunks with a fresh split of ``text``.

    Uses the paragraph-aware splitter already used for uploaded markdown.
    Safe to call repeatedly — we delete-then-insert rather than upsert.
    """
    oid = _oid(doc_id)
    await _chunks().delete_many({"user_id": user_id, "document_id": oid})
    if not text.strip():
        await _docs().update_one(
            {"_id": oid, "user_id": user_id},
            {"$set": {"num_chunks": 0, "updated_at": _now()}},
        )
        return
    parts = user_kb_parser.chunk_text(text, chunk_size=800, overlap=120)
    docs_to_insert: list[dict] = []
    for idx, part in enumerate(parts):
        if not part.strip():
            continue
        docs_to_insert.append({
            "user_id": user_id,
            "document_id": oid,
            "chunk_index": idx,
            "text": part,
            "tokens": user_kb_tokenizer.tokenize(part),
        })
    if docs_to_insert:
        await _chunks().insert_many(docs_to_insert)
    await _docs().update_one(
        {"_id": oid, "user_id": user_id},
        {"$set": {"num_chunks": len(docs_to_insert), "updated_at": _now()}},
    )


async def update_markdown_document(
    user_id: str, document_id: str, content_md: str,
    *, allow_public_admin: bool = False,
) -> dict | None:
    """Save a markdown doc's content. Returns the updated serialized doc.

    Re-runs chunking so the RAG index matches the new content. Same
    permission rules as ``update_spreadsheet_data``.
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    text = (content_md or "").strip("﻿")
    max_bytes = 1_024 * 1_024
    if len(text.encode("utf-8")) > max_bytes:
        raise ValueError(f"markdown content exceeds {max_bytes} bytes")
    preview = text[: _INLINE_TEXT_MAX_CHARS]
    update = {
        "$set": {
            "content_md": text,
            "extracted_text": preview,
            "extracted_char_count": len(text),
            "file_size_bytes": len(text.encode("utf-8")),
            "updated_at": _now(),
        },
    }
    res = await _docs().update_one(
        {"_id": oid, "user_id": user_id, "doc_type": DOC_TYPE_MARKDOWN},
        update,
    )
    owner_uid = user_id
    if res.matched_count == 0 and allow_public_admin:
        res = await _docs().update_one(
            {"_id": oid, "scope": SCOPE_PUBLIC, "doc_type": DOC_TYPE_MARKDOWN},
            update,
        )
        if res.matched_count > 0:
            pub = await _docs().find_one({"_id": oid}, {"user_id": 1})
            if pub:
                owner_uid = pub.get("user_id") or user_id
    if res.matched_count == 0:
        return None
    try:
        await _reindex_markdown_chunks(owner_uid, document_id, text)
    except Exception as e:
        logger.warning("markdown reindex after save failed for %s: %s", document_id, e)
    row = await _docs().find_one({"_id": oid})
    return _serialize_doc(row) if row else None


async def get_markdown_content(
    user_id: str, document_id: str,
) -> dict | None:
    """Return ``{document_id, title, content_md}`` iff the caller can read it."""
    doc = await get_accessible_document(user_id, document_id)
    if doc is None:
        return None
    if doc.get("doc_type") != DOC_TYPE_MARKDOWN:
        return None
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    row = await _docs().find_one(
        {"_id": oid}, {"content_md": 1, "title": 1},
    )
    if row is None:
        return None
    return {
        "document_id": document_id,
        "title": row.get("title") or "",
        "content_md": row.get("content_md") or "",
    }


async def get_spreadsheet_data(
    user_id: str, document_id: str,
) -> dict | None:
    """Return a spreadsheet doc's grid JSON if the caller can read it.

    Returns the canonical multi-sheet workbook shape, regardless of whether
    the stored row uses the legacy flat ``{rows,cols,cells}`` layout or the
    new ``{sheets:[...]}`` layout. The ``doc_type`` in the response follows
    the stored row (``spreadsheet`` vs ``workbook``) so clients that only
    care about the data payload can ignore it.

    Read scope: owner's personal doc OR any public doc (same rule as
    ``get_accessible_document``).
    """
    doc = await get_accessible_document(user_id, document_id)
    if doc is None:
        return None
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    row = await _docs().find_one(
        {"_id": oid},
        {"spreadsheet_data": 1, "doc_type": 1, "title": 1},
    )
    if row is None:
        return None
    stored = row.get("spreadsheet_data") or {}
    workbook = user_kb_workbook.normalize_for_read(stored)
    return {
        "document_id": document_id,
        "title": row.get("title") or "",
        "doc_type": row.get("doc_type") or DOC_TYPE_FILE,
        "spreadsheet_data": workbook,
    }


async def update_spreadsheet_data(
    user_id: str, document_id: str, data: dict,
    *, allow_public_admin: bool = False,
) -> bool:
    """Save a spreadsheet doc's grid JSON.

    Permission: uploader can always write; if ``allow_public_admin`` then
    an admin/boss caller can also write any public spreadsheet (API layer
    gates this based on role).
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        return False
    try:
        canonical = user_kb_workbook.validate_for_write(data)
    except user_kb_workbook.WorkbookValidationError as e:
        raise ValueError(str(e))

    # Promote legacy rows in-place to the new doc_type so the next round of
    # queries touches less legacy code. This is cosmetic — the read path
    # normalizes either, but keeping the doc_type fresh makes admin queries
    # cleaner.
    update = {
        "$set": {
            "spreadsheet_data": canonical,
            "doc_type": DOC_TYPE_WORKBOOK,
            "updated_at": _now(),
        },
    }
    # Try owner-scoped update first. Match either legacy `spreadsheet` or
    # new `workbook` doc_type so mid-migration rows still work.
    res = await _docs().update_one(
        {"_id": oid, "user_id": user_id,
         "doc_type": {"$in": _SPREADSHEET_DOC_TYPES}},
        update,
    )
    if res.matched_count > 0:
        return True
    # Fall back to public+admin override.
    if allow_public_admin:
        res = await _docs().update_one(
            {"_id": oid, "scope": SCOPE_PUBLIC,
             "doc_type": {"$in": _SPREADSHEET_DOC_TYPES}},
            update,
        )
        return res.matched_count > 0
    return False


async def find_default_spreadsheet_in_folder(
    user_id: str, scope: str, folder_id: str,
) -> str | None:
    """Return the id of the "default" spreadsheet inside a folder.

    "Default" = the oldest spreadsheet-kind doc (``spreadsheet`` or
    ``workbook``) in the folder. Stock folders auto-get one at creation
    time, so this is what the UI surfaces as the folder's 估值表.
    """
    match = _scope_match_filter(user_id, scope)
    match["folder_id"] = folder_id
    match["doc_type"] = {"$in": _SPREADSHEET_DOC_TYPES}
    row = await _docs().find_one(match, sort=[("created_at", ASCENDING)])
    return str(row["_id"]) if row else None


# ── Folder/scope support ───────────────────────────────────────
#
# Every document carries two optional fields linking it to the folder tree:
#   - ``scope`` ∈ {"personal","public"}; legacy docs without this field are
#     treated as "personal" for backward compatibility.
#   - ``folder_id`` — the UUID of a kb_folders row (stored as string). NULL /
#     missing means "unfiled" — it surfaces under the scope's "(unfiled)"
#     pseudo-folder in the UI.

def _scope_match_filter(user_id: str, scope: str) -> dict[str, Any]:
    """Mongo filter selecting the docs a user should see in a given scope.

    * personal → only their own uploads, treating legacy docs (no scope field)
      as personal by default.
    * public   → every public doc, regardless of uploader.
    """
    if scope == SCOPE_PUBLIC:
        return {"scope": SCOPE_PUBLIC}
    return {
        "user_id": user_id,
        # match legacy docs (no scope) and explicit personal docs
        "$or": [{"scope": {"$exists": False}}, {"scope": SCOPE_PERSONAL}],
    }


async def create_document(
    *,
    user_id: str,
    original_filename: str,
    data: bytes,
    title: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    folder_id: str | None = None,
    scope: str = SCOPE_PERSONAL,
) -> UploadOutcome:
    """Persist the file binary to GridFS and create a ``pending`` documents row.

    * Validates extension + size.
    * Computes a SHA-256 content hash; if the user already uploaded the same
      bytes, returns the existing id with ``was_duplicate=True`` and no new
      storage is consumed.
    * The heavy parse work is scheduled by the caller (the API layer) via
      ``asyncio.create_task(parse_document(...))`` so the HTTP response
      returns immediately.
    """
    settings = get_settings()
    if not user_id:
        raise ValueError("user_id is required")
    if not original_filename or not original_filename.strip():
        raise ValueError("filename is required")
    if len(original_filename) > settings.user_kb_max_filename_length:
        raise ValueError(
            f"filename too long ({len(original_filename)} chars, "
            f"limit {settings.user_kb_max_filename_length})"
        )
    if not user_kb_parser.is_supported(original_filename):
        raise ValueError(f"unsupported file type: {original_filename}")
    size = len(data)
    if size == 0:
        raise ValueError("file is empty")
    # Audio files get a larger ceiling — meeting-length mp3s easily dwarf
    # typical PDF/docx uploads. Keeping them under separate settings lets
    # an operator tune one without affecting the other.
    max_bytes = (
        settings.user_kb_max_audio_bytes
        if user_kb_parser.is_audio(original_filename)
        else settings.user_kb_max_file_bytes
    )
    if size > max_bytes:
        raise ValueError(
            f"file too large: {size} bytes (limit {max_bytes})"
        )

    await ensure_indexes()

    # Quota gate — count is an estimate because we don't hold a lock, but
    # with a low-concurrency user this is essentially correct, and the
    # penalty for the rare overrun is at most a handful of extra docs.
    # Tightening this further (transactions, a counter doc) is overkill
    # for the single-user upload cadence this UI produces.
    total = await count_user_documents(user_id)
    if total >= settings.user_kb_max_docs_per_user:
        raise ValueError(
            f"document quota reached ({settings.user_kb_max_docs_per_user})"
        )

    content_hash = hashlib.sha256(data).hexdigest()
    ext = original_filename.lower().rsplit(".", 1)[-1] if "." in original_filename else ""

    scope = scope if scope in (SCOPE_PERSONAL, SCOPE_PUBLIC) else SCOPE_PERSONAL

    # Dedup check — re-uploading identical bytes returns the existing record.
    # Scope matters: a user can have the same bytes in both personal and
    # public, but we still dedupe within a single (user, scope, folder) slot
    # to match the obvious "did I already upload this?" UX.
    dedup_query: dict[str, Any] = {
        "user_id": user_id,
        "content_hash": content_hash,
        "scope": scope,
    }
    if folder_id is not None:
        dedup_query["folder_id"] = folder_id
    existing = await _docs().find_one(dedup_query)
    if existing:
        return UploadOutcome(
            document_id=str(existing["_id"]),
            was_duplicate=True,
        )

    # 1. Stream the bytes into GridFS.
    fs = _gridfs()
    gridfs_file_id = await fs.upload_from_stream(
        original_filename,
        data,
        metadata={"user_id": user_id, "content_hash": content_hash},
    )

    # 2. Create the document row. If Mongo rejects the insert (schema drift,
    #    disk full, network hiccup), roll back the GridFS blob so we don't
    #    leak an orphaned binary.
    now = _now()
    doc_title = (title or "").strip() or _title_from_filename(original_filename)
    row = {
        "user_id": user_id,
        "original_filename": original_filename,
        "file_extension": ext,
        "content_type": user_kb_parser.content_type_for(original_filename),
        "file_size_bytes": size,
        "content_hash": content_hash,
        "gridfs_file_id": gridfs_file_id,
        "upload_status": UploadStatus.COMPLETED,
        "upload_error": None,
        "parse_status": ParseStatus.PENDING,
        "parse_error": None,
        "parse_started_at": None,
        "parse_completed_at": None,
        "parser_backend": None,
        "parse_warnings": [],
        # Progress fields — populated by the ASR path (audio uploads go through
        # the jumpbox Qwen3-ASR service, which reports percent + phase back
        # during transcription). For sync-parser file types these stay at 0/""
        # through completion; the UI treats `parse_status` as the source of
        # truth when percent=0.
        "parse_progress_percent": 0,
        "parse_phase": "",
        "extracted_text": "",
        "extracted_char_count": 0,
        "num_chunks": 0,
        "title": doc_title,
        "description": (description or "").strip(),
        "tags": list(tags or []),
        "folder_id": folder_id,
        "scope": scope,
        "created_at": now,
        "updated_at": now,
    }
    try:
        result = await _docs().insert_one(row)
    except Exception:
        # Roll back the just-uploaded GridFS blob; otherwise it sits there
        # with no documents row pointing at it, wasting space forever.
        try:
            await fs.delete(gridfs_file_id)
        except Exception:
            logger.warning(
                "gridfs rollback failed for blob %s after insert failure",
                gridfs_file_id,
            )
        raise
    return UploadOutcome(document_id=str(result.inserted_id), was_duplicate=False)


def _title_from_filename(name: str) -> str:
    """Strip extension and trim for a readable default title."""
    if "." in name:
        name = name.rsplit(".", 1)[0]
    return name.strip() or "Untitled"


# ── Background parse ───────────────────────────────────────────


# Preview excerpt length stored on the documents row for list UI.
# The full text lives in the chunks collection — 10 KB inline is a compromise
# so clients can show a content preview without a second Mongo round trip.
_INLINE_TEXT_MAX_CHARS = 10_000


# ── Bounded-concurrency parse queue ────────────────────────────


# One semaphore per event loop. Motor is loop-bound; so is the JVM process
# pool we'd like to throttle. Each loop gets its own gate.
_parse_semaphores: dict[int, asyncio.Semaphore] = {}


def _parse_semaphore() -> asyncio.Semaphore:
    """Return the per-loop Semaphore limiting concurrent parse tasks."""
    loop_id = id(asyncio.get_running_loop())
    sem = _parse_semaphores.get(loop_id)
    if sem is None:
        limit = max(1, int(get_settings().user_kb_parse_concurrency))
        sem = asyncio.Semaphore(limit)
        _parse_semaphores[loop_id] = sem
    return sem


async def parse_document(document_id: str) -> None:
    """Parse a single document: extract text, chunk, index.

    Idempotent — safe to call twice on the same doc. If another parse is
    already in flight (parse_status=="parsing") we bail out early to avoid
    racing and producing duplicate chunks.

    The parse itself is bounded by:
      * a process-wide semaphore (``user_kb_parse_concurrency``) so a burst
        of uploads can't spawn unbounded JVMs, and
      * a per-doc hard timeout (``user_kb_parse_timeout_seconds``) so one
        pathological PDF can't starve the queue forever.

    Never raises; all failures are surfaced in the ``parse_status`` /
    ``parse_error`` fields of the documents row.
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        logger.warning("parse_document called with bad id %s", document_id)
        return

    # Atomic "claim" — only one worker transitions pending/failed → parsing.
    # We include "parsing" if parse_started_at is older than the stale
    # threshold, which lets us reclaim a doc whose worker died mid-flight
    # while the process stayed up.
    settings = get_settings()
    stale_cutoff = _now().replace(microsecond=0) - _timedelta(
        seconds=settings.user_kb_parse_stale_seconds,
    )
    claim_query = {
        "_id": oid,
        "$or": [
            {"parse_status": {"$in": [ParseStatus.PENDING, ParseStatus.FAILED]}},
            # Stale "parsing" doc — the worker is gone.
            {
                "parse_status": ParseStatus.PARSING,
                "parse_started_at": {"$lt": stale_cutoff},
            },
        ],
    }
    claimed = await _docs().find_one_and_update(
        claim_query,
        {
            "$set": {
                "parse_status": ParseStatus.PARSING,
                "parse_started_at": _now(),
                "parse_error": None,
                "updated_at": _now(),
            },
        },
        return_document=True,
    )
    if claimed is None:
        logger.info(
            "parse_document %s not claimable (already parsing, or gone)", document_id,
        )
        return

    user_id = claimed["user_id"]
    filename = claimed["original_filename"]
    gridfs_file_id = claimed["gridfs_file_id"]

    # Bound concurrent parses AND cap each one at a wall-clock timeout. The
    # semaphore must be entered *outside* the timeout so waiting in the
    # queue doesn't count against a document's parse budget.
    #
    # Audio gets a much longer budget: transcription itself can take tens of
    # minutes for long meetings, and the ASR client does its own retries on
    # tunnel hiccups. We pad the jumpbox-side job timeout with 15 minutes for
    # in-client retries and chunking overhead.
    if user_kb_parser.is_audio(filename):
        parse_timeout = float(
            settings.asr_service_job_timeout_seconds + 15 * 60,
        )
    else:
        parse_timeout = float(settings.user_kb_parse_timeout_seconds)
    sem = _parse_semaphore()
    async with sem:
        try:
            await asyncio.wait_for(
                _do_parse(oid, user_id, filename, gridfs_file_id),
                timeout=parse_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "user_kb parse timed out after %ss for %s (user=%s, file=%s)",
                parse_timeout, document_id, user_id, filename,
            )
            await _docs().update_one(
                {"_id": oid},
                {
                    "$set": {
                        "parse_status": ParseStatus.FAILED,
                        "parse_completed_at": _now(),
                        "parse_error": (
                            f"parse timeout after {int(parse_timeout)}s"
                        ),
                        "updated_at": _now(),
                    },
                },
            )
        except user_kb_asr_client.AsrUnavailable as e:
            # Transport-level ASR outage (tunnel down, jumpbox rebooting,
            # ReadTimeout, etc.) — deliberately NOT a per-doc failure. The
            # file is already in GridFS; reset parse_status back to PENDING
            # so ``asr_recovery_sweep_loop`` re-enqueues it the moment the
            # service is reachable again. This is the contract the UI banner
            # "音频会排队，服务恢复后会自动续转" promises to users.
            logger.info(
                "user_kb audio parse %s deferred (ASR unavailable): %s",
                document_id, e,
            )
            await _docs().update_one(
                {"_id": oid},
                {
                    "$set": {
                        "parse_status": ParseStatus.PENDING,
                        "parse_started_at": None,
                        "parse_progress_percent": 0,
                        "parse_phase": "ASR 服务暂不可用，已加入队列，服务恢复后会自动续转",
                        "parse_error": None,
                        "updated_at": _now(),
                    },
                },
            )
        except Exception as e:
            logger.exception("user_kb parse failed for %s: %s", document_id, e)
            await _docs().update_one(
                {"_id": oid},
                {
                    "$set": {
                        "parse_status": ParseStatus.FAILED,
                        "parse_completed_at": _now(),
                        "parse_error": f"{type(e).__name__}: {e}"[:500],
                        "updated_at": _now(),
                    },
                },
            )


# Backoff schedule used by _parse_audio when the ASR service is unreachable.
# Kept short — the outer ``asr_recovery_sweep_loop`` handles longer outages
# without holding a parse slot. After these are exhausted we re-raise
# AsrUnavailable; ``parse_document`` then releases the doc back to PENDING
# (not FAILED) so the sweep re-enqueues it the moment the tunnel recovers.
_ASR_RETRY_BACKOFFS_SEC: tuple[int, ...] = (10, 30)


async def _do_parse(
    oid: ObjectId,
    user_id: str,
    filename: str,
    gridfs_file_id: Any,
) -> None:
    """Core parse pipeline; wrapped by ``parse_document`` for timeout/error handling.

    Two routes based on file type:
      * Audio (mp3/wav/m4a/...) → jumpbox Qwen3-ASR service via async HTTP.
        Emits live progress (percent + phase) via periodic ``$set`` writes
        on the docs row so the UI progress bar updates ~every 2 s.
      * Everything else → the sync parser registry in user_kb_parser, run
        in a worker thread to keep the event loop responsive.

    Both routes feed into the same chunk + vector + ``$text`` indexing
    pipeline so retrieval behaves identically regardless of the original
    file format.
    """
    # Pull bytes back out of GridFS.
    fs = _gridfs()
    stream = await fs.open_download_stream(gridfs_file_id)
    data = await stream.read()

    settings = get_settings()

    # ── Route: audio vs text ───────────────────────────────────
    audio_meta: dict = {}
    if user_kb_parser.is_audio(filename):
        text, parser_backend, warnings, audio_meta = await _parse_audio(
            oid, data, filename,
        )
    else:
        # Run the (potentially JVM-heavy) parser in a worker thread.
        result = await asyncio.to_thread(user_kb_parser.parse_file, filename, data)
        text = result.text or ""
        parser_backend = result.parser
        warnings = list(result.warnings)

    chunks = await asyncio.to_thread(
        user_kb_parser.chunk_text,
        text,
        chunk_size=settings.user_kb_chunk_size,
        overlap=settings.user_kb_chunk_overlap,
    )

    # Replace any prior chunks for this document — both in Mongo (lexical
    # store) and Milvus (vector store). The vector-side delete is best-effort
    # because it's on the fail-open path; a stale vector chunk is harmless
    # but we take the shot.
    await _chunks().delete_many({"user_id": user_id, "document_id": oid})
    doc_id_str = str(oid)
    try:
        await user_kb_vector.delete_by_document(doc_id_str)
    except Exception as e:
        logger.warning(
            "user_kb: Milvus cleanup for reparse of %s failed (non-fatal): %s",
            doc_id_str, e,
        )

    if chunks:
        # Tokenize each chunk for the $text index. Runs in a worker thread
        # because jieba's C-adjacent dict lookup is CPU-bound and big chunks
        # can briefly block the loop.
        tokens_list = await asyncio.to_thread(
            lambda: [user_kb_tokenizer.tokenize(c) for c in chunks]
        )
        chunk_rows = [
            {
                "user_id": user_id,
                "document_id": oid,
                "chunk_index": i,
                "text": c,
                "tokens": tokens_list[i],
                "char_count": len(c),
            }
            for i, c in enumerate(chunks)
        ]
        await _chunks().insert_many(chunk_rows, ordered=False)

        # Embed + upsert to Milvus. Dense retrieval is a quality booster —
        # if it fails, we keep the BM25-only behavior and log a WARNING
        # for the user's parse_warnings list.
        await _embed_and_upsert_chunks(
            document_id=doc_id_str,
            user_id=user_id,
            chunks=chunks,
            warnings=warnings,
        )

    # Warn the user if extraction produced nothing useful — parse "succeeded"
    # but searchability is zero, which otherwise looks mysterious in the UI.
    if not chunks or not text.strip():
        warnings.append(
            "parser produced no searchable text — "
            "file may be scanned, encrypted, or empty"
        )

    final_set: dict = {
        "parse_status": ParseStatus.COMPLETED,
        "parse_completed_at": _now(),
        "parse_error": None,
        "parser_backend": parser_backend,
        "parse_warnings": warnings,
        "parse_progress_percent": 100,
        "parse_phase": "done",
        "extracted_text": text[:_INLINE_TEXT_MAX_CHARS],
        "extracted_char_count": len(text),
        "num_chunks": len(chunks),
        "updated_at": _now(),
    }
    if audio_meta:
        # Stored as a nested doc so new fields (speaker diarization, etc.)
        # can be added without touching the top-level schema again.
        final_set["audio"] = {
            "duration_seconds": audio_meta.get("duration_seconds"),
            "language": audio_meta.get("language"),
            "segments": audio_meta.get("segments") or [],
            # summary is generated lazily on-demand by the API layer.
            "summary": None,
            "summary_generated_at": None,
        }
    await _docs().update_one({"_id": oid}, {"$set": final_set})
    logger.info(
        "user_kb parsed %s for user=%s: %d chars, %d chunks via %s",
        oid, user_id, len(text), len(chunks), parser_backend,
    )


async def _parse_audio(
    oid: ObjectId,
    data: bytes,
    filename: str,
) -> tuple[str, str, list[str], dict]:
    """Audio route: submit to the jumpbox ASR service, poll, mirror progress.

    Returns ``(text, parser_backend, warnings, audio_meta)`` — the meta dict
    carries per-segment transcript + duration + language so the UI can
    render a proper audio player rather than a text blob.

    Reliability layers:
      * The ASR client retries transport errors up to
        ``asr_service_poll_retries`` times *inside* a single transcribe call.
      * Around that, we add an outer backoff loop on
        :class:`AsrUnavailable` so a genuinely-down ASR service (tunnel
        broken, jumpbox rebooting) doesn't immediately fail the parse.
      * After ``_ASR_RETRY_BACKOFFS_SEC`` is exhausted we raise; the outer
        handler marks the doc FAILED with a clear error the user can act on.
    """

    async def _on_progress(p: user_kb_asr_client.AsrProgress) -> None:
        # Progress writes are best-effort: a transient Mongo hiccup must
        # never break transcription. Persistent failures surface in logs.
        try:
            await _docs().update_one(
                {"_id": oid},
                {
                    "$set": {
                        "parse_progress_percent": int(p.percent),
                        "parse_phase": p.phase or "",
                        "updated_at": _now(),
                    },
                },
            )
        except Exception as e:
            logger.warning("asr progress write failed for %s: %s", oid, e)

    attempt = 0
    while True:
        try:
            result = await user_kb_asr_client.transcribe(
                data, filename, on_progress=_on_progress,
            )
        except user_kb_asr_client.AsrUnavailable as e:
            if attempt >= len(_ASR_RETRY_BACKOFFS_SEC):
                # Surface the exact type so parse_document can route this to
                # the "defer back to PENDING" branch instead of FAILED. The
                # recovery sweep (asr_recovery_sweep_loop) will re-enqueue
                # the moment ASR is reachable again.
                raise user_kb_asr_client.AsrUnavailable(
                    f"asr service unreachable after {attempt} in-loop retries: {e}"
                ) from e
            delay = _ASR_RETRY_BACKOFFS_SEC[attempt]
            logger.warning(
                "asr unavailable for %s (attempt %d/%d): %s — retrying in %ds",
                oid, attempt + 1, len(_ASR_RETRY_BACKOFFS_SEC), e, delay,
            )
            try:
                await _docs().update_one(
                    {"_id": oid},
                    {
                        "$set": {
                            "parse_phase": (
                                f"ASR 服务暂不可用，{delay}s 后重试"
                            ),
                            "updated_at": _now(),
                        },
                    },
                )
            except Exception:
                pass
            await asyncio.sleep(delay)
            attempt += 1
            continue
        break

    warnings: list[str] = []
    if result.language:
        warnings.append(f"detected language: {result.language}")
    if result.duration_seconds is not None:
        warnings.append(f"audio duration: {result.duration_seconds:.1f}s")
    audio_meta = {
        "duration_seconds": result.duration_seconds,
        "language": result.language,
        "segments": [s.to_dict() for s in result.segments],
    }
    return result.text or "", "qwen3-asr", warnings, audio_meta


async def _embed_and_upsert_chunks(
    *,
    document_id: str,
    user_id: str,
    chunks: list[str],
    warnings: list[str],
) -> None:
    """Embed every chunk of a just-parsed document and push to Milvus.

    Batched according to ``user_kb_embedding_batch_size`` so we get good
    throughput from OpenAI's batch API. On any failure (API down, circuit
    open, Milvus down) we log + append a parse warning and return — the
    lexical side is already persisted so search still works.
    """
    if not chunks:
        return
    settings = get_settings()
    created_ms = int(_now().timestamp() * 1000)

    try:
        batch_size = max(1, int(settings.user_kb_embedding_batch_size))
        vectors: list[list[float]] = []
        for i in range(0, len(chunks), batch_size):
            slice_ = chunks[i : i + batch_size]
            batch_vecs = await user_kb_embedder.embed_batch(slice_)
            if len(batch_vecs) != len(slice_):
                raise user_kb_embedder.EmbedderError(
                    f"embedder returned {len(batch_vecs)} vectors for {len(slice_)} chunks"
                )
            vectors.extend(batch_vecs)
    except user_kb_embedder.EmbedderUnavailable as e:
        logger.warning(
            "user_kb: embedder unavailable for doc=%s, dense search disabled "
            "for this document until reparse: %s",
            document_id, e,
        )
        warnings.append(f"dense embedding skipped (embedder unavailable): {e}")
        return
    except Exception as e:
        logger.exception("user_kb: embedding failed for doc=%s", document_id)
        warnings.append(f"dense embedding failed: {type(e).__name__}: {e}")
        return

    pending = [
        user_kb_vector.PendingChunk(
            chunk_id=user_kb_vector.make_chunk_id(document_id, i),
            document_id=document_id,
            user_id=user_id,
            chunk_index=i,
            text=chunks[i],
            created_at_ms=created_ms,
            dense_vector=vec,
        )
        for i, vec in enumerate(vectors)
        if vec  # skip empty (short-circuited) vectors defensively
    ]
    written = await user_kb_vector.upsert_chunks(pending)
    if written == 0 and pending:
        warnings.append("vector upsert skipped (Milvus unavailable)")
    else:
        logger.info(
            "user_kb: indexed %d/%d dense vectors for doc=%s",
            written, len(pending), document_id,
        )


def schedule_parse(document_id: str) -> asyncio.Task:
    """Schedule ``parse_document`` as a fire-and-forget asyncio task.

    We explicitly attach an exception handler so an unexpected raise never
    silently disappears into the loop's default handler (which would just log
    "Task exception was never retrieved").
    """
    loop = asyncio.get_running_loop()
    task = loop.create_task(parse_document(document_id), name=f"user_kb_parse:{document_id}")

    def _done_cb(t: asyncio.Task) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            logger.error(
                "user_kb parse task for %s raised unexpectedly: %r", document_id, exc,
            )
    task.add_done_callback(_done_cb)
    return task


async def backfill_chunk_tokens(
    batch_size: int = 500,
    *,
    force: bool = False,
) -> int:
    """Populate / refresh the ``tokens`` field on chunks.

    Modes:
      * ``force=False`` (default, called at startup): only touches chunks
        that lack a ``tokens`` field. Fast no-op after first deploy.
      * ``force=True``: re-tokenizes every chunk from its ``text``.
        Needed when the tokenizer algorithm itself changes (e.g. a new
        Unicode normalization step, a custom dictionary, etc.) so the
        stored tokens agree with what fresh queries will tokenize to.

    Processes in batches to keep memory bounded on large corpora.
    Returns the total number of chunks updated.
    """
    coll = _chunks()
    total = 0
    query: dict[str, Any] = {} if force else {"tokens": {"$exists": False}}
    last_id: ObjectId | None = None
    while True:
        cursor_query = dict(query)
        if last_id is not None:
            cursor_query["_id"] = {"$gt": last_id}
        batch = await coll.find(
            cursor_query,
            {"_id": 1, "text": 1},
        ).sort("_id", 1).limit(batch_size).to_list(length=batch_size)
        if not batch:
            break
        last_id = batch[-1]["_id"]
        # Tokenize off the event loop — each chunk is ~1 KB and jieba is
        # fast but CPU-bound.
        def _tokenize_batch(rows):
            return [
                (r["_id"], user_kb_tokenizer.tokenize(r.get("text") or ""))
                for r in rows
            ]
        pairs = await asyncio.to_thread(_tokenize_batch, batch)
        # Mongo has no bulk $set-from-value, so issue one updateOne per row.
        # UpdateMany would require us to group rows by their value which is
        # pointless (every row has distinct tokens). bulk_write is the win.
        from pymongo import UpdateOne
        ops = [
            UpdateOne({"_id": _id}, {"$set": {"tokens": tok}})
            for _id, tok in pairs
        ]
        if ops:
            await coll.bulk_write(ops, ordered=False)
        total += len(pairs)
        # The _id > last_id cursor pattern terminates naturally on empty batch
        # above; no early-exit needed here.
    if total:
        logger.info(
            "user_kb: %s tokens on %d chunk(s)",
            "refreshed" if force else "backfilled", total,
        )
    return total


async def backfill_embeddings(
    batch_size: int | None = None,
    *,
    force: bool = False,
) -> int:
    """Embed every chunk in Mongo that doesn't yet have a Milvus vector.

    Runs at startup so uploads made before the hybrid search was enabled
    become searchable by the dense side. Also callable manually for a
    post-deploy migration, or with ``force=True`` when the embedding
    model itself changes (forces a full re-embed).

    Strategy:
      1. Pull all chunk (_id, document_id, user_id, chunk_index, text)
         rows from Mongo that we need to embed.
      2. Deterministically compute each chunk's Milvus chunk_id.
      3. Unless ``force``, ask Milvus which chunk_ids already exist and
         skip those — crucial so a restart doesn't re-embed the whole
         corpus (which would be slow and expensive).
      4. Embed missing chunks in batches + upsert.

    Returns the number of chunks written to Milvus.
    """
    if not await user_kb_vector.ensure_collection():
        logger.warning("user_kb: Milvus unavailable — skipping embedding backfill")
        return 0

    settings = get_settings()
    batch_size = batch_size or settings.user_kb_embedding_batch_size

    # 1. Load all chunks that might need embedding.
    cursor = _chunks().find(
        {},
        {
            "_id": 1, "document_id": 1, "user_id": 1,
            "chunk_index": 1, "text": 1,
        },
    )
    all_chunks: list[dict] = []
    async for row in cursor:
        if row.get("text"):
            all_chunks.append(row)
    if not all_chunks:
        return 0

    # 2. Compute chunk_ids.
    candidates: list[tuple[str, dict]] = []
    for row in all_chunks:
        doc_id = str(row["document_id"])
        idx = int(row.get("chunk_index") or 0)
        candidates.append((user_kb_vector.make_chunk_id(doc_id, idx), row))

    # 3. Ask Milvus what's already there (unless forcing a full re-embed).
    existing_ids: set[str] = set()
    if not force:
        try:
            existing_ids = await asyncio.to_thread(
                _milvus_existing_ids, [cid for cid, _ in candidates],
            )
        except Exception as e:
            logger.warning(
                "user_kb: can't list existing Milvus ids, will re-embed all: %s", e,
            )
    todo = [(cid, row) for cid, row in candidates if cid not in existing_ids]
    if not todo:
        logger.info("user_kb: all %d chunks already have vectors", len(candidates))
        return 0

    logger.info(
        "user_kb: embedding %d/%d chunks%s",
        len(todo), len(candidates), " (force=True)" if force else "",
    )

    # 4. Embed + upsert in batches.
    written = 0
    now_ms = int(_now().timestamp() * 1000)
    for i in range(0, len(todo), batch_size):
        batch = todo[i : i + batch_size]
        texts = [row["text"] for _, row in batch]
        try:
            vectors = await user_kb_embedder.embed_batch(texts)
        except user_kb_embedder.EmbedderUnavailable as e:
            logger.warning(
                "user_kb: embedder down mid-backfill — wrote %d so far: %s",
                written, e,
            )
            return written
        except Exception as e:
            logger.exception("user_kb: embed_batch error during backfill: %s", e)
            return written

        pending = [
            user_kb_vector.PendingChunk(
                chunk_id=cid,
                document_id=str(row["document_id"]),
                user_id=str(row.get("user_id") or ""),
                chunk_index=int(row.get("chunk_index") or 0),
                text=row["text"],
                created_at_ms=now_ms,
                dense_vector=vec,
            )
            for (cid, row), vec in zip(batch, vectors)
            if vec
        ]
        n = await user_kb_vector.upsert_chunks(pending)
        written += n
        logger.info(
            "user_kb: embedded %d/%d (cumulative)", written, len(todo),
        )
    return written


def _milvus_existing_ids(chunk_ids: list[str]) -> set[str]:
    """Query Milvus for which of these chunk_ids are already stored.

    Sync helper — call via ``asyncio.to_thread``. Returns an empty set on
    any error so callers default to "re-embed everything" (safer than
    silently skipping missing rows).
    """
    if not chunk_ids:
        return set()
    from pymilvus import MilvusClient  # type: ignore  # noqa: F401 - import check
    try:
        mc = user_kb_vector._get_milvus_client()
    except user_kb_vector.VectorStoreUnavailable:
        return set()
    coll = get_settings().effective_user_kb_milvus_collection
    # Milvus filter expressions have a practical payload limit; page the
    # IN clause at 1000 ids per round-trip.
    existing: set[str] = set()
    PAGE = 1000
    for i in range(0, len(chunk_ids), PAGE):
        ids_slice = chunk_ids[i : i + PAGE]
        quoted = ", ".join(f'"{cid}"' for cid in ids_slice)
        try:
            rows = mc.query(
                collection_name=coll,
                filter=f"chunk_id in [{quoted}]",
                output_fields=["chunk_id"],
                limit=len(ids_slice),
            )
            for r in rows:
                existing.add(str(r.get("chunk_id", "")))
        except Exception as e:
            logger.debug("milvus query page failed: %s", e)
            continue
    return existing


async def recover_stuck_parses() -> int:
    """On process start, reclaim any doc whose parse was interrupted.

    Runs once at startup (and can be re-invoked manually). Re-enqueues:
      * ``pending`` docs that were uploaded but never parsed (upload
        happened, then crash before ``schedule_parse`` ran, or the fire-and-
        forget task was lost), and
      * ``parsing`` docs older than ``user_kb_parse_stale_seconds`` (the
        worker is presumed dead).

    Fresh ``parsing`` docs are NOT re-enqueued — they are probably being
    worked on right now by a live task, and re-enqueuing would race.

    Returns the number re-enqueued.
    """
    await ensure_indexes()
    settings = get_settings()
    stale_cutoff = _now() - _timedelta(seconds=settings.user_kb_parse_stale_seconds)
    count = 0
    cursor = _docs().find(
        {
            "$or": [
                {"parse_status": ParseStatus.PENDING},
                {
                    "parse_status": ParseStatus.PARSING,
                    "parse_started_at": {"$lt": stale_cutoff},
                },
            ],
        },
        {"_id": 1, "parse_status": 1, "parse_started_at": 1},
    )
    async for row in cursor:
        # Reset to pending so the atomic claim in parse_document can succeed.
        await _docs().update_one(
            {"_id": row["_id"]},
            {"$set": {"parse_status": ParseStatus.PENDING, "updated_at": _now()}},
        )
        schedule_parse(str(row["_id"]))
        count += 1
    if count:
        logger.info("user_kb recovery: re-enqueued %d stuck parses", count)
    return count


# Minimum age of a PENDING doc before the ASR recovery sweep touches it — keeps
# the sweep from racing the initial ``schedule_parse`` call fired by upload.
_ASR_SWEEP_MIN_PENDING_SECONDS = 20


async def asr_recovery_sweep_loop(interval_seconds: int = 60) -> None:
    """Background loop: re-enqueue pending audio docs when ASR recovers.

    Lifecycle contract for an audio upload:

    1. ``create_document`` writes bytes to GridFS + the documents row with
       ``parse_status=pending`` — the user's file is safe regardless of
       what happens next.
    2. ``schedule_parse`` fires ``_parse_audio`` → the ASR client. If the
       jumpbox tunnel is up, great: transcription proceeds.
    3. If the tunnel is DOWN, ``_parse_audio`` catches ``AsrUnavailable``
       after a short in-loop retry and ``parse_document`` releases the doc
       back to ``parse_status=pending`` with a user-visible phase string
       ("ASR 服务暂不可用，已加入队列…"). The doc is **not** marked FAILED.
    4. This loop probes ``user_kb_asr_client.probe()`` every
       ``interval_seconds``. When the probe succeeds, it finds all PENDING
       audio docs older than ``_ASR_SWEEP_MIN_PENDING_SECONDS`` and calls
       ``schedule_parse`` on them — realizing the "服务恢复后会自动续转"
       promise the UI banner makes.

    Non-audio PENDING docs are ignored here — they're handled by
    ``recover_stuck_parses`` at startup and shouldn't depend on ASR health.

    Exceptions inside the loop are logged and swallowed; the only way out
    is cancellation (on shutdown).
    """
    logger.info(
        "user_kb asr recovery sweep started (interval=%ds)", interval_seconds,
    )
    # Tiny delay so we don't hit probe() during the narrow window where other
    # lifespan bootstrap is still importing — everything else is idempotent.
    await asyncio.sleep(5)
    while True:
        try:
            ok, reason = await user_kb_asr_client.probe()
            if not ok:
                logger.debug(
                    "user_kb asr recovery sweep: service still down: %s", reason,
                )
                await asyncio.sleep(interval_seconds)
                continue
            cutoff = _now() - _timedelta(seconds=_ASR_SWEEP_MIN_PENDING_SECONDS)
            cursor = _docs().find(
                {
                    "parse_status": ParseStatus.PENDING,
                    "updated_at": {"$lt": cutoff},
                },
                {"_id": 1, "original_filename": 1},
            )
            scheduled = 0
            async for row in cursor:
                filename = row.get("original_filename") or ""
                if not user_kb_parser.is_audio(filename):
                    # Non-audio PENDING is a different bug (upload race or
                    # parser crash) — don't mass-reschedule those here.
                    continue
                schedule_parse(str(row["_id"]))
                scheduled += 1
            if scheduled:
                logger.info(
                    "user_kb asr recovery sweep: re-scheduled %d pending audio doc(s)",
                    scheduled,
                )
        except asyncio.CancelledError:
            logger.info("user_kb asr recovery sweep cancelled")
            raise
        except Exception:
            logger.exception(
                "user_kb asr recovery sweep iteration failed (non-fatal)",
            )
        await asyncio.sleep(interval_seconds)


# ── Listing / detail / update / delete ─────────────────────────


async def list_documents(
    user_id: str,
    *,
    status: str | None = None,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
    scope: str = SCOPE_PERSONAL,
    folder_id: str | None = None,
    include_unfiled: bool = False,
) -> tuple[list[dict], int]:
    """Paginated listing. Returns (items, total).

    :param scope: ``personal`` (default) only the user's own uploads;
        ``public`` returns every public doc.
    :param folder_id: restrict to a specific folder. Pass ``None`` to include
        all folders in the scope; pass ``include_unfiled=True`` to restrict
        to docs whose folder_id is NULL/missing.
    """
    if scope == SCOPE_PERSONAL and not user_id:
        return [], 0
    await ensure_indexes()
    q: dict[str, Any] = dict(_scope_match_filter(user_id, scope))
    if status:
        q["parse_status"] = status
    if folder_id is not None:
        q["folder_id"] = folder_id
    elif include_unfiled:
        q["$and"] = [
            {"$or": [{"folder_id": {"$exists": False}}, {"folder_id": None}]},
        ]
    if search:
        # Simple case-insensitive substring against title + filename — more
        # predictable for "find my file" UX than $text here.
        needle = search.strip()
        if needle:
            q["$or"] = [
                {"title": {"$regex": _escape_regex(needle), "$options": "i"}},
                {"original_filename": {"$regex": _escape_regex(needle), "$options": "i"}},
            ]
    total = await _docs().count_documents(q)
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    cursor = (
        _docs()
        .find(q)
        .sort("created_at", DESCENDING)
        .skip(offset)
        .limit(limit)
    )
    rows = [_serialize_doc(d) async for d in cursor]
    return rows, total


def _escape_regex(s: str) -> str:
    return "".join("\\" + c if c in r"\.^$*+?()[]{}|" else c for c in s)


async def get_document(user_id: str, document_id: str) -> dict | None:
    """Fetch a document by id, enforcing user scope. Returns serialized dict or None.

    Use this for *management* (list / edit / delete) — a user must not be
    able to modify another user's docs. For cross-user *read* (the shared
    retrieval path used by the chat tool), use :func:`get_any_document`.
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    row = await _docs().find_one({"_id": oid, "user_id": user_id})
    return _serialize_doc(row) if row else None


async def get_accessible_document(user_id: str, document_id: str) -> dict | None:
    """Return a doc iff the user can read it — own personal or any public."""
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    row = await _docs().find_one({
        "_id": oid,
        "$or": [
            # own personal doc (including legacy docs without a scope field)
            {
                "user_id": user_id,
                "$or": [
                    {"scope": {"$exists": False}},
                    {"scope": SCOPE_PERSONAL},
                ],
            },
            # any public doc, regardless of uploader
            {"scope": SCOPE_PUBLIC},
        ],
    })
    if not row:
        return None
    out = _serialize_doc(row)
    # Legacy audio docs were parsed before per-segment transcripts existed
    # — the extracted text lives in the chunks collection as one blob but
    # ``audio.segments`` was never written. Synthesize pseudo-segments from
    # the chunks on read so the detail page renders without requiring a
    # reparse. Stored summary/chapters are untouched.
    if (
        user_kb_parser.is_audio(row.get("original_filename") or "")
        and row.get("parse_status") == "completed"
    ):
        audio_field = row.get("audio") if isinstance(row.get("audio"), dict) else None
        has_segments = (
            audio_field is not None
            and isinstance(audio_field.get("segments"), list)
            and len(audio_field["segments"]) > 0
        )
        if not has_segments:
            synth = await _synth_audio_segments(document_id, audio_field)
            if synth:
                existing = out.get("audio") if isinstance(out.get("audio"), dict) else {}
                out["audio"] = {
                    **(existing or {}),
                    "segments": synth,
                }
                # Backfill duration / language from the synth if absent, so
                # the UI shows correct chips in the header.
                if not out["audio"].get("duration_seconds"):
                    out["audio"]["duration_seconds"] = (
                        (synth[-1]["end_ms"] or 0) / 1000 if synth else None
                    )
    return out


async def _synth_audio_segments(
    document_id: str, audio_field: dict | None,
) -> list[dict]:
    """Produce pseudo-segments for a legacy audio doc.

    Splits the reassembled chunk text by blank lines or Chinese / English
    sentence terminators and spreads timestamps proportionally across the
    known duration (falling back to 1 hour). Used by ``get_accessible_document``
    so the frontend never has to handle two audio-shape variants.
    """
    text = await _get_document_content_scoped(document_id, max_chars=80_000)
    text = (text or "").strip()
    if not text:
        return []
    paras = [p.strip() for p in text.replace("\r\n", "\n").split("\n\n") if p.strip()]
    if len(paras) <= 2 and len(text) > 400:
        acc: list[str] = []
        cur = ""
        for ch in text:
            cur += ch
            if ch in "。！？!?\n" and len(cur.strip()) >= 90:
                acc.append(cur.strip())
                cur = ""
        if cur.strip():
            acc.append(cur.strip())
        if len(acc) > len(paras):
            paras = acc
    if not paras:
        return []
    dur_s = (audio_field or {}).get("duration_seconds") or 3600
    try:
        dur_s = float(dur_s)
    except (TypeError, ValueError):
        dur_s = 3600.0
    total_chars = sum(len(p) for p in paras) or 1
    offset = 0
    out: list[dict] = []
    for i, p in enumerate(paras):
        start = int((offset / total_chars) * dur_s * 1000)
        offset += len(p)
        end = int((offset / total_chars) * dur_s * 1000)
        out.append({
            "index": i,
            "start_ms": start,
            "end_ms": end,
            "text": p,
        })
    return out


async def get_accessible_document_content(
    user_id: str, document_id: str, *, max_chars: int | None = None,
) -> str | None:
    """Full extracted text if the user can read the doc (own or public)."""
    doc = await get_accessible_document(user_id, document_id)
    if doc is None:
        return None
    return await _get_document_content_scoped(
        document_id, max_chars=max_chars,
    )


async def get_folder_document_counts(
    user_id: str, scope: str,
) -> dict[str, int]:
    """Count visible docs per folder_id for a user in a given scope.

    Returns ``{folder_id_str: count}``. Docs without a folder_id are counted
    under the key ``""`` (empty string) for "unfiled".
    """
    match = _scope_match_filter(user_id, scope)
    pipeline: list[dict] = [
        {"$match": match},
        {"$group": {"_id": "$folder_id", "n": {"$sum": 1}}},
    ]
    counts: dict[str, int] = {}
    async for row in _docs().aggregate(pipeline):
        key = row.get("_id") or ""
        counts[str(key)] = int(row.get("n") or 0)
    return counts


async def delete_documents_by_folder_ids(
    folder_ids: list[str],
) -> int:
    """Cascade-delete all documents whose folder_id is in the given list.

    Used by the folder-delete API after Postgres removes the folder rows.
    Walks each doc so we can clean up GridFS + chunks + vectors too, not
    just orphan the Mongo row.
    """
    if not folder_ids:
        return 0
    cursor = _docs().find(
        {"folder_id": {"$in": list(folder_ids)}},
        {"_id": 1, "user_id": 1, "gridfs_file_id": 1},
    )
    deleted = 0
    async for row in cursor:
        try:
            doc_id_str = str(row["_id"])
            await _chunks().delete_many(
                {"user_id": row.get("user_id") or "", "document_id": row["_id"]},
            )
            try:
                await user_kb_vector.delete_by_document(doc_id_str)
            except Exception as e:
                logger.warning(
                    "user_kb Milvus cleanup failed for %s during folder delete: %s",
                    doc_id_str, e,
                )
            gridfs_id = row.get("gridfs_file_id")
            if gridfs_id is not None:
                try:
                    await _gridfs().delete(gridfs_id)
                except Exception as e:
                    logger.warning(
                        "gridfs delete failed for %s during folder delete: %s",
                        doc_id_str, e,
                    )
            await _docs().delete_one({"_id": row["_id"]})
            deleted += 1
        except Exception as e:
            logger.warning(
                "cascade delete failed for doc %s (folder removal): %s",
                row.get("_id"), e,
            )
    return deleted


async def get_any_document(document_id: str) -> dict | None:
    """Fetch a document by id **without** user scoping.

    The personal knowledge base is shared team-wide for retrieval, so the
    chat tool needs to be able to read any doc that ``search_chunks`` with
    no user filter surfaced. Callers that serve this endpoint to the user
    directly (HTTP, etc.) must double-check authorization — only the chat
    tool is expected to use the cross-user path.
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    row = await _docs().find_one({"_id": oid})
    return _serialize_doc(row) if row else None


async def get_document_content(
    user_id: str, document_id: str, *, max_chars: int | None = None,
) -> str | None:
    """Return the full extracted text for a user's own document (scoped)."""
    return await _get_document_content_scoped(
        document_id, max_chars=max_chars, user_id=user_id,
    )


async def get_any_document_content(
    document_id: str, *, max_chars: int | None = None,
) -> str | None:
    """Return the full extracted text for any user's document (cross-user).

    Used by the chat tool's ``user_kb_fetch_document`` — the KB is shared for
    reads. Management endpoints should keep using :func:`get_document_content`.
    """
    return await _get_document_content_scoped(document_id, max_chars=max_chars)


async def _get_document_content_scoped(
    document_id: str,
    *,
    max_chars: int | None,
    user_id: str | None = None,
) -> str | None:
    """Internal: assemble full extracted text. ``user_id=None`` means no scope."""
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    doc_filter: dict[str, Any] = {"_id": oid}
    if user_id:
        doc_filter["user_id"] = user_id
    row = await _docs().find_one(
        doc_filter,
        {"_id": 1, "num_chunks": 1, "extracted_text": 1,
         "extracted_char_count": 1, "user_id": 1},
    )
    if row is None:
        return None
    char_count = int(row.get("extracted_char_count") or 0)
    inline = row.get("extracted_text") or ""
    if char_count <= len(inline):
        return inline[:max_chars] if max_chars else inline
    # Reassemble from chunks. Always filter chunks by the owning user_id
    # (from the doc row, not the caller) so we don't leak a partial chunk
    # set if the chunks collection ever holds stray rows.
    owner = row.get("user_id") or ""
    chunk_filter: dict[str, Any] = {"document_id": oid}
    if owner:
        chunk_filter["user_id"] = owner
    parts: list[str] = []
    cursor = _chunks().find(chunk_filter).sort("chunk_index", ASCENDING)
    async for c in cursor:
        parts.append(c.get("text") or "")
        if max_chars and sum(len(p) for p in parts) >= max_chars:
            break
    text = "\n\n".join(parts)
    return text[:max_chars] if max_chars else text


async def generate_audio_summary(
    user_id: str, document_id: str, *, force: bool = False,
) -> tuple[str, bool] | None:
    """Generate or fetch a cached AI summary for an audio document.

    Returns ``(summary_text, cached)`` — ``cached=True`` means we returned
    a previously-generated summary and did not re-run the LLM. Returns
    ``None`` if the doc doesn't exist, isn't accessible to this user, isn't
    audio, or hasn't finished parsing.

    The summary is persisted to ``audio.summary`` so repeat opens of the
    drawer are instant.
    """
    # Lazy imports keep this module importable without chat_llm's heavy deps.
    from backend.app.services import chat_llm

    doc = await get_accessible_document(user_id, document_id)
    if doc is None:
        return None
    audio = doc.get("audio") if isinstance(doc.get("audio"), dict) else {}
    # Accept legacy audio docs that were parsed before we persisted per-
    # segment data — we can still summarize from the reassembled full text.
    if not user_kb_parser.is_audio(doc.get("original_filename") or ""):
        return None
    existing = audio.get("summary") if isinstance(audio, dict) else None
    if existing and not force:
        return str(existing), True

    # Prefer the segment array (authoritative for new uploads). Fall back
    # to the full reassembled extracted text so legacy docs still work.
    segs = audio.get("segments") if isinstance(audio, dict) else None
    if segs:
        full_text = "\n".join(
            str(s.get("text") or "").strip() for s in segs if isinstance(s, dict)
        ).strip()
    else:
        full_text = (
            await _get_document_content_scoped(document_id, max_chars=40_000)
            or ""
        ).strip()
    if not full_text:
        return None
    # Cap the input so extremely long meetings still fit the context window
    # comfortably. A 20-min clip at ~300 CN chars / 60s is ~6K chars; we
    # allow up to 40K which covers ~2 hours of dialog comfortably.
    truncated = full_text[:40_000]

    system = (
        "你是一位专业的会议纪要 / 访谈纪要助手。基于用户提供的录音转写文本，"
        "输出一份高质量、结构化的中文 Markdown 会议要点。严格遵守下列格式：\n\n"
        "# 会议要点\n"
        "## 1. <第一个核心议题的凝练标题>\n"
        "- <关键结论 1>\n"
        "- <关键数据 / 事实 2>\n"
        "- <关键观点 / 推断 3>\n"
        "## 2. <第二个核心议题的凝练标题>\n"
        "- ...\n\n"
        "要求：\n"
        "1. 拆分为 3 到 8 个编号议题，标题要短而有信息量（例如“OCS 市场需求与出货量预期”）。\n"
        "2. 每条 bullet 一句话，信息密度高，保留数据、公司名、时间点。\n"
        "3. 可以使用加粗 **关键词** 强化重点。\n"
        "4. 不重复原文，不虚构，不输出前言或结语。"
    )

    try:
        result = await chat_llm.call_model_sync(
            "openai/gpt-4o-mini",
            [
                {"role": "system", "content": system},
                {"role": "user", "content": truncated},
            ],
            max_tokens=1800,
        )
    except Exception as e:
        logger.exception("audio summary generation failed for %s", document_id)
        raise RuntimeError(f"summary generation failed: {type(e).__name__}: {e}") from e

    summary = (result.get("content") or "").strip()
    if result.get("error") or not summary:
        raise RuntimeError(
            f"summary generation returned empty: {result.get('error') or 'no content'}"
        )

    try:
        oid = _oid(document_id)
    except ValueError:
        return summary, False
    now = _now()
    await _docs().update_one(
        {"_id": oid},
        {
            "$set": {
                "audio.summary": summary,
                "audio.summary_generated_at": now,
                "updated_at": now,
            },
        },
    )
    return summary, False


# ── Segmented (per-chapter) summary ───────────────────────────

_CHAPTER_SUMMARY_MAX_CHAPTERS = 8
_CHAPTER_SUMMARY_MIN_CHAPTERS = 3


async def generate_audio_chapter_summary(
    user_id: str, document_id: str, *, force: bool = False,
) -> tuple[list[dict], bool] | None:
    """Generate per-chapter summary: group segments into 3–8 chapters, each
    with a title + 2–5 bullet points + ``start_ms`` anchor so the UI can
    link each chapter back to a seek point in the audio.

    Returns ``(chapters, cached)`` — ``cached=True`` means we returned a
    previously-generated chapter list and did not re-run the LLM. Returns
    ``None`` if the doc isn't accessible or doesn't qualify as audio.

    Result persisted to ``audio.chapters`` as:
    ``[{index, title, start_ms, end_ms, bullets: [str, ...]}]``.
    """
    import json as _json

    from backend.app.services import chat_llm

    doc = await get_accessible_document(user_id, document_id)
    if doc is None:
        return None
    if not user_kb_parser.is_audio(doc.get("original_filename") or ""):
        return None
    audio = doc.get("audio") if isinstance(doc.get("audio"), dict) else {}
    existing = audio.get("chapters") if isinstance(audio, dict) else None
    if existing and not force and isinstance(existing, list):
        return list(existing), True

    segs = audio.get("segments") if isinstance(audio, dict) else None
    segs = [s for s in (segs or []) if isinstance(s, dict)]

    # Legacy audio docs (parsed before per-segment transcripts existed) have
    # no segments on disk. Synthesize pseudo-segments by splitting the
    # reassembled extracted text on blank lines + sentence terminators, and
    # spread timestamps proportionally across the known duration (or a 1h
    # fallback). This keeps old uploads viewable without forcing a reparse.
    if not segs:
        full_text = (
            await _get_document_content_scoped(document_id, max_chars=60_000)
            or ""
        ).strip()
        if not full_text:
            return None
        # Prefer blank-line paragraphs; fall back to sentence splits.
        paras = [p.strip() for p in full_text.split("\n\n") if p.strip()]
        if len(paras) <= 2 and len(full_text) > 400:
            acc: list[str] = []
            cur = ""
            for ch in full_text:
                cur += ch
                if ch in "。！？!?\n" and len(cur.strip()) >= 90:
                    acc.append(cur.strip())
                    cur = ""
            if cur.strip():
                acc.append(cur.strip())
            if len(acc) > len(paras):
                paras = acc
        dur_s = (audio.get("duration_seconds") if isinstance(audio, dict) else None) or 3600
        total_chars_text = sum(len(p) for p in paras) or 1
        offset = 0
        synth: list[dict] = []
        for i, p in enumerate(paras):
            start = int((offset / total_chars_text) * dur_s * 1000)
            offset += len(p)
            end = int((offset / total_chars_text) * dur_s * 1000)
            synth.append({
                "index": i, "start_ms": start, "end_ms": end, "text": p,
            })
        segs = synth

    # Build a compact numbered-segment transcript for the LLM. We cap the
    # body so a very long meeting still fits in the context window.
    #
    # Keep ``start_ms`` inline so the model can pick anchor points that we
    # pass back through to the chapter object. The model doesn't need the
    # end_ms — each chapter's end is the next chapter's start.
    lines: list[str] = []
    total_chars = 0
    CAP = 40_000
    for s in segs:
        ms = int(s.get("start_ms") or 0)
        text = str(s.get("text") or "").strip()
        if not text:
            continue
        m = ms // 1000 // 60
        sec = (ms // 1000) % 60
        ts = f"{m:02d}:{sec:02d}"
        line = f"[{s.get('index', 0)}|{ts}] {text}"
        if total_chars + len(line) > CAP:
            break
        lines.append(line)
        total_chars += len(line) + 1
    if not lines:
        return None
    body = "\n".join(lines)

    system = (
        "你是一位专业的会议纪要助手。用户会给你一份带编号和时间戳的录音转写片段。"
        "你的任务：把整段录音切分为 3–8 个语义连贯的章节，每章一个小标题，"
        "再用 2–5 条 bullet 概括该章节要点。\n\n"
        "输出严格的 JSON（不要加任何解释性文字、不要用 ```json 包裹）：\n"
        "{\n"
        "  \"chapters\": [\n"
        "    {\n"
        "      \"title\": \"章节标题（8–20 字）\",\n"
        "      \"start_index\": <起始片段编号，整数>,\n"
        "      \"bullets\": [\"要点 1\", \"要点 2\"]\n"
        "    }\n"
        "  ]\n"
        "}\n\n"
        "硬性要求：\n"
        "- chapters 至少 3 条、最多 8 条，必须按 start_index 严格升序。\n"
        "- 第一个 chapter 的 start_index 必须是片段里最小的那个编号。\n"
        "- 标题简洁有信息量（例：“OCS 市场需求与出货量预期”）。\n"
        "- bullet 精炼，不重复原文，保留数据、公司、时间点。\n"
        "- 禁止虚构，禁止输出 JSON 之外的任何字符。"
    )

    try:
        result = await chat_llm.call_model_sync(
            "openai/gpt-4o-mini",
            [
                {"role": "system", "content": system},
                {"role": "user", "content": body},
            ],
            max_tokens=2000,
        )
    except Exception as e:
        logger.exception("audio chapter summary failed for %s", document_id)
        raise RuntimeError(
            f"chapter summary generation failed: {type(e).__name__}: {e}"
        ) from e

    raw = (result.get("content") or "").strip()
    if result.get("error") or not raw:
        raise RuntimeError(
            f"chapter summary returned empty: {result.get('error') or 'no content'}"
        )
    # Tolerate the model wrapping in a fenced block despite the instruction.
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    try:
        parsed = _json.loads(raw)
    except Exception as e:
        raise RuntimeError(
            f"chapter summary returned invalid JSON: {type(e).__name__}: {e}"
        ) from e
    chapters_raw = parsed.get("chapters") if isinstance(parsed, dict) else None
    if not isinstance(chapters_raw, list) or not chapters_raw:
        raise RuntimeError("chapter summary returned empty chapters list")

    # Map each chapter's start_index back to a real segment; compute end_ms
    # as the next chapter's start_ms (or the last segment's end for the tail).
    by_index = {int(s.get("index", -1)): s for s in segs}
    chapters: list[dict] = []
    for i, ch in enumerate(chapters_raw):
        if not isinstance(ch, dict):
            continue
        try:
            si = int(ch.get("start_index"))
        except (TypeError, ValueError):
            # Pin to the first segment — better than dropping the chapter.
            si = int(segs[0].get("index") or 0)
        seg = by_index.get(si)
        if seg is None:
            # Find closest segment >= si (handles model drift).
            cand = [s for s in segs if int(s.get("index", -1)) >= si]
            seg = cand[0] if cand else segs[0]
        title = str(ch.get("title") or "").strip() or f"章节 {i + 1}"
        bullets_raw = ch.get("bullets") if isinstance(ch, dict) else []
        bullets = [
            str(b).strip() for b in (bullets_raw or [])
            if isinstance(b, (str, int, float)) and str(b).strip()
        ]
        chapters.append({
            "index": i,
            "title": title[:120],
            "start_ms": int(seg.get("start_ms") or 0),
            "end_ms": 0,  # filled below
            "start_segment_index": int(seg.get("index", 0)),
            "bullets": bullets[:8],
        })

    if not chapters:
        raise RuntimeError("chapter summary produced no valid chapters")

    # Fill end_ms: each chapter ends where the next starts; the tail runs
    # to the final segment's end.
    final_end = int(segs[-1].get("end_ms") or segs[-1].get("start_ms") or 0)
    for i, ch in enumerate(chapters):
        ch["end_ms"] = (
            chapters[i + 1]["start_ms"] if i + 1 < len(chapters) else final_end
        )

    try:
        oid = _oid(document_id)
    except ValueError:
        return chapters, False
    now = _now()
    await _docs().update_one(
        {"_id": oid},
        {
            "$set": {
                "audio.chapters": chapters,
                "audio.chapters_generated_at": now,
                "updated_at": now,
            },
        },
    )
    return chapters, False


# ── Segment text editing ──────────────────────────────────────

async def update_audio_segment_text(
    user_id: str,
    document_id: str,
    segment_index: int,
    new_text: str,
) -> dict | None:
    """Edit a single transcript segment's text (ASR correction).

    Only the uploader (or an admin-side caller with already-validated access)
    can edit; we re-check here by requiring the segment to exist on a doc the
    user owns. Returns the refreshed audio meta dict, or ``None`` if the
    document / segment was not found.

    Editing a segment invalidates the cached ``summary`` and ``chapters``
    because the underlying text has changed.
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    doc = await _docs().find_one({
        "_id": oid,
        "user_id": user_id,
    })
    if doc is None:
        return None
    audio = doc.get("audio") if isinstance(doc.get("audio"), dict) else {}
    segs = list(audio.get("segments") or [])
    if not segs:
        return None
    found = False
    for s in segs:
        if isinstance(s, dict) and int(s.get("index", -1)) == int(segment_index):
            s["text"] = str(new_text or "").strip()
            found = True
            break
    if not found:
        return None
    now = _now()
    await _docs().update_one(
        {"_id": oid},
        {
            "$set": {
                "audio.segments": segs,
                "audio.summary": None,
                "audio.summary_generated_at": None,
                "audio.chapters": None,
                "audio.chapters_generated_at": None,
                "updated_at": now,
            },
        },
    )
    refreshed = await _docs().find_one({"_id": oid}, {"audio": 1})
    return (refreshed or {}).get("audio") or {}


async def update_document(
    user_id: str,
    document_id: str,
    *,
    title: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    folder_id: str | None = ...,  # sentinel — "..." means "don't touch"
    scope: str | None = None,
) -> dict | None:
    """Patch user-editable fields. Returns the updated serialized doc or None.

    ``folder_id`` uses the ``...`` sentinel so callers can pass ``None`` to
    mean "move to unfiled" versus omitting the field.
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    update: dict[str, Any] = {"updated_at": _now()}
    if title is not None:
        stripped = title.strip()
        if not stripped:
            raise ValueError("title cannot be empty")
        update["title"] = stripped[:500]
    if description is not None:
        update["description"] = description.strip()[:5000]
    if folder_id is not ...:
        update["folder_id"] = folder_id  # may be a UUID string or None
    if scope is not None:
        if scope not in (SCOPE_PERSONAL, SCOPE_PUBLIC):
            raise ValueError("scope must be 'personal' or 'public'")
        update["scope"] = scope
    if tags is not None:
        # Normalize: strip, dedupe preserving order, cap at 32 tags @ 64 chars.
        seen: set[str] = set()
        normalized: list[str] = []
        for t in tags:
            if not isinstance(t, str):
                continue
            t = t.strip()[:64]
            if t and t not in seen:
                seen.add(t)
                normalized.append(t)
                if len(normalized) >= 32:
                    break
        update["tags"] = normalized
    if len(update) == 1:  # only updated_at
        # Nothing substantive to change — return current state without writing.
        return await get_document(user_id, document_id)

    # Allow editing: either the uploader (user_id match) OR, if the doc is
    # public, any admin/boss user (the API layer gates this by refusing to
    # pass allow_public_override=True for non-admin callers).
    res = await _docs().find_one_and_update(
        {"_id": oid, "user_id": user_id},
        {"$set": update},
        return_document=True,
    )
    return _serialize_doc(res) if res else None


async def update_public_document(
    document_id: str,
    *,
    title: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    folder_id: str | None = ...,
    scope: str | None = None,
) -> dict | None:
    """Admin-only: patch a public doc regardless of original uploader.

    The API layer MUST check the caller's role before invoking this — the
    service layer deliberately does not re-check.
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        return None
    update: dict[str, Any] = {"updated_at": _now()}
    if title is not None:
        stripped = title.strip()
        if not stripped:
            raise ValueError("title cannot be empty")
        update["title"] = stripped[:500]
    if description is not None:
        update["description"] = description.strip()[:5000]
    if folder_id is not ...:
        update["folder_id"] = folder_id
    if scope is not None:
        if scope not in (SCOPE_PERSONAL, SCOPE_PUBLIC):
            raise ValueError("scope must be 'personal' or 'public'")
        update["scope"] = scope
    if tags is not None:
        seen: set[str] = set()
        normalized: list[str] = []
        for t in tags:
            if not isinstance(t, str):
                continue
            t = t.strip()[:64]
            if t and t not in seen:
                seen.add(t)
                normalized.append(t)
                if len(normalized) >= 32:
                    break
        update["tags"] = normalized
    res = await _docs().find_one_and_update(
        {"_id": oid, "scope": SCOPE_PUBLIC},
        {"$set": update},
        return_document=True,
    )
    return _serialize_doc(res) if res else None


async def delete_public_document(document_id: str) -> bool:
    """Admin-only: remove a public doc. Gated by caller role."""
    try:
        oid = _oid(document_id)
    except ValueError:
        return False
    row = await _docs().find_one({"_id": oid, "scope": SCOPE_PUBLIC})
    if row is None:
        return False
    owner = row.get("user_id") or ""
    await _chunks().delete_many({"user_id": owner, "document_id": oid})
    try:
        await user_kb_vector.delete_by_document(str(oid))
    except Exception as e:
        logger.warning("user_kb Milvus delete failed (public) for %s: %s", oid, e)
    gridfs_id = row.get("gridfs_file_id")
    if gridfs_id is not None:
        try:
            await _gridfs().delete(gridfs_id)
        except Exception as e:
            logger.warning(
                "gridfs delete failed (public) for %s: %s", oid, e,
            )
    result = await _docs().delete_one({"_id": oid, "scope": SCOPE_PUBLIC})
    return result.deleted_count > 0


async def delete_document(user_id: str, document_id: str) -> bool:
    """Delete the document row, its chunks, and its GridFS binary.

    Returns True if a row was deleted. Safe to call on a missing id — returns
    False. The three writes are sequenced (chunks → gridfs → row) so a crash
    mid-delete leaves the row as the authoritative "still exists" signal; the
    recovery sweeper can retry.
    """
    try:
        oid = _oid(document_id)
    except ValueError:
        return False
    row = await _docs().find_one({"_id": oid, "user_id": user_id})
    if row is None:
        return False
    await _chunks().delete_many({"user_id": user_id, "document_id": oid})
    # Cascade into the dense-vector store. Fail-open: a leftover vector
    # chunk is harmless (hybrid search joins on Mongo chunk ids and will
    # filter it out when it can't hydrate), but we still try.
    try:
        await user_kb_vector.delete_by_document(str(oid))
    except Exception as e:
        logger.warning("user_kb Milvus delete failed for doc=%s: %s", oid, e)
    gridfs_id = row.get("gridfs_file_id")
    if gridfs_id is not None:
        try:
            await _gridfs().delete(gridfs_id)
        except Exception as e:
            # Missing GridFS file is non-fatal — carry on deleting the row.
            logger.warning(
                "gridfs delete failed for doc=%s (gridfs=%s): %s",
                document_id, gridfs_id, e,
            )
    result = await _docs().delete_one({"_id": oid, "user_id": user_id})
    return result.deleted_count > 0


async def reparse_document(user_id: str, document_id: str) -> bool:
    """Kick off a fresh parse, typically after a failure. Returns True on schedule."""
    try:
        oid = _oid(document_id)
    except ValueError:
        return False
    res = await _docs().update_one(
        {"_id": oid, "user_id": user_id},
        {
            "$set": {
                "parse_status": ParseStatus.PENDING,
                "parse_error": None,
                "parse_started_at": None,
                "parse_completed_at": None,
                # Reset progress fields so the UI doesn't show a stale bar
                # from a previous failed ASR attempt.
                "parse_progress_percent": 0,
                "parse_phase": "",
                "updated_at": _now(),
            },
        },
    )
    if res.matched_count == 0:
        return False
    schedule_parse(document_id)
    return True


async def download_file(user_id: str, document_id: str) -> tuple[dict, bytes] | None:
    """Return (meta, bytes) of the original upload so the API can stream it."""
    meta = await _docs().find_one({"_id": _oid(document_id), "user_id": user_id})
    if meta is None:
        return None
    gridfs_id = meta.get("gridfs_file_id")
    if not gridfs_id:
        return None
    stream = await _gridfs().open_download_stream(gridfs_id)
    data = await stream.read()
    return meta, data


# ── Search (for chat tool) ─────────────────────────────────────


@dataclass
class SearchHit:
    document_id: str
    title: str
    original_filename: str
    chunk_index: int
    text: str
    score: float
    created_at: str
    # The user who originally uploaded this doc. The personal-KB is now
    # shared team-wide for retrieval, so surfacing the uploader lets the
    # reader go ask them for context if needed. Empty string when no
    # uploader is known (shouldn't happen for valid rows).
    uploader_user_id: str = ""


async def search_chunks(
    query: str,
    *,
    user_id: str | None = None,
    top_k: int = 5,
    document_ids: list[str] | None = None,
    mode: str = "hybrid",
) -> list[SearchHit]:
    """Retrieve chunks matching a natural-language query.

    :param query: Query in any language. Empty/whitespace → [].
    :param user_id: Optional per-user scope; ``None``/empty = team-shared
        (default for the chat tool). Set to a user_id to mimic
        ``scope=mine``.
    :param top_k: Final number of hits to return (1–30).
    :param document_ids: Optional: pre-filter to specific document ids.
    :param mode: ``"hybrid"`` (default) fuses BM25 + dense vector via
        Reciprocal Rank Fusion — the industry standard for production RAG.
        ``"lexical"`` uses BM25 only. ``"semantic"`` uses dense only.

    The hybrid path combines:

    * **BM25** — MongoDB ``$text`` over jieba-segmented Chinese tokens.
      High precision on exact terms (ticker codes, specific Chinese
      names, numbers).
    * **Dense vector** — OpenAI ``text-embedding-3-small`` over Milvus.
      Captures synonyms, paraphrases, and cross-lingual equivalence
      (``英伟达 ≈ NVDA``, which BM25 alone can never see).
    * **Reciprocal Rank Fusion** — robust to score-distribution
      differences between the two retrievers; standard in OpenSearch,
      Vespa, and Elasticsearch ≥ 8.9.

    Either retriever fails open: if OpenAI or Milvus is unreachable, the
    function silently degrades to whichever side is up, so chat never
    breaks on infra hiccups.
    """
    query = (query or "").strip()
    if not query:
        return []
    top_k = max(1, min(int(top_k), 30))
    mode = (mode or "hybrid").lower()
    if mode not in ("hybrid", "lexical", "semantic"):
        raise ValueError(f"invalid search mode: {mode}")

    # Over-fetch each side: ranks 20-30 on one retriever often rise to
    # top 5 after fusion if the other retriever loves them.
    candidates = max(top_k * 4, 30)

    lex_task: asyncio.Task[list[SearchHit]] | None = None
    vec_task: asyncio.Task[list[SearchHit]] | None = None
    if mode in ("hybrid", "lexical"):
        lex_task = asyncio.create_task(
            _bm25_search_chunks(
                query, user_id=user_id, top_k=candidates, document_ids=document_ids,
            )
        )
    if mode in ("hybrid", "semantic"):
        vec_task = asyncio.create_task(
            _vector_search_chunks(
                query, user_id=user_id, top_k=candidates, document_ids=document_ids,
            )
        )

    lex_hits: list[SearchHit] = []
    vec_hits: list[SearchHit] = []
    if lex_task is not None:
        try:
            lex_hits = await lex_task
        except Exception as e:
            logger.warning("user_kb BM25 search failed (degrading): %s", e)
    if vec_task is not None:
        try:
            vec_hits = await vec_task
        except Exception as e:
            logger.warning("user_kb dense search failed (degrading): %s", e)

    if mode == "lexical":
        return lex_hits[:top_k]
    if mode == "semantic":
        return vec_hits[:top_k]

    rrf_k = max(1, int(get_settings().user_kb_rrf_k))
    return _rrf_fuse(lex_hits, vec_hits, rrf_k=rrf_k)[:top_k]


# ── Per-retriever implementations ──────────────────────────────


async def _bm25_search_chunks(
    query: str,
    *,
    user_id: str | None,
    top_k: int,
    document_ids: list[str] | None,
) -> list[SearchHit]:
    """Lexical retrieval via MongoDB ``$text`` on jieba-tokenized chunks."""
    await ensure_indexes()
    # Tokenize the query the same way we tokenized stored text at index
    # time — so a Chinese query like "接口说明" hits the pre-split
    # "接口 说明" stored in each chunk's tokens field. Fallback to raw
    # query if tokenization produces nothing (query was all punctuation).
    tokenized_query = user_kb_tokenizer.tokenize_query(query) or query

    match: dict[str, Any] = {"$text": {"$search": tokenized_query}}
    if user_id:
        match["user_id"] = user_id
    if document_ids:
        oids: list[ObjectId] = []
        for d in document_ids:
            try:
                oids.append(_oid(d))
            except ValueError:
                continue
        if not oids:
            return []
        match["document_id"] = {"$in": oids}

    pipeline: list[dict] = [
        {"$match": match},
        {"$addFields": {"score": {"$meta": "textScore"}}},
        {"$sort": {"score": {"$meta": "textScore"}}},
        {"$limit": top_k},
        {
            "$lookup": {
                # Must match the env-scoped documents collection (prod: `documents`,
                # staging: `stg_documents`) so lookup hits the same dataset the
                # chunks came from.
                "from": get_settings().user_kb_docs_collection,
                "localField": "document_id",
                "foreignField": "_id",
                "as": "document",
            },
        },
        {"$unwind": "$document"},
    ]
    # When scoped, also require the joined document to belong to the same user —
    # defense in depth against stray chunks with a forged document_id.
    if user_id:
        pipeline.append({"$match": {"document.user_id": user_id}})

    try:
        cursor = _chunks().aggregate(pipeline)
        rows = [r async for r in cursor]
    except OperationFailure as e:
        # Narrow retry: only the "text index missing" class of errors is
        # safe to paper over by (re-)creating indexes. Anything else
        # (permissions, schema mismatch, query syntax) should propagate so
        # we see it in the logs instead of silently returning empty results.
        msg = str(e)
        is_missing_text_index = (
            "text index required" in msg
            or "no text index" in msg
            or "Unable to execute query that requires a text index" in msg
        )
        if not is_missing_text_index:
            raise
        logger.warning(
            "user_kb search failed (no text index yet): %s — rebuilding and retrying", e,
        )
        _reset_index_init_for_retry()
        await ensure_indexes()
        cursor = _chunks().aggregate(pipeline)
        rows = [r async for r in cursor]

    return [_row_to_hit(r) for r in rows]


async def _vector_search_chunks(
    query: str,
    *,
    user_id: str | None,
    top_k: int,
    document_ids: list[str] | None,
) -> list[SearchHit]:
    """Dense-vector retrieval via Milvus + OpenAI embeddings. Fails open."""
    try:
        qv = await user_kb_embedder.embed_query(query)
    except user_kb_embedder.EmbedderUnavailable:
        return []
    except Exception as e:
        logger.warning("user_kb query embedding failed: %s", e)
        return []

    vector_hits = await user_kb_vector.vector_search(
        qv,
        top_k=top_k,
        user_id=user_id,
        document_ids=document_ids,
    )
    if not vector_hits:
        return []

    # Hydrate document metadata from Mongo. Milvus carries ``text`` on
    # the row itself so we don't hit the chunks collection here — just
    # the documents collection for titles / filenames / created_at.
    doc_ids: list[ObjectId] = []
    for h in vector_hits:
        try:
            doc_ids.append(_oid(h.document_id))
        except ValueError:
            continue
    docs_by_id: dict[str, dict] = {}
    if doc_ids:
        cursor = _docs().find(
            {"_id": {"$in": doc_ids}},
            {"title": 1, "original_filename": 1, "created_at": 1, "user_id": 1},
        )
        async for d in cursor:
            docs_by_id[str(d["_id"])] = d

    hits: list[SearchHit] = []
    for vh in vector_hits:
        doc = docs_by_id.get(vh.document_id) or {}
        # Drop hits whose parent doc was deleted since the vector was written.
        if not doc:
            continue
        created = doc.get("created_at")
        hits.append(
            SearchHit(
                document_id=vh.document_id,
                title=doc.get("title") or doc.get("original_filename") or "(untitled)",
                original_filename=doc.get("original_filename") or "",
                chunk_index=vh.chunk_index,
                text=vh.text,
                score=vh.score,
                created_at=created.isoformat() if isinstance(created, datetime) else "",
                uploader_user_id=str(doc.get("user_id") or vh.user_id or ""),
            )
        )
    return hits


def _row_to_hit(r: dict) -> SearchHit:
    """Convert a Mongo ``$text`` aggregation row into a SearchHit."""
    doc = r.get("document") or {}
    created = doc.get("created_at")
    return SearchHit(
        document_id=str(r["document_id"]),
        title=doc.get("title") or doc.get("original_filename") or "(untitled)",
        original_filename=doc.get("original_filename") or "",
        chunk_index=int(r.get("chunk_index") or 0),
        text=r.get("text") or "",
        score=float(r.get("score") or 0.0),
        created_at=created.isoformat() if isinstance(created, datetime) else "",
        uploader_user_id=str(doc.get("user_id") or r.get("user_id") or ""),
    )


def _rrf_fuse(
    lex: list[SearchHit],
    vec: list[SearchHit],
    *,
    rrf_k: int = 60,
) -> list[SearchHit]:
    """Reciprocal Rank Fusion of two ranked SearchHit lists.

    Each retriever contributes ``1 / (rrf_k + rank + 1)`` to a chunk's
    combined score per list it appears in. Chunks present in both lists
    accumulate both contributions — which is exactly why RRF promotes
    "agreed on by both retrievers" over "liked by only one".

    Chunks are keyed by ``(document_id, chunk_index)`` so the same chunk
    surfacing from both sides accumulates into one row rather than
    appearing twice. For the returned SearchHit we prefer whichever side
    delivered the fuller text snippet (Milvus VARCHAR clamp vs Mongo's
    larger limit can diverge by a handful of bytes on long chunks).
    """
    combined: dict[tuple[str, int], tuple[SearchHit, float]] = {}

    def _accumulate(hits: list[SearchHit]) -> None:
        for rank, h in enumerate(hits):
            key = (h.document_id, h.chunk_index)
            boost = 1.0 / (rrf_k + rank + 1)
            if key in combined:
                existing, score = combined[key]
                prefer = existing if len(existing.text) >= len(h.text) else h
                combined[key] = (prefer, score + boost)
            else:
                combined[key] = (h, boost)

    _accumulate(lex)
    _accumulate(vec)

    fused: list[SearchHit] = []
    for _, (hit, score) in sorted(
        combined.items(), key=lambda kv: kv[1][1], reverse=True,
    ):
        fused.append(
            SearchHit(
                document_id=hit.document_id,
                title=hit.title,
                original_filename=hit.original_filename,
                chunk_index=hit.chunk_index,
                text=hit.text,
                score=round(score, 4),
                created_at=hit.created_at,
                uploader_user_id=hit.uploader_user_id,
            )
        )
    return fused


def _reset_index_init_for_retry() -> None:
    """Internal — let ensure_indexes() run again after a search-time failure."""
    _indexes_by_loop.clear()


# ── Stats ──────────────────────────────────────────────────────


async def get_user_stats(user_id: str) -> dict:
    """Small dashboard summary: counts per status, total size, etc."""
    if not user_id:
        return {"total": 0}
    pipeline = [
        {"$match": {"user_id": user_id}},
        {
            "$group": {
                "_id": None,
                "total_documents": {"$sum": 1},
                "total_bytes": {"$sum": "$file_size_bytes"},
                "total_chars": {"$sum": "$extracted_char_count"},
                "total_chunks": {"$sum": "$num_chunks"},
                "status_pending": {
                    "$sum": {"$cond": [{"$eq": ["$parse_status", "pending"]}, 1, 0]},
                },
                "status_parsing": {
                    "$sum": {"$cond": [{"$eq": ["$parse_status", "parsing"]}, 1, 0]},
                },
                "status_completed": {
                    "$sum": {"$cond": [{"$eq": ["$parse_status", "completed"]}, 1, 0]},
                },
                "status_failed": {
                    "$sum": {"$cond": [{"$eq": ["$parse_status", "failed"]}, 1, 0]},
                },
            }
        },
    ]
    cursor = _docs().aggregate(pipeline)
    async for r in cursor:
        r.pop("_id", None)
        return r
    return {
        "total_documents": 0,
        "total_bytes": 0,
        "total_chars": 0,
        "total_chunks": 0,
        "status_pending": 0,
        "status_parsing": 0,
        "status_completed": 0,
        "status_failed": 0,
    }
