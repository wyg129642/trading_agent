"""KB document viewer API — used by the cell-inspector citation pop-up.

Exposes a simple authenticated GET for KB doc content with optional
``snippet`` query param. The backend locates the snippet offsets; the
frontend scrolls to them and wraps a ``<mark>`` highlight.

This is deliberately a minimal wrapper on ``kb_service.fetch_document`` so
we never bypass the existing provenance / citation logic.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.deps import get_current_user
from backend.app.models.user import User
from backend.app.services.kb_service import fetch_document

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/doc")
async def get_doc(
    doc_id: str = Query(..., description="Stable KB doc_id: <db>:<collection>:<_id>"),
    snippet: str | None = Query(None, description="Snippet to locate + highlight"),
    max_chars: int = Query(20000, ge=1000, le=30000),
    _user: User = Depends(get_current_user),
):
    """Fetch a KB document with optional snippet highlight offsets."""
    doc = await fetch_document(
        doc_id=doc_id,
        max_chars=max_chars,
        highlight_snippet=snippet,
    )
    if not doc.get("found"):
        raise HTTPException(
            status_code=404,
            detail=doc.get("error") or "document not found",
        )
    return doc
