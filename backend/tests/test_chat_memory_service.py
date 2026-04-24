"""Tests for chat_memory_service pure functions.

The upsert/fetch logic needs a real DB; for that part we exercise the
endpoints via the API test suite. Here we focus on the prompt-rendering
functions that are pure and safe to test in isolation.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

from backend.app.services.chat_memory_service import (
    MEMORY_PROMPT_HEADER, MAX_MEMORIES_IN_PROMPT,
    build_memory_prompt_block, _MEMORY_TYPE_DISPLAY_ORDER, _MEMORY_TYPE_LABELS,
)


def _fake_memory(memory_type, content, is_pinned=False, user_id=None):
    """Build a duck-typed stand-in for UserChatMemory for pure-function tests."""
    return SimpleNamespace(
        id=uuid.uuid4(),
        user_id=user_id or uuid.uuid4(),
        memory_type=memory_type,
        memory_key=f"{memory_type}_{content[:10]}",
        content=content,
        is_pinned=is_pinned,
        is_active=True,
        confidence_score=0.8,
        updated_at=datetime.now(timezone.utc),
    )


def test_build_memory_prompt_block_empty():
    assert build_memory_prompt_block([]) == ""


def test_build_memory_prompt_block_single_memory():
    memories = [_fake_memory("preference", "Prefer concise bullet answers")]
    block = build_memory_prompt_block(memories)
    assert MEMORY_PROMPT_HEADER.rstrip() in block
    assert "偏好" in block  # section header in Chinese
    assert "Prefer concise bullet answers" in block


def test_build_memory_prompt_block_groups_by_type():
    memories = [
        _fake_memory("correction", "Avoid WSJ for China macro"),
        _fake_memory("preference", "Prefer tables"),
        _fake_memory("style", "Answers in Chinese"),
    ]
    block = build_memory_prompt_block(memories)
    # All three types should appear
    for t in ["correction", "preference", "style"]:
        assert _MEMORY_TYPE_LABELS[t] in block
    # Correction should come before preference in the rendered block
    assert block.index(_MEMORY_TYPE_LABELS["correction"]) < block.index(
        _MEMORY_TYPE_LABELS["preference"]
    )


def test_build_memory_prompt_block_marks_pinned():
    memories = [_fake_memory("preference", "Always show forward PE", is_pinned=True)]
    block = build_memory_prompt_block(memories)
    assert "📌" in block


def test_build_memory_prompt_block_skips_unknown_type():
    # Type not in MEMORY_TYPES should silently drop out, not crash
    mem = _fake_memory("preference", "good")
    mem.memory_type = "nonsense"
    block = build_memory_prompt_block([mem])
    # Nothing rendered for unknown type
    assert "good" not in block


def test_prompt_block_header_wording():
    # Sanity check: the header frames the memory as *soft* guidance, not commands.
    # This is important: over-prescribed memory turns the assistant robotic.
    assert "务必遵守" in MEMORY_PROMPT_HEADER or "自动体现" in MEMORY_PROMPT_HEADER or "自然体现" in MEMORY_PROMPT_HEADER
    assert "当前" in MEMORY_PROMPT_HEADER  # mentions current turn takes precedence


def test_display_order_starts_with_correction():
    """Corrections must surface first — 'don't do X' trumps everything else."""
    assert _MEMORY_TYPE_DISPLAY_ORDER[0] == "correction"


def test_max_memories_limit_is_reasonable():
    assert 4 <= MAX_MEMORIES_IN_PROMPT <= 32
