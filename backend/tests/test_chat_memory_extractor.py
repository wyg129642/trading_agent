"""Unit tests for chat_memory_extractor — pure parsing + sentiment logic.

These tests exercise the LLM-response parsing and the deterministic sentiment
fallback without making any real LLM calls.
"""
from __future__ import annotations

import json

import pytest

from backend.app.services.chat_memory_extractor import (
    ExtractionInput, ExtractedMemory, ExtractionResult,
    EXTRACTION_SYSTEM_PROMPT, MAX_MEMORIES_PER_EVENT,
    _build_user_prompt, _extract_json_object, _parse_extraction_result,
    _sentiment_from_signals, _strip_think_blocks,
)


# ─────────────────────────────────────────────────
# _sentiment_from_signals
# ─────────────────────────────────────────────────

def test_sentiment_positive_from_high_rating():
    assert _sentiment_from_signals(5, "", []) == "positive"
    assert _sentiment_from_signals(4, "", []) == "positive"


def test_sentiment_negative_from_low_rating():
    assert _sentiment_from_signals(1, "", []) == "negative"
    assert _sentiment_from_signals(2, "", []) == "negative"


def test_sentiment_neutral_default():
    assert _sentiment_from_signals(3, "", []) == "neutral"
    assert _sentiment_from_signals(None, "", []) == "neutral"


def test_sentiment_mixed_when_text_without_rating():
    # Text present but no rating → mixed signal
    assert _sentiment_from_signals(None, "some feedback", []) == "mixed"


def test_sentiment_negative_tags_override_rating():
    # Only negative tag → negative even if rating suggests otherwise
    assert _sentiment_from_signals(5, "", ["wrong"]) == "negative"


def test_sentiment_positive_tags_override_rating():
    assert _sentiment_from_signals(2, "", ["helpful"]) == "positive"


def test_sentiment_mixed_tags_produce_mixed():
    assert _sentiment_from_signals(3, "", ["helpful", "too_long"]) == "mixed"


# ─────────────────────────────────────────────────
# _strip_think_blocks
# ─────────────────────────────────────────────────

def test_strip_think_blocks_closed():
    raw = "<think>internal reasoning</think>{\"memories\": []}"
    assert _strip_think_blocks(raw) == '{"memories": []}'


def test_strip_think_blocks_unclosed():
    raw = "<think>reasoning got cut off by max_tokens"
    assert _strip_think_blocks(raw) == ""


def test_strip_think_blocks_fenced_json():
    raw = "```json\n{\"memories\": []}\n```"
    assert _strip_think_blocks(raw) == '{"memories": []}'


# ─────────────────────────────────────────────────
# _extract_json_object
# ─────────────────────────────────────────────────

def test_extract_json_plain():
    obj = _extract_json_object('{"memories": [{"memory_key": "foo"}]}')
    assert obj and obj["memories"][0]["memory_key"] == "foo"


def test_extract_json_with_preamble():
    raw = "Sure, here you go:\n{\"memories\": []}\nLet me know if you need more."
    obj = _extract_json_object(raw)
    assert obj == {"memories": []}


def test_extract_json_inside_think_tags():
    raw = "<think>planning</think>\n{\"memories\": [], \"sentiment\": \"neutral\"}"
    obj = _extract_json_object(raw)
    assert obj == {"memories": [], "sentiment": "neutral"}


def test_extract_json_malformed_returns_none():
    assert _extract_json_object("not json at all") is None
    assert _extract_json_object("{broken") is None


# ─────────────────────────────────────────────────
# _parse_extraction_result
# ─────────────────────────────────────────────────

def _valid_llm_output(memories=None, sentiment="positive"):
    return json.dumps({
        "memories": memories if memories is not None else [
            {
                "memory_type": "preference",
                "memory_key": "prefers_concise_bullets",
                "content": "用户偏好结论在前、分条列表的简洁回答",
                "confidence": 0.85,
            },
        ],
        "sentiment": sentiment,
    })


def test_parse_extraction_result_happy_path():
    result = _parse_extraction_result(_valid_llm_output(), "neutral")
    assert len(result.memories) == 1
    m = result.memories[0]
    assert m.memory_type == "preference"
    assert m.memory_key == "prefers_concise_bullets"
    assert m.confidence == 0.85
    assert result.sentiment == "positive"
    assert result.error is None


def test_parse_extraction_result_invalid_type_filtered():
    raw = json.dumps({
        "memories": [
            {"memory_type": "bogus_type", "memory_key": "k", "content": "c", "confidence": 0.5},
            {"memory_type": "preference", "memory_key": "real", "content": "real", "confidence": 0.7},
        ],
        "sentiment": "positive",
    })
    result = _parse_extraction_result(raw, "neutral")
    assert len(result.memories) == 1
    assert result.memories[0].memory_key == "real"


def test_parse_extraction_result_normalizes_memory_key():
    raw = json.dumps({
        "memories": [
            {
                "memory_type": "preference",
                "memory_key": "Prefers Concise Bullets!",
                "content": "content",
                "confidence": 0.7,
            },
        ],
        "sentiment": "positive",
    })
    result = _parse_extraction_result(raw, "neutral")
    assert result.memories[0].memory_key == "prefers_concise_bullets"


def test_parse_extraction_result_caps_batch_size():
    many = [
        {
            "memory_type": "preference",
            "memory_key": f"key_{i}",
            "content": f"c{i}",
            "confidence": 0.5,
        }
        for i in range(MAX_MEMORIES_PER_EVENT + 5)
    ]
    raw = json.dumps({"memories": many, "sentiment": "positive"})
    result = _parse_extraction_result(raw, "neutral")
    assert len(result.memories) == MAX_MEMORIES_PER_EVENT


def test_parse_extraction_result_missing_fields_skipped():
    raw = json.dumps({
        "memories": [
            {"memory_type": "preference"},  # no key/content
            {"memory_type": "preference", "memory_key": "ok", "content": "x", "confidence": 0.5},
        ],
        "sentiment": "positive",
    })
    result = _parse_extraction_result(raw, "neutral")
    assert len(result.memories) == 1


def test_parse_extraction_result_sentiment_fallback_on_invalid():
    raw = json.dumps({"memories": [], "sentiment": "purple"})
    result = _parse_extraction_result(raw, "positive")
    # Invalid sentiment should snap to fallback
    assert result.sentiment == "positive"


def test_parse_extraction_result_parse_failure():
    result = _parse_extraction_result("not valid json", "neutral")
    assert result.memories == []
    assert result.error == "parse_failed"
    assert result.sentiment == "neutral"


def test_parse_extraction_result_confidence_clamped():
    raw = json.dumps({
        "memories": [
            {
                "memory_type": "preference",
                "memory_key": "k",
                "content": "c",
                "confidence": 99.0,  # out of range
            },
        ],
        "sentiment": "positive",
    })
    result = _parse_extraction_result(raw, "neutral")
    assert result.memories[0].confidence == 1.0


# ─────────────────────────────────────────────────
# _build_user_prompt
# ─────────────────────────────────────────────────

def test_build_user_prompt_includes_all_fields():
    inp = ExtractionInput(
        user_message="What do you think of NVDA?",
        assistant_response="NVDA is a leader in GPUs...",
        model_name="Claude Opus 4.6",
        rating=5,
        feedback_tags=["clear", "helpful"],
        feedback_text="Perfect, exactly what I needed",
    )
    prompt = _build_user_prompt(inp)
    assert "NVDA" in prompt
    assert "Claude Opus 4.6" in prompt
    assert "5/5" in prompt
    assert "clear" in prompt and "helpful" in prompt
    assert "Perfect, exactly" in prompt


def test_build_user_prompt_handles_missing_rating():
    inp = ExtractionInput(
        user_message="hi", assistant_response="hello", model_name="M",
        rating=None, feedback_tags=[], feedback_text="",
    )
    prompt = _build_user_prompt(inp)
    assert "未打分" in prompt
    assert "(无)" in prompt  # tags
    assert "(无详细文字反馈)" in prompt


def test_build_user_prompt_truncates_long_response():
    big = "x" * 50000
    inp = ExtractionInput(
        user_message=big, assistant_response=big, model_name="M",
        rating=3, feedback_tags=[], feedback_text=big,
    )
    prompt = _build_user_prompt(inp)
    # Each of the 3 fields gets trimmed; overall prompt should be < 20000 chars
    assert len(prompt) < 20000


def test_extraction_system_prompt_non_empty():
    # Sanity check the format-expanded prompt actually has the max-memories
    # interpolation happen rather than leaving literal %d.
    assert str(MAX_MEMORIES_PER_EVENT) in EXTRACTION_SYSTEM_PROMPT
    assert "%d" not in EXTRACTION_SYSTEM_PROMPT
