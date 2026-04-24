"""Chinese-aware tokenizer for the personal knowledge base's BM25 index.

MongoDB's ``$text`` index uses whitespace tokenization — which is fine for
Latin scripts (words are already space-delimited) but hopeless for CJK
(pure Chinese has no whitespace, so an entire Chinese paragraph indexes as
a single unmatchable "token"). This module preprocesses text so Chinese
words become space-separated tokens before they hit the index.

We use ``jieba.cut_for_search`` as the segmenter:

* ``jieba`` is pure-Python, no Java/C dependencies, ~25 MB dictionary.
* ``cut_for_search`` mode returns **multiple overlapping tokens** for each
  Chinese span — the full word plus shorter constituents — which is
  explicitly designed for inverted-index use (recall over precision).
* Unknown words fall through to jieba's HMM character model, so freshly
  coined company names or product codes still produce *something* sensible
  rather than being dropped.

Two entry points: :func:`tokenize` for the text going into the index, and
:func:`tokenize_query` for the query we ship to Mongo. They're currently
the same implementation — split into two names so we can diverge later
(e.g. stricter segmentation on the query side) without changing call sites.

Safe to call from threads; jieba serializes its lazy initialization.
"""

from __future__ import annotations

import logging
import re
import threading
import unicodedata
from typing import Iterable

logger = logging.getLogger(__name__)


# Punctuation and CJK symbols we don't want as tokens. Keeping them out
# shrinks the index meaningfully on documents heavy with "，。；：" etc.
# We match only the Unicode punctuation classes we care about; anything else
# (actual letters/digits in any language) passes through.
_DROP_TOKEN_RE = re.compile(
    r"^["
    r"\s"                                        # all whitespace
    r"!-/:-@[-`{-~"  # ASCII punct
    r" -⁯"                             # general punctuation block
    r"　-〿"                             # CJK symbols and punctuation
    r"＀-￯"                             # full-width forms
    r"]+$"
)


_jieba_lock = threading.Lock()
_jieba_ready = False


def _ensure_jieba() -> None:
    """Lazy, thread-safe jieba initialization. Caches the prefix-dict build
    (~1 s cold) so the first search isn't delayed. Safe to call many times."""
    global _jieba_ready
    if _jieba_ready:
        return
    with _jieba_lock:
        if _jieba_ready:
            return
        import jieba  # noqa: F401  # import is the initializer
        import jieba as _jieba
        _jieba.initialize()
        _jieba_ready = True
        logger.debug("jieba dictionary loaded")


def _filter_tokens(raw: Iterable[str]) -> list[str]:
    out: list[str] = []
    for tok in raw:
        if not tok:
            continue
        tok = tok.strip()
        if not tok:
            continue
        if _DROP_TOKEN_RE.match(tok):
            continue
        out.append(tok)
    return out


def _normalize(text: str) -> str:
    """Unicode-normalize text so visually-identical codepoints map together.

    NFKC folds:
      * CJK *compatibility* ideographs and radicals into canonical ideographs
        — this matters in practice: PDF extractors frequently emit U+2F1D
        ``⼝`` (CJK RADICAL MOUTH) instead of U+53E3 ``口``. The two look
        identical to humans but are different codepoints, so without this
        fold a user typing "接口" on a keyboard will never match a PDF
        chunk that contains "接⼝".
      * Full-width ASCII (``Ａ１``) → half-width (``A1``).
      * Other "compatibility" variants (ligatures, superscripts, etc).

    We apply NFKC at both index time (inside :func:`tokenize`) and query
    time (:func:`tokenize_query` calls this too) so stored tokens and
    search tokens always agree on canonical forms.
    """
    return unicodedata.normalize("NFKC", text)


def tokenize(text: str) -> str:
    """Turn an arbitrary text into a space-separated token string suitable
    for indexing with MongoDB's ``$text`` analyzer.

    Steps:
      1. Unicode NFKC normalize — see :func:`_normalize`.
      2. jieba ``cut_for_search`` — emits multiple overlapping tokens per
         Chinese span for recall. Latin words pass through unchanged.
      3. Filter out pure-punctuation tokens.

    Returns the empty string for empty/whitespace-only input.
    """
    if not text:
        return ""
    stripped = _normalize(text).strip()
    if not stripped:
        return ""
    _ensure_jieba()
    import jieba
    tokens = _filter_tokens(jieba.cut_for_search(stripped))
    return " ".join(tokens)


def tokenize_query(query: str) -> str:
    """Tokenize a user search query for MongoDB ``$text``.

    Uses the same algorithm as indexing so tokens line up. Split into its
    own name to give us a future hook for query-time tweaks (stop-word
    removal, minimum token length, etc.) without touching the ingest path.
    """
    return tokenize(query)
