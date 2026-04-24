"""Event-freshness hard gate for the proactive scanner.

Mirrors industry practice at event-driven desks (Jane Street / Citadel /
Millennium): every signal carries both an *event_time* (when the market
learned of the event) and a *signal_time* (when we emitted the alert).
Alpha decays fast after the event — by the time an earnings story is
a week old, the market has already priced it in. Trusting the LLM to
self-report "新鲜度" is unreliable, so we enforce the rule in code:

    if event_age > novelty_hours
    and no fresh corroborating source in the last novelty_hours
    → suppress the alert (should_alert=False, novelty_status="stale").

The gate also annotates full_analysis with the computed event_age_hours
so the UI can render the true event age regardless of when the scan ran.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from engine.proactive.models import ProactiveScanResult

logger = logging.getLogger(__name__)

# Default: 48h — conservative for earnings / guidance. Can be tightened
# per-event-type in a later PR (M&A / regulatory → 6–12h).
DEFAULT_NOVELTY_HOURS = 48


def _parse_datetime_utc(value: Any) -> datetime | None:
    """Best-effort parse of a mixed-type timestamp into a UTC datetime.

    Accepts datetime, ISO string, "YYYY-MM-DD HH:MM", "YYYY-MM-DD", etc.
    Naive datetimes are assumed UTC. Returns None if unparseable.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    try:
        from dateutil import parser as dateutil_parser
        dt = dateutil_parser.parse(value, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, OverflowError, TypeError):
        return None


def _earliest_source_time(
    result: ProactiveScanResult,
    now: datetime | None = None,
) -> datetime | None:
    """Pick the earliest timestamp from all source-bearing fields.

    Priority: result.earliest_report_time (authoritative from novelty stage)
    → news_timeline[].time → referenced_sources[].date → full_analysis.sources[].date.

    `now` is threaded through so unit tests can freeze the clock — without
    it, a synthetic "future" timestamp in the test fixture would be filtered
    out as "clock-skew bogus" against the real system clock.
    """
    candidates: list[datetime] = []

    if result.earliest_report_time:
        candidates.append(result.earliest_report_time)

    for entry in result.news_timeline or []:
        dt = _parse_datetime_utc(entry.get("time") if isinstance(entry, dict) else None)
        if dt:
            candidates.append(dt)

    for src in result.referenced_sources or []:
        if not isinstance(src, dict):
            continue
        dt = _parse_datetime_utc(src.get("date") or src.get("time") or src.get("published_at"))
        if dt:
            candidates.append(dt)

    analysis = result.full_analysis or {}
    for src in analysis.get("sources", []) or []:
        if not isinstance(src, dict):
            continue
        dt = _parse_datetime_utc(src.get("date") or src.get("time") or src.get("published_at"))
        if dt:
            candidates.append(dt)

    if not candidates:
        return None

    # Filter out obviously-bogus future timestamps (>1h ahead of now).
    reference_now = now or datetime.now(timezone.utc)
    future_cutoff = reference_now + timedelta(hours=1)
    candidates = [c for c in candidates if c <= future_cutoff]
    if not candidates:
        return None
    return min(candidates)


def _has_fresh_corroboration(
    result: ProactiveScanResult,
    cutoff: datetime,
    future_cutoff: datetime,
) -> bool:
    """Return True if at least one source is timestamped in [cutoff, future_cutoff].

    This lets long-running stories (earnings report → next-day follow-ups
    → regulatory filing) stay alive as long as *new* material keeps landing.
    `future_cutoff` lets us reject hallucinated future timestamps that would
    otherwise fake "freshness".
    """

    def _in_window(dt: datetime | None) -> bool:
        return dt is not None and cutoff <= dt <= future_cutoff

    for entry in result.news_timeline or []:
        if not isinstance(entry, dict):
            continue
        if _in_window(_parse_datetime_utc(entry.get("time"))):
            return True

    for src in result.referenced_sources or []:
        if not isinstance(src, dict):
            continue
        if _in_window(_parse_datetime_utc(
            src.get("date") or src.get("time") or src.get("published_at")
        )):
            return True

    analysis = result.full_analysis or {}
    for src in analysis.get("sources", []) or []:
        if not isinstance(src, dict):
            continue
        if _in_window(_parse_datetime_utc(
            src.get("date") or src.get("time") or src.get("published_at")
        )):
            return True

    return False


def enforce_event_freshness(
    result: ProactiveScanResult,
    novelty_hours: int = DEFAULT_NOVELTY_HOURS,
    now: datetime | None = None,
) -> dict:
    """Post-LLM hard gate. Mutates `result` in place.

    Contract:
    - Computes event_age_hours from the earliest source timestamp.
    - Stamps `event_age_hours` into result.full_analysis so the UI always
      has a trustworthy age to display.
    - If event_age_hours > novelty_hours AND no source within the last
      novelty_hours, forces:
          novelty_status   = "stale"
          novelty_verified = False
          should_alert     = False
          alert_rationale += "(event_too_old: Xh)"
          full_analysis["rejection_reason"] = "event_too_old"
    - If novelty_hours is not exceeded, the result is left untouched
      (the LLM's novelty_status still wins in the grey zone).

    Returns a small dict describing what was decided, useful for logging
    and for the unit tests.
    """
    now_utc = now or datetime.now(timezone.utc)
    earliest = _earliest_source_time(result, now=now_utc)

    analysis = result.full_analysis or {}

    # If we can't determine event age, don't enforce — but record the
    # reason so operators can see why the gate was skipped.
    if earliest is None:
        analysis["event_age_hours"] = None
        analysis["freshness_gate"] = "skipped_no_earliest_time"
        result.full_analysis = analysis
        return {
            "enforced": False,
            "event_age_hours": None,
            "reason": "no_earliest_time",
        }

    event_age_hours = (now_utc - earliest).total_seconds() / 3600
    # Never record negative ages (clock skew / LLM hallucinating a
    # future date); clamp to 0 so the UI never shows "-3小时前".
    event_age_hours = max(0.0, event_age_hours)

    analysis["event_age_hours"] = round(event_age_hours, 2)
    analysis["earliest_source_time"] = earliest.isoformat()

    # Fresh by construction — nothing to enforce.
    if event_age_hours <= novelty_hours:
        analysis["freshness_gate"] = "pass_within_window"
        result.full_analysis = analysis
        return {
            "enforced": False,
            "event_age_hours": event_age_hours,
            "reason": "within_novelty_window",
        }

    # Stale-by-age: check whether a fresh corroborating source keeps
    # the story alive (ongoing guidance updates, follow-on filings, etc.).
    # Bounds reject both too-old timestamps and hallucinated future ones.
    fresh_cutoff = now_utc - timedelta(hours=novelty_hours)
    future_cutoff = now_utc + timedelta(hours=1)
    if _has_fresh_corroboration(result, fresh_cutoff, future_cutoff):
        analysis["freshness_gate"] = "pass_recent_corroboration"
        result.full_analysis = analysis
        return {
            "enforced": False,
            "event_age_hours": event_age_hours,
            "reason": "recent_corroboration",
        }

    # Hard rejection — this is the case that caused the 中际旭创 bug.
    previous_novelty = result.novelty_status
    previous_should_alert = result.should_alert

    result.novelty_status = "stale"
    result.novelty_verified = False
    result.should_alert = False

    suppression_note = (
        f"(suppressed: event_too_old — earliest source {earliest.isoformat()} "
        f"is {event_age_hours:.1f}h old, no new source within {novelty_hours}h)"
    )
    if result.alert_rationale:
        result.alert_rationale = f"{result.alert_rationale} {suppression_note}"
    else:
        result.alert_rationale = suppression_note

    analysis["freshness_gate"] = "rejected_event_too_old"
    analysis["rejection_reason"] = "event_too_old"
    analysis["novelty_status_llm_original"] = previous_novelty
    analysis["should_alert_llm_original"] = previous_should_alert
    result.full_analysis = analysis

    logger.info(
        "[FreshnessGate] ticker=%s suppressed: event_age=%.1fh > %dh, "
        "llm_novelty=%r → stale",
        result.holding.ticker if result.holding else "?",
        event_age_hours,
        novelty_hours,
        previous_novelty,
    )

    return {
        "enforced": True,
        "event_age_hours": event_age_hours,
        "reason": "event_too_old",
    }
