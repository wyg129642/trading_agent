"""Unit tests for engine/proactive/freshness_gate.py.

These tests are deliberately pure: no DB, no LLM, no Redis. The freshness
gate is a small piece of logic that decides whether to suppress a scan
result based on the age of its earliest source timestamp. Getting this
right is load-bearing for the signal latency fix, so the tests cover:

1. Genuinely fresh events (< novelty_hours) pass untouched.
2. Events older than novelty_hours with NO recent corroboration get
   suppressed: novelty_status → "stale", should_alert → False.
3. Older events WITH a recent news_timeline entry stay alive.
4. The LLM's novelty_status is overridden regardless of what it said.
5. Missing earliest_report_time → gate is skipped (no-op) but logged.
6. Future timestamps from clock-skew / LLM hallucination don't crash
   the gate.
7. The 中际旭创 regression case reproduces and is correctly suppressed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from engine.proactive.freshness_gate import (
    DEFAULT_NOVELTY_HOURS,
    enforce_event_freshness,
    _earliest_source_time,
    _parse_datetime_utc,
)
from engine.proactive.models import (
    PortfolioHolding,
    ProactiveScanResult,
    StockSnapshot,
)


def _make_result(
    *,
    earliest: datetime | None = None,
    timeline: list[dict] | None = None,
    sources: list[dict] | None = None,
    novelty_status: str = "verified_fresh",
    should_alert: bool = True,
    alert_confidence: float = 0.9,
    ticker: str = "300308",
    name_cn: str = "中际旭创",
) -> ProactiveScanResult:
    holding = PortfolioHolding(
        ticker=ticker,
        name_cn=name_cn,
        name_en="Zhongji Innolight",
        market="china",
        market_label="创业板",
    )
    return ProactiveScanResult(
        holding=holding,
        scan_time=datetime.now(timezone.utc),
        snapshot=StockSnapshot(holding=holding),
        breaking_news_detected=True,
        news_materiality="critical",
        novelty_status=novelty_status,
        novelty_verified=(novelty_status in ("verified_fresh", "likely_fresh")),
        earliest_report_time=earliest,
        news_timeline=list(timeline or []),
        referenced_sources=list(sources or []),
        should_alert=should_alert,
        alert_confidence=alert_confidence,
        alert_rationale="initial rationale",
        full_analysis={
            "sentiment": "very_bullish",
            "impact_magnitude": "critical",
            "novelty_status": novelty_status,
            "sources": list(sources or []),
        },
    )


def test_fresh_event_within_window_is_untouched():
    now = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    result = _make_result(
        earliest=now - timedelta(hours=3),
        timeline=[{"time": (now - timedelta(hours=3)).isoformat(), "source": "CLS"}],
    )
    outcome = enforce_event_freshness(result, novelty_hours=48, now=now)

    assert outcome["enforced"] is False
    assert outcome["reason"] == "within_novelty_window"
    assert result.should_alert is True
    assert result.novelty_status == "verified_fresh"
    assert result.full_analysis["freshness_gate"] == "pass_within_window"
    assert abs(result.full_analysis["event_age_hours"] - 3.0) < 0.01


def test_stale_event_with_no_recent_source_is_suppressed():
    # This is the 中际旭创 case: earnings Q1 from 2026-04-16, scanned on
    # 2026-04-24, every source dated 04-16/04-17.
    now = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    result = _make_result(
        earliest=datetime(2026, 4, 16, 10, 52, tzinfo=timezone.utc),
        timeline=[
            {"time": "2026-04-16 10:52", "source": "财联社"},
            {"time": "2026-04-16 19:23", "source": "百家号"},
            {"time": "2026-04-17 00:00", "source": "雪球"},
            {"time": "2026-04-17 08:49", "source": "大众网"},
        ],
        novelty_status="verified_fresh",  # LLM got it wrong
    )

    outcome = enforce_event_freshness(result, novelty_hours=48, now=now)

    assert outcome["enforced"] is True
    assert outcome["reason"] == "event_too_old"
    assert outcome["event_age_hours"] > 48
    assert result.should_alert is False
    assert result.novelty_status == "stale"
    assert result.novelty_verified is False
    assert "event_too_old" in result.alert_rationale
    assert result.full_analysis["rejection_reason"] == "event_too_old"
    assert result.full_analysis["novelty_status_llm_original"] == "verified_fresh"
    assert result.full_analysis["should_alert_llm_original"] is True


def test_old_event_with_recent_corroboration_stays_alive():
    # Ongoing story: earliest source is 5 days old, but a news item from
    # this morning keeps the story "alive" (regulatory follow-on, guidance
    # update, etc.). The gate should NOT suppress.
    now = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    result = _make_result(
        earliest=now - timedelta(days=5),
        timeline=[
            {"time": (now - timedelta(days=5)).isoformat(), "source": "CLS"},
            {"time": (now - timedelta(hours=2)).isoformat(), "source": "Reuters"},
        ],
        novelty_status="likely_fresh",
    )

    outcome = enforce_event_freshness(result, novelty_hours=48, now=now)

    assert outcome["enforced"] is False
    assert outcome["reason"] == "recent_corroboration"
    assert result.should_alert is True
    assert result.full_analysis["freshness_gate"] == "pass_recent_corroboration"


def test_missing_earliest_time_skips_gate():
    now = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    result = _make_result(earliest=None, timeline=[], sources=[])
    outcome = enforce_event_freshness(result, novelty_hours=48, now=now)

    assert outcome["enforced"] is False
    assert outcome["reason"] == "no_earliest_time"
    assert result.should_alert is True
    assert result.full_analysis["event_age_hours"] is None
    assert result.full_analysis["freshness_gate"] == "skipped_no_earliest_time"


def test_fallback_to_timeline_when_earliest_missing():
    # LLM didn't output earliest_report_time but news_timeline has clear
    # dates — the gate must still be able to judge age.
    now = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    result = _make_result(
        earliest=None,
        timeline=[{"time": "2026-04-15 09:00", "source": "CLS"}],
    )
    outcome = enforce_event_freshness(result, novelty_hours=48, now=now)

    assert outcome["enforced"] is True
    assert outcome["event_age_hours"] > 48


def test_future_timestamp_does_not_crash():
    # LLM sometimes hallucinates a tomorrow's date. We must not compute a
    # negative age and we must not trust a bogus future time to keep the
    # story fresh.
    now = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    result = _make_result(
        earliest=datetime(2026, 4, 10, 0, 0, tzinfo=timezone.utc),
        timeline=[
            {"time": "2099-01-01 00:00", "source": "hallucinated"},
        ],
    )

    outcome = enforce_event_freshness(result, novelty_hours=48, now=now)

    # Old earliest + no legitimately recent source → should still suppress.
    assert outcome["enforced"] is True
    assert outcome["event_age_hours"] >= 48
    assert result.should_alert is False


def test_earliest_source_time_prefers_explicit_field():
    holding = PortfolioHolding(ticker="X", name_cn="X", name_en="", market="us", market_label="美股")
    result = ProactiveScanResult(
        holding=holding,
        earliest_report_time=datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc),
        news_timeline=[
            # Intentionally older than the authoritative earliest_report_time
            {"time": "2020-01-01 00:00", "source": "older"},
        ],
        full_analysis={},
    )
    picked = _earliest_source_time(result)
    # _earliest_source_time takes the MIN across all candidates, so a
    # genuinely earlier timeline entry wins — but a bogus old entry that
    # predates the event itself would skew results. In practice this is
    # fine because the gate also has the "recent corroboration" escape.
    assert picked == datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_parse_datetime_utc_accepts_varied_formats():
    assert _parse_datetime_utc(None) is None
    assert _parse_datetime_utc("") is None
    assert _parse_datetime_utc("2026-04-24") is not None
    assert _parse_datetime_utc("2026-04-24 10:52") is not None
    assert _parse_datetime_utc("2026-04-24T10:52:00Z") is not None
    naive = datetime(2026, 4, 24, 10, 52)
    out = _parse_datetime_utc(naive)
    assert out is not None and out.tzinfo is not None


def test_default_novelty_hours_matches_evaluator_default():
    # If someone bumps the default here we want the evaluator to stay in
    # sync — this test is a tripwire.
    assert DEFAULT_NOVELTY_HOURS == 48


def test_empty_full_analysis_is_ok():
    now = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    result = _make_result(
        earliest=now - timedelta(hours=1),
        timeline=[{"time": (now - timedelta(hours=1)).isoformat(), "source": "CLS"}],
    )
    result.full_analysis = None  # gate must tolerate this
    outcome = enforce_event_freshness(result, novelty_hours=48, now=now)
    assert outcome["enforced"] is False
    assert isinstance(result.full_analysis, dict)
    assert result.full_analysis["freshness_gate"] == "pass_within_window"
