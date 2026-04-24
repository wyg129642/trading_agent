"""Multi-stage LLM evaluation pipeline for proactive monitoring.

v3: Event-driven breaking news detection with historical price impact validation.

Stage 0: Time Gate (no LLM) — filter by published_at < 24h
Stage 1: Breaking News Triage — single LLM call: is this material?
Stage 2: Novelty Verification + Deep Research — multi-round: is it truly new?
Stage 3: Historical Price Impact (no LLM) — fetch actual price data for precedents
Stage 4: Final Assessment — alert decision with historical evidence
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from engine.proactive.models import (
    BreakingNewsItem,
    PortfolioHolding,
    ProactiveScanResult,
    StockBaseline,
    StockSnapshot,
)
from engine.proactive.freshness_gate import enforce_event_freshness
from engine.proactive.prompts import (
    TRIAGE_ROUND1_SYSTEM_PROMPT,
    TRIAGE_ROUND2_SYSTEM_PROMPT,
    NOVELTY_RESEARCH_SYSTEM_PROMPT,
    FINAL_ASSESSMENT_WITH_EVIDENCE_SYSTEM_PROMPT,
    build_breaking_news_triage_prompt,
    build_novelty_research_prompt,
    build_final_assessment_prompt,
)

logger = logging.getLogger(__name__)

MAX_DEEP_RESEARCH_ITERATIONS = 5


class StockEvaluator:
    """5-stage evaluation pipeline for breaking news detection."""

    def __init__(
        self,
        llm,
        settings: dict,
        content_fetcher=None,
        uqer_token: str = "",
    ):
        self._llm = llm
        self._settings = settings
        self._content_fetcher = content_fetcher
        self._uqer_token = uqer_token

        # LLM config
        self._model = settings.get("llm", {}).get("model_researcher", "MiniMax-M2")
        self._temperature_triage = 0.1
        self._temperature_research = 0.4
        self._temperature_assess = 0.3
        self._max_tokens = settings.get("llm", {}).get("max_tokens_researcher", 15000)

        # Search API keys
        self._baidu_api_key = settings.get("baidu", {}).get("api_key", "")
        self._tavily_api_key = settings.get("tavily", {}).get("api_key", "")
        self._jina_api_key = settings.get("jina", {}).get("api_key", "")

        # Proactive config
        proactive_cfg = settings.get("proactive_monitoring", {})
        self._max_iterations = proactive_cfg.get("max_research_iterations", MAX_DEEP_RESEARCH_ITERATIONS)
        self._alert_confidence_min = proactive_cfg.get("alert_confidence_min", 0.8)
        self._breaking_news_hours = proactive_cfg.get("breaking_news_window_hours", 24)
        self._novelty_hours = proactive_cfg.get("novelty_verification_hours", 48)

    # ------------------------------------------------------------------
    # Stage 0: Time Gate (no LLM cost)
    # ------------------------------------------------------------------

    async def time_gate(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
        snapshot: StockSnapshot,
    ) -> list[BreakingNewsItem]:
        """Filter snapshot data to only items published within the time window.

        Returns list of BreakingNewsItem. Empty = nothing recent, stop pipeline.
        All timestamps are handled in UTC.
        """
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(hours=self._breaking_news_hours)
        recent_items: list[BreakingNewsItem] = []

        # Process external search results
        for item in snapshot.external_context.search_results.get("all", []):
            pub_dt = item.get("_published_at_utc")
            if not pub_dt:
                # Try parsing date field
                date_str = item.get("date", "")
                if date_str:
                    pub_dt = self._parse_date_utc(date_str, item.get("_source_engine", ""))
                    if pub_dt:
                        item["_published_at_utc"] = pub_dt

            if pub_dt and pub_dt >= cutoff:
                age_hours = (now_utc - pub_dt).total_seconds() / 3600
                recent_items.append(BreakingNewsItem(
                    title=item.get("title", ""),
                    url=item.get("url", ""),
                    content=item.get("content", "")[:1000],
                    source_engine=item.get("_source_engine", item.get("source", "")),
                    source_label=item.get("website", ""),
                    published_at=pub_dt,
                    is_date_verified=bool(item.get("_date_verified")),
                    age_hours=age_hours,
                ))

        # Also check items from the DataSourceResult directly (web_search plugin
        # already post-filters, but the items are in the result.items list)
        ext_items = getattr(snapshot.external_context, '_raw_items', None)
        if ext_items is None:
            # Items were stored in the DataSourceResult via WebSearchPlugin
            # They're already in search_results, handled above.
            # But let's also check the formatted items list
            pass

        # Process internal data items
        for source_name, items in snapshot.internal_context.source_items.items():
            for item in items:
                # Internal DB items have timestamps from the platform
                pub_dt = self._extract_internal_pub_dt(item)
                if pub_dt and pub_dt >= cutoff:
                    age_hours = (now_utc - pub_dt).total_seconds() / 3600
                    recent_items.append(BreakingNewsItem(
                        title=item.get("title", ""),
                        url=item.get("url", ""),
                        content=item.get("content", item.get("summary", ""))[:1000],
                        source_engine="internal",
                        source_label=source_name,
                        published_at=pub_dt,
                        is_date_verified=True,  # DB timestamps are reliable
                        age_hours=age_hours,
                    ))

        # Sort by age (most recent first)
        recent_items.sort(key=lambda x: x.age_hours)

        logger.info(
            "[Proactive:%s] Time gate: %d items within %dh window",
            holding.ticker, len(recent_items), self._breaking_news_hours,
        )

        return recent_items

    def _parse_date_utc(self, date_str: str, source_engine: str = "") -> datetime | None:
        """Parse a date string into UTC-aware datetime."""
        from zoneinfo import ZoneInfo
        from dateutil import parser as dateutil_parser

        if not date_str:
            return None

        try:
            dt = dateutil_parser.parse(date_str, fuzzy=True)
            if dt.tzinfo is None:
                if source_engine == "baidu":
                    dt = dt.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
                else:
                    dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (ValueError, OverflowError):
            return None

    def _extract_internal_pub_dt(self, item: dict) -> datetime | None:
        """Extract publication datetime from an internal DB item."""
        # Internal items store time in various fields
        for key in ("published_at", "publish_time", "pub_time", "cmnt_date", "stime", "time"):
            val = item.get(key)
            if val is None:
                continue
            if isinstance(val, datetime):
                if val.tzinfo is None:
                    val = val.replace(tzinfo=timezone.utc)
                return val
            if isinstance(val, str) and val:
                try:
                    from dateutil import parser as dateutil_parser
                    dt = dateutil_parser.parse(val)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except (ValueError, OverflowError):
                    continue
        return None

    # ------------------------------------------------------------------
    # Stage 1: Breaking News Triage (two-round with full-text fetch)
    # ------------------------------------------------------------------

    async def triage(
        self,
        holding: PortfolioHolding,
        recent_items: list[BreakingNewsItem],
        snapshot: StockSnapshot,
        known_events: list[str],
    ) -> dict:
        """Two-round triage: Round 1 sees summaries and can request full text,
        Round 2 sees full text and makes final decision."""

        # Build formatted recent items text (with URLs for LLM to pick from)
        recent_items_text = self._format_recent_items(recent_items)

        # --- Round 1: LLM sees summaries, may request full text ---
        user_prompt = build_breaking_news_triage_prompt(
            stock_name=holding.name_cn,
            ticker=holding.ticker,
            market_label=holding.market_label,
            tags=holding.tags,
            recent_items_text=recent_items_text,
            known_events=known_events,
            internal_data_text=snapshot.internal_context.formatted_text,
            is_round2=False,
        )

        response = await self._llm.chat(
            system_prompt=TRIAGE_ROUND1_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model=self._model,
            max_tokens=2000,
            temperature=self._temperature_triage,
            stage="proactive_triage_r1",
            source_name=f"proactive:{holding.ticker}",
        )

        round1 = response.get("parsed") or {}

        # If LLM requests full text and we have a content fetcher, do Round 2
        urls_to_fetch = round1.get("urls_to_fetch", [])
        need_full_text = round1.get("need_full_text", False)

        if urls_to_fetch and need_full_text and self._content_fetcher:
            logger.info(
                "[Proactive:%s] Triage R1: fetching %d URLs for full text",
                holding.ticker, len(urls_to_fetch),
            )
            fetched_text = await self._fetch_urls(urls_to_fetch[:5])

            if fetched_text:
                # --- Round 2: LLM sees full text, makes final decision ---
                user_prompt_r2 = build_breaking_news_triage_prompt(
                    stock_name=holding.name_cn,
                    ticker=holding.ticker,
                    market_label=holding.market_label,
                    tags=holding.tags,
                    recent_items_text=recent_items_text,
                    known_events=known_events,
                    internal_data_text=snapshot.internal_context.formatted_text,
                    fetched_pages_text=fetched_text,
                    is_round2=True,
                )

                response_r2 = await self._llm.chat(
                    system_prompt=TRIAGE_ROUND2_SYSTEM_PROMPT,
                    user_prompt=user_prompt_r2,
                    model=self._model,
                    max_tokens=2000,
                    temperature=self._temperature_triage,
                    stage="proactive_triage_r2",
                    source_name=f"proactive:{holding.ticker}:r2",
                )

                round2 = response_r2.get("parsed") or {}
                logger.info(
                    "[Proactive:%s] Triage R2: materiality=%s (R1 was %s)",
                    holding.ticker,
                    round2.get("materiality", "?"),
                    round1.get("materiality", "?"),
                )
                return round2

        # No full text needed or no fetcher — use Round 1 result directly
        return round1

    def _format_recent_items(self, recent_items: list[BreakingNewsItem]) -> str:
        """Format recent items for triage prompt, including URLs."""
        lines = []
        for i, item in enumerate(recent_items[:20], 1):
            age_str = f"{item.age_hours:.0f}h前" if item.age_hours else "时间未知"
            pub_str = ""
            if item.published_at:
                from zoneinfo import ZoneInfo
                pub_cst = item.published_at.astimezone(ZoneInfo("Asia/Shanghai"))
                pub_str = pub_cst.strftime("%m-%d %H:%M")
            lines.append(
                f"[{i}] [{pub_str} ({age_str})] {item.title}\n"
                f"    来源: {item.source_label or item.source_engine}\n"
                f"    URL: {item.url}\n"
                f"    {item.content[:400]}"
            )
        return "\n\n".join(lines) if lines else "（无近期新闻）"

    # ------------------------------------------------------------------
    # Stage 2: Novelty Verification + Deep Research
    # ------------------------------------------------------------------

    async def novelty_and_research(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
        snapshot: StockSnapshot,
        breaking_events: list[dict],
    ) -> dict:
        """Verify novelty of breaking news + deep research.

        First iteration focuses on novelty verification (finding earliest reports).
        Subsequent iterations do standard deep research + historical precedent finding.

        Returns dict with: novelty_status, historical_events, key_findings,
        news_timeline, referenced_sources, fetched_pages_text, etc.
        """
        from engine.tools.web_search import parallel_search

        all_key_findings: list[str] = []
        all_timeline: list[dict] = []
        all_referenced_sources: list[dict] = []
        all_fetched_pages_text = ""
        all_historical_events: list[dict] = []
        novelty_status = "likely_fresh"
        earliest_report = None
        first_reported_timeline: list[dict] = []
        total_tokens = 0
        total_cost = 0.0

        # Build event summaries for query generation
        event_summaries = [e.get("summary", "") for e in breaking_events if e.get("summary")]
        primary_event = event_summaries[0] if event_summaries else holding.name_cn

        for iteration in range(1, self._max_iterations + 1):
            logger.info(
                "[Proactive:%s] Novelty+research iteration %d/%d",
                holding.ticker, iteration, self._max_iterations,
            )

            # Generate search queries
            if iteration == 1:
                # Focus on novelty verification: when was this first reported?
                baidu_queries = [
                    f"{holding.name_cn} {primary_event[:20]} 最早报道",
                    f"{holding.name_cn} {primary_event[:20]} 消息来源 时间",
                ]
                google_queries = [
                    f"{holding.ticker} {holding.name_en} {primary_event[:30]} first reported when",
                    f"{holding.ticker} news timeline {primary_event[:30]}",
                ]
            else:
                baidu_queries = iter_parsed.get("new_baidu_queries", [])
                google_queries = iter_parsed.get("new_google_queries", [])

            # Run parallel search
            search_results = {}
            if baidu_queries or google_queries:
                try:
                    search_results = await parallel_search(
                        baidu_queries=baidu_queries,
                        google_queries=google_queries,
                        baidu_api_key=self._baidu_api_key,
                        tavily_api_key=self._tavily_api_key,
                        jina_api_key=self._jina_api_key,
                        max_results=10,
                    )
                except Exception as e:
                    logger.warning("[Proactive:%s] Search error iter %d: %s", holding.ticker, iteration, e)

            search_text = self._format_search_results(search_results)

            # Fetch URLs from previous iteration
            if iteration > 1:
                urls_to_fetch = iter_parsed.get("urls_to_fetch", [])
                if urls_to_fetch and self._content_fetcher:
                    new_pages = await self._fetch_urls(urls_to_fetch[:3])
                    if new_pages:
                        all_fetched_pages_text += "\n" + new_pages

            # Build prompt
            system_prompt = NOVELTY_RESEARCH_SYSTEM_PROMPT.replace(
                "{novelty_hours}", str(self._novelty_hours),
            )

            user_prompt = build_novelty_research_prompt(
                stock_name=holding.name_cn,
                ticker=holding.ticker,
                market_label=holding.market_label,
                breaking_events=breaking_events,
                internal_data_text=snapshot.internal_context.formatted_text,
                price_data_text=snapshot.price_data,
                iteration=iteration,
                max_iterations=self._max_iterations,
                current_search_results_text=search_text,
                previous_findings=all_key_findings,
                accumulated_timeline=all_timeline,
                fetched_pages_text=all_fetched_pages_text,
                novelty_hours=self._novelty_hours,
            )

            # LLM call
            response = await self._llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=self._model,
                max_tokens=self._max_tokens,
                temperature=self._temperature_research,
                stage="proactive_research",
                source_name=f"proactive:{holding.ticker}:iter{iteration}",
            )

            usage = response.get("usage", {})
            total_tokens += usage.get("total_tokens", 0)
            total_cost += usage.get("cost_cny", 0.0)

            iter_parsed = response.get("parsed") or {}

            # Extract novelty info (primarily from iteration 1)
            if iter_parsed.get("novelty_status"):
                novelty_status = iter_parsed["novelty_status"]
            if iter_parsed.get("earliest_report"):
                earliest_report = iter_parsed["earliest_report"]
            if iter_parsed.get("first_reported_timeline"):
                first_reported_timeline = iter_parsed["first_reported_timeline"]

            # Accumulate findings
            findings = iter_parsed.get("key_findings", [])
            all_key_findings.extend(findings)

            timeline = iter_parsed.get("news_timeline", [])
            all_timeline = self._deduplicate_timeline(all_timeline + timeline)

            refs = iter_parsed.get("referenced_sources", [])
            all_referenced_sources.extend(refs)

            # Collect historical events for Stage 3
            hist_events = iter_parsed.get("historical_events", [])
            all_historical_events.extend(hist_events)

            # Sufficiency check
            is_sufficient = bool(iter_parsed.get("sufficient", False))

            # System overrides
            if is_sufficient and iteration == 1 and len(all_key_findings) < 3:
                logger.info("[Proactive:%s] Override: forcing iteration 2 (thin results)", holding.ticker)
                is_sufficient = False

            if iteration >= self._max_iterations - 1:
                is_sufficient = True

            # Early exit if novelty check says stale
            if novelty_status in ("stale", "repackaged") and iteration >= 2:
                logger.info(
                    "[Proactive:%s] Novelty check: %s → stopping research",
                    holding.ticker, novelty_status,
                )
                break

            # Fallback query generation
            if not is_sufficient:
                if not iter_parsed.get("new_baidu_queries") and not iter_parsed.get("new_google_queries"):
                    iter_parsed["new_baidu_queries"], iter_parsed["new_google_queries"] = \
                        self._generate_fallback_queries(
                            holding, event_summaries, all_timeline, all_key_findings,
                        )

            if is_sufficient:
                logger.info(
                    "[Proactive:%s] Research sufficient after %d iterations (%d findings, %d precedents)",
                    holding.ticker, iteration, len(all_key_findings), len(all_historical_events),
                )
                break

        return {
            "novelty_status": novelty_status,
            "earliest_report": earliest_report,
            "first_reported_timeline": first_reported_timeline,
            "historical_events": all_historical_events,
            "key_findings": all_key_findings,
            "news_timeline": all_timeline,
            "referenced_sources": all_referenced_sources,
            "fetched_pages_text": all_fetched_pages_text,
            "iterations_used": iteration,
            "tokens_used": total_tokens,
            "cost_cny": total_cost,
        }

    # ------------------------------------------------------------------
    # Stage 3: Historical Price Impact (no LLM cost)
    # ------------------------------------------------------------------

    async def historical_price_analysis(
        self,
        holding: PortfolioHolding,
        historical_events: list[dict],
    ) -> list[dict]:
        """Fetch actual price data around historical event dates via akshare.

        Returns list of precedent dicts with return_1d, return_3d, return_5d.
        """
        from engine.proactive.data_sources.market_data import MarketDataPlugin

        if not historical_events:
            return []

        market_plugin = MarketDataPlugin()
        try:
            precedents = await market_plugin.fetch_event_study(historical_events)
            logger.info(
                "[Proactive:%s] Historical price analysis: %d/%d events had valid data",
                holding.ticker, len(precedents), len(historical_events),
            )
            return precedents
        except Exception as e:
            logger.warning("[Proactive:%s] Historical price analysis failed: %s", holding.ticker, e)
            return []

    # ------------------------------------------------------------------
    # Stage 4: Final Assessment with Historical Evidence
    # ------------------------------------------------------------------

    async def final_assessment(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
        snapshot: StockSnapshot,
        breaking_events: list[dict],
        novelty_status: str,
        research_result: dict,
        historical_precedents: list[dict],
    ) -> dict:
        """Final assessment with historical price evidence."""
        from engine.proactive.data_sources.market_data import MarketDataPlugin

        # Format historical evidence table
        historical_evidence_text = MarketDataPlugin.format_event_study_table(
            historical_precedents, stock_name=holding.name_cn,
        )

        user_prompt = build_final_assessment_prompt(
            stock_name=holding.name_cn,
            ticker=holding.ticker,
            market_label=holding.market_label,
            breaking_events=breaking_events,
            novelty_status=novelty_status,
            research_findings=research_result.get("key_findings", []),
            news_timeline=research_result.get("news_timeline", []),
            referenced_sources=research_result.get("referenced_sources", []),
            historical_price_evidence=historical_evidence_text,
            internal_data_text=snapshot.internal_context.formatted_text,
            price_data_text=snapshot.price_data,
            fetched_pages_text=research_result.get("fetched_pages_text", ""),
        )

        response = await self._llm.chat(
            system_prompt=FINAL_ASSESSMENT_WITH_EVIDENCE_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model=self._model,
            max_tokens=self._max_tokens,
            temperature=self._temperature_assess,
            stage="proactive_assess",
            source_name=f"proactive:{holding.ticker}:assess",
        )

        parsed = response.get("parsed") or {}
        usage = response.get("usage", {})

        return {
            **parsed,
            "tokens_used": usage.get("total_tokens", 0),
            "cost_cny": usage.get("cost_cny", 0.0),
        }

    # ------------------------------------------------------------------
    # Full evaluation pipeline (v3)
    # ------------------------------------------------------------------

    async def evaluate(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
        snapshot: StockSnapshot,
    ) -> ProactiveScanResult:
        """Run the full 5-stage breaking news detection pipeline.

        Stage 0 → Stage 1 → Stage 2 → Stage 3 → Stage 4
        Most scans short-circuit at Stage 0 (no recent items = zero LLM cost).
        """
        scan_time = datetime.now(timezone.utc)
        total_tokens = 0
        total_cost = 0.0

        # --- Stage 0: Time Gate ---
        recent_items = await self.time_gate(holding, baseline, snapshot)

        result = ProactiveScanResult(
            holding=holding,
            scan_time=scan_time,
            snapshot=snapshot,
            recent_items_count=len(recent_items),
        )

        if not recent_items:
            logger.info("[Proactive:%s] Time gate: 0 recent items → skip", holding.ticker)
            return result

        # --- Stage 1: Breaking News Triage ---
        triage_result = await self.triage(
            holding, recent_items, snapshot, baseline.known_developments,
        )

        has_breaking = triage_result.get("has_breaking_news", False)
        materiality = triage_result.get("materiality", "none")
        breaking_events = triage_result.get("breaking_events", [])

        result.breaking_news_detected = has_breaking
        result.news_materiality = materiality
        result.news_summary = triage_result.get("reasoning", "")
        result.new_developments = [e.get("summary", "") for e in breaking_events]

        if not has_breaking or materiality in ("none", "routine"):
            logger.info(
                "[Proactive:%s] Triage: materiality=%s → no further research",
                holding.ticker, materiality,
            )
            return result

        # --- Stage 2: Novelty Verification + Deep Research ---
        logger.info(
            "[Proactive:%s] Triage: materiality=%s → starting novelty verification + research",
            holding.ticker, materiality,
        )
        research_result = await self.novelty_and_research(
            holding, baseline, snapshot, breaking_events,
        )

        novelty_status = research_result.get("novelty_status", "likely_fresh")
        result.novelty_status = novelty_status
        result.novelty_verified = novelty_status in ("verified_fresh", "likely_fresh")
        result.deep_research_performed = True
        result.research_iterations = research_result.get("iterations_used", 0)
        result.key_findings = research_result.get("key_findings", [])
        result.news_timeline = research_result.get("news_timeline", [])
        result.referenced_sources = research_result.get("referenced_sources", [])
        total_tokens += research_result.get("tokens_used", 0)
        total_cost += research_result.get("cost_cny", 0.0)

        # Parse earliest report time
        earliest = research_result.get("earliest_report")
        if earliest and earliest.get("time"):
            result.earliest_report_time = self._parse_date_utc(earliest["time"])

        if novelty_status in ("stale", "repackaged"):
            logger.info(
                "[Proactive:%s] Novelty check: %s → suppressing alert",
                holding.ticker, novelty_status,
            )
            result.tokens_used = total_tokens
            result.cost_cny = total_cost
            return result

        # --- Stage 3: Historical Price Impact ---
        historical_events = research_result.get("historical_events", [])
        precedents = await self.historical_price_analysis(holding, historical_events)
        result.historical_precedents = precedents

        # --- Stage 4: Final Assessment ---
        assessment = await self.final_assessment(
            holding, baseline, snapshot, breaking_events,
            novelty_status, research_result, precedents,
        )

        result.should_alert = bool(assessment.get("should_alert", False))
        result.alert_confidence = float(assessment.get("alert_confidence", 0.0))
        result.alert_rationale = assessment.get("alert_rationale", "")
        result.full_analysis = assessment
        result.news_summary = assessment.get("summary", result.news_summary)
        total_tokens += assessment.get("tokens_used", 0)
        total_cost += assessment.get("cost_cny", 0.0)

        # Apply event-freshness hard gate — overrides LLM novelty_status
        # and suppresses alerts where the underlying event is too old and
        # no recent corroborating source exists.
        gate_outcome = enforce_event_freshness(result, novelty_hours=self._novelty_hours)
        if gate_outcome.get("enforced"):
            logger.info(
                "[Proactive:%s] Freshness gate suppressed alert: age=%.1fh > %dh",
                holding.ticker,
                gate_outcome.get("event_age_hours") or 0.0,
                self._novelty_hours,
            )

        # Apply confidence gate (0.8 threshold)
        if result.should_alert and result.alert_confidence < self._alert_confidence_min:
            logger.info(
                "[Proactive:%s] Alert suppressed: confidence %.2f < threshold %.2f",
                holding.ticker, result.alert_confidence, self._alert_confidence_min,
            )
            result.should_alert = False
            result.alert_rationale += (
                f" (suppressed: confidence {result.alert_confidence:.2f} "
                f"< {self._alert_confidence_min})"
            )

        result.tokens_used = total_tokens
        result.cost_cny = total_cost

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _format_search_results(self, results: dict[str, list[dict]]) -> str:
        """Format search results dict into text for LLM."""
        if not results:
            return ""

        lines = []
        idx = 1
        for query, items in results.items():
            lines.append(f"\n查询: {query}")
            for item in items[:8]:
                title = item.get("title", "")[:100]
                website = item.get("website", "")
                date = item.get("date", "")
                content = item.get("content", "")[:500]
                url = item.get("url", "")
                source = item.get("source", "")

                lines.append(f"[{idx}] {title} — {website} ({date})")
                if content:
                    lines.append(f"    {content}")
                lines.append(f"    URL: {url}")
                lines.append(f"    [source: {source}]")
                idx += 1

        return "\n".join(lines)

    async def _fetch_urls(self, urls: list[str]) -> str:
        """Fetch full content from URLs using the content fetcher."""
        if not self._content_fetcher or not urls:
            return ""

        lines = []
        for url in urls[:3]:
            try:
                result = await self._content_fetcher.fetch(url)
                text = result[0] if isinstance(result, tuple) else result
                if text:
                    lines.append(f"--- {url} ---")
                    lines.append(str(text)[:5000])
                    lines.append("")
            except Exception as e:
                logger.debug("URL fetch failed for %s: %s", url, e)

        return "\n".join(lines) if lines else ""

    def _deduplicate_timeline(self, timeline: list[dict]) -> list[dict]:
        """Deduplicate news timeline by URL or title similarity."""
        seen: set[str] = set()
        deduped = []
        for entry in timeline:
            key = entry.get("url") or entry.get("title", "")
            if key and key not in seen:
                seen.add(key)
                deduped.append(entry)
        deduped.sort(key=lambda x: x.get("time", ""), reverse=False)
        return deduped

    def _generate_fallback_queries(
        self,
        holding: PortfolioHolding,
        event_summaries: list[str],
        timeline: list[dict],
        findings: list[str],
    ) -> tuple[list[str], list[str]]:
        """Generate fallback queries when LLM doesn't provide new ones."""
        baidu = []
        google = []

        name = holding.name_cn
        ticker = holding.ticker
        name_en = holding.name_en or ticker
        event_hint = event_summaries[0][:20] if event_summaries else ""

        if len(timeline) < 3:
            baidu.append(f"{name} {event_hint} 最新报道")
            google.append(f"{ticker} {name_en} latest news")

        if not any("历史" in f or "precedent" in f.lower() for f in findings):
            baidu.append(f"{name} 历史事件 影响分析 股价")
            google.append(f"{ticker} historical similar event stock price impact")

        return baidu, google
