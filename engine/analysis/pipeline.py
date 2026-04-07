"""Three-phase analysis pipeline: Initial Evaluation → Deep Research → Final Assessment.

Phase 1 (Initial Evaluation):
    Input: Title + Full Text + Publish Time
    Output: relevance_score, may_affect_market, related_stocks, search_queries, google_queries
    Gate: Only items with may_affect_market=True AND relevance_score >= threshold proceed.

Phase 2 (Deep Research — max 3 iterations):
    Baidu (Chinese) + Google (English) searches run in parallel from iteration 1.
    Each iteration: LLM decides sufficient?, urls_to_fetch?, new_queries?
    Collects: citations, news_timeline, referenced_sources.
    Special: A-share tickers → Uqer API for price data.

Phase 3 (Final Assessment):
    With all accumulated context → surprise_factor, sentiment, impact assessment.
    Includes news timeline and all referenced sources for display.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from engine.analysis.llm_client import LLMClient
from engine.analysis.prompts import get_prompts
from engine.analysis.signal_scorer import score_signal
from engine.database import Database
from engine.models import (
    AnalysisResult,
    DeepResearchResult,
    FetchedPage,
    FilterResult,
    FinalAssessment,
    InitialEvaluation,
    NewsItem,
    ResearchIteration,
    ResearchReport,
    SearchResultItem,
    SearchVerification,
)
from engine.tools.web_search import (
    batch_search,
    format_search_results,
    multi_search,
    parallel_search,
)
from backend.app.services.stock_verifier import get_stock_verifier

logger = logging.getLogger(__name__)

MAX_DEEP_RESEARCH_ITERATIONS = 5


class AnalysisPipeline:
    """Three-phase LLM analysis pipeline.

    Phase 1: Initial Evaluation — relevance + market impact + bilingual search queries
    Phase 2: Deep Research — parallel Baidu+Google search + URL fetch + price data
    Phase 3: Final Assessment — surprise factor + sentiment
    """

    def __init__(
        self,
        llm: LLMClient,
        db: Database,
        settings: dict,
        tool_executor=None,
        content_fetcher=None,
        uqer_token: str = "",
    ):
        self.llm = llm
        self.db = db
        self.settings = settings
        self.tool_executor = tool_executor
        self.content_fetcher = content_fetcher
        self.uqer_token = uqer_token

        fetch_cfg = settings.get("content_fetch", {})
        self.min_existing_content = fetch_cfg.get("min_existing_content", 200)
        self.max_article_age_hours = fetch_cfg.get("max_article_age_hours", 72)
        self.max_analysis_content_chars = fetch_cfg.get("max_analysis_content_chars", 15000)
        self.max_filter_content_chars = fetch_cfg.get("max_filter_content_chars", 20000)

        thresholds = settings.get("thresholds", {})
        self.relevance_min = thresholds.get("relevance_min", 0.4)

        # Search engine API keys
        self.baidu_api_key = settings.get("baidu", {}).get("api_key", "")
        self.tavily_api_key = settings.get("tavily", {}).get("api_key", "")
        self.jina_api_key = settings.get("jina", {}).get("api_key", "")
        self.jina_use_reader = settings.get("jina", {}).get("use_reader", False)

        # Load prompts
        prompt_lang = settings.get("llm", {}).get("prompt_language", "zh")
        self.prompts = get_prompts(prompt_lang)

    async def process(
        self,
        item: NewsItem,
        test_mode: bool = False,
        debug_trace: bool = False,
    ) -> dict:
        """Run a news item through the full 3-phase pipeline.

        Args:
            item: The news item to process.
            test_mode: Bypass filter/stale checks, force all phases, capture traces.
            debug_trace: Capture traces without bypassing thresholds.

        Returns dict with:
          - 'stage': how far it progressed (1, 2, 2.5, or 3)
          - 'filter': FilterResult (legacy compat)
          - 'analysis': AnalysisResult (legacy compat)
          - 'research': ResearchReport (legacy compat)
          - 'signal_score': SignalScore
          - 'alert_level': str
          - 'search_verification': SearchVerification (legacy compat)
          - 'initial_evaluation': InitialEvaluation
          - 'deep_research': DeepResearchResult
          - 'final_assessment': FinalAssessment
          - 'llm_traces': list of trace dicts
        """
        result = {
            "stage": 1,
            "filter": None,
            "analysis": None,
            "research": None,
            "signal_score": None,
            "alert_level": None,
            "news_item": item,
            "search_verification": None,
            "initial_evaluation": None,
            "deep_research": None,
            "final_assessment": None,
            "llm_traces": [],
        }

        capture = test_mode or debug_trace

        # === Fetch full content BEFORE Phase 1 ===
        await self._fetch_content(item)

        # === Phase 1: Initial Evaluation ===
        evaluation, phase1_trace = await self._phase1_evaluate(item, capture=capture)
        result["initial_evaluation"] = evaluation
        if phase1_trace:
            result["llm_traces"].append(phase1_trace)

        # Convert to legacy FilterResult for DB storage
        filter_result = FilterResult(
            news_item_id=item.id,
            is_relevant=evaluation.may_affect_market,
            relevance_score=evaluation.relevance_score,
            reason=evaluation.reason,
        )
        result["filter"] = filter_result
        await self.db.save_filter_result(filter_result)

        # Gate check
        if not evaluation.may_affect_market or evaluation.relevance_score < self.relevance_min:
            if test_mode:
                logger.info(
                    "[Pipeline][TEST] Bypassing filter: %s (score=%.2f, affect=%s)",
                    item.title[:50], evaluation.relevance_score, evaluation.may_affect_market,
                )
            else:
                if debug_trace:
                    logger.info(
                        "[Pipeline][DEBUG] Filtered out: %s (score=%.2f) — stopped at Phase 1",
                        item.title[:50], evaluation.relevance_score,
                    )
                else:
                    logger.debug(
                        "[Pipeline] Filtered out: %s (score=%.2f)",
                        item.title[:50], evaluation.relevance_score,
                    )
                return result

        # === Stale article check (skipped in test mode) ===
        if not test_mode and not debug_trace and item.published_at and self.max_article_age_hours > 0:
            from datetime import timezone
            now = datetime.now(timezone.utc)
            pub = item.published_at
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            age_hours = (now - pub).total_seconds() / 3600
            if age_hours > self.max_article_age_hours:
                logger.info(
                    "[Pipeline] Stale article (%.0fh old): %s",
                    age_hours, item.title[:50],
                )
                return result

        # === Phase 2: Deep Research (iterative) ===
        result["stage"] = 2
        deep_research, phase2_traces = await self._phase2_deep_research(
            item, evaluation, capture=capture,
        )
        result["deep_research"] = deep_research
        result["llm_traces"].extend(phase2_traces)

        # Convert to legacy SearchVerification for Feishu compat
        search_verification = self._build_search_verification(item.id, deep_research)
        result["search_verification"] = search_verification
        result["stage"] = 2.5

        # === Phase 3: Final Assessment ===
        result["stage"] = 3
        assessment, phase3_trace = await self._phase3_assess(
            item, evaluation, deep_research, capture=capture,
        )
        result["final_assessment"] = assessment
        if phase3_trace:
            result["llm_traces"].append(phase3_trace)

        # Convert to legacy AnalysisResult for DB storage (with ticker verification)
        analysis = await self._build_analysis_result(item.id, evaluation, assessment)

        # === Tagging Phase: concept + industry tags via LLM ===
        concept_tags, industry_tags = await self._phase_tagging(
            item, evaluation, assessment, capture=capture,
        )
        analysis.concept_tags = concept_tags
        analysis.industry_tags = industry_tags

        result["analysis"] = analysis
        await self.db.save_analysis_result(analysis)

        # Signal score
        signal = score_signal(
            news_item_id=item.id,
            sentiment=assessment.sentiment,
            impact_magnitude=assessment.impact_magnitude,
            surprise_factor=assessment.surprise_factor,
            is_routine=False,
            timeliness=assessment.timeliness,
        )
        result["signal_score"] = signal

        # Alert level
        impact_to_alert = {"critical": "critical", "high": "high", "medium": "medium", "low": "low"}
        result["alert_level"] = impact_to_alert.get(assessment.impact_magnitude, "low")

        # Convert to legacy ResearchReport for DB storage (includes deep_research_data)
        research_report = self._build_research_report(item.id, assessment, deep_research)
        result["research"] = research_report
        await self.db.save_research_report(research_report)

        logger.info(
            "[Pipeline] Final: sentiment=%s impact=%s surprise=%.2f timeliness=%s | %s | tickers=%s",
            assessment.sentiment, assessment.impact_magnitude,
            assessment.surprise_factor, assessment.timeliness,
            assessment.category if hasattr(assessment, 'category') else 'N/A',
            [s.get("ticker", "") for s in evaluation.related_stocks],
        )

        return result

    # ── Content fetching ─────────────────────────────────────

    async def _fetch_content(self, item: NewsItem) -> None:
        """Fetch full article content if needed."""
        if not self.content_fetcher:
            return
        if item.content and len(item.content) >= self.min_existing_content:
            return
        if not item.url:
            return

        text, published_at, error = await self.content_fetcher.fetch(item.url)

        if published_at and not item.published_at:
            item.published_at = published_at
            item.metadata["published_at_source"] = "html_extraction"

        if text:
            item.content = text
            item.metadata["content_fetched"] = True
            item.metadata["content_chars"] = len(text)
            logger.info("[Pipeline] Fetched %d chars for: %s", len(text), item.title[:50])
            try:
                await self.db.update_news_content(
                    item.id, item.content, item.metadata, published_at=item.published_at,
                )
            except Exception as e:
                logger.debug("[Pipeline] DB content update failed: %s", e)
        else:
            item.metadata["content_fetch_failed"] = error

    # ── Phase 1: Initial Evaluation ──────────────────────────

    async def _phase1_evaluate(
        self, item: NewsItem, capture: bool = False,
    ) -> tuple[InitialEvaluation, dict | None]:
        """Phase 1: Evaluate news relevance, market impact, generate bilingual search queries."""
        published_at_str = ""
        if item.published_at:
            published_at_str = item.published_at.strftime("%Y-%m-%d %H:%M")
        else:
            published_at_str = "未知"

        user_prompt = self.prompts["PHASE1_USER_TEMPLATE"].format(
            source=item.source_name,
            title=item.title,
            published_at=published_at_str,
            content=item.snippet(self.max_filter_content_chars),
        )

        resp = await self.llm.chat(
            system_prompt=self.prompts["PHASE1_SYSTEM_PROMPT"],
            user_prompt=user_prompt,
            model=self.llm.model_analyzer,
            max_tokens=self.llm.max_tokens_analyzer,
            temperature=self.llm.temp_analyzer,
            stage="phase1_evaluate",
            source_name=item.source_name,
            capture_prompts=capture,
        )

        trace = None
        if capture:
            trace = {
                "stage": "phase1_evaluate",
                "model": self.llm.model_analyzer,
                "system_prompt": resp.get("_system_prompt", ""),
                "user_prompt": resp.get("_user_prompt", ""),
                "raw_response": resp.get("content", ""),
                "parsed": resp.get("parsed"),
                "usage": resp.get("usage", {}),
            }

        parsed = resp.get("parsed") or {}

        evaluation = InitialEvaluation(
            news_item_id=item.id,
            relevance_score=float(parsed.get("relevance_score", 0.0)),
            may_affect_market=bool(parsed.get("may_affect_market", False)),
            reason=str(parsed.get("reason", "")),
            title_zh=str(parsed.get("title_zh", "")),
            related_stocks=parsed.get("related_stocks", []),
            related_sectors=parsed.get("related_sectors", []),
            search_queries=parsed.get("search_queries", {}),
            google_queries=parsed.get("google_queries", {}),
            model_used=self.llm.model_analyzer,
        )

        # Store title_zh in news item metadata for frontend display
        if evaluation.title_zh and evaluation.title_zh != item.title:
            item.metadata["title_zh"] = evaluation.title_zh
            try:
                await self.db.update_news_content(
                    item.id, item.content, item.metadata, published_at=item.published_at,
                )
            except Exception as e:
                logger.debug("[Pipeline] Failed to save title_zh: %s", e)

        logger.info(
            "[Phase1] score=%.2f affect=%s stocks=%s sectors=%s baidu_queries=%d google_queries=%d | %s",
            evaluation.relevance_score,
            evaluation.may_affect_market,
            [s.get("ticker", "") for s in evaluation.related_stocks[:3]],
            evaluation.related_sectors[:3],
            sum(len(v) for v in evaluation.search_queries.values()),
            sum(len(v) for v in evaluation.google_queries.values()),
            item.title[:50],
        )

        return evaluation, trace

    # ── Phase 2: Deep Research ───────────────────────────────

    async def _phase2_deep_research(
        self,
        item: NewsItem,
        evaluation: InitialEvaluation,
        capture: bool = False,
    ) -> tuple[DeepResearchResult, list[dict]]:
        """Phase 2: Iterative deep research with parallel Baidu+Google search.

        From iteration 1: Baidu (Chinese queries) + Google (English queries) in parallel.
        Max iterations: 3.
        Collects: citations, news timeline, referenced sources.
        """
        traces: list[dict] = []
        research = DeepResearchResult(news_item_id=item.id)
        all_key_findings: list[str] = []
        all_timeline: list[dict] = []
        all_referenced_sources: list[dict] = []

        # Collect initial search queries (bilingual)
        baidu_queries_by_cat: dict[str, list[str]] = evaluation.search_queries or {}
        google_queries_by_cat: dict[str, list[str]] = evaluation.google_queries or {}

        baidu_queries: list[tuple[str, str]] = []  # (query, category)
        google_queries: list[tuple[str, str]] = []  # (query, category)

        for category, queries in baidu_queries_by_cat.items():
            for q in queries:
                baidu_queries.append((q, category))
        for category, queries in google_queries_by_cat.items():
            for q in queries:
                google_queries.append((q, category))

        # Fallback if no queries generated
        if not baidu_queries and not google_queries:
            logger.warning("[Phase2] No search queries from Phase 1, generating fallbacks")
            baidu_queries = [(item.title[:50], "news_coverage")]
            if evaluation.related_stocks:
                ticker = evaluation.related_stocks[0].get("ticker", "")
                name = evaluation.related_stocks[0].get("name", "")
                if ticker:
                    baidu_queries.append((f"{name} {ticker} 最新消息 股价", "stock_performance"))
                    baidu_queries.append((f"{name} 历史影响 股价波动", "historical_impact"))

        # Ensure at least one Baidu query exists (Chinese search is essential for A-share analysis)
        if not baidu_queries and google_queries:
            # Translate first Google query category to Chinese using title
            baidu_queries = [(item.title[:50], "news_coverage")]
            logger.info("[Phase2] Added fallback Baidu query from title since only Google queries exist")

        # Ensure at least one Google query exists (English search for international coverage)
        if not google_queries and baidu_queries:
            # Use title as English query fallback
            google_queries = [(item.title[:60], "news_coverage")]
            logger.info("[Phase2] Added fallback Google query from title since only Baidu queries exist")

        # Store all queries for later reference
        research.search_queries_used = {
            "baidu": [q for q, _ in baidu_queries],
            "google": [q for q, _ in google_queries],
        }

        # Track all queries ever executed to avoid duplicates across iterations
        executed_queries: set[str] = set()

        # Fetch A-share price data once (parallel with first search)
        price_data_task = self._fetch_price_data(evaluation.related_stocks)

        for iteration in range(1, MAX_DEEP_RESEARCH_ITERATIONS + 1):
            # Deduplicate queries against previously executed ones
            baidu_queries = [(q, c) for q, c in baidu_queries if q not in executed_queries]
            google_queries = [(q, c) for q, c in google_queries if q not in executed_queries]
            executed_queries.update(q for q, _ in baidu_queries)
            executed_queries.update(q for q, _ in google_queries)
            logger.info(
                "[Phase2] Iteration %d — %d baidu queries, %d google queries",
                iteration, len(baidu_queries), len(google_queries),
            )
            iter_result = ResearchIteration(iteration=iteration)

            # Run Baidu + Google searches in parallel
            baidu_q_strings = [q for q, _ in baidu_queries]
            google_q_strings = [q for q, _ in google_queries]

            search_results_map = await parallel_search(
                baidu_queries=baidu_q_strings,
                google_queries=google_q_strings,
                baidu_api_key=self.baidu_api_key,
                tavily_api_key=self.tavily_api_key,
                jina_api_key=self.jina_api_key,
                max_results=10,
            )

            # Organize Baidu results by category
            for (query, category) in baidu_queries:
                results = search_results_map.get(query, [])
                for sr in results:
                    search_item = SearchResultItem(
                        title=sr.get("title", ""),
                        url=sr.get("url", ""),
                        content=sr.get("content", ""),
                        date=sr.get("date", ""),
                        score=sr.get("score", 0),
                        source=sr.get("source", ""),
                        website=sr.get("website", ""),
                        category=category,
                        query=query,
                    )
                    iter_result.search_results.append(search_item)
                    research.all_search_results.append(search_item)

                    if category not in research.citations:
                        research.citations[category] = []
                    research.citations[category].append({
                        "title": sr.get("title", ""),
                        "url": sr.get("url", ""),
                        "content": sr.get("content", "")[:300],
                        "date": sr.get("date", ""),
                        "source": sr.get("source", ""),
                        "website": sr.get("website", ""),
                    })

            # Organize Google results by category
            for (query, category) in google_queries:
                results = search_results_map.get(query, [])
                for sr in results:
                    search_item = SearchResultItem(
                        title=sr.get("title", ""),
                        url=sr.get("url", ""),
                        content=sr.get("content", ""),
                        date=sr.get("date", ""),
                        score=sr.get("score", 0),
                        source=sr.get("source", ""),
                        website=sr.get("website", ""),
                        category=category,
                        query=query,
                    )
                    iter_result.search_results.append(search_item)
                    research.all_search_results.append(search_item)

                    if category not in research.citations:
                        research.citations[category] = []
                    research.citations[category].append({
                        "title": sr.get("title", ""),
                        "url": sr.get("url", ""),
                        "content": sr.get("content", "")[:300],
                        "date": sr.get("date", ""),
                        "source": sr.get("source", ""),
                        "website": sr.get("website", ""),
                    })

            # --- Ensure all three engines were called at least once ---
            if iteration == 1:
                engines_used = set()
                for sr in iter_result.search_results:
                    engines_used.add(sr.source)

                missing_engines = []
                if "baidu" not in engines_used and self.baidu_api_key:
                    missing_engines.append("baidu")
                if "tavily" not in engines_used and self.tavily_api_key:
                    missing_engines.append("tavily")
                if "jina" not in engines_used and self.jina_api_key:
                    missing_engines.append("jina")

                if missing_engines:
                    logger.info(
                        "[Phase2] Engines missing from iteration 1: %s — running supplementary searches",
                        missing_engines,
                    )
                    # Use the news title as a fallback query for missing engines
                    fallback_query = item.title[:60]
                    supplementary_tasks = []

                    if "baidu" in missing_engines:
                        from engine.tools.web_search import baidu_search
                        supplementary_tasks.append(
                            baidu_search(fallback_query, self.baidu_api_key, max_results=5)
                        )
                    if "tavily" in missing_engines:
                        from engine.tools.web_search import tavily_search
                        # Use English version of title or first google query
                        en_query = google_q_strings[0] if google_q_strings else fallback_query
                        supplementary_tasks.append(
                            tavily_search(en_query, self.tavily_api_key, max_results=5, topic="news")
                        )
                    if "jina" in missing_engines:
                        from engine.tools.web_search import jina_search
                        en_query = google_q_strings[0] if google_q_strings else fallback_query
                        supplementary_tasks.append(
                            jina_search(en_query, self.jina_api_key, max_results=5)
                        )

                    supp_results = await asyncio.gather(*supplementary_tasks, return_exceptions=True)
                    for r in supp_results:
                        if isinstance(r, list):
                            for sr_dict in r:
                                search_item = SearchResultItem(
                                    title=sr_dict.get("title", ""),
                                    url=sr_dict.get("url", ""),
                                    content=sr_dict.get("content", ""),
                                    date=sr_dict.get("date", ""),
                                    score=sr_dict.get("score", 0),
                                    source=sr_dict.get("source", ""),
                                    website=sr_dict.get("website", ""),
                                    category="supplementary",
                                    query=fallback_query,
                                )
                                iter_result.search_results.append(search_item)
                                research.all_search_results.append(search_item)
                                if "supplementary" not in research.citations:
                                    research.citations["supplementary"] = []
                                research.citations["supplementary"].append({
                                    "title": sr_dict.get("title", ""),
                                    "url": sr_dict.get("url", ""),
                                    "content": sr_dict.get("content", "")[:300],
                                    "date": sr_dict.get("date", ""),
                                    "source": sr_dict.get("source", ""),
                                    "website": sr_dict.get("website", ""),
                                })
                        elif isinstance(r, Exception):
                            logger.warning("[Phase2] Supplementary search error: %s", r)

                    logger.info(
                        "[Phase2] After supplementary searches: total %d results from engines: %s",
                        len(iter_result.search_results),
                        {sr.source for sr in iter_result.search_results},
                    )

            # Get price data (wait for it on first iteration)
            if iteration == 1:
                try:
                    price_data = await price_data_task
                    iter_result.price_data = price_data
                    research.price_data = price_data
                except Exception as e:
                    logger.warning("[Phase2] Price data fetch failed: %s", e)

            # Build context for LLM
            search_results_text = self._format_search_results_for_llm(iter_result.search_results)
            fetched_pages_text = self._format_fetched_pages_for_llm(research.all_fetched_pages)
            price_data_text = self._format_price_data_for_llm(research.price_data)
            previous_findings_text = ""
            if all_key_findings:
                previous_findings_text = (
                    "【之前各轮的关键发现】\n" +
                    "\n".join(f"- {f}" for f in all_key_findings)
                )

            stocks_str = ", ".join(
                f"{s.get('name', '')}({s.get('ticker', '')})"
                for s in evaluation.related_stocks
            ) or "无"

            user_prompt = self.prompts["PHASE2_USER_TEMPLATE"].format(
                title=item.title,
                source=item.source_name,
                published_at=item.published_at or "未知",
                content_summary=item.snippet(2000),
                related_stocks=stocks_str,
                related_sectors=", ".join(evaluation.related_sectors) or "无",
                iteration=iteration,
                search_results=search_results_text,
                fetched_pages=fetched_pages_text,
                price_data=price_data_text,
                previous_findings=previous_findings_text,
            )

            resp = await self.llm.chat(
                system_prompt=self.prompts["PHASE2_SYSTEM_PROMPT"],
                user_prompt=user_prompt,
                model=self.llm.model_researcher,
                max_tokens=self.llm.max_tokens_researcher,
                temperature=self.llm.temp_researcher,
                stage="phase2_research",
                source_name=item.source_name,
                capture_prompts=capture,
            )

            if capture:
                traces.append({
                    "stage": f"phase2_iteration_{iteration}",
                    "model": self.llm.model_researcher,
                    "system_prompt": resp.get("_system_prompt", ""),
                    "user_prompt": resp.get("_user_prompt", ""),
                    "raw_response": resp.get("content", ""),
                    "parsed": resp.get("parsed"),
                    "usage": resp.get("usage", {}),
                    "search_count": len(iter_result.search_results),
                })

            parsed = resp.get("parsed") or {}
            iter_result.is_sufficient = bool(parsed.get("sufficient", False))

            # Minimum research quality gate: don't stop on iteration 1 if results are thin
            if iter_result.is_sufficient and iteration == 1 and len(iter_result.search_results) < 5:
                logger.info(
                    "[Phase2] Overriding early sufficiency — only %d results in iteration 1, "
                    "forcing at least one more iteration",
                    len(iter_result.search_results),
                )
                iter_result.is_sufficient = False

            iter_result.new_queries = parsed.get("new_queries", [])
            iter_result.urls_to_fetch = parsed.get("urls_to_fetch", [])
            iter_result.llm_response = parsed
            research.research_summary = parsed.get("reasoning", "")

            # Record key findings
            findings = parsed.get("key_findings", [])
            all_key_findings.extend(findings)

            # Collect news timeline items
            timeline_items = parsed.get("news_timeline", [])
            if isinstance(timeline_items, list):
                for ti in timeline_items:
                    if isinstance(ti, dict) and ti.get("title"):
                        all_timeline.append(ti)

            # Collect referenced sources
            ref_sources = parsed.get("referenced_sources", [])
            if isinstance(ref_sources, list):
                for rs in ref_sources:
                    if isinstance(rs, dict) and rs.get("url"):
                        all_referenced_sources.append(rs)

            logger.info(
                "[Phase2] Iteration %d: sufficient=%s, urls_to_fetch=%d, new_queries=%d, "
                "findings=%d, timeline=%d, refs=%d",
                iteration, iter_result.is_sufficient,
                len(iter_result.urls_to_fetch), len(iter_result.new_queries),
                len(findings), len(timeline_items), len(ref_sources),
            )

            # Fetch requested URLs
            if iter_result.urls_to_fetch and self.content_fetcher:
                fetched = await self._fetch_urls(iter_result.urls_to_fetch)
                iter_result.fetched_pages = fetched
                research.all_fetched_pages.extend(fetched)

            research.iterations.append(iter_result)
            research.total_iterations = iteration

            # Stop if sufficient
            if iter_result.is_sufficient:
                logger.info("[Phase2] Information sufficient after %d iterations", iteration)
                break

            # Prepare next round queries
            new_baidu = parsed.get("new_queries", [])
            new_google = parsed.get("new_google_queries", [])

            if new_baidu or new_google:
                baidu_queries = [(q, "supplementary") for q in new_baidu[:3]]
                google_queries = [(q, "supplementary") for q in new_google[:3]]
                # Track additional queries
                research.search_queries_used.setdefault("baidu", []).extend(
                    [q for q, _ in baidu_queries]
                )
                research.search_queries_used.setdefault("google", []).extend(
                    [q for q, _ in google_queries]
                )
            elif not iter_result.is_sufficient and iteration < MAX_DEEP_RESEARCH_ITERATIONS:
                # LLM said insufficient but didn't provide new queries — auto-generate
                # fallback queries based on what's missing
                logger.info("[Phase2] No new queries but info insufficient — auto-generating fallbacks")
                stocks_str = " ".join(
                    s.get("name", "") for s in evaluation.related_stocks[:2]
                )
                fallback_baidu = []
                fallback_google = []
                if not research.news_timeline:
                    fallback_baidu.append(f"{item.title[:30]} 最新报道 消息")
                    fallback_google.append(f"{item.title[:40]} latest news coverage")
                if not research.price_data and stocks_str:
                    fallback_baidu.append(f"{stocks_str} 股价走势 近期")
                    fallback_google.append(f"{stocks_str} stock price trend recent")
                if fallback_baidu or fallback_google:
                    baidu_queries = [(q, "auto_fallback") for q in fallback_baidu]
                    google_queries = [(q, "auto_fallback") for q in fallback_google]
                    research.search_queries_used.setdefault("baidu", []).extend(
                        [q for q, _ in baidu_queries]
                    )
                    research.search_queries_used.setdefault("google", []).extend(
                        [q for q, _ in google_queries]
                    )
                else:
                    logger.info("[Phase2] No fallback queries possible, stopping research")
                    break
            else:
                logger.info("[Phase2] No new queries, stopping research")
                break

        # Store aggregated timeline and references
        research.news_timeline = _deduplicate_timeline(all_timeline)
        research.referenced_sources = _deduplicate_refs(all_referenced_sources)

        # Log search engine coverage summary
        engine_counts: dict[str, int] = {}
        for sr in research.all_search_results:
            engine_counts[sr.source] = engine_counts.get(sr.source, 0) + 1
        logger.info(
            "[Phase2] Research complete: %d iterations, %d total results, engine coverage: %s, "
            "timeline entries: %d, fetched pages: %d",
            research.total_iterations, len(research.all_search_results),
            engine_counts, len(research.news_timeline), len(research.all_fetched_pages),
        )

        return research, traces

    async def _fetch_price_data(
        self, related_stocks: list[dict],
    ) -> dict[str, str]:
        """Fetch A-share price data for related stocks via Uqer API."""
        if not self.uqer_token or not related_stocks:
            return {}

        a_share_tickers = [
            s for s in related_stocks
            if re.match(r'^\d{6}$', s.get("ticker", ""))
        ]

        if not a_share_tickers:
            return {}

        from engine.tools.uqer_api import get_market_data

        today = datetime.now()
        end_date = today.strftime("%Y%m%d")
        begin_date = (today - timedelta(days=10)).strftime("%Y%m%d")

        price_data = {}

        async def _fetch_one(stock: dict) -> tuple[str, str]:
            ticker = stock["ticker"]
            name = stock.get("name", ticker)
            try:
                data = await get_market_data(ticker, begin_date, end_date, token=self.uqer_token)
                return ticker, f"{name}({ticker}):\n{data}"
            except Exception as e:
                return ticker, f"{name}({ticker}): 获取失败 — {e}"

        tasks = [_fetch_one(s) for s in a_share_tickers[:5]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                ticker, data = r
                price_data[ticker] = data

        return price_data

    async def _fetch_urls(self, urls: list[str]) -> list[FetchedPage]:
        """Fetch full content from specific URLs.

        Uses the primary content_fetcher first, falls back to Jina Reader API
        (r.jina.ai) if the primary fetcher fails and Jina is configured.
        """
        from engine.tools.web_search import jina_read_url

        pages = []

        async def _fetch_one(url: str) -> FetchedPage:
            error = None
            # Try primary content fetcher first
            try:
                text, _, error = await self.content_fetcher.fetch(url)
                if text and len(text) > 200:
                    return FetchedPage(
                        url=url, title="", content=text[:8000],
                        fetch_success=True,
                    )
            except Exception as e:
                error = str(e)

            # Fallback: try Jina Reader if configured
            if self.jina_api_key and self.jina_use_reader:
                try:
                    jina_text = await jina_read_url(url, self.jina_api_key, max_chars=8000)
                    if jina_text and len(jina_text) > 200:
                        return FetchedPage(
                            url=url, title="", content=jina_text,
                            fetch_success=True,
                        )
                except Exception as e:
                    logger.debug("[Pipeline] Jina reader also failed for %s: %s", url[:60], e)

            return FetchedPage(url=url, fetch_success=False, error=error or "unknown")

        tasks = [_fetch_one(u) for u in urls[:5]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, FetchedPage):
                pages.append(r)
            elif isinstance(r, Exception):
                pages.append(FetchedPage(url="", fetch_success=False, error=str(r)))

        return pages

    # ── Phase 3: Final Assessment ────────────────────────────

    async def _phase3_assess(
        self,
        item: NewsItem,
        evaluation: InitialEvaluation,
        research: DeepResearchResult,
        capture: bool = False,
    ) -> tuple[FinalAssessment, dict | None]:
        """Phase 3: Final assessment with all accumulated context including timeline."""
        published_at_str = ""
        if item.published_at:
            published_at_str = item.published_at.strftime("%Y-%m-%d %H:%M")
        else:
            published_at_str = "未知"

        # Format related stocks
        stocks_str = "\n".join(
            f"- {s.get('name', '')} ({s.get('ticker', '')})"
            for s in evaluation.related_stocks
        ) or "无"

        # Format citations by category
        news_cov = self._format_citations(research.citations.get("news_coverage", []))
        hist_impact = self._format_citations(research.citations.get("historical_impact", []))
        stock_perf = self._format_citations(research.citations.get("stock_performance", []))

        # Include supplementary citations
        supplementary = research.citations.get("supplementary", [])
        if supplementary:
            stock_perf += "\n" + self._format_citations(supplementary)

        # Legacy compat: also check "stock_info" category
        stock_info_citations = research.citations.get("stock_info", [])
        if stock_info_citations:
            stock_perf += "\n" + self._format_citations(stock_info_citations)

        # Format price data
        price_data_text = self._format_price_data_for_llm(research.price_data)

        # Format news timeline
        timeline_text = self._format_timeline_for_llm(research.news_timeline)

        # Format research findings
        findings_text = research.research_summary
        if not findings_text:
            all_findings = []
            for iteration in research.iterations:
                findings = iteration.llm_response.get("key_findings", [])
                all_findings.extend(findings)
            if all_findings:
                findings_text = "\n".join(f"- {f}" for f in all_findings)
            else:
                findings_text = "无深度研究发现"

        user_prompt = self.prompts["PHASE3_USER_TEMPLATE"].format(
            title=item.title,
            source=item.source_name,
            published_at=published_at_str,
            content=item.snippet(self.max_analysis_content_chars),
            related_stocks=stocks_str,
            related_sectors=", ".join(evaluation.related_sectors) or "无",
            research_findings=findings_text,
            news_timeline=timeline_text,
            news_coverage_citations=news_cov or "无",
            historical_impact_citations=hist_impact or "无",
            stock_performance_citations=stock_perf or "无",
            price_data=price_data_text,
        )

        resp = await self.llm.chat(
            system_prompt=self.prompts["PHASE3_SYSTEM_PROMPT"],
            user_prompt=user_prompt,
            model=self.llm.model_analyzer,
            max_tokens=self.llm.max_tokens_analyzer,
            temperature=self.llm.temp_analyzer,
            stage="phase3_assess",
            source_name=item.source_name,
            capture_prompts=capture,
        )

        trace = None
        if capture:
            trace = {
                "stage": "phase3_assess",
                "model": self.llm.model_analyzer,
                "system_prompt": resp.get("_system_prompt", ""),
                "user_prompt": resp.get("_user_prompt", ""),
                "raw_response": resp.get("content", ""),
                "parsed": resp.get("parsed"),
                "usage": resp.get("usage", {}),
            }

        parsed = resp.get("parsed") or {}

        try:
            surprise = float(parsed.get("surprise_factor", 0.5))
            surprise = max(0.0, min(1.0, surprise))
        except (ValueError, TypeError):
            surprise = 0.5

        try:
            confidence = float(parsed.get("confidence", 0.5))
        except (ValueError, TypeError):
            confidence = 0.5

        # Parse per-stock and per-sector sentiment
        raw_per_stock = parsed.get("per_stock_sentiment", [])
        if not isinstance(raw_per_stock, list):
            raw_per_stock = []
        per_stock_sentiment = []
        for entry in raw_per_stock:
            if not isinstance(entry, dict) or not entry.get("ticker"):
                continue
            parsed_entry = {
                "ticker": str(entry["ticker"]).strip(),
                "name": str(entry.get("name", "")).strip(),
                "reason": str(entry.get("reason", "")).strip(),
            }
            # Auto-derive sentiment_score from label (LLM only outputs sentiment + confidence)
            _SCORE_FROM_LABEL = {"very_bullish": 0.8, "bullish": 0.5, "neutral": 0.0, "bearish": -0.5, "very_bearish": -0.8}

            # Extract multi-timeframe predictions
            has_horizons = False
            for tf in ("short_term", "medium_term", "long_term"):
                tf_data = entry.get(tf)
                if isinstance(tf_data, dict) and tf_data.get("sentiment"):
                    has_horizons = True
                    sent_label = str(tf_data["sentiment"]).strip()
                    # If LLM still returns sentiment_score, use it; otherwise derive from label
                    if "sentiment_score" in tf_data and tf_data["sentiment_score"] is not None:
                        try:
                            score = max(-1.0, min(1.0, float(tf_data["sentiment_score"])))
                        except (ValueError, TypeError):
                            score = _SCORE_FROM_LABEL.get(sent_label, 0.0)
                    else:
                        score = _SCORE_FROM_LABEL.get(sent_label, 0.0)
                    try:
                        conf = max(0.0, min(1.0, float(tf_data.get("confidence", 0.5))))
                    except (ValueError, TypeError):
                        conf = 0.5
                    parsed_entry[tf] = {
                        "sentiment": sent_label,
                        "sentiment_score": score,
                        "confidence": conf,
                    }
            # Backward compat: old-style flat "sentiment" field without horizons
            if not has_horizons and entry.get("sentiment"):
                fallback_sent = str(entry["sentiment"]).strip()
                # Derive numerical score from categorical label
                fallback_score = _SCORE_FROM_LABEL.get(fallback_sent, 0.0)
                try:
                    fallback_conf = max(0.0, min(1.0, float(entry.get("confidence", 0.5))))
                except (ValueError, TypeError):
                    fallback_conf = 0.5
                for tf in ("short_term", "medium_term", "long_term"):
                    parsed_entry[tf] = {
                        "sentiment": fallback_sent,
                        "sentiment_score": fallback_score,
                        "confidence": fallback_conf,
                    }
            # Only add if we have at least one horizon
            if any(tf in parsed_entry for tf in ("short_term", "medium_term", "long_term")):
                per_stock_sentiment.append(parsed_entry)

        raw_per_sector = parsed.get("per_sector_sentiment", [])
        if not isinstance(raw_per_sector, list):
            raw_per_sector = []
        per_sector_sentiment = []
        for entry in raw_per_sector:
            if not isinstance(entry, dict) or not entry.get("sector"):
                continue
            parsed_entry = {
                "sector": str(entry["sector"]).strip(),
                "reason": str(entry.get("reason", "")).strip(),
            }
            # Auto-derive sentiment_score from label for sectors too
            _SCORE_FROM_LABEL_S = {"very_bullish": 0.8, "bullish": 0.5, "neutral": 0.0, "bearish": -0.5, "very_bearish": -0.8}

            # Extract multi-timeframe predictions
            has_horizons = False
            for tf in ("short_term", "medium_term", "long_term"):
                tf_data = entry.get(tf)
                if isinstance(tf_data, dict) and tf_data.get("sentiment"):
                    has_horizons = True
                    sent_label = str(tf_data["sentiment"]).strip()
                    if "sentiment_score" in tf_data and tf_data["sentiment_score"] is not None:
                        try:
                            score = max(-1.0, min(1.0, float(tf_data["sentiment_score"])))
                        except (ValueError, TypeError):
                            score = _SCORE_FROM_LABEL_S.get(sent_label, 0.0)
                    else:
                        score = _SCORE_FROM_LABEL_S.get(sent_label, 0.0)
                    try:
                        conf = max(0.0, min(1.0, float(tf_data.get("confidence", 0.5))))
                    except (ValueError, TypeError):
                        conf = 0.5
                    parsed_entry[tf] = {
                        "sentiment": sent_label,
                        "sentiment_score": score,
                        "confidence": conf,
                    }
            # Backward compat: old-style flat "sentiment" field without horizons
            if not has_horizons and entry.get("sentiment"):
                fallback_sent = str(entry["sentiment"]).strip()
                fallback_score = _SCORE_FROM_LABEL_S.get(fallback_sent, 0.0)
                try:
                    fallback_conf = max(0.0, min(1.0, float(entry.get("confidence", 0.5))))
                except (ValueError, TypeError):
                    fallback_conf = 0.5
                for tf in ("short_term", "medium_term", "long_term"):
                    parsed_entry[tf] = {
                        "sentiment": fallback_sent,
                        "sentiment_score": fallback_score,
                        "confidence": fallback_conf,
                    }
            # Only add if we have at least one horizon
            if any(tf in parsed_entry for tf in ("short_term", "medium_term", "long_term")):
                per_sector_sentiment.append(parsed_entry)

        assessment = FinalAssessment(
            news_item_id=item.id,
            surprise_factor=surprise,
            sentiment=parsed.get("sentiment", "neutral"),
            impact_magnitude=parsed.get("impact_magnitude", "low"),
            impact_timeframe=parsed.get("impact_timeframe", "short_term"),
            timeliness=parsed.get("timeliness", "timely"),
            summary=parsed.get("summary", ""),
            key_findings=parsed.get("key_findings", []),
            bull_case=parsed.get("bull_case", ""),
            bear_case=parsed.get("bear_case", ""),
            market_expectation=parsed.get("market_expectation", ""),
            recommended_action=parsed.get("recommended_action", ""),
            confidence=confidence,
            model_used=self.llm.model_analyzer,
            per_stock_sentiment=per_stock_sentiment,
            per_sector_sentiment=per_sector_sentiment,
        )

        # Store category as an attribute (used in logging)
        assessment.category = parsed.get("category", "other")

        return assessment, trace

    # ── Formatting helpers ───────────────────────────────────

    def _format_search_results_for_llm(self, results: list[SearchResultItem]) -> str:
        """Format search results for LLM consumption."""
        if not results:
            return "无搜索结果"

        lines = []
        for i, r in enumerate(results, 1):
            line = f"[{i}] {r.title}"
            if r.website:
                line += f" — {r.website}"
            if r.date:
                line += f" ({r.date})"
            line += f"\n    {r.content[:500]}"
            if r.url:
                line += f"\n    URL: {r.url}"
            line += f"\n    [来源: {r.source}, 类别: {r.category}]"
            lines.append(line)

        return "\n\n".join(lines)

    def _format_fetched_pages_for_llm(self, pages: list[FetchedPage]) -> str:
        """Format fetched pages for LLM consumption."""
        if not pages:
            return ""

        lines = ["【已获取的网页完整内容】"]
        for p in pages:
            if p.fetch_success:
                lines.append(f"\n--- {p.url} ---\n{p.content[:5000]}")
            else:
                lines.append(f"\n--- {p.url} ---\n获取失败: {p.error}")

        return "\n".join(lines)

    def _format_price_data_for_llm(self, price_data: dict[str, str]) -> str:
        """Format price data for LLM consumption."""
        if not price_data:
            return "无股价数据（无A股标的或数据获取失败）"

        lines = ["【A股近期行情数据】"]
        for ticker, data in price_data.items():
            lines.append(f"\n{data}")

        return "\n".join(lines)

    def _format_timeline_for_llm(self, timeline: list[dict]) -> str:
        """Format news timeline for LLM consumption."""
        if not timeline:
            return "暂无新闻传播时间轴数据"

        lines = []
        for i, t in enumerate(timeline, 1):
            time_str = t.get("time", "未知时间")
            source = t.get("source", "未知来源")
            title = t.get("title", "")
            url = t.get("url", "")
            line = f"{i}. [{time_str}] {source}: {title}"
            if url:
                line += f"\n   URL: {url}"
            lines.append(line)

        return "\n".join(lines)

    def _format_citations(self, citations: list[dict]) -> str:
        """Format citations for Phase 3 prompt."""
        if not citations:
            return ""

        lines = []
        # Deduplicate by URL
        seen_urls: set[str] = set()
        for c in citations:
            url = c.get("url", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)

            title = c.get("title", "N/A")[:60]
            content = c.get("content", "")[:200]
            date = c.get("date", "")
            source = c.get("source", "")
            website = c.get("website", "")

            line = f"- [{title}]"
            if website:
                line += f" ({website})"
            if date:
                line += f" [{date}]"
            if content:
                line += f"\n  摘要: {content}"
            if url:
                line += f"\n  URL: {url}"
            lines.append(line)

        return "\n".join(lines[:8])  # Limit to 8 citations per category

    # ── Legacy model builders ────────────────────────────────

    def _build_search_verification(
        self, news_item_id: str, research: DeepResearchResult,
    ) -> SearchVerification:
        """Convert DeepResearchResult to legacy SearchVerification."""
        related_news = []
        for sr in research.all_search_results:
            related_news.append({
                "title": sr.title,
                "url": sr.url,
                "date": sr.date,
                "snippet": sr.content[:200],
                "category": sr.category,
                "source": sr.source,
            })

        return SearchVerification(
            news_item_id=news_item_id,
            related_news=related_news,
            price_data=research.price_data,
            verification_summary=research.research_summary,
            timeliness_info="",
            search_results_raw=[],
        )

    # ── Tagging Phase ────────────────────────────────────────────────

    async def _phase_tagging(
        self,
        item: NewsItem,
        evaluation: InitialEvaluation,
        assessment: FinalAssessment,
        capture: bool = False,
    ) -> tuple[list[str], list[str]]:
        """Tag news with THS concept board names and CITIC level-1 industries.

        Single LLM call that returns both concept tags (0-3) and industry tags (1-3).
        Uses the filter model (fast, cheap) since this is a classification task.

        Returns (concept_tags, industry_tags).
        """
        from config.tags import ACTIVE_CONCEPTS, CITIC_INDUSTRIES

        concept_list_str = "、".join(ACTIVE_CONCEPTS)
        industry_list_str = "、".join(CITIC_INDUSTRIES)

        system_prompt = (
            "你是A股市场的新闻标签分类专家。你的任务是给新闻打上「概念板块」和「行业板块」标签。\n"
            "严格遵守以下规则：\n"
            "1. 概念标签：从提供的概念列表中选择0-3个最相关的概念。只选真正相关的，宁缺毋滥。如果新闻与任何概念都不相关，返回空列表。\n"
            "2. 行业标签：从提供的中信一级行业列表中选择1-3个最相关的行业。至少选1个。\n"
            "3. 必须从给定列表中原样选择，不要修改或创造新名称。\n"
            "4. 返回严格JSON格式，不要包含任何其他内容。"
        )

        user_prompt = (
            f"## 新闻信息\n"
            f"标题：{item.title}\n"
            f"摘要：{assessment.summary or item.snippet(500)}\n"
            f"相关股票：{', '.join(s.get('name', '') for s in evaluation.related_stocks[:5])}\n"
            f"相关行业（初步）：{', '.join(evaluation.related_sectors[:5])}\n\n"
            f"## 概念板块列表\n{concept_list_str}\n\n"
            f"## 中信一级行业列表\n{industry_list_str}\n\n"
            f"请返回JSON：\n"
            f'{{"concept_tags": ["概念1", "概念2"], "industry_tags": ["行业1"]}}'
        )

        # Use a fast, non-reasoning model for classification tasks.
        tagger_model = self.settings.get("llm", {}).get("model_tagger", "MiniMax-M2")

        try:
            resp = await self.llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=tagger_model,
                max_tokens=200,
                temperature=0.1,
                stage="tagging",
                source_name=item.source_name,
                capture_prompts=capture,
            )

            parsed = resp.get("parsed") or {}
            raw_concepts = parsed.get("concept_tags", [])
            raw_industries = parsed.get("industry_tags", [])

            # Validate against known lists
            concept_set = set(ACTIVE_CONCEPTS)
            industry_set = set(CITIC_INDUSTRIES)

            concept_tags = [c for c in raw_concepts if c in concept_set][:3]
            industry_tags = [i for i in raw_industries if i in industry_set][:3]

            logger.info(
                "[Pipeline] Tags: concepts=%s industries=%s | %s",
                concept_tags, industry_tags, item.title[:50],
            )
            return concept_tags, industry_tags

        except Exception as e:
            logger.warning("[Pipeline] Tagging failed: %s — skipping tags", e)
            return [], []

    async def _build_analysis_result(
        self,
        news_item_id: str,
        evaluation: InitialEvaluation,
        assessment: FinalAssessment,
    ) -> AnalysisResult:
        """Convert new models to legacy AnalysisResult for DB storage."""
        stocks = evaluation.related_stocks  # [{"name": "...", "ticker": "..."}]
        raw_tickers = [s.get("ticker", "") for s in stocks if s.get("ticker")]
        # Verify tickers against stock lists + Baidu search
        try:
            verifier = get_stock_verifier(baidu_api_key=self.baidu_api_key)
            tickers = await verifier.verify_news_tickers(raw_tickers, stock_hints=stocks)
        except Exception as e:
            logger.warning("[Pipeline] Ticker verification failed: %s — using LLM names", e)
            # Fallback: format from LLM-provided name+ticker
            tickers = []
            for s in stocks:
                name = s.get("name", "").strip()
                ticker = s.get("ticker", "").strip()
                if name and ticker and name != ticker:
                    tickers.append(f"{name}({ticker})")
                elif ticker:
                    tickers.append(ticker)

        # Build per-stock sentiment mapping: verified ticker → multi-horizon entry
        ticker_sentiments: dict[str, Any] = {}
        if assessment.per_stock_sentiment:
            for pss in assessment.per_stock_sentiment:
                raw_ticker = pss.get("ticker", "").strip()
                pss_name = pss.get("name", "").strip()
                # Match against verified tickers
                matched_key = None
                for verified in tickers:
                    if raw_ticker and (
                        raw_ticker in verified
                        or verified.startswith(raw_ticker)
                        or (pss_name and pss_name in verified)
                    ):
                        matched_key = verified
                        break
                if not matched_key and raw_ticker:
                    matched_key = raw_ticker
                if matched_key:
                    # Build multi-horizon entry
                    entry: dict[str, Any] = {"reason": pss.get("reason", "")}
                    for tf in ("short_term", "medium_term", "long_term"):
                        if tf in pss and isinstance(pss[tf], dict):
                            entry[tf] = pss[tf]
                    ticker_sentiments[matched_key] = entry

        # Build per-sector sentiment mapping
        sector_sentiments: dict[str, Any] = {}
        if assessment.per_sector_sentiment:
            for pss in assessment.per_sector_sentiment:
                sector = pss.get("sector", "").strip()
                if sector:
                    entry: dict[str, Any] = {"reason": pss.get("reason", "")}
                    for tf in ("short_term", "medium_term", "long_term"):
                        if tf in pss and isinstance(pss[tf], dict):
                            entry[tf] = pss[tf]
                    sector_sentiments[sector] = entry

        return AnalysisResult(
            news_item_id=news_item_id,
            sentiment=assessment.sentiment,
            impact_magnitude=assessment.impact_magnitude,
            impact_timeframe=assessment.impact_timeframe,
            affected_tickers=tickers,
            affected_sectors=evaluation.related_sectors,
            category=getattr(assessment, 'category', 'other'),
            summary=assessment.summary,
            key_facts=assessment.key_findings,
            bull_case=assessment.bull_case,
            bear_case=assessment.bear_case,
            requires_deep_research=False,
            research_questions=[],
            model_used=assessment.model_used,
            surprise_factor=assessment.surprise_factor,
            is_routine=False,
            market_expectation=assessment.market_expectation,
            quantified_evidence=[],
            search_questions=[],
            ticker_sentiments=ticker_sentiments,
            sector_sentiments=sector_sentiments,
        )

    def _build_research_report(
        self,
        news_item_id: str,
        assessment: FinalAssessment,
        research: DeepResearchResult,
    ) -> ResearchReport:
        """Convert new models to legacy ResearchReport for DB storage.

        Includes deep_research_data with structured citations, timeline, and references.
        """
        # Build the structured deep research data for frontend display
        deep_research_data = {
            "citations": [],
            "news_timeline": research.news_timeline,
            "referenced_sources": research.referenced_sources,
            "search_queries": getattr(research, "search_queries_used", {}),
            "total_iterations": research.total_iterations,
            "total_search_results": len(research.all_search_results),
            "total_fetched_pages": len(research.all_fetched_pages),
            "fetched_urls": [p.url for p in research.all_fetched_pages if p.fetch_success],
        }

        # Flatten all citations for display
        seen_urls: set[str] = set()
        for category, cites in research.citations.items():
            for c in cites:
                url = c.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    deep_research_data["citations"].append({
                        "title": c.get("title", ""),
                        "url": url,
                        "snippet": c.get("content", "")[:200],
                        "date": c.get("date", ""),
                        "source_engine": c.get("source", ""),
                        "website": c.get("website", ""),
                        "category": category,
                    })

        return ResearchReport(
            news_item_id=news_item_id,
            executive_summary=assessment.summary,
            context=research.research_summary,
            affected_securities="",
            historical_precedent="",
            bull_scenario=assessment.bull_case,
            bear_scenario=assessment.bear_case,
            recommended_actions=assessment.recommended_action,
            risk_factors="",
            confidence=assessment.confidence,
            full_report=json.dumps(deep_research_data, ensure_ascii=False),
            market_data_snapshot=research.price_data or {},
            model_used=assessment.model_used,
        )


def _deduplicate_timeline(items: list[dict]) -> list[dict]:
    """Deduplicate timeline items by URL, sort by time."""
    seen = set()
    result = []
    for item in items:
        url = item.get("url", "")
        title = item.get("title", "")
        key = url or title
        if key and key not in seen:
            seen.add(key)
            result.append(item)
    # Sort by time (best effort — some may have incomplete timestamps)
    result.sort(key=lambda x: x.get("time", "9999"))
    return result


def _deduplicate_refs(items: list[dict]) -> list[dict]:
    """Deduplicate referenced sources by URL."""
    seen = set()
    result = []
    for item in items:
        url = item.get("url", "")
        if url and url not in seen:
            seen.add(url)
            result.append(item)
    return result
