# CLAUDE.md

## 🛑 READ THIS FIRST — Prod vs Staging

This project runs as **two simultaneous deployments** on this host:

| | Production | Staging (default for iteration) |
|---|---|---|
| Working directory | `/home/ygwang/trading_agent`         | `/home/ygwang/trading_agent_staging` |
| Git branch        | `main` (promoted from staging only)  | `staging` (where all iteration happens) |
| Backend port      | `:8000`                              | `:20301` (`http://39.105.42.197:20301`) |
| Role              | employees depend on this; stay stable | experiments, bug fixes, new features |

### Rules for any Claude session working on this repo

1. **Default to the staging worktree for code changes.** If `cwd` is `/home/ygwang/trading_agent` (prod)
   and the user asks for code changes, `cd /home/ygwang/trading_agent_staging` first. Only edit prod for
   emergency hotfix / rollback / deploy.
2. **Restart = restart STAGING.** "重启 / restart / redeploy" without env qualifier means staging:
   `./start_web.sh restart` in `/home/ygwang/trading_agent_staging`. Prod restarts only via
   `./scripts/promote.sh` + `./start_web.sh deploy` in the prod worktree.
3. **Crawlers live in STAGING (since 2026-04-24); engine / scanner / memory processor remain prod-only.**
   The real invariant is "exactly one worktree writes per credential" — enforced by
   `_check_other_worktree_clear` in `start_web.sh`, which refuses `crawl start` on either worktree if the
   other has any `scraper.py` / `crawler_monitor.py` process alive. Staging's monitor listens on
   `:8081` (set via `MONITOR_PORT=8081` in staging `.env`); prod's default is `:8080`. `_prod_only_guard`
   still gates `run_proactive.py` (scanner) + `run_chat_memory_processor.py` + the trading engine — those
   push to users / manage trade state and must not run in staging.
4. **Environment scoping is automatic.** `APP_ENV=staging` in staging `.env` auto-suffixes Postgres DB
   (`trading_agent_staging`), Redis DB index (1), Milvus collections (`kb_chunks_staging`,
   `user_kb_chunks_staging`), ClickHouse DB (`db_spider_staging`), Mongo collection names in shared DBs
   (`stg_documents`, `stg_chunks`, `stg_fs.*`). Helpers in
   `backend/app/config.py` (`_suffixed`, `_prefixed`, `effective_*`).
5. **Promotion is a one-liner.** `cd /home/ygwang/trading_agent_staging && ./scripts/promote.sh`
   fast-forwards `main`, tags the commit. Operator still runs `./start_web.sh deploy` in the prod
   worktree. `promote.sh` runs `scripts/smoke.sh` against staging on `:20301` FIRST and refuses to
   proceed if any probe fails. Bypass with `SKIP_SMOKE=1` (don't).
6. **Migrations are forward-compatible.** Never drop/rename columns in a single release; additive-only.
   See `DEPLOYMENT.md` § "Migration discipline".
7. **Frontend dev loop = `npm run dev:staging`, not `npm run build:staging`.** `dev:staging` starts
   Vite on `:5173` with HMR, proxies `/api` + `/ws` → staging backend on `:20301`. Use `build:staging`
   only for a bundled artifact at `http://39.105.42.197:20301`. `dev:prod` targets `:8000` for hotfix
   verification.
8. **`start_web.sh deploy` is transactional.** Runs via `scripts/deploy_with_rollback.sh`: build →
   record alembic rev + git HEAD → migrate → restart → smoke. Auto-rolls back code + schema on any
   failure after migrate. Set `LEGACY_DEPLOY=1` to opt out (not recommended).
9. **CI runs on push to `staging` and PRs into `main`** (`.github/workflows/ci.yml`): `pytest
   backend/tests`, frontend prod + staging builds, Alembic linear-history check. Failing CI doesn't
   auto-block `promote.sh`; the smoke gate inside `promote.sh` is your live backstop.

Full layout + rationale: **`DEPLOYMENT.md`** at repo root.

## Iteration + Deploy Safety Nets

| Layer | File | Runs when | What it checks |
|---|---|---|---|
| CI | `.github/workflows/ci.yml` | Push to `staging`; PR into `main` | `pytest backend/tests`, frontend builds, Alembic single-head |
| Smoke | `scripts/smoke.sh` | Invoked by `promote.sh` + `deploy_with_rollback.sh` | `/api/health`, `/openapi.json`, `/api/news`, `/api/sources/health`, `/api/analytics/system`, SPA root |
| Promote gate | `scripts/promote.sh` | Operator runs to fast-forward `main` | Smokes `:20301` first; refuses if staging is red |
| Rollback deploy | `scripts/deploy_with_rollback.sh` | Called by `./start_web.sh deploy` | build → record alembic+git → migrate → restart → smoke; rolls back on failure |

**Rules:** Never push directly to `main`. Don't bypass smoke casually. Add probes to `scripts/smoke.sh`
when adding new critical endpoints. Add new external-service tests to `--ignore=` in CI workflow.

## Project Overview

Trading Intelligence Platform — web-based AI research assistant for stock/investment analysis. FastAPI
backend + React frontend, Postgres + Redis + MongoDB + Milvus + ClickHouse, 8 upstream crawlers feeding
a shared corpus, hybrid retrieval (BM25 + dense) the chat LLM tool-calls.

## AI Chat Debug Logging

The AI assistant has a dedicated debug log: **`logs/chat_debug.log`** (50MB rotation, 10 backups).
Every request gets a `trace_id` (e.g. `trace=c5bd6fc1ca1d`); grep by trace_id to follow a request
end-to-end across all models and tools.

**Records (full schema in `backend/app/services/chat_debug.py::ChatTrace`):**
- Request lifecycle: `REQUEST_START`, `MESSAGES_PAYLOAD`, `REQUEST_END`, `REQUEST_SUMMARY` (rounds,
  tools, queries, URLs, citations, tokens)
- Per-round model decisions: `LLM_REQUEST`, `MODEL_REASONING`, `TOOL_CALLS_DETECTED`,
  `GEMINI_FUNC_CALLS`, `LLM_DONE`, `LLM_RESPONSE_CONTENT`, `LLM_FULL_RESPONSE`
- Web search internals: `SEARCH_KEYWORDS`, `WEB_SEARCH_ENGINES`, `SEARCH_ENGINE_CALL`,
  `SEARCH_URLS_RETURNED`, `WEB_SEARCH_RESULTS`, `SEARCH_TOP_RESULTS`, `SEARCH_CACHE_HIT`,
  `WEBPAGE_READ`, `GEMINI_GROUNDING`
- Tool execution: `TOOL_EXEC_START/DONE`, `TOOL_TIMEOUT`, `WEB_SEARCH_TOOL_ENTRY/EXIT`,
  `KB_SEARCH_*` / `USER_KB_*`
- Gemini-specific: `ROUTE_GEMINI`, `GEMINI_ROUND_START`, `GEMINI_NO_FUNC_CALLS`,
  `GEMINI_SYNTHESIS_INJECTED`, `SYNTHESIS_FALLBACK`, `SYNTHESIS_DONE`

```bash
tail -f logs/chat_debug.log
grep "trace=<id>" logs/chat_debug.log               # one request end-to-end
grep "REQUEST_SUMMARY" logs/chat_debug.log -A 10    # per-request roll-ups
grep "WEBPAGE_READ" logs/chat_debug.log             # which URLs models actually read
```

Trace emission wired through `chat_llm.py`, `web_search_tool.py`, `kb_service.py`, `user_kb_service.py`,
and `src/tools/web_search.py`.

## Key Architecture

- **Backend:** FastAPI at `backend/app/`, uvicorn on port 8000 (prod) / 20301 (staging)
- **Chat API:** `backend/app/api/chat.py` — SSE endpoint `/chat/conversations/{id}/messages/stream`
- **LLM Service:** `backend/app/services/chat_llm.py` — routes to OpenRouter (Claude), Google native
  (Gemini), OpenAI native (GPT)
- **Chat Tool Services** (LLM-callable):
  - `kb_service.py` / `kb_vector_query.py` — unified `kb_search` / `kb_fetch_document` /
    `kb_list_facets` across all 8 crawler platforms (parallel Phase A keyword + Phase B Milvus hybrid,
    RRF-merged)
  - `user_kb_service.py` / `user_kb_tools.py` — per-team personal KB (`user_kb_search`,
    `user_kb_fetch_document`)
  - `web_search_tool.py` — Tavily + Jina + `read_webpage` (Baidu disabled 2026-04-27)
  - `alphapai_service.py` / `jinmen_service.py` — **retired to deprecation shims** (2026-04-24).
    `*_TOOLS = []`; frontend `alphapai_enabled` / `jinmen_enabled` toggles coerce `kb_enabled=True`.
- **Quote Service:** `backend/app/services/stock_quote.py` routes to `quote_providers/` (Futu primary,
  Alpaca/ClickHouse/yfinance fallbacks); 90s Redis cache, warmed every 60s by lifespan loop
- **Consensus Forecast:** `backend/app/services/consensus_forecast.py` pulls A-share 一致预期 from Wind
  MySQL `ASHARECONSENSUS*` (30-min Redis cache, re-warmed every 25 min)
- **Frontend:** React + Vite at `frontend/`, built to `frontend/dist/`

## Server Management

`start_web.sh` manages four process groups **per worktree**:

- **infra** — docker: `ta-postgres-dev`, `ta-redis-dev`, `crawl_data` (Mongo). Containers shared
  across prod+staging; staging `infra stop` refuses so prod stays up.
- **asr** — flock-guarded SSH tunnel `127.0.0.1:8760 → jumpbox:8760` (Qwen3-ASR); kept alive by
  crontab. Shared with prod; staging's `asr stop` is a no-op.
- **web** — uvicorn (auto-starts engine subprocess in prod) + `run_proactive.py` (持仓突发监控) +
  `run_chat_memory_processor.py` (chat feedback → long-term memory). Staging starts **only** uvicorn;
  engine/scanner/memory blocked by `_prod_only_guard`.
- **crawl** — `crawler_monitor.py --web --port 8080` + ~18 scraper watchers across 8 platforms
  (auto-spawned via `/api/start-all`). Crawlers now run in **staging** — the real invariant is
  "exactly one worktree writes per credential" (enforced by `_check_other_worktree_clear` in
  `start_web.sh`). Both worktrees see the same shared Mongo at `127.0.0.1:27018`.

**Staging bootstrap:** `./start_web.sh init-staging` (idempotent) — `CREATE DATABASE
trading_agent_staging` + Alembic migrations. Milvus/Mongo collections lazy-created.

```bash
./start_web.sh start | stop | restart | restart-all | status   # restart = web group only
./start_web.sh {infra|asr|web|crawl} {start|stop|status}
./start_web.sh logs | engine-logs | scanner-logs | crawl-logs
./start_web.sh build | deploy | migrate
```

Logs: `logs/{backend,engine,proactive_daemon,crawler_monitor,chat_debug,asr_tunnel}.log`.
PIDs: `logs/{backend,proactive,crawler_monitor}.pid`.

Scrapers spawn with `start_new_session=True`, surviving monitor death — `crawl stop` SIGTERM+SIGKILL
by matching cwd or scraper.py path. Backend can also start/stop via
`backend/app/services/crawler_manager.py` (the `/data-sources` admin UI).

All health/status `curl` calls use `--noproxy '*'` because shell has `HTTP_PROXY=127.0.0.1:7890` (Clash).

## Tool Integration Details

### Knowledge Base — shared corpus (`kb_search` / `kb_fetch_document` / `kb_list_facets`)
- **Phase A** (`kb_service.py`): metadata filter (ticker/date/doc_type/source) + in-memory char-bigram
  scoring across all crawler collections concurrently. No embeddings.
- **Phase B** (`kb_vector_query.py`): Milvus 2.5 hybrid — dense top-100 (TEI Qwen3-Embedding-8B on
  jumpbox 192.168.31.224:8080) + BM25 top-100 via Milvus Function API, RRF-fused, per-doc cap=3.
  Ingestion (`kb_vector_ingest.py`) is Markdown-aware + atomic-regex-guarded.
- Phase A and B run **in parallel** and merge via RRF — covers un-indexed new collections automatically
  (see `kb_search_consolidation_2026_04_24` memory). WeChat articles excluded by default.
- Routing flags: `KB_SEARCH_LEGACY=True` rolls back to Phase A only; `VECTOR_SYNC_ENABLED=False`
  disables the poller/delete-sweep/reaper. Stack under `scripts/kb_vector/` + `docker-compose.vector.yml`.

### Personal Knowledge Base — per-team uploads (`user_kb_search` / `user_kb_fetch_document`)
- User-uploaded files (PDF/MD/DOCX/XLSX/TXT/audio) → chunks → local Mongo `ti-user-knowledge-base`
  (see `user_kb_remote_mongo` memory). Collections `documents`, `chunks`, `fs.files`, `fs.chunks`
  scoped by `user_id` in shared collections (per-user collections would split GridFS bucket). Dense
  vectors in Milvus `user_kb_chunks` (OpenAI `text-embedding-3-small`, 1536-dim).
- Folder tree in Postgres (`kb_folders`): `scope ∈ {personal, public}` × `folder_type ∈ {stock,
  industry, general}`, 6 levels. First read auto-creates "持仓股票" folder per user.
- Audio: upload → Qwen3-ASR (via `asr` SSH tunnel) → transcript → chunk/index. Live progress in
  `MyKnowledgeBase` UI; `asr_recovery_sweep_loop` re-enqueues pending audio every 60s on tunnel recovery.
- `user_kb_search` is team-wide (not user-scoped). BM25 stays available even if Milvus is down.

### Web Search
- 2 engines in parallel for the chat assistant: Tavily (intl) + Jina (intl, via proxy). Keys in `.env`:
  `TAVILY_API_KEY`, `JINA_API_KEY`. **Baidu is disabled for chat (2026-04-27)** — `BAIDU_API_KEY` is
  still consumed by `stock_verifier`, AlphaPai/Jiuqian enrichment processors, and the prod analysis
  pipeline (`src/analysis/pipeline.py`), so the key stays in `.env`. `read_webpage` for LLM-initiated
  deep reads.

### Platform homepage widgets
- `/api/platform-info` proxies homepage widgets (hot searches, hot stocks, daily topics) from AlphaPai
  / Jinmen / Gangtise SPA APIs, using crawler's saved credential — 20s in-process cache.

## Crawler System

Raw research data scraped from **8 external platforms** (~18 watcher variants). All under `crawl/`:
one subdir + `scraper.py` per platform, shared throttle/monitor/auto-login. **Mongo target is local
`ta-mongo-crawl` container on `127.0.0.1:27018`** (no auth, data dir
`/home/ygwang/crawl_data/mongo`); PDFs in GridFS. The 8 DBs migrated to the remote ops cluster
2026-04-23 then back to local 2026-04-26 — DB names retained the `-full` suffixes from the remote
era. See `crawler_data_local_mongo` memory for the mapping.

| Platform | Dir | Mongo DB | Collections | Auth |
|---|---|---|---|---|
| AlphaPai | `crawl/alphapai_crawl/` | `alphapai-full` | `roadshows`, `reports`, `comments`, `wechat_articles` | JWT bearer (localStorage `token`) |
| Jinmen | `crawl/jinmen/` | `jinmen-full` | `meetings`, `reports`, `oversea_reports` | base64 JSON + AES-CBC decryption |
| Meritco | `crawl/meritco_crawl/` | `jiuqian-full` | `forum` (type 2 pro + type 3 internal) | RSA-signed `X-My-Header` |
| Third Bridge | `crawl/third_bridge/` | `third-bridge` | `interviews` | Full AWS Cognito cookie jar |
| Funda | `crawl/funda/` | `funda` | `posts`, `earnings_reports`, `earnings_transcripts`, `sentiments` | `session-token` cookie (tRPC superjson) |
| Gangtise | `crawl/gangtise/` | `gangtise-full` | `summaries`, `researches`, `chief_opinions` | bearer (`G_token`); CDN bans proxies |
| AceCamp | `crawl/AceCamp/` | `acecamp` | `articles`, `events` | Cookie 三件套 |
| AlphaEngine | `crawl/alphaengine/` | `alphaengine` | `summaries`, `china_reports`, `foreign_reports`, `news_items` | localStorage `token` JWT + `refresh_token` rotation |
| SentimenTrader | `crawl/sentimentrader/` | `funda.sentimentrader_indicators` | (merged into funda DB) | email + password (Playwright daily) |

**IR Filings corpus** (added 2026-04-28; 41 portfolio holdings × US/HK/JP/KR exchanges, primary
source for revenue-segment modeling). Stored in dedicated **`ir_filings`** Mongo DB on the same
`:27018` instance. Per-source PDFs at `/home/ygwang/crawl_data/ir_pdfs/<source>/`. Shared registry +
schema helpers at `crawl/ir_filings/{tickers,common}.py`; per-source scraper as a sibling top-level
crawler dir (so `crawler_monitor.py` + admin UI find them with the standard convention).

| Source | Dir | Mongo coll | In-scope tickers | Auth | Endpoints |
|---|---|---|---|---|---|
| **SEC EDGAR** | `crawl/sec_edgar/` | `ir_filings.sec_edgar` (+ `sec_xbrl_facts`) | 22 US | Declared User-Agent only | `data.sec.gov/submissions`, `/api/xbrl/companyfacts`, Archives PDFs |
| **HKEXnews** | `crawl/hkex/` | `ir_filings.hkex` | 14 HK | Cookie+Referer | `www1.hkexnews.hk/search/titleSearchServlet.do` |
| **ASX** | `crawl/asx/` | `ir_filings.asx` (+ `asx_key_statistics`) | 1 AU (SGQ) | None | Markit Digital JSON (`asx.api.markitdigital.com`) → `displayAnnouncement.do` interstitial → `announcements.asx.com.au` PDF |
| **EDINET v2** | `crawl/edinet/` | `ir_filings.edinet` | 2 JP (deferred; awaiting key) | `Subscription-Key` query param (free, MFA registration) | `api.edinet-fsa.go.jp/api/v2/documents.json` + `/documents/{id}?type={1=zip,2=pdf,5=csv}` |
| **TDnet** | `crawl/tdnet/` | `ir_filings.tdnet` | 2 JP (deferred) | None | `webapi.yanoshin.jp/webapi/tdnet/list/{ticker}.json` (mirror; bypasses TDnet 31-day retention + UA gate) |
| **DART** | `crawl/dart/` | `ir_filings.dart` (+ `dart_fnltt`) | 3 KR (deferred) | `crtfc_key` (free, single key per member) | `opendart.fss.or.kr/api/list.json`, `/document.xml`, `/fnlttSinglAcntAll.json` |
| **IR Pages** | `crawl/ir_pages/` | `ir_filings.ir_pages` | 9 US + 1 HK + 3 INTL (Phase 2 — investor decks/presentations/fact sheets from per-company IR sites curated in `config/portfolio_sources.yaml`) | Browser-class UA + Playwright for JS-heavy SPAs | `urls from portfolio_sources.yaml ... [tags: IR]` |

Schema is the unified shape from `crawl/ir_filings/common.py::make_filing_doc` — `release_time_ms`,
`pdf_local_path`, `_canonical_tickers`, etc. are aligned with the existing 8-platform conventions, so
`extract_pdf_texts.py` (writes `pdf_text_md`), `kb_search` Phase A (registered via `kb_service.py`
SPECS_LIST as `sec_filing` / `hkex_filing` / `edinet_filing` / `tdnet_disclosure` / `dart_filing` doc
types), and Milvus ingest pick them up uniformly. New ticker aliases for JP (5801, 285A, 古河,
キオクシア, Kioxia, Furukawa) + KR (Samsung, SK Hynix, 신성이엔지) + AU (SGQ) added to `aliases.json`.
`005930.KS` / `000660.KS` / `011930.KQ` are the canonical KR forms (matches existing tushare
conventions); `SGQ.AU` for AU.

**Mirror API endpoint**: `/api/ir-filings-db/*` — single FastAPI router (`backend/app/api/ir_filings_db.py`)
covers all 7 sources because the schema is unified. Routes: `/sources`, `/stats`, `/sources/{src}`,
`/sources/{src}/{id}`, `/sources/{src}/{id}/pdf`, plus `/xbrl/{ticker}` (SEC companyfacts time series),
`/fnltt/{ticker}` (DART line items), `/key-statistics/{ticker}` (ASX 3-yr revenue panel).

**crawler_monitor / admin UI**: 6 IR sources registered in `crawler_monitor.ALL_SCRAPERS` and
`crawler_manager.SPECS` for spawn/stop tracking + the `/data-sources` admin tab. EDINET/DART surface
"missing key" errors in their watch.log when scraper exits non-zero.

Credentials: drop `{"subscription_key": "..."}` into `crawl/edinet/credentials.json` and
`{"crtfc_key": "..."}` into `crawl/dart/credentials.json` (both gitignored). SEC EDGAR / HKEX / ASX /
TDnet / IR Pages need no auth.

**Shared CLI** — every `scraper.py` supports `--max N`, `--resume`, `--watch --interval N`, `--force`,
`--today [--date]`, `--show-state`, `--auth TOKEN`, `--since-hours N`, `--pdf-dir/--skip-pdf`.

**Shared anti-bot** (`crawl/antibot.py`): `AdaptiveThrottle` (base 3s + 2s jitter, 30–60s burst cooldown
every 30–40 reqs, exponential backoff on 429/5xx), `DailyCap` (300–500/session), `SessionDead` (exit
on 401/403), `parse_retry_after`, `is_auth_dead`. Third Bridge stricter (base 4s/jitter 3s/cap 300)
for AWS WAF. Real-time watchers loosen to `--throttle-base 1.5 --burst-size 0 --daily-cap 0`.

**Credential / auto-login subsystem** (April 2026):
- `crawl/auto_login_common.py` — shared Playwright login skeleton (per-platform locale/timezone,
  playwright-stealth, OTP relay via Redis BLPOP). Per-platform `auto_login.py`; `credentials.json` gitignored.
- `backend/app/services/credential_manager.py` — single read/write for all 8 platforms; health probe
  runs `python scraper.py --show-state` as subprocess.
- `backend/app/services/auto_login_runner.py` — spawns `auto_login.py` out-of-process (in-process
  Playwright stalls uvicorn); status + OTP via Redis hashes.
- `backend/app/services/cdp_screencast_session.py` — CDP `Page.screencastFrame` → WebSocket to
  `DataSources` page for live OTP/QR.
- `backend/app/services/crawler_manager.py` — per-platform spawn/stop tracker (Redis PIDs); aligned
  with `crawler_monitor.ALL_SCRAPERS`.

**Mongo doc schema (common):** raw payload (`list_item`, `detail_result`); extracted text (`title`,
`release_time`, `release_time_ms`, `content_md` / `summary_md` / `transcript_md` / `insight_md`);
PDF (`pdf_rel_path`, `pdf_local_path`, `pdf_size_bytes`, `pdf_download_error`); checkpoint
(`{_id: "crawler_<type>", top_id, last_processed_id, ...}`); daily stats (`{_id: "daily_<type>_<date>"}`);
account metadata; enrichment (`_canonical_tickers`, `_unmatched_raw`, `_canonical_tickers_at`).

**Cross-platform ticker normalization** — `scripts/enrich_tickers.py` +
`backend/app/services/ticker_normalizer.py` + alias table
`backend/app/services/ticker_data/aliases.json` normalize heterogeneous tickers into canonical
`CODE.MARKET`. Cron-safe:
```bash
PYTHONPATH=. python3 scripts/enrich_tickers.py --incremental
# coll.find({"_canonical_tickers": "NVDA.US"})
```

**LLM-input ticker resolution** (2026-04-25) — `kb_service.normalize_ticker_input` (Phase A
+ Phase B both call it) routes through `ticker_normalizer.normalize_one()` first, then a
heuristic fallback. The alias table is **layered**: `aliases_bulk.json` (~50k auto-generated
CN+EN names from Tushare `stock_basic`/`hk_basic` + prod `/home/ygwang/trading_agent/data/
us_stock_list.csv` ∩ Tushare `us_basic`, plus iterative legal-suffix stem stripping for
`Tencent Holdings Ltd.→Tencent`, `阿里巴巴-W→阿里巴巴`, `苹果公司→苹果`) + `aliases.json`
(~270 hand-curated entries that override bulk on conflict — e.g. `Alibaba→BABA.US` vs bulk's
`09988.HK`). Rebuild: `TUSHARE_TOKEN=<hex64> python scripts/rebuild_aliases_bulk.py` (Tushare
token is 64-hex; `AIzaSy*` is a Google API key, not Tushare). Daily/JP/KR intentionally
skipped (only native scripts) and AU has no source. `kb_search` tool description tells the
LLM Chinese/English company names work, so passing `'英伟达'` or `'宁德时代'` resolves before
hitting Mongo `_canonical_tickers` / Milvus `tickers`.

**PDF storage** — GridFS on each platform's local Mongo DB (`fs.files`/`fs.chunks`); `filename` is
original relative path. `backend/app/services/pdf_storage.py::stream_pdf_or_file` reads GridFS first,
falls back to local disk for un-migrated new PDFs. Local
`/home/ygwang/crawl_data/{alphapai,jinmen,gangtise,meritco,alphaengine,acecamp}_pdfs/` kept as
fallback; `pdf_full/` (~501 GB historical) stays local; `milvus_data/` holds vector volumes.

**Monitoring & orchestration:**
- `crawl/crawler_monitor.py` — unified dashboard. Modes: CLI Rich (default), `--web --port 8080`,
  `--json`, `--push-feishu` (5min watchdog). `ALL_SCRAPERS` is topology source of truth — keep in
  sync with `crawler_manager.CrawlerSpec`.
- `crawl/weekend_backfill.sh` — stops watchers, parallel platforms with aggressive throttling
  (1.5s base, 0s cap), 24–48h. Snapshot every 30min; hard cap 48h.
- `crawl/weekend_realtime_sidecar.sh` — parallel Meritco type 2 + type 3.
- `crawl/weekend_summary.sh` / `crawl/weekend_tail.sh`.

Reference docs: `crawl/CRAWLERS.md`, `crawl/README.md`, `crawl/BOT_USAGE.md`,
`crawl/TICKER_AGGREGATION.md`.

## Database Architecture

Five stores, distinct roles:

1. **PostgreSQL 16** (`localhost:5432`) — primary operational store: auth, watchlists, chat,
   predictions, alerts, KB folder tree, enriched mirrors of AlphaPai/Jiuqian. Async SQLAlchemy via
   `asyncpg`, pool 20 + 10 overflow, `pool_pre_ping=True`. Config: `database_url` in
   `backend/app/config.py`.
2. **Redis 7** (`localhost:6379`) — rate-limit counters, login session state, OTP relay, scraper
   PIDs, quote cache, consensus cache.
3. **MongoDB** at `127.0.0.1:27018` (local `ta-mongo-crawl` container, no auth, data dir
   `/home/ygwang/crawl_data/mongo`) — crawler output (8 platforms), personal KB
   (`ti-user-knowledge-base`). Backed up to remote ops cluster `192.168.31.176:35002` for failover.
   A second tiny instance `ta-mongo-state` on `127.0.0.1:27017` holds the kb_vector_sync lease /
   tombstone state (separate from the corpus on purpose).
4. **Milvus 2.5** (`docker-compose.vector.yml`, persisted to `/home/ygwang/crawl_data/milvus_data/`)
   — hybrid vector + BM25. `kb_chunks` (Qwen3-Embedding-8B 4096-dim) shared corpus; `user_kb_chunks`
   (OpenAI `text-embedding-3-small` 1536-dim) personal KB.
5. **ClickHouse** — optional OLAP/time-series (generic node disabled by default). Used by engine for
   backtesting + ticker sentiment. **Second** node at `192.168.31.137:38123` holds A-share klines
   (`db_market.t_realtime_kline_1m`, `t_adj_daily_data`), queried live by portfolio dashboard when
   Futu is down.

Plus **Wind MySQL** at `192.168.31.176:3306` — `wind.ASHARECONSENSUS*` +
`ASHARESTOCKRATINGCONSUSHIS` for A-share 一致预期. No indexes → 15s cold query, 30-min Redis cache,
pre-warmed every 25 min.

**PostgreSQL models** in `backend/app/models/` (one module per domain): `user`, `user_preference`,
`watchlist`, `news`, `chat`, `alert_rule`, `alphapai`, `jiuqian`, `kb_folder`, `prediction`,
`leaderboard`, `source`, `api_key`, `token_usage`, plus feature-specific (`chat_memory`, `feedback`,
`kb_skill_template`, `recipe`, `revenue_model`, etc.) — check `backend/app/models/` for current set.

**Alembic** — `backend/alembic/versions/` is source of truth (`alembic history`, `alembic current`).
Run from repo root: `PYTHONPATH=. alembic upgrade head`. See `alembic_invocation` memory — conda
`agent` env required, Postgres on 5432.

**ClickHouse tables** (`engine/clickhouse_store.py`): `news_analysis`
(ReplacingMergeTree(analyzed_at)), `news_ticker_events` (ReplacingMergeTree(outcome_updated_at)),
`token_usage` (MergeTree), `stock_prices` (ReplacingMergeTree(updated_at)). Partitioned by
`toYYYYMM(event_time)`.

**Connection management:** `backend/app/core/database.py` (`create_async_engine` +
`async_sessionmaker(expire_on_commit=False)`); `backend/app/deps.py` (`get_db()` per-request yield;
`request.app.state.redis`). Engine has two paths: `engine/database.py` (SQLite fallback) and
`engine/pg_database.py` (Postgres drop-in).

**External data API contract** — every crawled MongoDB DB exposed via `backend/app/api/<source>_db.py`:
```
GET /api/{source}-db/stats                      # card metrics
GET /api/{source}-db/{collection}[?ticker=...]  # list + filter
GET /api/{source}-db/{collection}/{id}          # full doc
GET /api/{source}-db/{collection}/{id}/pdf      # PDF stream (alphapai, gangtise, jinmen, meritco, alphaengine)
GET /api/unified/by-symbol/{canonical_id}       # cross-platform by ticker
GET /api/unified/symbols/search?q=...           # alias search
```

## Vector Retrieval & ASR Infrastructure

The hybrid RAG kernel and audio pipeline depend on jumpbox services (`192.168.31.224` LAN, SSH-tunnelled).

**TEI** — `ops/embed_jumpbox/server.py` serves Qwen3-Embedding-8B on port 8080;
`backend/app/services/tei_client.py` wraps with LRU-cached single-query, batch path, 3-strike circuit
breaker (60s cooldown), strict timeouts (cf. `infra_futu_opend_required` memory).

**Qwen3-ASR** — `ops/asr_jumpbox/server.py` on port 8760, via `ops/asr_tunnel/asr_tunnel.sh`. Client:
`backend/app/services/user_kb_asr_client.py`. Job lifecycle `queued → running → done|error|cancelled`;
progress polled every 2s, timeouts 600s upload / 3600s overall. Bearer in `.env.secrets`.

**Milvus 2.5** — two collections both fail-open (Milvus outage → BM25-only, not chat outage):
- `kb_chunks` — crawled corpus, Qwen3-Embedding-8B 4096-dim + built-in BM25. Ingested by `kb_vector_ingest.py`.
- `user_kb_chunks` — personal uploads, OpenAI `text-embedding-3-small` 1536-dim + built-in BM25.

**Proxy bypass** — every Milvus/TEI client seeds `os.environ["NO_PROXY"] +=
",127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24"` at import time (cf. `infra_proxy`
memory). Clash on 7890 silently eats local gRPC and LAN HTTP otherwise.

## Quote & Portfolio Dashboard

`backend/app/services/stock_quote.py` routes by `stock_market`:

| Market | Primary | Fallback |
|---|---|---|
| 美股 | Futu `US.AAPL` | Alpaca `/v2/stocks/snapshots` (IEX free) → yfinance for mcap/PE |
| 港股 | Futu `HK.00700` | yfinance (15-min delayed) |
| 主板 / 创业板 / 科创板 | Futu (if `futu_ashare_enabled`) | ClickHouse `db_market` kline/adj_daily + yfinance for PE |
| 韩股 / 日股 / 澳股 | yfinance only | — |

`quote_providers/futu_provider.py` holds lazy singleton `OpenQuoteContext` to FutuOpenD on
`127.0.0.1:11111`, 120s circuit breaker (cf. `infra_futu_opend_required` — OpenD must be running or
uvicorn stalls). `quote_providers/clickhouse_provider.py` strips proxy envs (direct LAN).

Dashboard's 持仓概览 also calls `consensus_forecast.fetch_consensus()` for A-share target prices /
ratings / forward PE.

## Web Application

### Backend — FastAPI (`backend/app/main.py`)

API routers mounted in `create_app()` under `/api/*` — `main.py` is the source of truth as features
land. Notable surfaces:

- **Per-platform DBs:** `/api/{alphapai,jinmen,meritco,thirdbridge,funda,gangtise,acecamp,
  alphaengine}-db` plus `/api/unified` for cross-platform ticker lookup
- **Chat:** `/api/chat` (SSE streaming + tool use across all KB/web tools)
- **KB:** `/api/user-kb` (personal/team uploads + folder tree)
- **Quote/Portfolio:** `/api/portfolio` (+ quote/consensus warmers in lifespan)
- **Admin:** `/api/admin`, `/api/admin/database-overview`, `/api/data-sources`
  (credential manager + live CDP screencast)
- **Auth:** `/api/auth` JWT access (60min) + refresh (7d)

**WebSocket** — `/ws/feed` pushes live news via Redis pub/sub (JWT-auth on first message).
`/api/data-sources/{key}/screencast/ws` streams JPEG frames + receives pointer/keyboard for
interactive auto-login.

**Auth** — `backend/app/api/auth.py` / `services/auth_service.py`. Frontend attaches `Authorization:
Bearer` via `frontend/src/services/api.ts`; 401 triggers logout+redirect. Roles: `user`, `boss`,
`admin`. Route guards in `frontend/src/App.tsx`.

**Chat streaming** — `POST /api/chat/conversations/{id}/messages/stream` returns SSE `data: {json}`.
Fan-out concurrent across requested models; each uses `call_model_stream_with_tools` looping rounds
calling `kb_search`, `kb_fetch_document`, `user_kb_search`, `user_kb_fetch_document`, `web_search`,
`read_webpage`. See "AI Chat Debug Logging" above.

**Lifespan background services** (`main.py`, in startup order): Redis pool → EngineManager auto-start
→ AlphaPai sync + LLM enrichment → Jiuqian sync + enrichment → hot news LLM filter → daily backtest
scheduler → tracking alert evaluator → personal KB recovery (Mongo indexes, jieba backfill, Milvus
ensure + dense backfill, stuck-parse re-enqueue, ASR recovery sweep 60s) → daily AI-chat
recommendation → quote warmer (60s) → consensus warmer (25min).

### Frontend — React + Vite (`frontend/`)

Stack: React 18 + TS + Vite, Ant Design, **Zustand** (auth store with localStorage tokens only),
**axios**, **i18next** (`zh` default, `en`; `frontend/src/i18n/{zh,en}.json`).

Pages live in `frontend/src/pages/` and map 1:1 to backend routers. Key pages: `Dashboard`,
`Portfolio` (with embedded `SentimentTraderCards`), `AIChat`, `MyKnowledgeBase`, `DataSources`
(credential manager + live browser via `CdpViewer`), `DatabaseOverview`, plus per-platform DB pages.

Shared components in `frontend/src/components/`: `AppLayout`, `MarkdownRenderer` (GFM + Prism),
`CitationRenderer` (inline citation popups, strips LLM-generated trailing source sections),
`FavoriteButton`, `FundaSentimentCard`, `SentimentTraderCards`, `CdpViewer` (WebSocket JPEG +
pointer/keyboard proxy), `SpreadsheetEditor`, `DailyIngestionChart`.

### Build & deployment

**Development** — `./start_web.sh start` brings up Postgres + Redis (`docker-compose.dev.yml`), ASR
tunnel, uvicorn (`backend.app.main:app` on `APP_PORT`, single worker), and (prod only) crawler
monitor + watchers. Ports: prod=8000, staging=20301 (per worktree's `.env`).

**Frontend dev loop (recommended):**
```bash
cd /home/ygwang/trading_agent_staging/frontend
npm run dev:staging   # Vite on :5173 with HMR, proxies /api + /ws → :20301
```
- `dev:staging` → staging backend `:20301`. **Default for iteration.**
- `dev:prod` → prod backend `:8000`. Hotfix verification only.
- `dev` → legacy alias for `dev:prod`.

Bundled builds: `npm run build:staging` → `dist-staging/` for `http://39.105.42.197:20301`;
`npm run build` → `dist/` (run via `./start_web.sh deploy` after promotion, not by hand).

Milvus stack runs separately: `docker compose -f docker-compose.vector.yml up -d`.

**Production** (`docker-compose.yml` + `nginx.conf`) — Postgres, Redis, FastAPI backend, React static
from `frontend/dist/`, Nginx reverse proxy. Key nginx: `/api/` → FastAPI **SSE timeouts 600s,
buffering off** (tool loops 2–5 min); `/ws/` WebSocket upgrade with 86400s connection timeout; `/` →
React SPA (static via nginx, or FastAPI catch-all in single-binary mode).

## Config Files (`config/`)

- `portfolio_sources.yaml` — company holdings: `stock_ticker`, `stock_market`
  (美股/港股/主板/创业板/科创板/韩股/日股), news source URL, CSS selectors, tags. Consumed by quote
  warmer, news scraper dispatcher, dashboard.
- `sources.yaml` — general news sources (RSS/web_scraper/api, priorities p0-p3, categories
  `ai_technology` / `semiconductors` / `financial_news` / `central_banks` / etc.).
- `settings.yaml` — engine-side defaults.
- `tags.py` — `CITIC_INDUSTRIES` (30 level-1) and `ACTIVE_CONCEPTS` (~390 同花顺 concepts, refreshed
  from remote DB on engine startup).
