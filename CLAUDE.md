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

1. **Default to the staging worktree for code changes.** If the session's `cwd` is `/home/ygwang/trading_agent`
   (the prod worktree) and the user asks you to modify code, first `cd /home/ygwang/trading_agent_staging` and
   iterate there — never edit prod directly. The only time prod is the right target is when the user asks
   for an emergency hotfix / rollback / deploy.
2. **Restart = restart STAGING.** When the user says "重启 / restart / redeploy" without specifying an env,
   it means staging. Run `./start_web.sh restart` inside `/home/ygwang/trading_agent_staging`. Prod should
   only restart via `./scripts/promote.sh` + `./start_web.sh deploy` in the prod worktree.
3. **Never run crawlers / engine / scanner / memory processor in staging.** They are gated by
   `_prod_only_guard` in `start_web.sh` and will refuse. Use the prod worktree for anything that writes to
   the shared crawler Mongo corpus or burns VIP daily quotas.
4. **Environment scoping is automatic.** `APP_ENV=staging` in `/home/ygwang/trading_agent_staging/.env`
   auto-suffixes Postgres DB (`trading_agent_staging`), Redis DB index (1), Milvus collections
   (`kb_chunks_staging`, `user_kb_chunks_staging`), ClickHouse DB (`db_spider_staging`), and Mongo
   collection names in shared databases (`stg_documents`, `stg_chunks`, `stg_fs.*`, `stg_research_sessions`).
   Helpers live in `backend/app/config.py` (`_suffixed`, `_prefixed`, `effective_*`).
5. **Promotion is a one-liner.** When staging is green, `cd /home/ygwang/trading_agent_staging &&
   ./scripts/promote.sh` fast-forwards `main`, tags the commit, and hands off — operator still runs
   `./start_web.sh deploy` in the prod worktree. **The new `promote.sh` runs `scripts/smoke.sh` against
   staging on `:20301` FIRST and refuses to proceed if any probe fails.** Bypass with `SKIP_SMOKE=1`.
6. **Migrations are forward-compatible.** Never drop/rename columns in a single release; additive-only
   migrations are the default. See `DEPLOYMENT.md` § "Migration discipline" for the full rules.
7. **Frontend dev loop = `npm run dev:staging`, not `npm run build:staging`.** `dev:staging` starts
   Vite on `:5173` with HMR and proxies `/api` + `/ws` to the running staging backend on `:20301`, so
   code changes show up instantly. Only run `npm run build:staging` when you need a bundled artifact
   for `http://39.105.42.197:20301` (i.e. when you want someone else to see the UI without running the
   dev server). `dev:prod` is the same but targets `:8000` for hotfix verification.
8. **`start_web.sh deploy` is now transactional.** It runs via `scripts/deploy_with_rollback.sh`:
   build → record alembic rev + git HEAD → migrate → restart → smoke. Any failure after the migrate
   step auto-rolls back code + schema and restarts the prior version. Set `LEGACY_DEPLOY=1` to opt
   back into the linear path (not recommended outside cold-machine bootstrap).
9. **CI runs on every push to `staging` and on PRs into `main`.** See `.github/workflows/ci.yml` —
   it runs `pytest backend/tests`, the frontend prod + staging builds, and an Alembic linear-history
   check. A failing CI run does not auto-block `promote.sh` (no GitHub enforcement set up on the
   server side yet), but the smoke gate inside `promote.sh` is your live backstop.

Full layout + rationale: **`DEPLOYMENT.md`** at the repo root.

## Iteration + Deploy Safety Nets

Added 2026-04-24 to harden the staging → prod pipeline. Every future session
should respect these layers rather than bypassing them:

| Layer | File | Runs when | What it checks |
|---|---|---|---|
| CI | `.github/workflows/ci.yml` | Push to `staging`; PR into `main` | `pytest backend/tests` (unit slice), `npm run build` + `build:staging`, Alembic single-head check |
| Smoke | `scripts/smoke.sh` | Invoked by `promote.sh` + `deploy_with_rollback.sh` | `/api/health`, `/openapi.json`, `/api/news`, `/api/sources/health`, `/api/analytics/system`, SPA root |
| Promote gate | `scripts/promote.sh` | Operator runs to fast-forward `main` | Runs `smoke.sh` against `:20301` first; refuses if staging is red. Bypass: `SKIP_SMOKE=1` (don't) |
| Rollback deploy | `scripts/deploy_with_rollback.sh` | Called by `./start_web.sh deploy` | build → record alembic+git → migrate → restart → smoke. Rolls back schema + code on any failure after step 2. Opt out: `LEGACY_DEPLOY=1` |

**Rules for Claude sessions:**
- **Never push directly to `main`.** Always commit on `staging`, let `promote.sh` fast-forward.
- **Never bypass smoke with `SKIP_SMOKE=1` casually.** If smoke fails, fix the underlying issue.
- **When adding a new critical endpoint** (e.g. a new API surface employees depend on), add a probe line to `scripts/smoke.sh` so regressions catch it.
- **When adding a new Python test** that needs external services (Milvus/Mongo/LLM keys), add it to the `--ignore=` list in `.github/workflows/ci.yml` so CI stays deterministic.

## Project Overview

Trading Intelligence Platform — a web-based AI research assistant for stock/investment analysis. FastAPI backend + React frontend, Postgres + Redis + MongoDB + Milvus + ClickHouse behind it, 8 upstream crawlers feeding a shared corpus, and a hybrid retrieval stack (BM25 + dense) the chat LLM can tool-call.

## AI Chat Debug Logging

The AI assistant (AI 聊天) has a dedicated debug logging system that records the full lifecycle of every chat request.

**Log file:** `logs/chat_debug.log` (50MB rotation, 10 backups)

**What it records (full query → response lifecycle):**

Request lifecycle
- `REQUEST_START` — user, models, tools enabled, system prompt length, history length
- `MESSAGES_PAYLOAD` — full messages array sent to LLM (≤8000 chars)
- `REQUEST_END` — total elapsed time
- `REQUEST_SUMMARY` — final roll-up: rounds used, tools called, unique search queries, URLs found, URLs read, citations, final content length, total tokens

Model decisions (per round)
- `LLM_REQUEST` — per-round request details (round number, mode, tools)
- `MODEL_REASONING` — any text the model emitted before or alongside tool calls (its stated "plan")
- `TOOL_CALLS_DETECTED` — what tools the LLM decided to call, with full arguments
- `GEMINI_FUNC_CALLS` — Gemini native function call details
- `LLM_DONE` — per-round content length, tokens, latency, finish_reason
- `LLM_RESPONSE_CONTENT` — response text preview (≤1500 chars)
- `LLM_FULL_RESPONSE` — larger dump of final response (≤8000 chars)

Web search internals
- `SEARCH_KEYWORDS` — query_cn / query_en / search_type / recency / cn_stock the LLM chose
- `WEB_SEARCH_ENGINES` — which engines will run for this query
- `SEARCH_ENGINE_CALL` — per-engine API call: status, latency, result count, error
- `SEARCH_URLS_RETURNED` — full list of titles + URLs + websites + dates + scores from each engine
- `WEB_SEARCH_RESULTS` — per-engine success/failure stats, total deduped
- `SEARCH_TOP_RESULTS` — the final top-N reranked results with citation indices and content previews (what the model actually reads)
- `SEARCH_CACHE_HIT` — cache hit for a repeat query
- `WEBPAGE_READ` — read_webpage URL, status, latency, content length, preview (which URL the model chose to dive into and what it got)
- `GEMINI_GROUNDING` — Gemini Google Search grounding info

Tool execution
- `TOOL_EXEC_START/DONE` — per-tool execution timing and result preview
- `TOOL_TIMEOUT` — when a tool exceeds its timeout
- `WEB_SEARCH_TOOL_ENTRY/EXIT` — web search tool entry/exit markers
- `KB_SEARCH_*` / `USER_KB_*` — kb_search / user_kb_search entry, phase A/B timing, result counts, top hits

Gemini-specific flow
- `ROUTE_GEMINI` / `GEMINI_ROUND_START` / `GEMINI_NO_FUNC_CALLS` / `GEMINI_SYNTHESIS_INJECTED` — Gemini routing events
- `SYNTHESIS_FALLBACK` / `SYNTHESIS_DONE` — post-loop synthesis pass

**How to use:** Every request gets a `trace_id` (e.g. `trace=c5bd6fc1ca1d`). Grep by trace_id to follow one request end-to-end across all models and tools.

```bash
# Tail live
tail -f logs/chat_debug.log

# Follow one request end-to-end
grep "trace=c5bd6fc1ca1d" logs/chat_debug.log

# See all search keywords an LLM picked in a session
grep "SEARCH_KEYWORDS" logs/chat_debug.log

# See which URLs a model actually read
grep "WEBPAGE_READ" logs/chat_debug.log

# See final summary for each request
grep "REQUEST_SUMMARY" logs/chat_debug.log -A 10
```

**Code:** `backend/app/services/chat_debug.py` — the `ChatTrace` class and `setup_chat_debug_logging()`. Trace emission is wired through `chat_llm.py`, `web_search_tool.py`, `kb_service.py`, `user_kb_service.py`, and the underlying `src/tools/web_search.py` engines.

Parallel to the live debug log, every request is also persisted to remote Mongo as a `research_sessions` document by `backend/app/services/research_interaction_log.py` and replayed by the admin-only `/admin/research-logs` page. Target collection: `ti-user-knowledge-base.research_sessions` (prod) / `ti-user-knowledge-base.stg_research_sessions` (staging). Writes are best-effort — connection failures degrade to a no-op so the chat path never blocks on logging.

## Key Architecture

- **Backend:** FastAPI at `backend/app/`, runs on port 8000 via uvicorn
- **Chat API:** `backend/app/api/chat.py` — SSE streaming endpoint at `/chat/conversations/{id}/messages/stream`
- **LLM Service:** `backend/app/services/chat_llm.py` — routes to OpenRouter (Claude), Google native API (Gemini), OpenAI native API (GPT)
- **Chat Tool Services** (all tool-callable from the LLM):
  - `kb_service.py` / `kb_vector_query.py` — unified `kb_search` / `kb_fetch_document` / `kb_list_facets` across all 8 crawler platforms (parallel Phase A keyword + Phase B Milvus hybrid, RRF-merged)
  - `user_kb_service.py` / `user_kb_tools.py` — per-team personal knowledge base (`user_kb_search`, `user_kb_fetch_document`)
  - `web_search_tool.py` — Baidu + Tavily + Jina web search + `read_webpage`
  - `alphapai_service.py` / `jinmen_service.py` — **retired to deprecation shims** (2026-04-24). `*_TOOLS = []`, frontend `alphapai_enabled` / `jinmen_enabled` toggles now coerce `kb_enabled=True` for backward compat.
- **Quote Service:** `backend/app/services/stock_quote.py` routes tickers to `quote_providers/` (Futu primary, Alpaca/ClickHouse/yfinance fallbacks); 90s Redis cache, warmed every 60s by the lifespan loop in `main.py`
- **Consensus Forecast:** `backend/app/services/consensus_forecast.py` pulls A-share 一致预期 from Wind MySQL `ASHARECONSENSUS*` tables (30-min Redis cache, re-warmed every 25 min; see Wind memory)
- **Frontend:** React + Vite at `frontend/`, built to `frontend/dist/`

## Server Management

`start_web.sh` manages four process groups **per worktree**:

- **infra** — docker: `ta-postgres-dev`, `ta-redis-dev`, `crawl_data` (MongoDB for crawlers). Containers
  are **shared** across prod+staging; a staging `./start_web.sh infra stop` refuses to tear them down so
  prod stays up.
- **asr** — flock-guarded SSH tunnel `127.0.0.1:8760 → jumpbox:8760` (Qwen3-ASR); kept alive by a
  `* * * * *` crontab entry the `asr start` subcommand installs. Also shared with prod; staging's
  `asr stop` is a no-op.
- **web** — uvicorn backend (auto-starts engine subprocess in prod) + `run_proactive.py` (持仓突发监控
  scanner) + `run_chat_memory_processor.py` (chat feedback → long-term user memory). Staging starts
  **only** the uvicorn backend; engine/scanner/memory processor refuse to start on staging (blocked by
  `_prod_only_guard`).
- **crawl** — `crawler_monitor.py --web --port 8080` + ~18 scraper watchers across 8 platforms (monitor
  auto-spawns them via its `/api/start-all`). **Prod-only** — staging reads the same remote Mongo corpus
  read-only so the crawler account, VIP quotas, and daily rate caps stay single-writer.

**New subcommand for staging bootstrap:** `./start_web.sh init-staging` (idempotent) —
`CREATE DATABASE trading_agent_staging` + run all Alembic migrations against it. Milvus
`kb_chunks_staging` / `user_kb_chunks_staging` collections and Mongo `stg_*` collections are created
lazily on first use.

```bash
# Top-level
./start_web.sh start           # Start all four groups
./start_web.sh stop            # Stop all four groups
./start_web.sh restart         # Restart WEB GROUP ONLY (matches old muscle memory)
./start_web.sh restart-all     # Restart all four groups
./start_web.sh status          # Full status of all groups

# Per-group
./start_web.sh infra {start|stop|status}
./start_web.sh asr   {start|stop|restart|status}
./start_web.sh web   {start|stop|restart|status}
./start_web.sh crawl {start|stop|restart|status}

# Logs
./start_web.sh logs            # backend log (engine output embedded)
./start_web.sh engine-logs     # engine log (falls back to backend log)
./start_web.sh scanner-logs    # run_proactive.py log
./start_web.sh crawl-logs      # crawler_monitor wrapper log

# Other
./start_web.sh build | deploy | migrate
```

Logs: `logs/backend.log`, `logs/engine.log`, `logs/proactive_daemon.log`, `logs/crawler_monitor.log`, `logs/chat_debug.log`, `logs/asr_tunnel.log`.
PID files: `logs/backend.pid`, `logs/proactive.pid`, `logs/crawler_monitor.pid`.

Scrapers are spawned by `crawler_monitor.py` with `start_new_session=True`, so they survive monitor death — `crawl stop` explicitly SIGTERM+SIGKILL them by matching cwd under `crawl/` OR the scraper.py absolute path on cmdline. The backend itself can also start/stop scrapers via `backend/app/services/crawler_manager.py` (used by the `/data-sources` admin UI).

All health/status `curl` calls use `--noproxy '*'` because the shell has `HTTP_PROXY=http://127.0.0.1:7890` (Clash), which would otherwise intercept localhost.

## Tool Integration Details

### Knowledge Base — shared corpus (`kb_search` / `kb_fetch_document` / `kb_list_facets`)
- **Phase A** (`kb_service.py`): metadata filter (ticker / date / doc_type / source) + in-memory char-bigram scoring across all crawler collections concurrently. No embeddings.
- **Phase B** (`kb_vector_query.py`): Milvus 2.5 hybrid search — dense top-100 (TEI Qwen3-Embedding-8B on jumpbox 192.168.31.224:8080) + BM25 top-100 via Milvus Function API, RRF-fused, per-doc cap=3 for diversity. Ingestion (`kb_vector_ingest.py`) is Markdown-aware + atomic-regex-guarded.
- Phase A and Phase B run **in parallel** and merge via RRF — covers un-indexed new collections automatically (see `kb_search_consolidation_2026_04_24` memory). WeChat articles excluded by default via `CollectionSpec.low_quality` + Milvus `doc_type != "wechat_article"` filter.
- Routing flags: `KB_SEARCH_LEGACY=True` rolls back to Phase A only; `VECTOR_SYNC_ENABLED=False` disables the poller/delete-sweep/reaper. Stack under `scripts/kb_vector/` + `docker-compose.vector.yml`.

### Personal Knowledge Base — per-team uploads (`user_kb_search` / `user_kb_fetch_document`)
- User-uploaded files (PDF / MD / DOCX / XLSX / TXT / audio) parsed into chunks → remote Mongo `ti-user-knowledge-base` (see `user_kb_remote_mongo` memory). Collections `documents`, `chunks`, `fs.files`, `fs.chunks` — all rows and GridFS blobs scoped by `user_id` in shared collections (per-user collections would blow past Mongo's soft limit and split the GridFS bucket). Dense vectors in Milvus `user_kb_chunks` (OpenAI `text-embedding-3-small`, 1536-dim).
- Folder tree in Postgres (`kb_folders`): `scope ∈ {personal, public}` × `folder_type ∈ {stock, industry, general}`, 6-level deep. First read auto-creates a "持仓股票" folder per user (gated by `user_preferences.kb_holdings_initialized_at`).
- Audio path: upload → Qwen3-ASR service on jumpbox (via the `asr` SSH tunnel) → transcript → same chunk/index pipeline. Live progress surfaced in `MyKnowledgeBase` UI; `asr_recovery_sweep_loop` re-enqueues pending audio every 60s when the tunnel recovers.
- Shared across team members — `user_kb_search` is team-wide (not user-scoped). BM25 stays available even if Milvus is unreachable (fail-open).

### Web Search
- 3 engines in parallel: Baidu (domestic, fast), Tavily (international), Jina (international, via proxy). API keys in `.env`: `BAIDU_API_KEY`, `TAVILY_API_KEY`, `JINA_API_KEY`.
- `read_webpage` tool for LLM-initiated deep reads of specific URLs.

### Platform homepage widgets
- `/api/platform-info` proxies homepage widgets (hot searches, hot stocks, daily topics, institution-preferred stocks) from AlphaPai / Jinmen / Gangtise SPA APIs, using the crawler's saved credential — 20s in-process cache. Consumed by `PlatformInfo` / `JinmenPlatformInfo` / `GangtisePlatformInfo` pages.

## Crawler System

Raw research data (analyst notes, expert calls, roadshow transcripts, earnings transcripts, WeChat articles, sentiment indicators, etc.) is scraped from **8 external platforms** (~18 parallel watcher variants). All crawlers live under `crawl/` and share a common architecture: one subdirectory + one `scraper.py` per platform, one shared throttle module, one shared monitor, and a common Playwright auto-login skeleton. **Mongo target is the remote ops cluster `192.168.31.176:35002` (u_spider auth)** — migrated 2026-04-23 from localhost; PDFs now live in GridFS on each DB. See `crawler_data_remote_mongo` memory for the full DB-name mapping.

**Data sources:**

| Platform | Dir | Remote Mongo DB | Collections | Auth mechanism |
|---|---|---|---|---|
| AlphaPai (Alpha派) | `crawl/alphapai_crawl/` | `alphapai-full` | `roadshows`, `reports`, `comments`, `wechat_articles` | JWT bearer (localStorage `token`) |
| Jinmen (进门财经) | `crawl/jinmen/` | `jinmen-full` | `meetings`, `reports`, `oversea_reports` | base64 JSON + AES-CBC response decryption |
| Meritco (久谦中台) | `crawl/meritco_crawl/` | `jiuqian-full` | `forum` (type 2 pro + type 3 internal) | RSA-signed `X-My-Header` |
| Third Bridge (高临咨询) | `crawl/third_bridge/` | `third-bridge` | `interviews` | Full AWS Cognito cookie jar (hardest) |
| Funda (funda.ai US equities) | `crawl/funda/` | `funda` | `posts`, `earnings_reports`, `earnings_transcripts`, `sentiments` | `session-token` cookie (tRPC superjson) |
| Gangtise (港推 HK) | `crawl/gangtise/` | `gangtise-full` | `summaries`, `researches`, `chief_opinions` | bearer (`G_token`); **CDN bans proxies — no proxy envs** |
| AceCamp | `crawl/AceCamp/` | `acecamp` | `articles`, `events` | Cookie 三件套 (`user_token` JWT 90d + Rails session 7d + `aceid`) |
| AlphaEngine (阿尔法引擎) | `crawl/alphaengine/` | `alphaengine` | `summaries`, `china_reports`, `foreign_reports`, `news_items` | localStorage `token` JWT + `refresh_token` rotation |
| SentimenTrader | `crawl/sentimentrader/` | `funda.sentimentrader_indicators` | merged into funda DB (u_spider can't create new DBs) | email + password (Playwright-driven, daily) |

**Shared CLI** — every `crawl/*/scraper.py` supports the same flags:

```
--max N              # one-shot max items
--resume             # incremental via top_id checkpoint
--watch --interval N # real-time loop every N seconds
--force              # re-fetch already-stored
--today [--date]     # today-only stats
--show-state         # checkpoint + auth health
--auth TOKEN         # override hardcoded/env token
--since-hours N      # filter by last N hours
--pdf-dir/--skip-pdf # PDF download control
```

**Shared anti-bot** (`crawl/antibot.py`): `AdaptiveThrottle` (base 3s + 2s jitter, 30–60s burst cooldown every 30–40 reqs, exponential backoff on 429/5xx), `DailyCap` (300–500 items/session hard cap), `SessionDead` (exit on 401/403, no retry loop), `parse_retry_after`, `is_auth_dead`. Third Bridge uses stricter defaults (base 4s / jitter 3s / cap 300) because of AWS WAF. Real-time watcher variants loosen to `--throttle-base 1.5 --burst-size 0 --daily-cap 0` because they're only chasing the day's deltas.

**Credential / auto-login subsystem** (new — April 2026):

- `crawl/auto_login_common.py` — shared Playwright login skeleton (locale/timezone per platform, playwright-stealth, OTP relay via Redis BLPOP).
- Each platform has `auto_login.py` wrapping the skeleton; `credentials.json` is gitignored per platform.
- `backend/app/services/credential_manager.py` — single read/write surface for all 8 platforms' credentials; health probe runs `python scraper.py --show-state` as a subprocess so it shares each platform's auth validation.
- `backend/app/services/auto_login_runner.py` — spawns `auto_login.py` as an out-of-process subprocess (never in-tree, otherwise Playwright would stall uvicorn's event loop); status + OTP prompts are relayed through Redis hashes (`login:{platform}:{session_id}` / `otp:{session_id}`).
- `backend/app/services/cdp_screencast_session.py` — live CDP `Page.screencastFrame` → WebSocket to the `DataSources` page, so users can watch the login happen and type OTP/scan QR in real time.
- `backend/app/services/crawler_manager.py` — per-platform scraper spawn/stop tracker backed by Redis PIDs; its variant list is aligned with `crawler_monitor.ALL_SCRAPERS` so the dashboard and admin UI see the same topology.

**MongoDB document schema (common to all platforms):**

- Raw API payload: `list_item`, `detail_result`
- Extracted text: `title`, `release_time`, `release_time_ms`, `content_md` / `summary_md` / `transcript_md` / `insight_md`
- PDF: `pdf_rel_path`, `pdf_local_path`, `pdf_size_bytes`, `pdf_download_error`
- Checkpoint doc: `{_id: "crawler_<type>", top_id, last_processed_id, in_progress, last_run_stats}`
- Daily stats doc: `{_id: "daily_<type>_YYYY-MM-DD", total_on_platform, in_db, not_in_db}`
- Account metadata doc: `{_id: <endpoint_name>, endpoint, response, updated_at}`
- Derived by enrichment: `_canonical_tickers: ["NVDA.US", ...]`, `_unmatched_raw: [...]`, `_canonical_tickers_at`

**Cross-platform ticker normalization** — `scripts/enrich_tickers.py` + `backend/app/services/ticker_normalizer.py` + alias table `backend/app/services/ticker_data/aliases.json` normalize heterogeneous ticker strings (AlphaPai objects, Jinmen `hk03896`, Meritco raw strings, Third Bridge `"1211 HK"`, Funda plain tickers, AceCamp nested objects, AlphaEngine doc-level arrays) into canonical `CODE.MARKET`. Run incrementally (cron-safe):

```bash
PYTHONPATH=. python3 scripts/enrich_tickers.py --incremental
# query after enrichment
# coll.find({"_canonical_tickers": "NVDA.US"})
```

**PDF storage** — PDFs now live in **GridFS** on each platform's remote Mongo DB (`fs.files` / `fs.chunks`), `filename` is the original relative path (`alphapai_pdfs/2025-11/xxx.pdf`). `backend/app/services/pdf_storage.py::stream_pdf_or_file` is the unified reader: GridFS first, fall back to local disk for un-migrated new PDFs. Local `/home/ygwang/crawl_data/{alphapai,jinmen,gangtise,meritco,alphaengine,acecamp}_pdfs/` kept as rollback fallback; `pdf_full/` ~501 GB historical archive stays local; `milvus_data/` holds the vector-stack volumes.

**Monitoring & orchestration:**

- `crawl/crawler_monitor.py` — unified dashboard. Modes: CLI Rich UI (default), `--web --port 8080` (HTTP dashboard, 10s refresh), `--json` (snapshot), `--push-feishu` (5 min watchdog, alerts only on state changes). `ALL_SCRAPERS` is the topology source of truth — keep it in sync with `crawler_manager.CrawlerSpec` so admin UI and monitor agree. Surfaces: doc count today + cumulative, latest docs, checkpoint, process status, auth health inferred from log tail.
- `crawl/weekend_backfill.sh` — stops all watchers, launches all platforms in parallel with aggressive throttling (1.5s base, 0s cap), runs until list exhaustion (24–48 h). Snapshot every 30 min; hard cap 48 h.
- `crawl/weekend_realtime_sidecar.sh` — parallel Meritco type 2 + type 3 with coordinated timing.
- `crawl/weekend_summary.sh` — diff counts vs backfill start.
- `crawl/weekend_tail.sh` — colored multi-log tail.

**Reference docs in repo:** `crawl/CRAWLERS.md` (master ops guide, 8 platforms), `crawl/README.md` (shared architecture + new-platform playbook + antibot), `crawl/BOT_USAGE.md`, `crawl/TICKER_AGGREGATION.md`.

**Ops quick reference:**

```bash
# all states
for d in alphapai_crawl jinmen meritco_crawl third_bridge funda gangtise AceCamp alphaengine sentimentrader; do
  (cd crawl/$d && python3 scraper.py --show-state); done

# incremental (cron-friendly)
cd crawl/alphapai_crawl && python3 scraper.py --resume --max 200

# real-time watcher
cd crawl/alphapai_crawl && python3 scraper.py --watch --resume --interval 600 &

# dashboard
python3 crawl/crawler_monitor.py --web --port 8080
```

## Database Architecture

Five stores, each with a distinct role:

1. **PostgreSQL 16** (`localhost:5432`) — primary operational store. All user/app state (auth, watchlists, chat, predictions, alerts, KB folder tree, enriched mirrors of AlphaPai/Jiuqian). Async SQLAlchemy via `asyncpg`, pool 20 + 10 overflow, `pool_pre_ping=True`. Config: `database_url` in `backend/app/config.py`.
2. **Redis 7** (`localhost:6379`) — rate-limit counters for open-API keys, login session state (`login:{platform}:{session_id}`), OTP relay, scraper PIDs, quote cache, consensus cache.
3. **Remote Mongo** at `192.168.31.176:35002` (u_spider auth) — crawler output (8 platforms, see table above), personal KB (`ti-user-knowledge-base`), and AI research session log (`ti-user-knowledge-base.research_sessions` — co-hosted in the KB DB because u_spider can't create new DBs). Accessed by `*_db.py` routers and the KB / research-log services.
4. **Milvus 2.5** (`docker-compose.vector.yml`, standalone + etcd + MinIO, persisted to `/home/ygwang/crawl_data/milvus_data/`) — hybrid vector + BM25 retrieval. `kb_chunks` (Qwen3-Embedding-8B 4096-dim) for shared corpus; `user_kb_chunks` (OpenAI `text-embedding-3-small` 1536-dim) for personal KB.
5. **ClickHouse** — optional OLAP / time-series (generic node disabled by default; `clickhouse_enabled=False`). Used by engine for backtesting + ticker sentiment aggregation. A **second** ClickHouse node at `192.168.31.137:38123` holds A-share klines (`db_market.t_realtime_kline_1m`, `t_adj_daily_data`) and is queried live by the portfolio dashboard when Futu is down.

Plus one external MySQL dependency: **Wind MySQL** at `192.168.31.176:3306` — `wind.ASHARECONSENSUS*` + `ASHARESTOCKRATINGCONSUSHIS` for A-share 一致预期. No indexes → 15s cold query, 30-min Redis cache, pre-warmed every 25 min.

**PostgreSQL models** (`backend/app/models/`, one module per domain):

- `user.py`, `user_preference.py` — accounts, role (`user`/`boss`/`admin`), language, digest schedule, feed columns, `kb_holdings_initialized_at` timestamp
- `watchlist.py` — `watchlists` + `watchlist_items` (ticker or sector)
- `news.py` — `news_items`, `filter_results` (relevance verdict), `analysis_results` (sentiment, impact, affected tickers, sector sentiments, concept/industry tags), `research_reports` (deep-dive + `deep_research_data` JSONB), `source_health`, `user_sources`, `user_news_read`
- `chat.py` — `chat_conversations`, `chat_messages`, `chat_model_responses` (per-model response: tokens, latency, rating, sources JSONB, `debate_round`), `chat_prompt_templates`, `chat_tracking_topics`, `chat_tracking_alerts`, `chat_recommended_questions`
- `alert_rule.py` — `alert_rules` (JSONB conditions, channels)
- `alphapai.py` — Postgres mirror populated by `alphapai_sync.py`: `alphapai_articles`, `alphapai_comments`, `alphapai_roadshows_cn/us`, `alphapai_sync_state`, `alphapai_digests`
- `jiuqian.py` — `jiuqian_forum` (~50 expert calls), `jiuqian_minutes` (~16 k), `jiuqian_wechat` (~25 k)
- `kb_folder.py` — `kb_folders` tree (scope × folder_type) for both personal and public KB
- `prediction.py` — `stock_predictions` (UUID; direction, horizon 1w–6m, confidence 1–5, target_price), `prediction_edit_logs`, `prediction_evaluation`
- `leaderboard.py` — `signal_evaluations` (per-news accuracy at t0/t1/t5/t20 with prices, returns, correctness flags)
- `source.py` — source config / health
- `api_key.py` — `api_keys` (SHA256-hashed, per-key rate_limit)
- `token_usage.py` — LLM cost tracking per stage (filter/analysis/research/enrich)
- Plus feature-specific modules added as new surfaces land: `chat_memory.py`, `feedback.py`, `kb_skill_template.py`, `recipe.py`, `revenue_model.py`, etc. — check `backend/app/models/` for the current set.

**Alembic migrations** — `backend/alembic/versions/` is the source of truth (`alembic history` to list, `alembic current` for DB state). Run migrations from repo root:

```bash
PYTHONPATH=. alembic upgrade head
```

See `alembic_invocation` memory — the conda `agent` env is required and Postgres is on 5432, not 5433.

**ClickHouse tables** (`engine/clickhouse_store.py`):

- `news_analysis` (ReplacingMergeTree(analyzed_at)) — denormalized news + analysis snapshot
- `news_ticker_events` (ReplacingMergeTree(outcome_updated_at)) — fan-out: one row per (news, ticker), carries t0/t1/t3/t5 prices + returns + correctness
- `token_usage` (MergeTree) — LLM cost events
- `stock_prices` (ReplacingMergeTree(updated_at)) — daily OHLCV
- Partitioned by `toYYYYMM(event_time)`

**Connection management:**

- `backend/app/core/database.py` — `create_async_engine` + `async_sessionmaker(expire_on_commit=False)`
- `backend/app/deps.py` — `get_db()` per-request yield; `request.app.state.redis` holds Redis client (best-effort)
- Engine script has two paths: `engine/database.py` (SQLite fallback for standalone news loop) and `engine/pg_database.py` (Postgres drop-in that syncs engine output → Postgres)

**External data API contract** — every crawled MongoDB DB is exposed via `backend/app/api/<source>_db.py` with a uniform shape:

```
GET /api/{source}-db/stats                      # card metrics (today's counts)
GET /api/{source}-db/{collection}[?ticker=...]  # list + filter
GET /api/{source}-db/{collection}/{id}          # full doc
GET /api/{source}-db/{collection}/{id}/pdf      # PDF stream (alphapai, gangtise, jinmen, meritco, alphaengine)
GET /api/unified/by-symbol/{canonical_id}       # cross-platform by ticker
GET /api/unified/symbols/search?q=...           # alias search
```

## Vector Retrieval & ASR Infrastructure

The hybrid RAG kernel and audio pipeline both depend on services on the jumpbox (`192.168.31.224` in LAN, forwarded on this host via SSH tunnels).

**TEI (Text Embeddings Inference)** — `ops/embed_jumpbox/server.py` serves Qwen3-Embedding-8B on port 8080; `backend/app/services/tei_client.py` wraps it with an LRU-cached single-query path, a high-throughput batch path, a 3-strike circuit breaker (60s cooldown), and strict connect/read timeouts (cf. `infra_futu_opend_required` memory — we never let a hung upstream starve uvicorn).

**Qwen3-ASR** — `ops/asr_jumpbox/server.py` on port 8760, reached through `ops/asr_tunnel/asr_tunnel.sh`. Client: `backend/app/services/user_kb_asr_client.py`. Job lifecycle `queued → running → done|error|cancelled`; progress streamed via `GET /jobs/{id}` polling at 2s cadence, timeouts 600s/job-upload and 3600s/job-overall. Bearer token written to `.env.secrets` by the installer.

**Milvus 2.5** — `docker-compose.vector.yml`. Two collections:

- `kb_chunks` — crawled corpus, Qwen3-Embedding-8B 4096-dim + built-in BM25 function. Ingested by `kb_vector_ingest.py`.
- `user_kb_chunks` — personal uploads, OpenAI `text-embedding-3-small` 1536-dim + built-in BM25. Ingested inline on parse.

Both fail-open: a Milvus outage degrades hybrid search to BM25-only rather than taking down the chat path.

**Proxy bypass** — every Milvus/TEI client seeds `os.environ["NO_PROXY"] += ",127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24"` at import time, matching the `infra_proxy` memory. Clash on 7890 will otherwise silently eat local gRPC and LAN HTTP.

## Quote & Portfolio Dashboard

`backend/app/services/stock_quote.py` routes tickers by `stock_market` label to the right provider:

| Market | Primary | Fallback |
|---|---|---|
| 美股 | Futu `US.AAPL` | Alpaca `/v2/stocks/snapshots` (IEX free) → yfinance for mcap/PE |
| 港股 | Futu `HK.00700` | yfinance (15-min delayed) |
| 主板 / 创业板 / 科创板 | Futu (if `futu_ashare_enabled`) | ClickHouse `db_market` kline/adj_daily + yfinance for PE |
| 韩股 / 日股 / 澳股 | yfinance only | — |

`quote_providers/futu_provider.py` holds a lazy singleton `OpenQuoteContext` to FutuOpenD on `127.0.0.1:11111` and a 120s circuit breaker (see `infra_futu_opend_required` memory — OpenD must be running or uvicorn stalls every ~120s). `quote_providers/clickhouse_provider.py` temporarily strips proxy envs because ClickHouse HTTP is direct LAN.

The dashboard's 持仓概览 also calls `consensus_forecast.fetch_consensus()` for A-share target prices / ratings / forward PE.

## Web Application

### Backend — FastAPI (`backend/app/main.py`)

API routers are mounted in `create_app()` under `/api/*`. Core set (check `main.py` for the current list — new routers are added as features land):

| Router | Prefix | Purpose |
|---|---|---|
| `auth` | `/api/auth` | Register, login, JWT access (60 min) + refresh (7 d), profile |
| `news` | `/api/news` | Feed, detail, search, read state |
| `watchlist` | `/api/watchlists` | User watchlists + items (ticker / sector) |
| `sources` | `/api/sources` | System sources config, portfolio holdings, source health |
| `analytics` | `/api/analytics` | System metrics, source accuracy, trends |
| `admin` | `/api/admin` | User management (admin only) |
| `alphapai` | `/api/alphapai` | Enriched Postgres-mirrored AlphaPai (digest, articles, roadshows, comments) |
| `alphapai_db` | `/api/alphapai-db` | Raw MongoDB AlphaPai + PDF stream |
| `jinmen_db` | `/api/jinmen-db` | Jinmen MongoDB (meetings, reports, oversea_reports) + PDFs |
| `meritco_db` | `/api/meritco-db` | Meritco forum + PDFs |
| `thirdbridge_db` | `/api/thirdbridge-db` | Third Bridge expert interviews |
| `funda_db` | `/api/funda-db` | Funda US equity research + sentiments |
| `gangtise_db` | `/api/gangtise-db` | Gangtise HK research + PDFs |
| `acecamp_db` | `/api/acecamp-db` | AceCamp articles + events |
| `alphaengine_db` | `/api/alphaengine-db` | AlphaEngine summaries / china_reports / foreign_reports / news_items + PDFs |
| `sentimentrader` | `/api/sentimentrader` | 3 US sentiment indicators + PNG assets (Smart/Dumb, Fear/Greed, QQQ Optix) |
| `unified` | `/api/unified` | Cross-platform lookup by canonical ticker |
| `jiuqian` | `/api/jiuqian` | Jiuqian (forum / minutes / wechat) |
| `engine` | `/api/engine` | Start / stop / restart trading engine |
| `favorites` | `/api/favorites` | Bookmarks across all sources |
| `stock_search` | `/api/stock` | Unified stock search (CSV market data) |
| `leaderboard` | `/api/leaderboard` | Source signal accuracy rankings (t0/t1/t5/t20) |
| `signals` | `/api/signals` | Trading signals CRUD |
| `analyst_rating` | `/api/analyst-rating` | Analyst prediction accuracy |
| `chat` | `/api/chat` | Multi-model AI chat with SSE streaming + tool use |
| `predictions` | `/api/predictions` | Stock prediction submission + backtest |
| `open` | `/api/open` | External agent API (API-key-authed, Redis rate limited) |
| `portfolio_news` | `/api/portfolio` | Portfolio-scoped news + quotes + consensus |
| `data_sources` | `/api/data-sources` | Admin credential manager + auto-login runner + live CDP screencast |
| `research_logs` | `/api/research-logs` | Admin-only replay of chat session lifecycle (MongoDB-backed) |
| `user_kb` | `/api/user-kb` | Personal / team knowledge base (upload, parse, search, folder tree) |
| `database_overview` | `/api/admin/database-overview` | Admin dashboard: Postgres row counts (planner estimate + fallback count) + Mongo estimates + Redis DBSIZE |
| `platform_info` | `/api/platform-info` | AlphaPai homepage widget proxy (hot searches / stocks / topics) |

**WebSocket** — `/ws/feed` in `backend/app/ws/feed.py` pushes live news via Redis pub/sub; JWT-auth on first message. `/api/data-sources/{key}/screencast/ws` streams JPEG frames from a CDP-attached Chromium + receives pointer/keyboard events for interactive auto-login.

**Auth** — JWT access + refresh (`backend/app/api/auth.py` / `services/auth_service.py`). Frontend attaches `Authorization: Bearer` automatically (`frontend/src/services/api.ts`). 401 triggers logout + redirect. Role tiers: `user`, `boss`, `admin`. Route guards in `frontend/src/App.tsx`: `ProtectedRoute`, `AdminRoute`, `BossOrAdminRoute`.

**Chat streaming** — `POST /api/chat/conversations/{id}/messages/stream` returns SSE `data: {json}` chunks. Fan-out is concurrent across every requested model; each model uses `call_model_stream_with_tools`, which loops many rounds calling `kb_search`, `kb_fetch_document`, `user_kb_search`, `user_kb_fetch_document`, `web_search`, `read_webpage`. See "AI Chat Debug Logging" above for the per-request trace schema; every request is also persisted to MongoDB via `research_interaction_log.py`.

**Lifespan background services** (`main.py` lifespan, in startup order):

1. Redis connection pool wiring
2. `EngineManager` auto-start (trading engine subprocess)
3. AlphaPai sync + LLM enrichment (if `alphapai_sync_enabled`)
4. Jiuqian sync + LLM enrichment
5. Hot news LLM filter (title → market relevance)
6. Daily backtest scheduler
7. Tracking alert evaluator (topic-based news monitoring)
8. Personal KB startup recovery — Mongo indexes, jieba token backfill, Milvus collection ensure + dense-vector backfill, stuck-parse re-enqueue, ASR recovery sweep loop (60s)
9. Research-interaction recorder init (best-effort Mongo auth)
10. Daily AI-chat recommendation scheduler
11. Quote warmer (60s loop, pre-warms all portfolio tickers into Redis)
12. Consensus forecast warmer (25-min loop, pre-warms Wind queries)

### Frontend — React + Vite (`frontend/`)

Stack: React 18 + TypeScript + Vite, Ant Design UI, **Zustand** for state (auth store with localStorage persistence for tokens only), **axios** for API, **i18next** for bilingual UI (`zh` default, `en` available; `frontend/src/i18n/{zh,en}.json`).

**Page → backend router map** (pages in `frontend/src/pages/`):

| Feature | Pages | Backing routers |
|---|---|---|
| Core | Dashboard, NewsFeed, NewsDetail, Login, Settings | `/api/news`, `/api/sources`, `/api/auth` |
| Watchlist | Watchlist, Favorites | `/api/watchlists`, `/api/favorites` |
| Portfolio | Portfolio | `/api/portfolio` (+ quote/consensus warmers) |
| AI Chat | AIChat | `/api/chat` (SSE) + all tool routers |
| Knowledge Base | MyKnowledgeBase | `/api/user-kb` (folder tree + upload + ASR progress) |
| AlphaPai | AlphaPaiDigest, AlphaPaiFeed, AlphaPaiRoadshows, AlphaPaiComments, AlphaPaiReports, AlphaPaiDB, PlatformInfo | `/api/alphapai`, `/api/alphapai-db`, `/api/platform-info` |
| Jinmen | JinmenDB, JinmenReports, JinmenPlatformInfo | `/api/jinmen-db`, `/api/platform-info` |
| Jiuqian | JiuqianForum, JiuqianMinutes, JiuqianWechat | `/api/jiuqian` |
| Meritco | MeritcoDB | `/api/meritco-db` |
| ThirdBridge | ThirdBridgeDB | `/api/thirdbridge-db` |
| Funda | FundaDB, FundaSentiment | `/api/funda-db` |
| Gangtise | GangtiseDB, GangtisePlatformInfo | `/api/gangtise-db`, `/api/platform-info` |
| AceCamp | AceCampDB (`acecamp/:category`) | `/api/acecamp-db` |
| AlphaEngine | AlphaEngineDB (`alphaengine/:category`) | `/api/alphaengine-db` |
| SentimenTrader | Embedded on Portfolio (via `SentimentTraderCards`) | `/api/sentimentrader` |
| Discover | StockSearch | `/api/stock` |
| Rankings | Leaderboard, AnalystRating | `/api/leaderboard`, `/api/analyst-rating` |
| Prediction | PredictionList, PredictionSubmit, PredictionBacktest (boss/admin) | `/api/predictions` |
| Ops | DataSources (credential manager + live browser), DatabaseOverview | `/api/data-sources` + WS screencast, `/api/admin/database-overview` |
| Admin | Admin, AdminFeed, EngineStatus, Analytics, ResearchLogs | `/api/admin`, `/api/engine`, `/api/analytics`, `/api/research-logs` |
| Sources | Sources | `/api/sources` |

**Shared components** (`frontend/src/components/`): `AppLayout.tsx` (Ant sidebar + header search + user menu), `MarkdownRenderer.tsx` (GFM + Prism), `CitationRenderer.tsx` (inline citation popups, strips LLM-generated trailing source sections), `FavoriteButton.tsx`, `FundaSentimentCard.tsx`, `SentimentTraderCards.tsx` (3-card sentiment strip at the top of Portfolio), `CdpViewer.tsx` (WebSocket JPEG player + pointer/keyboard proxy for the DataSources login viewer), `SpreadsheetEditor.tsx` (inline editing of spreadsheet-type KB documents), `DailyIngestionChart.tsx` (recharts area chart for crawler volume on DatabaseOverview).

### Build & deployment

**Development** — `./start_web.sh start` brings up Postgres + Redis (`docker-compose.dev.yml`), the ASR tunnel, uvicorn (`backend.app.main:app` on `APP_PORT`, single worker), and (prod only) the crawler monitor + watchers. Port defaults: prod=8000, staging=20301 — read from each worktree's `.env`.

**Frontend dev loop (the recommended iteration path):**

```bash
cd /home/ygwang/trading_agent_staging/frontend
npm run dev:staging   # Vite on :5173 with HMR, proxies /api + /ws → :20301
# Open http://localhost:5173 — code changes are live in <1s.
```

- `npm run dev:staging` → staging backend (`:20301`). **Default for iteration.**
- `npm run dev:prod` → prod backend (`:8000`). Use only to verify a hotfix.
- `npm run dev` → legacy shorthand, same as `dev:prod` (kept for muscle memory).

Bundled builds:

- `npm run build:staging` → `frontend/dist-staging/`, served at `http://39.105.42.197:20301`. Only needed when someone else needs to see the UI through the staging backend without running the dev server.
- `npm run build` → `frontend/dist/`, served by prod. Run via `./start_web.sh deploy` after promotion, not by hand.

The Milvus vector stack is started separately (kept out of the default dev flow because it's heavy):

```bash
docker compose -f docker-compose.vector.yml up -d
docker compose -f docker-compose.vector.yml logs -f milvus
```

**Production** (`docker-compose.yml` + `nginx.conf`) — Postgres, Redis, FastAPI backend (Dockerfile), React static from `frontend/dist/`, Nginx reverse proxy. Key nginx settings:

- `/api/` → FastAPI; **SSE timeouts 600s, buffering off** (tool loops can take 2–5 min)
- `/ws/` → FastAPI WebSocket upgrade; 86400 s connection timeout
- `/` → React SPA (static via nginx, or FastAPI catch-all in single-binary mode)

FastAPI can also serve the built frontend itself: `/assets` mount + SPA catch-all in `main.py` lifespan block.

## Config Files (`config/`)

- `portfolio_sources.yaml` — company holdings: one entry per stock, fields include `stock_ticker`, `stock_market` (美股/港股/主板/创业板/科创板/韩股/日股), `news source URL`, CSS selectors, tags. Consumed by the quote warmer, news scraper dispatcher, and dashboard.
- `sources.yaml` — general news sources (RSS/web_scraper/api, priorities p0-p3, categories `ai_technology` / `semiconductors` / `financial_news` / `central_banks` / etc.).
- `settings.yaml` — engine-side defaults.
- `tags.py` — hardcoded fallback lists for `CITIC_INDUSTRIES` (30 level-1 industries) and `ACTIVE_CONCEPTS` (~390 同花顺 concepts, refreshed from the remote DB on engine startup).
