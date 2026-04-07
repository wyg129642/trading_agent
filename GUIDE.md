# Trading Intelligence Platform — System Guide

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [Quick Start](#3-quick-start)
4. [Management Commands](#4-management-commands)
5. [Deployment & Updates](#5-deployment--updates)
6. [LLM Analysis Engine](#6-llm-analysis-engine)
7. [Data Sources & Monitoring](#7-data-sources--monitoring)
8. [User Guide](#8-user-guide)
9. [API Reference](#9-api-reference)
10. [Troubleshooting](#10-troubleshooting)
11. [Configuration Reference](#11-configuration-reference)
12. [Maintenance Procedures](#12-maintenance-procedures)

---

## 1. System Overview

The Trading Intelligence Platform is a real-time news monitoring and analysis system
designed to help traders identify market-moving events. It monitors 40+ news sources
across US, China A-share, Hong Kong, Korea, and Japan markets, runs a 3-phase LLM
analysis pipeline (GLM-5), and delivers actionable intelligence via web UI, WebSocket,
and Feishu alerts.

**Key Capabilities:**
- Real-time monitoring of 40+ news sources (RSS, Web Scraping, SEC EDGAR, Federal Register)
- 3-phase LLM analysis: Initial Evaluation → Deep Research → Final Assessment
- Portfolio-aware: monitors 29 held stocks across 5 markets
- Multi-channel alerting: Web UI, WebSocket live feed, Feishu webhook
- Bilingual (Chinese/English) interface
- Token budget management with daily cost tracking

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Web Browser                             │
│  React 18 + Ant Design 5 + ECharts + WebSocket                │
└────────────────┬──────────────────────────────────┬─────────────┘
                 │ HTTP/REST                         │ WebSocket
┌────────────────▼──────────────────────────────────▼─────────────┐
│                    FastAPI Backend (port 8000)                   │
│  Auth (JWT) │ News API │ Sources API │ Engine API │ Analytics   │
├─────────────┴──────────┴─────────────┴────────────┴─────────────┤
│              Engine Manager (subprocess control)                 │
└────────┬──────────────────┬──────────────────┬──────────────────┘
         │                  │                  │
┌────────▼────────┐  ┌─────▼──────┐  ┌───────▼──────────────────┐
│  PostgreSQL 16  │  │  Redis 7   │  │  Trading Engine Process  │
│  (shared DB)    │  │  (pub/sub) │  │  42 monitors + pipeline  │
└─────────────────┘  └────────────┘  │  Playwright + GLM-5 LLM  │
                                     └───────────┬──────────────┘
                                                 │
                                     ┌───────────▼──────────────┐
                                     │  External Services       │
                                     │  Zhipu GLM-5 (LLM)     │
                                     │  Baidu AI Search         │
                                     │  Uqer Market Data        │
                                     │  Feishu Webhook          │
                                     └──────────────────────────┘
```

**Data Flow:**
1. Monitors poll news sources at configured intervals (30s-120s)
2. New items saved to PostgreSQL (shared with web backend)
3. Pipeline consumer picks items from queue → 3-phase LLM analysis
4. Results saved to PostgreSQL → broadcast via Redis pub/sub → WebSocket
5. Feishu alerts sent for high-impact items
6. Frontend displays real-time results

---

## 3. Quick Start

### Prerequisites
- Python 3.10+ (conda `agent` environment)
- Node.js 20+ (installed via conda)
- Docker (for PostgreSQL + Redis)
- Playwright browsers: `playwright install chromium`

### First-Time Setup

```bash
cd /home/ygwang/trading_agent

# 1. Start infrastructure (PostgreSQL + Redis)
# 2. Run database migrations
# 3. Start backend (which auto-starts the engine)
./start_web.sh start
```

This single command:
- Starts PostgreSQL 16 + Redis 7 via Docker Compose
- Runs Alembic migrations
- Starts the FastAPI backend on port 8000
- Auto-starts the LLM analysis engine as a managed subprocess
- Serves the built React frontend

### Access Points
| Service | URL |
|---------|-----|
| Web UI (LAN) | http://192.168.31.97:8000 |
| Web UI (Public) | http://116.239.28.36:8000 |
| API Docs | http://192.168.31.97:8000/docs |

### Default Login
- Username: `admin`
- Password: `admin123`
- Role: admin (can manage users, control engine)

---

## 4. Management Commands

All management is through `start_web.sh`:

```bash
./start_web.sh start      # Start everything (DB + Redis + backend + engine)
./start_web.sh stop       # Stop everything gracefully
./start_web.sh restart    # Restart backend + engine (keeps DB running)
./start_web.sh status     # Show health of all components
./start_web.sh logs       # Tail backend logs (includes engine output)
./start_web.sh build      # Rebuild frontend (after code changes)
./start_web.sh deploy     # Full deploy: build frontend → migrate DB → restart
./start_web.sh migrate    # Run database migrations only
```

### Typical Workflows

**View system status:**
```bash
./start_web.sh status
```

**Deploy code updates:**
```bash
git pull
./start_web.sh deploy
```

**View logs:**
```bash
./start_web.sh logs           # All logs
```

---

## 5. Deployment & Updates

### Standard Update Procedure

When code is updated (e.g., engine pipeline improvements, frontend changes):

```bash
cd /home/ygwang/trading_agent

# 1. Pull latest code
git pull origin main

# 2. Install any new Python dependencies
pip install -r backend/requirements.txt

# 3. Install any new Node.js dependencies
cd frontend && npm install && cd ..

# 4. Full deploy (builds frontend, runs migrations, restarts)
./start_web.sh deploy
```

### Engine-Only Update

If only the engine code changed (no frontend/migration changes):

```bash
./start_web.sh restart
```

This stops the backend (which gracefully stops the engine), then restarts both.
The engine auto-starts as a subprocess of the backend.

### Database Migrations

When models change:

```bash
# Generate migration (if needed)
PYTHONPATH=. alembic -c backend/alembic.ini revision --autogenerate -m "description"

# Apply migrations
./start_web.sh migrate
```

### Zero-Downtime Considerations

The engine processes a queue of news items. On restart:
- In-progress LLM calls may be lost (the item will be re-fetched on next poll)
- Monitors warm-start from the database (no duplicate processing)
- Token usage is persisted every 5 minutes; a restart may lose up to 5 min of tracking

---

## 6. LLM Analysis Engine

### Pipeline Overview

The engine runs as a managed subprocess of the backend, sharing PostgreSQL and Redis.

**Phase 1: Initial Evaluation** (GLM-5)
- Evaluates relevance score (0.0-1.0)
- Identifies related stocks and sectors
- Generates search queries for deep research
- Gate: `may_affect_market=True AND relevance_score >= 0.4`

**Phase 2: Deep Research** (GLM-5, up to 3 iterations)
- Baidu AI Search for news coverage, stock info, historical impact
- LLM decides if research is sufficient or needs more queries
- Fetches A-share price data from Uqer API
- URL content fetching via HTTP or Playwright

**Phase 3: Final Assessment** (GLM-5)
- Generates: sentiment, impact magnitude, surprise factor, timeframe
- Bull/bear cases, recommended action, market expectation
- Triggers Feishu alert for medium+ impact items

### Engine Health Monitoring

**Via Web UI:** Navigate to "Engine" (分析引擎) page
- Shows running/stopped status, PID, uptime
- Monitor count, queue size, last heartbeat
- Processing statistics (news processed, analyzed, researched)
- View logs button (admin only)
- Start/Stop/Restart controls (admin only)

**Via API:**
```bash
# Get engine status (requires auth)
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/engine/status

# Get recent logs
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/engine/logs?lines=100
```

**Via Command Line:**
```bash
./start_web.sh status
```

### Auto-Restart

The engine auto-restarts on crash (10-second delay). This can be toggled:
```bash
# Via API (admin only)
curl -X POST "http://localhost:8000/api/engine/auto-restart?enable=true" \
  -H "Authorization: Bearer $TOKEN"
```

### Token Budget

- Daily budget: ¥200 CNY (configurable in `config/settings.yaml`)
- Budget alerts at 50% usage via Feishu
- Rate limit: 1,000,000 TPM
- Usage visible on Analytics page

---

## 7. Data Sources & Monitoring

### Source Types

| Type | Method | Examples |
|------|--------|---------|
| RSS | feedparser | TechCrunch AI, VentureBeat AI, Semiconductor Engineering |
| Web Scraper | Playwright/HTTP | OpenAI Blog, Anthropic, NVIDIA, TSMC, Intel |
| API | Custom clients | SEC EDGAR, Federal Register, SSE/SZSE disclosures |

### Portfolio Holdings (29 stocks)

The portfolio is configured in `config/portfolio_sources.yaml`. These stocks
have dedicated news monitoring sources and are highlighted in analysis.

**Markets covered:**
- US (14): GLW, COHR, AXTI, INTC, SNDK, MU, BA, AAOI, TSM, GOOGL, SGML, LITE
- A-share (13): 天孚通信, 星环科技, 普冉股份, 中际旭创, 新易盛, 盛科通信, 拓荆科技, 长电科技, 宏盛股份, 必创科技, 格林美, 泸州老窖, 炬光科技
- HK (1): 长飞光纤光缆
- International (3): SK Hynix (KR), Kioxia (JP), Samsung (KR)

### Adding Sources

**System sources:** Edit `config/sources.yaml` or `config/portfolio_sources.yaml`, then restart.

**User sources:** Via the web UI → Sources page → "My Subscriptions" tab → Add button.
Each user can add their own news sources and stock subscriptions with market + ticker.

---

## 8. User Guide

### Navigation

| Menu Item | Description |
|-----------|-------------|
| Dashboard (仪表盘) | System overview: news volume, sentiment, source health, engine status |
| News Feed (新闻流) | All analyzed news with filters (sentiment, impact, time), WebSocket live updates |
| Watchlists (自选股) | Create watchlists of tickers, sectors, keywords |
| Sources (数据源) | View system sources, portfolio holdings, manage personal subscriptions |
| Engine (分析引擎) | Engine health monitoring, start/stop controls, log viewer |
| Analytics (分析) | Token usage charts, pipeline statistics, cost breakdown |
| Settings (设置) | Profile settings, language preference |
| Admin (管理) | User management (admin only) |

### Key Pages

**Dashboard:**
- News volume (today/week), sentiment distribution (bullish/bearish counts)
- High-impact signal count, source health overview
- Click "View All" to see critical alerts in the news feed

**News Feed:**
- Filter by sentiment (5 levels), impact (4 levels), time range
- Full-text search across titles
- Each card shows: impact tag, sentiment tag, affected tickers, summary, surprise meter
- Click any item for detailed analysis (bull/bear cases, deep research, pipeline trace)
- Live updates via WebSocket (new items appear at top)

**Sources:**
- **System Sources tab:** All 16 monitoring sources from config with health status
- **Portfolio Holdings tab:** 29 tracked stocks with market, ticker, tags
- **My Subscriptions tab:** Add custom sources with stock market + ticker

**Engine Status:**
- Real-time status with auto-refresh (10s interval)
- Statistics: news processed, analyzed, filtered, deep-researched
- Admin controls: Start, Stop, Restart, View Logs
- Log viewer with color-coded output (errors red, warnings yellow, info green)

### User Roles

| Role | Permissions |
|------|-------------|
| admin | Full access: user management, engine control, all features |
| trader | All features except: user management, engine control |
| viewer | Read-only access to all data |

---

## 9. API Reference

Full API documentation available at `/docs` (Swagger UI).

### Key Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/auth/login` | POST | Login, returns JWT token |
| `/api/news` | GET | List news (filters: sentiment, impact, hours, ticker, source) |
| `/api/news/{id}` | GET | News detail with full analysis |
| `/api/sources` | GET | List all sources (system + user) |
| `/api/sources/config` | GET | System sources from YAML config |
| `/api/sources/portfolio` | GET | Portfolio holdings list |
| `/api/sources` | POST | Add user custom source/subscription |
| `/api/engine/status` | GET | Engine health and statistics |
| `/api/engine/start` | POST | Start engine (admin) |
| `/api/engine/stop` | POST | Stop engine (admin) |
| `/api/engine/restart` | POST | Restart engine (admin) |
| `/api/engine/logs` | GET | Recent engine log lines |
| `/api/analytics/token-usage` | GET | Token usage statistics |
| `/api/analytics/pipeline` | GET | Pipeline processing statistics |
| `/api/watchlists` | GET/POST | Watchlist management |
| `/api/health` | GET | System health check |

---

## 10. Troubleshooting

### Engine Not Starting

```bash
# Check logs
./start_web.sh logs

# Common issues:
# 1. Missing Playwright browsers
playwright install chromium

# 2. Missing Python dependencies
pip install -r backend/requirements.txt

# 3. Database not running
./start_web.sh start  # starts DB first
```

### Pages Show Empty Data

1. Check engine is running: `./start_web.sh status`
2. Engine needs time to poll sources and run LLM analysis (first results in ~2 minutes)
3. Check engine logs for errors: visit Engine page → View Logs
4. Verify database has data: check Dashboard news volume count

### Token Budget Exceeded

If the daily budget (¥200) is exceeded, the engine stops making LLM calls.
- Check usage on Analytics page
- Budget resets at midnight
- Adjust in `config/settings.yaml`: `token_budget.daily_budget_cny`

### Source Fetch Failures

- Sources behind GFW (Google, Reuters) are disabled by default
- Sources requiring Playwright show `requires_browser: true` in config
- Check source health on Dashboard or Sources page
- Consecutive failures > 3 triggers a Feishu system alert

### Database Connection Issues

```bash
# Check PostgreSQL
sg docker -c "docker exec ta-postgres-dev pg_isready -U trading_agent"

# Check Redis
sg docker -c "docker exec ta-redis-dev redis-cli ping"

# Restart infrastructure
./start_web.sh stop
./start_web.sh start
```

---

## 11. Configuration Reference

### config/settings.yaml

| Key | Default | Description |
|-----|---------|-------------|
| `llm.provider` | zhipu | LLM provider (zhipu/minimax/openrouter) |
| `llm.model_analyzer` | glm-5 | Model for Phase 1 & 3 |
| `llm.model_researcher` | glm-5 | Model for Phase 2 |
| `token_budget.daily_budget_cny` | 200 | Daily token budget in CNY |
| `token_budget.rate_limit_tpm` | 1000000 | Tokens per minute limit |
| `thresholds.relevance_min` | 0.4 | Phase 1 relevance gate |
| `intervals.p0_critical` | 30 | P0 source polling interval (seconds) |
| `intervals.p1_high` | 60 | P1 source polling interval |
| `intervals.p2_medium` | 120 | P2 source polling interval |
| `content_fetch.max_content_chars` | 25000 | Max article content to fetch |
| `alert_management.dedup_window_minutes` | 30 | Alert deduplication window |

### Environment Variables (.env)

| Variable | Description |
|----------|-------------|
| `POSTGRES_*` | PostgreSQL connection (host, port, db, user, password) |
| `REDIS_HOST/PORT` | Redis connection |
| `JWT_SECRET_KEY` | JWT signing key (change in production!) |
| `LLM_API_KEY` | Zhipu API key |
| `FEISHU_WEBHOOK_URL` | Feishu alert webhook |
| `BAIDU_API_KEY` | Baidu AI Search API key |
| `UQER_TOKEN` | Uqer/DataYes market data API token |
| `ALPHAPAI_APP_AGENT` | AlphaPai integration key |
| `MINIMAX_API_KEY` | MiniMax LLM key (for AlphaPai enrichment) |

---

## 12. Maintenance Procedures

### Daily

- Monitor the Dashboard for source health (red tags = failures)
- Check Analytics page for token spend
- Engine auto-restarts on crash; check restart count on Engine page

### Weekly

- Review engine logs for recurring errors
- Check if any sources have high consecutive failure counts
- Monitor token cost trends on Analytics page

### Monthly

- Review and update portfolio holdings in `config/portfolio_sources.yaml`
- Update source configurations if websites change structure
- Check for Python/Node.js dependency updates
- Review and rotate JWT secret key if needed

### Database Maintenance

```bash
# Backup PostgreSQL
sg docker -c "docker exec ta-postgres-dev pg_dump -U trading_agent trading_agent" > backup_$(date +%Y%m%d).sql

# Check database size
sg docker -c "docker exec ta-postgres-dev psql -U trading_agent -c \"SELECT pg_size_pretty(pg_database_size('trading_agent'))\""
```

### Log Rotation

Engine logs are at `logs/agent_YYYYMMDD.log` (rotated daily by filename).
Backend logs are at `logs/backend.log` (append-only, manage manually):

```bash
# Truncate backend log if it gets too large
> logs/backend.log
./start_web.sh restart
```

### Upgrading the LLM Model

1. Edit `config/settings.yaml`: change `llm.model_analyzer` and `llm.model_researcher`
2. Update pricing in `engine/utils/token_tracker.py` if needed
3. Restart: `./start_web.sh restart`

### Adding a New Stock to Portfolio

1. Edit `config/portfolio_sources.yaml`
2. Add a new source entry with the stock's IR/news page
3. Set `stock_ticker`, `stock_name`, `stock_market` fields
4. Restart: `./start_web.sh restart`
5. The stock will appear on the Portfolio Holdings tab in the Sources page
