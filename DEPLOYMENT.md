# Deployment — Production & Staging

This repo runs as **two simultaneous deployments on the same host**, sharing
infrastructure but fully isolated at the data layer.

| Axis                        | Production (`main`)                         | Staging (`staging`)                            |
|-----------------------------|---------------------------------------------|------------------------------------------------|
| Working directory           | `/home/ygwang/trading_agent`                | `/home/ygwang/trading_agent_staging`           |
| Git branch                  | `main` (fast-forward only, tagged releases) | `staging` (default dev branch)                 |
| Public endpoint             | old domain (`APP_PORT=8000`)                | `http://39.105.42.197:20301`                   |
| PostgreSQL database         | `trading_agent`                             | `trading_agent_staging` (same container)       |
| Redis logical DB            | index 0                                     | index 1 (same container)                       |
| Milvus collections (crawled)| `kb_chunks`                                 | `kb_chunks_staging`                            |
| Milvus collections (user KB)| `user_kb_chunks`                            | `user_kb_chunks_staging`                       |
| ClickHouse DB               | `db_spider`                                 | `db_spider_staging`                            |
| Mongo (crawler corpus)      | writes                                      | **read-only** (shared)                         |
| Mongo (personal KB)         | `documents` / `chunks` / `fs.files` / `fs.chunks` | `stg_documents` / `stg_chunks` / `stg_fs.files` / `stg_fs.chunks` |
| Mongo (research sessions)   | `research_sessions`                         | `stg_research_sessions`                        |
| Crawlers / engine / scanner | run                                         | **refuse to start** (guarded in `start_web.sh`) |
| ASR tunnel / FutuOpenD      | run once                                    | shared with prod                               |
| Wind MySQL / Market CH      | read                                        | read (same connection)                         |

Both deployments are driven by a single file-based switch: **`APP_ENV`** in
each worktree's `.env`. Every other scoping decision (DB name, Redis index,
Milvus collection, Mongo collection prefix) derives from it via helpers in
`backend/app/config.py`.

---

## First-time setup

### 1. Create the staging worktree

From the prod worktree (the one this repo was originally cloned into):

```bash
cd /home/ygwang/trading_agent

# Create the staging branch if it doesn't exist yet. If someone else already
# pushed `staging`, this is a no-op.
git fetch origin
git branch staging origin/staging 2>/dev/null || git branch staging main

# Second working tree, sharing the same .git directory.
git worktree add /home/ygwang/trading_agent_staging staging
```

### 2. Seed the staging env file

```bash
cp .env.staging.example /home/ygwang/trading_agent_staging/.env
# Fill in real secrets (LLM keys, JWT, Futu credentials). See that file
# for which values must differ from prod and which can be copied verbatim.
```

Mirror any per-host secrets your prod `.env.secrets` carries (TEI /
ASR tokens) into `/home/ygwang/trading_agent_staging/.env.secrets`.

### 3. Bootstrap databases + schema

```bash
cd /home/ygwang/trading_agent_staging
./start_web.sh init-staging
```

That command idempotently:
1. Runs `CREATE DATABASE trading_agent_staging` inside the shared Postgres container.
2. Applies the full Alembic migration set against the staging DB.
3. Prints a reminder that Milvus / Mongo staging collections are created lazily.

### 4. Build the staging frontend bundle

```bash
cd /home/ygwang/trading_agent_staging/frontend
npm install          # first time only
npm run build:staging  # outputs to frontend/dist-staging/
```

### 5. Start staging

```bash
cd /home/ygwang/trading_agent_staging
./start_web.sh start
# backend on :20301, no crawler / engine / scanner (those are prod-only)
```

Open `http://39.105.42.197:20301` and smoke-test.

---

## Daily iteration loop

```
┌─────────────────────────────────────────────────────────────────────┐
│ developer (main workstation)                                         │
│   git checkout staging                                               │
│   … edit code, commit …                                              │
│   git push origin staging                                            │
├─────────────────────────────────────────────────────────────────────┤
│ staging worktree (auto or manual)                                    │
│   cd /home/ygwang/trading_agent_staging                              │
│   git pull                                                           │
│   ./start_web.sh deploy     # build + migrate + restart web          │
│   # verify on http://39.105.42.197:20301                             │
├─────────────────────────────────────────────────────────────────────┤
│ promote (when staging is green)                                      │
│   ./scripts/promote.sh                                                │
│     ├── fast-forwards main to staging                                │
│     ├── tags the commit (v2026.04.25-HHMM)                           │
│     └── pushes main + tag to origin                                  │
├─────────────────────────────────────────────────────────────────────┤
│ prod worktree (manual)                                               │
│   cd /home/ygwang/trading_agent                                      │
│   ./start_web.sh deploy     # build + migrate + restart web          │
└─────────────────────────────────────────────────────────────────────┘
```

Don't run migrations directly in prod without the staging soak — see
"Migration discipline" below.

---

## Migration discipline

**All Alembic migrations MUST be forward-compatible.** The window between
running the migration on prod and restarting the prod backend is measured
in seconds, but during that window old code is reading the new schema.

Rules:
1. **Never remove columns in one release.** First stop writing to them
   (release N), then drop them (release N+1).
2. **Never rename columns.** Add the new column, backfill, switch reads,
   drop the old in the next release.
3. **Never tighten nullability on a column already written to by prod.**
   Add the column nullable, backfill, then tighten.
4. **Additive-only migrations are ideal.** They never break rollback.
5. Run the migration on staging first (`./start_web.sh migrate` in the
   staging worktree). Leave it overnight. Only then promote.

Promotion never runs migrations automatically — `./start_web.sh deploy`
runs them synchronously before restarting the backend, so you see failures
before any downtime.

---

## What *not* to run in staging

The staging worktree's `start_web.sh` hard-refuses to start these:

- `run_proactive.py`   (portfolio alert scanner)
- `run_chat_memory_processor.py` (LLM feedback → long-term memory)
- `crawler_monitor.py` + all 24 scrapers
- The engine subprocess (auto-started by the backend — also prod-only)

Staging still READs crawler output (shared remote Mongo), the Wind MySQL
1-致预期 numbers, and the ClickHouse kline stream. The guardrail is
`_prod_only_guard` in `start_web.sh`.

If you need to experiment with one of these services in isolation, do it
in a throwaway branch on the prod worktree during a maintenance window,
not by flipping the guard off in staging.

---

## Rollback

Every promotion creates an annotated tag (`vYYYY.MM.DD-HHMM`). To roll
prod back to the previous release:

```bash
cd /home/ygwang/trading_agent
git log --oneline --decorate -n 10        # find prior tag
git reset --hard v2026.04.24-1530          # example
./start_web.sh deploy
```

If the bad release contained a schema migration, also run a *downgrade*
Alembic migration before `deploy`:

```bash
PYTHONPATH=. alembic -c backend/alembic.ini downgrade -1
```

(That only works if the migration author wrote a proper `downgrade()`
body — cross-check before relying on it. The safer rollback for a
schema-touching release is a forward fix-release.)

---

## Port / URL cheat-sheet

| What                          | Prod                              | Staging                              |
|-------------------------------|-----------------------------------|--------------------------------------|
| Frontend + API                | `:8000` (or behind Docker nginx)  | `:20301`                             |
| API docs                      | `:8000/docs`                      | `:20301/docs`                        |
| WebSocket feed                | `:8000/ws/feed`                   | `:20301/ws/feed`                     |
| Crawler monitor UI            | `:8080`                           | (n/a)                                |
| Dev proxy (`npm run dev`)     | defaults to `:8000`               | `VITE_DEV_PROXY_TARGET=http://localhost:20301 npm run dev` |

---

## Why these isolation choices

- **Same Postgres container, different DB.** Running a second Postgres
  doubles the RAM footprint (buffers, WAL, background workers) for little
  real benefit — `CREATE DATABASE` is already a strong isolation boundary
  at the storage layer. If prod's container dies, both environments are
  down, which is a monitoring/alert signal rather than a reliability gap.

- **Same Redis, logical DB.** Redis `SELECT 1` is transparent. Two Redis
  instances would complicate `REDIS_URL` env-var plumbing for every
  background daemon (engine, scanner, memory processor).

- **Mongo collection prefix, not a separate DB.** The remote cluster's
  `u_spider` role cannot issue `createDatabase`, only `createCollection`
  inside the DBs it was granted. So staging's personal-KB + research-log
  collections live in the same DB as prod with a `stg_` prefix. Shared
  crawler data is read-only from staging.

- **Milvus collection suffix.** Milvus `CREATE COLLECTION` is the natural
  isolation boundary; running a second Milvus instance would be massive
  overkill for testing.

- **Crawlers prod-only.** AceCamp and a couple of other platforms cap the
  crawler account at a few hundred items/day; two concurrent crawlers
  would starve both. Research data is a shared corpus by nature — having
  two copies would also burn disk on ~700 GB of historical PDFs.

See `CLAUDE.md` "Database Architecture" for the full picture of which
store holds what.
