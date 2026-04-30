#!/bin/bash
# ==============================================================
# Trading Agent — Unified Management Script
# ==============================================================
# Groups:
#   infra  = docker (postgres, redis, crawl_data mongo)
#   asr    = supervised SSH tunnel to jumpbox Qwen3-ASR service (127.0.0.1:8760)
#   web    = uvicorn backend (auto-starts engine subprocess) + proactive scanner (run_proactive.py)
#                          + chat memory processor (run_chat_memory_processor.py)
#   crawl  = crawler_monitor.py --web dashboard + 24 scraper.py watchers (spawned by monitor)
#
# Multi-environment (prod vs staging) is driven by APP_ENV in .env:
#   APP_ENV=production  → full pipeline (default)
#   APP_ENV=staging     → web only; crawlers/engine/scanner/memory refuse to
#                         start, logs/PIDs live under this worktree's logs/,
#                         backend binds to APP_PORT (usually 20301). Prod and
#                         staging share the same Postgres/Redis containers
#                         (scoped by DB name / DB index) so `./start_web.sh
#                         infra start` from either worktree is a no-op if
#                         the container is already up.
#
# Top-level (all groups, unless noted):
#   ./start_web.sh start           Start everything
#   ./start_web.sh stop            Stop everything
#   ./start_web.sh restart         Restart WEB GROUP ONLY (backend+engine+scanner) — matches old muscle memory
#   ./start_web.sh restart-all     Restart everything
#   ./start_web.sh status          Full status snapshot
#
# Per-group:
#   ./start_web.sh infra {start|stop|status}
#   ./start_web.sh asr   {start|stop|restart|status}
#   ./start_web.sh web   {start|stop|restart|status}
#   ./start_web.sh crawl {start|stop|restart|status}
#
# Logs:
#   ./start_web.sh logs            Backend log (engine output is embedded)
#   ./start_web.sh engine-logs     Engine log (fallback: backend log)
#   ./start_web.sh scanner-logs    Proactive portfolio scanner log
#   ./start_web.sh memory-logs     Chat memory processor log (AI self-evolution)
#   ./start_web.sh crawl-logs      Crawler monitor wrapper log
#
# Other:
#   ./start_web.sh build           Rebuild frontend (prod or staging bundle, picked from APP_ENV)
#   ./start_web.sh deploy          Build + migrate + restart web
#   ./start_web.sh migrate         Run DB migrations
#   ./start_web.sh init-staging    One-shot: CREATE DATABASE trading_agent_staging + migrate
#   ./start_web.sh sync-users-from-prod
#                                  Staging only. Manual fallback — the staging
#                                  backend auto-runs this on boot and every 15
#                                  min (see staging_user_sync.py), so under
#                                  normal operation you never need to call this.
#                                  Useful for forcing an immediate refresh
#                                  without restarting the backend.
# ==============================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# ---- Environment (prod vs staging) ----
# Source .env to read APP_ENV and APP_PORT without requiring a Python import.
# We parse conservatively: KEY=VALUE lines only, `#` comments ignored. The
# live Python app still does its full pydantic-settings load at import time
# — this cheap parse is only to decide which ports / guards apply here.
_load_env_var() {
    local key="$1"
    local default="$2"
    if [ -f "$PROJECT_DIR/.env" ]; then
        local val
        val=$(grep -E "^[[:space:]]*${key}=" "$PROJECT_DIR/.env" 2>/dev/null \
              | tail -n 1 | sed -E "s/^[[:space:]]*${key}=//" | sed -E 's/^"(.*)"$/\1/; s/^'\''(.*)'\''$/\1/')
        if [ -n "$val" ]; then echo "$val"; return; fi
    fi
    echo "$default"
}

APP_ENV="$(_load_env_var APP_ENV production)"
APP_PORT="$(_load_env_var APP_PORT 8000)"

# Normalize — anything not "staging" is treated as production so typos
# don't silently mutate into a third env.
case "$APP_ENV" in
    staging|STAGING|Staging) APP_ENV="staging" ;;
    *) APP_ENV="production" ;;
esac

# ---- Paths ----
BACKEND_PID_FILE="$PROJECT_DIR/logs/backend.pid"
BACKEND_LOG="$PROJECT_DIR/logs/backend.log"
ENGINE_LOG="$PROJECT_DIR/logs/engine.log"

SCANNER_PID_FILE="$PROJECT_DIR/logs/proactive.pid"
SCANNER_LOG="$PROJECT_DIR/logs/proactive_daemon.log"
SCANNER_SCRIPT="run_proactive.py"

# Chat memory processor — distills user feedback on AI chat responses into
# long-term per-user memories that are injected into future system prompts.
# See backend/app/services/chat_memory_extractor.py + chat_memory_service.py
# and run_chat_memory_processor.py for design details.
MEMORY_PID_FILE="$PROJECT_DIR/logs/memory_processor.pid"
MEMORY_LOG="$PROJECT_DIR/logs/memory_processor.log"
MEMORY_SCRIPT="run_chat_memory_processor.py"

# StockHub LLM card-summary worker — qwen-plus generates {tldr, bullets,
# sentiment(看多/看空/中性)} for portfolio-holding docs across every collection
# StockHub renders (see crawl/local_ai_summary/targets.py — kept in sync with
# backend/app/api/stock_hub.py:_SOURCE_INDEX). Two roles in one process:
#
#   1. Backfill: 90-day window, processed time-desc so the freshest docs
#      always get the next LLM budget — i.e. the same loop that backfills
#      history is what catches new doc inserts (typically <5 min after a
#      scraper writes them, given a 300s interval + 200/cycle budget).
#   2. Dynamic holding alignment: load_holdings() re-reads
#      config/portfolio_sources.yaml each cycle, so swapping a holding
#      flips coverage on the next pass.
#
# Runs on BOTH prod and staging (no quote/trade side effects); the dedup
# gate (local_ai_summary.v) prevents duplicate spend across worktrees.
SUMMARY_PID_FILE="$PROJECT_DIR/logs/local_ai_summary.pid"
SUMMARY_LOG="$PROJECT_DIR/logs/local_ai_summary.log"
SUMMARY_INTERVAL="${LOCAL_AI_SUMMARY_INTERVAL:-60}"
SUMMARY_SINCE_DAYS="${LOCAL_AI_SUMMARY_SINCE_DAYS:-90}"
SUMMARY_PER_CYCLE="${LOCAL_AI_SUMMARY_MAX_PER_CYCLE:-1000}"

MONITOR_PID_FILE="$PROJECT_DIR/logs/crawler_monitor.pid"
MONITOR_LOG="$PROJECT_DIR/logs/crawler_monitor.log"
MONITOR_PORT="$(_load_env_var MONITOR_PORT 8080)"
MONITOR_DIR="$PROJECT_DIR/crawl"

# Crawler MongoDB container (not in docker-compose.dev.yml; started independently)
CRAWL_MONGO_CONTAINER="crawl_data"

# ASR SSH tunnel: 127.0.0.1:8760 on this host -> 127.0.0.1:8760 on jumpbox.
# Supervised by cron (every minute) — tunnel is flock-guarded so duplicate
# invocations exit immediately.
ASR_TUNNEL_SCRIPT="$PROJECT_DIR/ops/asr_tunnel/asr_tunnel.sh"
ASR_TUNNEL_LOG="$PROJECT_DIR/logs/asr_tunnel.log"
ASR_LOCAL_PORT=8760

# ---- Colors ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC}  $1"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()   { echo -e "${RED}[ERR]${NC}   $1"; }

# Guard: refuse to run prod-only processes on the staging deployment.
# Returns 0 when we're ALLOWED to run the named process, 1 when we should
# quietly skip it. Print a single [STAGING] notice per call.
_prod_only_guard() {
    local what="$1"
    if [ "$APP_ENV" = "staging" ]; then
        warn "[STAGING] Skipping ${what} — staging shares prod's ${what} output. Run it in the prod worktree instead."
        return 1
    fi
    return 0
}

# Cross-worktree crawler lock: refuse to start scrapers / crawler_monitor if
# the *other* worktree still has any live crawler process. This is the real
# invariant — "one writer per credential" — and replaces the old APP_ENV=prod
# gate for the crawl group. Whichever worktree cleans up first wins the lock.
_check_other_worktree_clear() {
    local other=""
    case "$PROJECT_DIR" in
        /home/ygwang/trading_agent_staging) other="/home/ygwang/trading_agent" ;;
        /home/ygwang/trading_agent)         other="/home/ygwang/trading_agent_staging" ;;
        *) return 0 ;;  # unknown layout — don't block
    esac
    local conflict=""
    for pid in $(pgrep -f 'scraper\.py|crawler_monitor\.py' 2>/dev/null); do
        local cwd
        cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
        case "$cwd" in
            "$other"/crawl|"$other"/crawl/*) conflict="$conflict $pid" ;;
        esac
    done
    if [ -n "$conflict" ]; then
        err "Refusing to start crawlers: other worktree ($other) still has crawler processes:"
        err "  PIDs:$conflict"
        err "  Stop them first (manually, or: cd $other && ./start_web.sh crawl stop)"
        return 1
    fi
    return 0
}

# ==============================================================
# INFRA GROUP (docker: postgres, redis, crawl_data mongo)
# ==============================================================

start_infra() {
    # Docker containers are shared across prod+staging. On staging this is a
    # no-op when the container is already up (idempotent), so we still run
    # it — never block infra start on staging because that would leave
    # staging unable to bring up a cold machine from scratch.
    info "Starting PostgreSQL + Redis (docker-compose.dev.yml)..."
    sg docker -c "docker compose -f docker-compose.dev.yml up -d" >/dev/null 2>&1
    for i in {1..30}; do
        if sg docker -c "docker exec ta-postgres-dev pg_isready -U trading_agent" &>/dev/null; then
            ok "PostgreSQL: ready"; break
        fi
        sleep 1
        [ $i -eq 30 ] && err "PostgreSQL: failed to start"
    done
    for i in {1..10}; do
        if sg docker -c "docker exec ta-redis-dev redis-cli ping" 2>/dev/null | grep -q PONG; then
            ok "Redis: ready"; break
        fi
        sleep 1
        [ $i -eq 10 ] && err "Redis: failed to start"
    done

    # Crawler MongoDB (one-off container, not in compose). Idempotent: docker start
    # on a running container is a no-op; if the container doesn't exist at all, skip.
    if sg docker -c "docker inspect $CRAWL_MONGO_CONTAINER" >/dev/null 2>&1; then
        sg docker -c "docker start $CRAWL_MONGO_CONTAINER" >/dev/null 2>&1
        if sg docker -c "docker exec $CRAWL_MONGO_CONTAINER mongosh --quiet --eval 'db.runCommand({ping:1}).ok'" 2>/dev/null | grep -q '^1$'; then
            ok "MongoDB (crawl_data): ready"
        else
            warn "MongoDB (crawl_data): container up, ping not responding yet"
        fi
    else
        warn "MongoDB (crawl_data): container not found — skipping (first-time setup?)"
    fi
}

stop_infra() {
    # Postgres + Redis containers are SHARED with prod. A staging ./stop
    # must not tear them down or prod dies too.
    if [ "$APP_ENV" = "staging" ]; then
        info "[STAGING] Skipping infra stop — Postgres/Redis containers are shared with prod."
        return 0
    fi
    info "Stopping PostgreSQL + Redis..."
    sg docker -c "docker compose -f docker-compose.dev.yml down" >/dev/null 2>&1
    ok "PostgreSQL + Redis stopped"
    # Leave crawl_data mongo running — it has restart=unless-stopped and the
    # crawler watchers depend on it. Only the user's explicit "docker stop" should kill it.
    info "MongoDB (crawl_data): left running (restart=unless-stopped)"
}

status_infra() {
    echo "[INFRA]"
    if sg docker -c "docker exec ta-postgres-dev pg_isready -U trading_agent" &>/dev/null; then
        ok "PostgreSQL:         running"
    else
        err "PostgreSQL:         STOPPED"
    fi
    if sg docker -c "docker exec ta-redis-dev redis-cli ping" 2>/dev/null | grep -q PONG; then
        ok "Redis:              running"
    else
        err "Redis:              STOPPED"
    fi
    if sg docker -c "docker exec $CRAWL_MONGO_CONTAINER mongosh --quiet --eval 'db.runCommand({ping:1}).ok'" 2>/dev/null | grep -q '^1$'; then
        ok "MongoDB (crawl):    running"
    else
        err "MongoDB (crawl):    STOPPED or MISSING"
    fi
}

# ==============================================================
# ASR GROUP (supervised SSH tunnel to jumpbox Qwen3-ASR service)
# ==============================================================
#
# The tunnel script (ops/asr_tunnel/asr_tunnel.sh) keeps an SSH -L forward
# running and is kicked every minute by a cron entry (flock-guarded, so
# duplicate kicks exit immediately). After a reboot the cron entry revives
# the tunnel within ~60s on its own; `start_asr` just makes the bring-up
# immediate and ensures the cron line is present.

_asr_port_bound() {
    ss -tln 2>/dev/null | grep -q "127.0.0.1:${ASR_LOCAL_PORT} "
}

_asr_cron_line() { echo "* * * * * ${ASR_TUNNEL_SCRIPT} >/dev/null 2>&1"; }

_asr_cron_present() {
    crontab -l 2>/dev/null | grep -Fq "$ASR_TUNNEL_SCRIPT"
}

_asr_cron_add() {
    if _asr_cron_present; then return 0; fi
    local current new
    current=$(crontab -l 2>/dev/null || true)
    new=$({ [ -n "$current" ] && echo "$current"; _asr_cron_line; })
    echo "$new" | crontab -
}

_asr_cron_remove() {
    if ! _asr_cron_present; then return 0; fi
    crontab -l 2>/dev/null | grep -vF "$ASR_TUNNEL_SCRIPT" | crontab -
}

start_asr() {
    if [ ! -x "$ASR_TUNNEL_SCRIPT" ]; then
        warn "ASR tunnel script not found or not executable: $ASR_TUNNEL_SCRIPT"
        return 1
    fi

    if _asr_cron_present; then
        info "ASR cron entry already installed"
    else
        info "Installing ASR cron entry (every minute, flock-guarded)"
        _asr_cron_add
    fi

    if _asr_port_bound; then
        ok "ASR tunnel: already bound on 127.0.0.1:${ASR_LOCAL_PORT}"
        return 0
    fi

    info "Kicking ASR tunnel (detached supervisor)..."
    nohup setsid "$ASR_TUNNEL_SCRIPT" >/dev/null 2>&1 < /dev/null &
    disown 2>/dev/null || true

    for i in {1..20}; do
        if _asr_port_bound; then
            ok "ASR tunnel: 127.0.0.1:${ASR_LOCAL_PORT} bound"
            # Probe the jumpbox health endpoint to confirm the far side is up too.
            if curl --noproxy '*' -sS -m 5 "http://127.0.0.1:${ASR_LOCAL_PORT}/health" 2>/dev/null | grep -q '"ok":true'; then
                ok "ASR service: /health responding"
            else
                warn "ASR tunnel bound but /health not responding (jumpbox service may be starting)"
            fi
            return 0
        fi
        sleep 1
    done

    warn "ASR tunnel: port ${ASR_LOCAL_PORT} did not bind within 20s"
    warn "  Check: tail -n 50 $ASR_TUNNEL_LOG"
    return 1
}

stop_asr() {
    # ASR tunnel is a single shared resource — both envs use the same
    # jumpbox service on :8760. Staging must not remove the cron entry
    # or kill the supervisor, or prod's audio uploads will start queueing.
    if [ "$APP_ENV" = "staging" ]; then
        info "[STAGING] Skipping ASR stop — tunnel is shared with prod."
        return 0
    fi
    info "Removing ASR cron entry..."
    _asr_cron_remove
    info "Killing ASR tunnel processes..."
    # Supervisor script (keeps ssh running); kill it first so it doesn't relaunch ssh.
    pkill -f "$ASR_TUNNEL_SCRIPT" 2>/dev/null
    # Kill the forwarded ssh child — match on the -L forward spec to avoid
    # hitting unrelated ssh sessions.
    pkill -f "ssh .*-L 127.0.0.1:${ASR_LOCAL_PORT}:127.0.0.1:" 2>/dev/null
    sleep 1
    if _asr_port_bound; then
        warn "ASR tunnel: port ${ASR_LOCAL_PORT} still bound — a straggler ssh is holding it"
    else
        ok "ASR tunnel stopped"
    fi
}

restart_asr() {
    stop_asr
    sleep 2
    start_asr
}

status_asr() {
    echo "[ASR]"
    if _asr_port_bound; then
        ok "Tunnel port:        127.0.0.1:${ASR_LOCAL_PORT} bound"
    else
        err "Tunnel port:        NOT BOUND (tunnel down)"
    fi
    if pgrep -f "$ASR_TUNNEL_SCRIPT" >/dev/null 2>&1; then
        ok "Supervisor:         running"
    else
        warn "Supervisor:         not running (cron will re-fire within 60s if entry present)"
    fi
    if _asr_cron_present; then
        ok "Cron entry:         installed"
    else
        err "Cron entry:         MISSING — tunnel will not auto-recover on reboot"
    fi
    # First request through a freshly re-established SSH tunnel sometimes
    # takes >5s while the forward settles. Retry once before declaring it
    # down so status doesn't flash red right after a tunnel restart.
    local health=""
    for attempt in 1 2; do
        health=$(curl --noproxy '*' -sS -m 5 "http://127.0.0.1:${ASR_LOCAL_PORT}/health" 2>/dev/null)
        if echo "$health" | grep -q '"ok":true'; then break; fi
        sleep 1
    done
    if echo "$health" | grep -q '"ok":true'; then
        if echo "$health" | grep -q '"model_loaded":true'; then
            ok "Jumpbox service:    /health ok, model loaded"
        else
            warn "Jumpbox service:    /health ok, model NOT loaded yet"
        fi
    else
        err "Jumpbox service:    /health NOT responding"
    fi
}

# ==============================================================
# WEB GROUP (backend+engine + proactive scanner)
# ==============================================================

start_backend() {
    if [ -f "$BACKEND_PID_FILE" ]; then
        old_pid=$(cat "$BACKEND_PID_FILE")
        if kill -0 "$old_pid" 2>/dev/null; then
            info "Stopping old backend (PID: $old_pid)..."
            kill "$old_pid" 2>/dev/null
            sleep 3
            kill -9 "$old_pid" 2>/dev/null
        fi
    fi

    info "Starting backend [${APP_ENV}] on port ${APP_PORT} (engine auto-starts as managed subprocess)..."
    mkdir -p "$PROJECT_DIR/logs" "$PROJECT_DIR/data"
    export HTTP_PROXY="$(_load_env_var HTTP_PROXY http://192.168.31.97:30801)"
    export HTTPS_PROXY="$(_load_env_var HTTPS_PROXY http://192.168.31.97:30801)"
    export ALL_PROXY="$(_load_env_var ALL_PROXY http://192.168.31.97:30801)"
    export NO_PROXY="$(_load_env_var NO_PROXY localhost,127.0.0.1,.local,jumpbox,116.239.28.36,192.168.31.0/24)"
    PYTHONPATH="$PROJECT_DIR" nohup uvicorn backend.app.main:app \
        --host 0.0.0.0 --port "${APP_PORT}" --workers 1 --log-level info \
        >> "$BACKEND_LOG" 2>&1 &
    echo $! > "$BACKEND_PID_FILE"
    sleep 3

    for i in {1..5}; do
        if curl -s --noproxy '*' "http://localhost:${APP_PORT}/api/health" 2>/dev/null | grep -q '"ok"'; then
            ok "Backend: running (PID: $(cat $BACKEND_PID_FILE), port: ${APP_PORT})"
            return 0
        fi
        sleep 2
    done
    err "Backend: FAILED to start. Check $BACKEND_LOG"
    return 1
}

stop_backend() {
    if [ -f "$BACKEND_PID_FILE" ]; then
        pid=$(cat "$BACKEND_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            info "Stopping backend (PID: $pid, port: ${APP_PORT})..."
            kill "$pid"
            for i in {1..15}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            if kill -0 "$pid" 2>/dev/null; then
                warn "Force-killing backend..."
                kill -9 "$pid" 2>/dev/null
            fi
            rm -f "$BACKEND_PID_FILE"
            ok "Backend stopped"
        else
            info "Backend not running (stale PID file)"
            rm -f "$BACKEND_PID_FILE"
        fi
    else
        info "No backend PID file. Searching for uvicorn on :${APP_PORT}..."
        # Narrow pkill to OUR port so prod's ./stop never kills staging and
        # vice versa (both worktrees use the same uvicorn import path).
        pkill -f "uvicorn.*backend\.app\.main:app.*--port[= ]${APP_PORT}" 2>/dev/null
    fi
    # Orphaned engine subprocess safety net. In prod the backend intentionally
    # spawns run.py; in staging it SHOULDN'T (gated in main.py lifespan) but we
    # still sweep here in case the guard regresses — orphans accumulate fast
    # and exhaust Postgres max_connections (2026-04-24 incident: 13 staging
    # orphans holding ~100 conns). Pattern is scoped to this worktree's
    # run.py via /proc/$pid/cwd so stopping staging never touches prod and
    # vice versa.
    for pid in $(pgrep -f "python.*run\.py" 2>/dev/null); do
        cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
        [ "$cwd" = "$PROJECT_DIR" ] && kill "$pid" 2>/dev/null
    done
}

start_scanner() {
    _prod_only_guard "proactive scanner (run_proactive.py)" || return 0
    if [ -f "$SCANNER_PID_FILE" ]; then
        old_pid=$(cat "$SCANNER_PID_FILE")
        if kill -0 "$old_pid" 2>/dev/null; then
            info "Stopping old scanner (PID: $old_pid)..."
            kill "$old_pid" 2>/dev/null
            sleep 2
            kill -9 "$old_pid" 2>/dev/null
        fi
    fi

    info "Starting proactive portfolio scanner (run_proactive.py)..."
    mkdir -p "$PROJECT_DIR/logs"
    cd "$PROJECT_DIR"
    PYTHONPATH="$PROJECT_DIR" nohup python "$SCANNER_SCRIPT" >> "$SCANNER_LOG" 2>&1 &
    echo $! > "$SCANNER_PID_FILE"
    sleep 4
    if kill -0 "$(cat $SCANNER_PID_FILE)" 2>/dev/null; then
        ok "Scanner: running (PID: $(cat $SCANNER_PID_FILE))"
    else
        err "Scanner: FAILED to start. Check $SCANNER_LOG"
        rm -f "$SCANNER_PID_FILE"
        return 1
    fi
}

stop_scanner() {
    # Staging never runs the scanner — and its pgrep fallback would happily
    # kill prod's scanner if we let it run. Gate the whole stop path.
    if [ "$APP_ENV" = "staging" ]; then
        return 0
    fi
    # PID file first
    if [ -f "$SCANNER_PID_FILE" ]; then
        pid=$(cat "$SCANNER_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            info "Stopping scanner (PID: $pid)..."
            kill "$pid"
            for i in {1..8}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -9 "$pid" 2>/dev/null
            ok "Scanner stopped"
        fi
        rm -f "$SCANNER_PID_FILE"
    fi
    # Fallback: kill any lingering run_proactive.py owned by THIS worktree
    # (cwd must be $PROJECT_DIR). Naive pgrep would reach across worktrees.
    for pid in $(pgrep -f "run_proactive\.py" 2>/dev/null); do
        cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
        [ "$cwd" != "$PROJECT_DIR" ] && continue
        info "Killing stray run_proactive.py (PID: $pid)..."
        kill "$pid" 2>/dev/null
        sleep 1
        kill -9 "$pid" 2>/dev/null
    done
}

# ---- Chat memory processor ----

start_memory_processor() {
    _prod_only_guard "chat memory processor (run_chat_memory_processor.py)" || return 0
    if [ -f "$MEMORY_PID_FILE" ]; then
        old_pid=$(cat "$MEMORY_PID_FILE")
        if kill -0 "$old_pid" 2>/dev/null; then
            info "Stopping old memory processor (PID: $old_pid)..."
            kill "$old_pid" 2>/dev/null
            sleep 2
            kill -9 "$old_pid" 2>/dev/null
        fi
    fi

    info "Starting chat memory processor ($MEMORY_SCRIPT)..."
    mkdir -p "$PROJECT_DIR/logs"
    cd "$PROJECT_DIR"
    # Bypass the Clash HTTP_PROXY for LLM calls; the daemon uses Dashscope
    # (CN endpoint) and httpx already has trust_env=False internally, but we
    # clear the env just in case a future dep picks it up.
    PYTHONPATH="$PROJECT_DIR" nohup python "$MEMORY_SCRIPT" >> "$MEMORY_LOG" 2>&1 &
    echo $! > "$MEMORY_PID_FILE"
    # Give it time to import + exit-early on missing LLM key
    sleep 4
    if kill -0 "$(cat $MEMORY_PID_FILE)" 2>/dev/null; then
        ok "Memory processor: running (PID: $(cat $MEMORY_PID_FILE))"
    else
        # Did it exit? (e.g. no LLM key). Surface the tail so user knows.
        err "Memory processor: FAILED to start or exited early. Last lines of $MEMORY_LOG:"
        tail -n 5 "$MEMORY_LOG" 2>/dev/null | sed 's/^/    /'
        rm -f "$MEMORY_PID_FILE"
        return 1
    fi
}

stop_memory_processor() {
    # Staging never runs this daemon and must not kill prod's copy.
    if [ "$APP_ENV" = "staging" ]; then
        return 0
    fi
    if [ -f "$MEMORY_PID_FILE" ]; then
        pid=$(cat "$MEMORY_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            info "Stopping memory processor (PID: $pid)..."
            kill "$pid"
            for i in {1..8}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -9 "$pid" 2>/dev/null
            ok "Memory processor stopped"
        fi
        rm -f "$MEMORY_PID_FILE"
    fi
    # Fallback: kill only this worktree's stray memory processors.
    for pid in $(pgrep -f "run_chat_memory_processor\.py" 2>/dev/null); do
        cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
        [ "$cwd" != "$PROJECT_DIR" ] && continue
        info "Killing stray memory processor (PID: $pid)..."
        kill "$pid" 2>/dev/null
        sleep 1
        kill -9 "$pid" 2>/dev/null
    done
}

# ---- StockHub card-summary worker (local_ai_summary) ----
# Cross-worktree dedup is enforced at the document level (local_ai_summary.v),
# so prod + staging running concurrently is safe (each cycle naturally races to
# claim the next pending doc; whichever loses just sees v>=current and skips).

start_summary_worker() {
    if [ -f "$SUMMARY_PID_FILE" ]; then
        old_pid=$(cat "$SUMMARY_PID_FILE")
        if kill -0 "$old_pid" 2>/dev/null; then
            info "Stopping old summary worker (PID: $old_pid)..."
            kill "$old_pid" 2>/dev/null
            sleep 2
            kill -9 "$old_pid" 2>/dev/null
        fi
    fi

    info "Starting StockHub summary worker (--watch --since-days ${SUMMARY_SINCE_DAYS} --max ${SUMMARY_PER_CYCLE} --interval ${SUMMARY_INTERVAL})..."
    mkdir -p "$PROJECT_DIR/logs"
    cd "$PROJECT_DIR"
    PYTHONPATH="$PROJECT_DIR" nohup python -m crawl.local_ai_summary.runner \
        --watch \
        --since-days "$SUMMARY_SINCE_DAYS" \
        --max "$SUMMARY_PER_CYCLE" \
        --interval "$SUMMARY_INTERVAL" \
        >> "$SUMMARY_LOG" 2>&1 &
    echo $! > "$SUMMARY_PID_FILE"
    sleep 4
    if kill -0 "$(cat $SUMMARY_PID_FILE)" 2>/dev/null; then
        ok "Summary worker: running (PID: $(cat $SUMMARY_PID_FILE))"
    else
        err "Summary worker: FAILED to start or exited early. Last lines of $SUMMARY_LOG:"
        tail -n 5 "$SUMMARY_LOG" 2>/dev/null | sed 's/^/    /'
        rm -f "$SUMMARY_PID_FILE"
        return 1
    fi
}

stop_summary_worker() {
    if [ -f "$SUMMARY_PID_FILE" ]; then
        pid=$(cat "$SUMMARY_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            info "Stopping summary worker (PID: $pid)..."
            kill "$pid"
            for i in {1..8}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -9 "$pid" 2>/dev/null
            ok "Summary worker stopped"
        fi
        rm -f "$SUMMARY_PID_FILE"
    fi
    # Cwd-scoped stray cleanup so we never reach across to the other worktree
    for pid in $(pgrep -f "crawl\.local_ai_summary\.runner" 2>/dev/null); do
        cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
        [ "$cwd" != "$PROJECT_DIR" ] && continue
        info "Killing stray summary worker (PID: $pid)..."
        kill "$pid" 2>/dev/null
        sleep 1
        kill -9 "$pid" 2>/dev/null
    done
}

status_web() {
    echo "[WEB]"
    if [ -f "$BACKEND_PID_FILE" ] && kill -0 "$(cat $BACKEND_PID_FILE)" 2>/dev/null; then
        ok "Backend:            running (PID: $(cat $BACKEND_PID_FILE))"
    else
        err "Backend:            STOPPED"
    fi

    HEALTH_FILE="$PROJECT_DIR/data/engine_health.json"
    if [ -f "$HEALTH_FILE" ]; then
        e_status=$(python3 -c "import json; print(json.load(open('$HEALTH_FILE')).get('status','?'))" 2>/dev/null)
        e_pid=$(python3 -c "import json; print(json.load(open('$HEALTH_FILE')).get('pid','?'))" 2>/dev/null)
        e_monitors=$(python3 -c "import json; print(json.load(open('$HEALTH_FILE')).get('monitors',0))" 2>/dev/null)
        if [ "$e_status" = "running" ] && kill -0 "$e_pid" 2>/dev/null; then
            ok "Engine:             running (PID: $e_pid, monitors: $e_monitors)"
        elif [ "$e_status" = "starting" ]; then
            warn "Engine:             starting..."
        else
            warn "Engine:             STOPPED (status=$e_status)"
        fi
    else
        warn "Engine:             no health file"
    fi

    if [ -f "$SCANNER_PID_FILE" ] && kill -0 "$(cat $SCANNER_PID_FILE)" 2>/dev/null; then
        s_pid=$(cat $SCANNER_PID_FILE)
        s_etime=$(ps -o etime= -p $s_pid 2>/dev/null | tr -d ' ')
        ok "Scanner:            running (PID: $s_pid, uptime: $s_etime)"
    else
        # Detect unmanaged instance
        stray_pid=$(pgrep -f "run_proactive\.py" 2>/dev/null | head -1)
        if [ -n "$stray_pid" ]; then
            warn "Scanner:            running but UNMANAGED (PID: $stray_pid — restart to adopt)"
        else
            err "Scanner:            STOPPED"
        fi
    fi

    if [ -f "$MEMORY_PID_FILE" ] && kill -0 "$(cat $MEMORY_PID_FILE)" 2>/dev/null; then
        m_pid=$(cat $MEMORY_PID_FILE)
        m_etime=$(ps -o etime= -p $m_pid 2>/dev/null | tr -d ' ')
        ok "Memory processor:   running (PID: $m_pid, uptime: $m_etime)"
    else
        stray_pid=$(pgrep -f "run_chat_memory_processor\.py" 2>/dev/null | head -1)
        if [ -n "$stray_pid" ]; then
            warn "Memory processor:   running but UNMANAGED (PID: $stray_pid — restart to adopt)"
        else
            err "Memory processor:   STOPPED"
        fi
    fi

    if [ -f "$SUMMARY_PID_FILE" ] && kill -0 "$(cat $SUMMARY_PID_FILE)" 2>/dev/null; then
        sm_pid=$(cat $SUMMARY_PID_FILE)
        sm_etime=$(ps -o etime= -p $sm_pid 2>/dev/null | tr -d ' ')
        ok "Summary worker:     running (PID: $sm_pid, uptime: $sm_etime, ${SUMMARY_SINCE_DAYS}d window)"
    else
        stray_pid=$(pgrep -f "crawl\.local_ai_summary\.runner" 2>/dev/null | head -1)
        if [ -n "$stray_pid" ]; then
            warn "Summary worker:     running but UNMANAGED (PID: $stray_pid — restart to adopt)"
        else
            err "Summary worker:     STOPPED"
        fi
    fi

    if curl -s --noproxy '*' "http://localhost:${APP_PORT}/api/health" 2>/dev/null | grep -q '"ok"'; then
        ok "API health:         OK  (port ${APP_PORT})"
    else
        err "API health:         UNREACHABLE  (port ${APP_PORT})"
    fi
}

start_web_group() {
    start_backend
    start_scanner
    start_memory_processor
    start_summary_worker
}

stop_web_group() {
    stop_summary_worker
    stop_memory_processor
    stop_scanner
    stop_backend
}

restart_web_group() {
    stop_web_group
    sleep 2
    start_web_group
}

# ==============================================================
# CRAWL GROUP (crawler_monitor + 24 scraper watchers)
# ==============================================================

start_crawler_monitor() {
    _check_other_worktree_clear || return 1
    if [ -f "$MONITOR_PID_FILE" ]; then
        old_pid=$(cat "$MONITOR_PID_FILE")
        if kill -0 "$old_pid" 2>/dev/null; then
            info "Stopping old crawler_monitor (PID: $old_pid)..."
            kill "$old_pid" 2>/dev/null
            sleep 2
            kill -9 "$old_pid" 2>/dev/null
        fi
    fi
    # Also kill any unmanaged monitor (started manually before this script)
    for pid in $(pgrep -f "crawler_monitor\.py.*--web" 2>/dev/null); do
        [ -f "$MONITOR_PID_FILE" ] && [ "$pid" = "$(cat $MONITOR_PID_FILE 2>/dev/null)" ] && continue
        info "Killing stray crawler_monitor (PID: $pid)..."
        kill "$pid" 2>/dev/null
        sleep 1
        kill -9 "$pid" 2>/dev/null
    done

    info "Starting crawler_monitor.py --web --port $MONITOR_PORT..."
    mkdir -p "$PROJECT_DIR/logs"
    cd "$MONITOR_DIR"
    # trust_env behavior: crawler_monitor uses requests to hit localhost — Clash
    # is in NO_PROXY, so HTTP_PROXY set above is harmless. Leave it alone.
    nohup python3 -u crawler_monitor.py --web --port $MONITOR_PORT \
        >> "$MONITOR_LOG" 2>&1 &
    echo $! > "$MONITOR_PID_FILE"
    cd "$PROJECT_DIR"

    # Wait for the dashboard to come up (can take ~5s for FastAPI import + uvicorn boot)
    for i in {1..20}; do
        if curl -s --noproxy '*' "http://127.0.0.1:$MONITOR_PORT/api/status" >/dev/null 2>&1; then
            ok "Crawler monitor: running (PID: $(cat $MONITOR_PID_FILE), port: $MONITOR_PORT)"
            return 0
        fi
        sleep 1
    done
    err "Crawler monitor: FAILED to respond on port $MONITOR_PORT. Check $MONITOR_LOG"
    return 1
}

_is_our_scraper() {
    # A scraper belongs to us if either its cwd is under $PROJECT_DIR/crawl,
    # OR the scraper.py path on its cmdline is under $PROJECT_DIR/crawl.
    # (The second case covers scrapers launched with an absolute path from the
    # project root — like old manually-started gangtise watchers.)
    local pid="$1"
    local cwd cmdline
    cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
    case "$cwd" in
        "$PROJECT_DIR"/crawl|"$PROJECT_DIR"/crawl/*) return 0 ;;
    esac
    cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)
    case "$cmdline" in
        *"$PROJECT_DIR/crawl/"*scraper.py*) return 0 ;;
        *"$PROJECT_DIR/crawl/"*scraper_home.py*) return 0 ;;
    esac
    return 1
}

stop_scrapers() {
    # Scrapers are spawned with start_new_session=True so they survive
    # crawler_monitor death — must be killed explicitly. `_is_our_scraper`
    # scopes the kill to $PROJECT_DIR/crawl, so each worktree only stops its
    # own scrapers.
    local killed=0
    for pid in $(pgrep -f "scraper\.py" 2>/dev/null); do
        if _is_our_scraper "$pid"; then
            kill "$pid" 2>/dev/null && killed=$((killed + 1))
        fi
    done
    [ $killed -gt 0 ] && info "Sent SIGTERM to $killed scraper process(es)"
    sleep 3
    for pid in $(pgrep -f "scraper\.py" 2>/dev/null); do
        if _is_our_scraper "$pid"; then
            kill -9 "$pid" 2>/dev/null
        fi
    done
}

stop_crawler_monitor() {
    if [ -f "$MONITOR_PID_FILE" ]; then
        pid=$(cat "$MONITOR_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            info "Stopping crawler_monitor (PID: $pid)..."
            kill "$pid"
            for i in {1..8}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -9 "$pid" 2>/dev/null
            ok "Crawler monitor stopped"
        fi
        rm -f "$MONITOR_PID_FILE"
    fi
    # Fallback: only this worktree's stray monitor (match by cwd).
    for pid in $(pgrep -f "crawler_monitor\.py.*--web" 2>/dev/null); do
        cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
        # crawler_monitor runs from $PROJECT_DIR/crawl
        case "$cwd" in
            "$PROJECT_DIR"/crawl|"$PROJECT_DIR"/crawl/*) ;;
            *) continue ;;
        esac
        info "Killing stray crawler_monitor (PID: $pid)..."
        kill "$pid" 2>/dev/null
        sleep 1
        kill -9 "$pid" 2>/dev/null
    done
}

trigger_scrapers_start() {
    _check_other_worktree_clear || return 1
    # POST to crawler_monitor's /api/start-all — idempotent (internally kills
    # existing scrapers by cwd + re-spawns all configured ones).
    info "Triggering /api/start-all on crawler monitor..."
    local resp
    resp=$(curl -sS --noproxy '*' --max-time 60 -X POST "http://127.0.0.1:$MONITOR_PORT/api/start-all?mode=realtime" 2>&1)
    if echo "$resp" | grep -q '"ok": *true'; then
        local started total
        started=$(echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('started','?'))" 2>/dev/null)
        total=$(echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('total','?'))" 2>/dev/null)
        ok "Scrapers: started $started / $total"
    else
        err "Scraper start-all FAILED. Response: $(echo $resp | head -c 200)"
        return 1
    fi
}

status_crawl() {
    echo "[CRAWL]"
    if [ -f "$MONITOR_PID_FILE" ] && kill -0 "$(cat $MONITOR_PID_FILE)" 2>/dev/null; then
        m_pid=$(cat $MONITOR_PID_FILE)
        m_etime=$(ps -o etime= -p $m_pid 2>/dev/null | tr -d ' ')
        ok "Crawler monitor:    running (PID: $m_pid, port: $MONITOR_PORT, uptime: $m_etime)"
    else
        stray_pid=$(pgrep -f "crawler_monitor\.py.*--web" 2>/dev/null | head -1)
        if [ -n "$stray_pid" ]; then
            warn "Crawler monitor:    running but UNMANAGED (PID: $stray_pid)"
        else
            err "Crawler monitor:    STOPPED"
        fi
    fi

    # Count our scrapers by platform (inferred from cwd, or from the
    # scraper.py path on cmdline if cwd is the project root).
    local total=0
    declare -A per_platform
    for pid in $(pgrep -f "scraper\.py" 2>/dev/null); do
        _is_our_scraper "$pid" || continue
        local cwd cmdline platform
        cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
        case "$cwd" in
            "$PROJECT_DIR"/crawl/*) platform=$(basename "$cwd") ;;
            *)
                cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)
                platform=$(echo "$cmdline" | grep -oE "$PROJECT_DIR/crawl/[^/]+" | head -1 | sed "s|$PROJECT_DIR/crawl/||")
                ;;
        esac
        [ -z "$platform" ] && platform="<unknown>"
        per_platform[$platform]=$((${per_platform[$platform]:-0} + 1))
        total=$((total + 1))
    done
    if [ $total -gt 0 ]; then
        local breakdown=""
        for k in "${!per_platform[@]}"; do
            breakdown="$breakdown${k}:${per_platform[$k]} "
        done
        ok "Scrapers:           $total running ($breakdown)"
    else
        err "Scrapers:           none running"
    fi
}

start_crawl_group() {
    start_crawler_monitor || return 1
    trigger_scrapers_start
}

stop_crawl_group() {
    stop_scrapers
    stop_crawler_monitor
}

restart_crawl_group() {
    stop_crawl_group
    sleep 2
    start_crawl_group
}

# ==============================================================
# COMPOSITE (all three groups)
# ==============================================================

start_all() {
    start_infra
    run_migrations
    # ASR tunnel before web: the backend's ASR recovery sweep tolerates a
    # late tunnel, but starting it first means fresh .m4a uploads transcribe
    # immediately rather than waiting for the next 60s sweep tick.
    start_asr
    start_web_group
    start_crawl_group
    show_banner
}

stop_all() {
    stop_crawl_group
    stop_web_group
    stop_asr
    stop_infra
}

restart_all() {
    stop_all
    sleep 2
    start_all
}

show_banner() {
    echo ""
    echo "============================================"
    echo "  Trading Agent Platform is LIVE  [${APP_ENV^^}]"
    echo "  LAN:        http://192.168.31.97:${APP_PORT}"
    echo "  Public:     http://39.105.42.197:${APP_PORT}"
    echo "  API docs:   http://192.168.31.97:${APP_PORT}/docs"
    if [ "$APP_ENV" = "production" ]; then
        echo "  Crawl UI:   http://192.168.31.97:$MONITOR_PORT"
    else
        echo "  Crawl UI:   (disabled — staging reads prod's crawler output)"
    fi
    echo "============================================"
}

show_status() {
    echo ""
    echo "=== Trading Agent Platform Status ==="
    echo ""
    status_infra
    echo ""
    status_asr
    echo ""
    status_web
    echo ""
    status_crawl
    echo ""
    if [ -f "$PROJECT_DIR/data/engine_health.json" ]; then
        ts=$(python3 -c "import json; print(json.load(open('data/engine_health.json')).get('timestamp','?'))" 2>/dev/null)
        info "Engine heartbeat:   $ts"
    fi
    echo ""
}

# ==============================================================
# MISC (migrations, frontend, deploy)
# ==============================================================

run_migrations() {
    info "Running database migrations (target: $(_load_env_var POSTGRES_DB trading_agent)${APP_ENV:+, env=$APP_ENV})..."
    cd "$PROJECT_DIR"
    # Alembic reads settings.database_url which now auto-suffixes with
    # _staging when APP_ENV=staging, so migrations land on the right DB.
    PYTHONPATH="$PROJECT_DIR" python -m alembic -c backend/alembic.ini upgrade head 2>&1
    if [ $? -eq 0 ]; then
        ok "Migrations applied"
    else
        warn "Migration failed — check output above"
    fi
}

sync_users_from_prod() {
    # MANUAL FALLBACK. The backend already auto-runs this same sync on
    # startup and then every 15 min via the lifespan task registered in
    # `backend/app/services/staging_user_sync.py`, so under normal
    # operation nobody needs to call this by hand.
    #
    # Use this shell path only when:
    #   * you want to force an immediate refresh without restarting the
    #     backend (rare), OR
    #   * you need to diagnose the pipeline without the async indirection.
    #
    # Copies user/auth + workspace-skeleton tables from prod's Postgres
    # DB into staging's, so employees can log in with their normal creds
    # and see their folder tree on the MyKnowledgeBase page.
    # Runs inside the shared Postgres container via pg_dump | psql.
    #
    # Tables synced:
    #   users, user_preferences     — auth + profile (required for login)
    #   user_sources                — subscribed news sources
    #   kb_folders                  — personal/public KB folder tree
    #   watchlists, watchlist_items — portfolio (makes Dashboard non-empty)
    #
    # Everything else (chat history, predictions, feedback, memories,
    # token_usage, signal_evaluations, etc.) stays staging-local so
    # experiments don't spill across.
    if [ "$APP_ENV" != "staging" ]; then
        err "sync-users-from-prod must be run from a staging worktree (current APP_ENV=$APP_ENV)"
        return 1
    fi
    local pg_user pg_db
    pg_user="$(_load_env_var POSTGRES_USER trading_agent)"
    pg_db="$(_load_env_var POSTGRES_DB trading_agent)"
    local src_db="$pg_db"
    # Strip a staging suffix the operator may have pre-applied.
    case "$src_db" in *_staging) src_db="${src_db%_staging}" ;; esac
    local dst_db="${src_db}_staging"

    info "Syncing user + workspace tables:  $src_db  →  $dst_db"
    # --data-only because schemas already match (init-staging ran Alembic).
    # --on-conflict-do-nothing via the `--inserts` format + manual TRUNCATE:
    # safest is to TRUNCATE the dst tables first (preserving schema) then
    # INSERT from the dump. PG's `pg_dump --clean` would DROP the tables
    # (too aggressive — it'd nuke FK refs).
    local tables=(
        users user_preferences user_sources
        kb_folders
        watchlists watchlist_items
    )
    local tables_csv=""
    local t_flags=""
    for t in "${tables[@]}"; do
        tables_csv+="${t},"
        t_flags+=" -t $t"
    done
    tables_csv="${tables_csv%,}"

    # Truncate destination (respecting FK order: children first).
    info "Truncating destination tables in $dst_db..."
    if ! sg docker -c "docker exec ta-postgres-dev psql -U ${pg_user} -d ${dst_db} -c 'TRUNCATE watchlist_items, watchlists, kb_folders, user_sources, user_preferences, users RESTART IDENTITY CASCADE'" >/dev/null 2>&1; then
        err "TRUNCATE failed — does database '$dst_db' exist? Run ./start_web.sh init-staging first."
        return 1
    fi

    # Dump from src, pipe into dst. kb_folders has a self-referential
    # parent_id FK — `pg_dump --data-only` inserts in an unpredictable
    # order that can violate it. We wrap the restore in one transaction
    # with `session_replication_role = replica` to suppress triggers +
    # FK checks during the bulk load, then reset at the end.
    info "Dumping + restoring ${#tables[@]} tables ($tables_csv)..."
    (
        echo "BEGIN;"
        echo "SET session_replication_role = replica;"
        sg docker -c "docker exec ta-postgres-dev pg_dump -U ${pg_user} -d ${src_db} --data-only --column-inserts $t_flags"
        echo "SET session_replication_role = origin;"
        echo "COMMIT;"
    ) | sg docker -c "docker exec -i ta-postgres-dev psql -U ${pg_user} -d ${dst_db} -q --set ON_ERROR_STOP=on" \
         >/tmp/sync_users.log 2>&1
    if [ $? -eq 0 ]; then
        # Count rows as sanity check
        local rows
        rows=$(sg docker -c "docker exec ta-postgres-dev psql -U ${pg_user} -d ${dst_db} -tAc \"SELECT COUNT(*) FROM users\"" 2>/dev/null | tr -d '[:space:]')
        ok "Synced. staging users table now has ${rows} rows."
        info "Employees can now log in to staging with their prod credentials."
    else
        err "Sync failed. Log tail:"
        tail -n 20 /tmp/sync_users.log | sed 's/^/    /'
        return 1
    fi
}

init_staging() {
    # One-shot bootstrap for a fresh staging deployment. Must be run from
    # the staging worktree (checked here by APP_ENV).
    if [ "$APP_ENV" != "staging" ]; then
        err "init-staging must be run from a worktree whose .env has APP_ENV=staging (current: $APP_ENV)"
        return 1
    fi
    info "Bootstrapping staging environment..."

    # 1. Create the staging Postgres database inside the shared instance.
    #    Uses the prod container's superuser (POSTGRES_USER from env).
    local pg_user pg_db
    pg_user="$(_load_env_var POSTGRES_USER trading_agent)"
    pg_db="$(_load_env_var POSTGRES_DB trading_agent)"
    # Idempotent: skip creation if the suffixed DB already exists.
    local target_db="${pg_db}_staging"
    case "$pg_db" in *_staging) target_db="$pg_db" ;; esac
    info "Ensuring Postgres database '${target_db}' exists..."
    local exists
    exists=$(sg docker -c "docker exec ta-postgres-dev psql -U ${pg_user} -tAc \"SELECT 1 FROM pg_database WHERE datname='${target_db}'\"" 2>/dev/null | tr -d '[:space:]')
    if [ "$exists" = "1" ]; then
        ok "Postgres DB '${target_db}' already exists — skipping create"
    else
        if sg docker -c "docker exec ta-postgres-dev psql -U ${pg_user} -c 'CREATE DATABASE \"${target_db}\" OWNER ${pg_user}'" >/dev/null 2>&1; then
            ok "Postgres DB '${target_db}' created"
        else
            err "Failed to CREATE DATABASE ${target_db} — does ${pg_user} have CREATEDB?"
            return 1
        fi
    fi

    # 2. Apply full schema via Alembic (settings will target the staging DB
    #    because APP_ENV=staging).
    run_migrations

    # 3. Milvus staging collections + Mongo stg_ collections are created
    #    lazily on first use by the backend (ensure_indexes, _ensure_collection).
    #    Nothing to do here — flag this to the user for clarity.
    ok "Milvus/Mongo staging collections will be auto-created on first access."
    ok "Staging bootstrap complete. Next: ./start_web.sh start"
}

build_frontend() {
    info "Building frontend (env: ${APP_ENV}, target: frontend/$( [ "$APP_ENV" = "staging" ] && echo dist-staging || echo dist )/)..."
    cd "$PROJECT_DIR/frontend"
    if [ "$APP_ENV" = "staging" ]; then
        # Staging bundle lives in frontend/dist-staging so the prod bundle
        # in frontend/dist is never clobbered. The backend's SPA catch-all
        # (main.py lifespan) serves from /dist by default; nginx steers
        # staging traffic to dist-staging explicitly. See nginx.staging.conf.
        npm run build:staging 2>&1
    else
        npx vite build 2>&1
    fi
    local rc=$?
    cd "$PROJECT_DIR"
    if [ $rc -eq 0 ]; then
        ok "Frontend built"
    else
        err "Frontend build failed"
        return 1
    fi
}

full_deploy() {
    # Transactional deploy with rollback: build → migrate → restart → smoke.
    # If any step fails after migration, the script auto-rolls back code + schema
    # and restarts the prior version. See scripts/deploy_with_rollback.sh.
    #
    # The legacy linear path is still available via LEGACY_DEPLOY=1 for edge
    # cases (e.g. cold machine with no prior HEAD to roll back to).
    if [ "${LEGACY_DEPLOY:-0}" = "1" ]; then
        warn "LEGACY_DEPLOY=1 — using naive linear deploy (no rollback)"
        echo "============================================"
        echo "  Full Deployment (legacy)"
        echo "============================================"
        build_frontend || exit 1
        echo ""
        run_migrations
        echo ""
        stop_web_group
        sleep 2
        start_web_group
        show_banner
        return
    fi

    if [ -x "$PROJECT_DIR/scripts/deploy_with_rollback.sh" ]; then
        "$PROJECT_DIR/scripts/deploy_with_rollback.sh"
        local rc=$?
        if [ $rc -eq 0 ]; then show_banner; fi
        return $rc
    fi

    # Fallback if the rollback script has been removed/moved — linear deploy.
    warn "scripts/deploy_with_rollback.sh not found — falling back to linear deploy"
    echo "============================================"
    echo "  Full Deployment (fallback)"
    echo "============================================"
    build_frontend || exit 1
    echo ""
    run_migrations
    echo ""
    stop_web_group
    sleep 2
    start_web_group
    show_banner
}

# ==============================================================
# DISPATCH
# ==============================================================

usage() {
    grep -E "^#" "$0" | head -40
    echo ""
    echo "Run without args or with an unknown arg to see this usage."
    exit 1
}

case "${1:-start}" in
    start)        start_all ;;
    stop)         stop_all ;;
    restart)      restart_web_group ;;          # web group only — old muscle memory
    restart-all)  restart_all ;;
    status)       show_status ;;

    logs)         tail -f "$BACKEND_LOG" ;;
    engine-logs)
        if [ -f "$ENGINE_LOG" ]; then
            tail -f "$ENGINE_LOG"
        else
            info "Engine logs are embedded in backend log."
            tail -f "$BACKEND_LOG"
        fi
        ;;
    scanner-logs) tail -f "$SCANNER_LOG" ;;
    memory-logs)  tail -f "$MEMORY_LOG" ;;
    crawl-logs)   tail -f "$MONITOR_LOG" ;;

    build)                 build_frontend ;;
    deploy)                full_deploy ;;
    migrate)               run_migrations ;;
    init-staging)          init_staging ;;
    sync-users-from-prod)  sync_users_from_prod ;;

    infra)
        case "${2:-status}" in
            start)   start_infra ;;
            stop)    stop_infra ;;
            status)  status_infra ;;
            *) echo "Usage: $0 infra {start|stop|status}"; exit 1 ;;
        esac ;;
    asr)
        case "${2:-status}" in
            start)   start_asr ;;
            stop)    stop_asr ;;
            restart) restart_asr ;;
            status)  status_asr ;;
            *) echo "Usage: $0 asr {start|stop|restart|status}"; exit 1 ;;
        esac ;;
    web)
        case "${2:-status}" in
            start)   start_web_group ;;
            stop)    stop_web_group ;;
            restart) restart_web_group ;;
            status)  status_web ;;
            *) echo "Usage: $0 web {start|stop|restart|status}"; exit 1 ;;
        esac ;;
    crawl)
        case "${2:-status}" in
            start)   start_crawl_group ;;
            stop)    stop_crawl_group ;;
            restart) restart_crawl_group ;;
            status)  status_crawl ;;
            *) echo "Usage: $0 crawl {start|stop|restart|status}"; exit 1 ;;
        esac ;;

    *) usage ;;
esac
