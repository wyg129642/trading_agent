#!/usr/bin/env bash
# ============================================================
# scripts/deploy_with_rollback.sh — transactional deploy
# ============================================================
#
# Replaces the naive `build → migrate → restart` sequence in
# `start_web.sh full_deploy` with one that rolls back on any
# failure:
#
#   1. Build frontend. Fail → exit, nothing touched.
#   2. Record current git HEAD + current Alembic revision.
#   3. Run migrations. Fail → alembic downgrade to recorded rev,
#      git reset --hard to recorded HEAD (frontend bundle stays,
#      since it was already rebuilt and the code will roll back
#      below), and exit.
#   4. Stop backend, start backend.
#   5. Smoke-check the backend on APP_PORT.
#      Fail → alembic downgrade, git reset --hard, rebuild bundle,
#      restart backend, exit.
#
# Usage: ./scripts/deploy_with_rollback.sh
# Invoked by start_web.sh's `deploy` subcommand.
# ============================================================

set -u

# Directory resolution — this script sits at scripts/, repo root is ..
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# ---- Pretty output ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
info()  { echo -e "${BLUE}[DEPLOY]${NC}   $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}       $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}     $*" >&2; }
warn()  { echo -e "${YELLOW}[WARN]${NC}    $*"; }

# ---- Load env (for APP_PORT / APP_ENV) ----
_load_env_var() {
    local key="$1" default="$2" val
    if [ -f "$PROJECT_DIR/.env" ]; then
        val=$(grep -E "^[[:space:]]*${key}=" "$PROJECT_DIR/.env" 2>/dev/null \
              | tail -n 1 | sed -E "s/^[[:space:]]*${key}=//" | sed -E 's/^"(.*)"$/\1/; s/^'\''(.*)'\''$/\1/')
        if [ -n "$val" ]; then echo "$val"; return; fi
    fi
    echo "$default"
}
APP_ENV="$(_load_env_var APP_ENV production)"
APP_PORT="$(_load_env_var APP_PORT 8000)"
case "$APP_ENV" in staging|STAGING|Staging) APP_ENV="staging" ;; *) APP_ENV="production" ;; esac

info "Environment: $APP_ENV  port: $APP_PORT  dir: $PROJECT_DIR"

# ---- Record rollback state ----
PREV_HEAD=$(git -C "$PROJECT_DIR" rev-parse HEAD)
PREV_ALEMBIC=$(PYTHONPATH="$PROJECT_DIR" python -m alembic -c backend/alembic.ini current 2>/dev/null \
    | grep -oE "[a-f0-9]{8,}" | head -1)
info "Rollback anchor: git=$PREV_HEAD  alembic=${PREV_ALEMBIC:-<none>}"

# ---- Rollback helpers ----
_rollback_alembic() {
    if [ -z "$PREV_ALEMBIC" ]; then
        warn "No previous alembic rev captured — skipping DB rollback."
        return 0
    fi
    info "Rolling back Alembic to $PREV_ALEMBIC..."
    PYTHONPATH="$PROJECT_DIR" python -m alembic -c backend/alembic.ini downgrade "$PREV_ALEMBIC" \
        || warn "Alembic downgrade failed — DB may be in an intermediate state."
}

_rollback_git() {
    info "Rolling back git to $PREV_HEAD..."
    git -C "$PROJECT_DIR" reset --hard "$PREV_HEAD" \
        || warn "git reset failed — working tree may be inconsistent."
}

_rebuild_bundle() {
    info "Rebuilding frontend after rollback..."
    if [ "$APP_ENV" = "staging" ]; then
        (cd "$PROJECT_DIR/frontend" && npm run build:staging) >/dev/null 2>&1 \
            || warn "Frontend rebuild failed during rollback."
    else
        (cd "$PROJECT_DIR/frontend" && npx vite build) >/dev/null 2>&1 \
            || warn "Frontend rebuild failed during rollback."
    fi
}

_restart_backend() {
    "$PROJECT_DIR/start_web.sh" web restart >/dev/null 2>&1 \
        || warn "Backend restart failed during rollback — manual intervention needed."
}

# ============================================================
# Step 1: Build frontend
# ============================================================
info "Step 1/4: Build frontend"
if [ "$APP_ENV" = "staging" ]; then
    (cd frontend && npm run build:staging) || { fail "Frontend build failed — aborting deploy, nothing touched."; exit 1; }
else
    (cd frontend && npx vite build)        || { fail "Frontend build failed — aborting deploy, nothing touched."; exit 1; }
fi
ok "Frontend built"

# ============================================================
# Step 2: Run migrations (with rollback on failure)
# ============================================================
info "Step 2/4: Run Alembic migrations"
if ! PYTHONPATH="$PROJECT_DIR" python -m alembic -c backend/alembic.ini upgrade head; then
    fail "Alembic upgrade failed — rolling back."
    _rollback_alembic
    _rollback_git
    _rebuild_bundle
    _restart_backend
    exit 1
fi
ok "Migrations applied"

# ============================================================
# Step 3: Restart backend
# ============================================================
info "Step 3/4: Restart web group"
if ! "$PROJECT_DIR/start_web.sh" web restart; then
    fail "Backend restart failed — rolling back."
    _rollback_alembic
    _rollback_git
    _rebuild_bundle
    _restart_backend
    exit 1
fi
ok "Backend restarted"

# ============================================================
# Step 4: Smoke-check
# ============================================================
info "Step 4/4: Smoke test on :${APP_PORT}"
# Give uvicorn a second to actually listen.
sleep 2
if ! "$PROJECT_DIR/scripts/smoke.sh" "$APP_PORT"; then
    fail "Smoke check failed — rolling back."
    _rollback_alembic
    _rollback_git
    _rebuild_bundle
    _restart_backend
    # Re-run smoke to confirm the rollback took effect; non-fatal if it fails
    # here since we've already surfaced the original error.
    "$PROJECT_DIR/scripts/smoke.sh" "$APP_PORT" || warn "Post-rollback smoke also failed — manual intervention needed."
    exit 1
fi
ok "Smoke checks passed"

# ============================================================
# Done
# ============================================================
echo ""
echo "============================================"
echo "  Deploy succeeded [$APP_ENV]"
echo "  Previous HEAD: $PREV_HEAD"
echo "  New HEAD:      $(git -C "$PROJECT_DIR" rev-parse HEAD)"
echo "  Previous rev:  ${PREV_ALEMBIC:-<none>}"
echo "  New rev:       $(PYTHONPATH="$PROJECT_DIR" python -m alembic -c backend/alembic.ini current 2>/dev/null | grep -oE "[a-f0-9]{8,}" | head -1)"
echo "============================================"
