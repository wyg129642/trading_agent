#!/usr/bin/env bash
# ============================================================
# scripts/smoke.sh — minimal post-deploy smoke test
# ============================================================
#
# Hits a small, curated set of read-only endpoints against a
# running backend and exits non-zero on any failure. Designed
# to be cheap (< 10 s total) and run:
#
#   1. at the end of `./start_web.sh deploy` (verify the new
#      backend actually serves),
#   2. as a pre-promotion gate inside `scripts/promote.sh`
#      (verify staging is green before fast-forwarding main).
#
# Usage:
#   scripts/smoke.sh                 # defaults to localhost:8000
#   scripts/smoke.sh 20301           # staging
#   PORT=20301 scripts/smoke.sh
#   HOST=192.168.31.97 PORT=8000 scripts/smoke.sh
# ============================================================

set -u

HOST="${HOST:-localhost}"
PORT="${1:-${PORT:-8000}}"
BASE="http://${HOST}:${PORT}"
TIMEOUT=8   # per-request wall clock

# ---- Pretty output ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
info()  { echo -e "${BLUE}[SMOKE]${NC}  $*"; }
ok()    { echo -e "${GREEN}[PASS]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*" >&2; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }

# Number of failures
rc=0

# ---- curl wrapper that bypasses Clash on localhost ----
_probe() {
    # $1 = path, $2 = expected-status (default 200), $3 = optional grep needle
    local path="$1"
    local want="${2:-200}"
    local needle="${3:-}"
    local url="${BASE}${path}"
    local out
    local status

    out=$(curl -sS -o /tmp/smoke_body.$$ -w "%{http_code}" \
        --max-time "$TIMEOUT" --noproxy '*' "$url" 2>/tmp/smoke_err.$$)
    status="$out"

    if [ "$status" != "$want" ]; then
        fail "$path → HTTP $status (expected $want)"
        echo "       body: $(head -c 200 /tmp/smoke_body.$$ 2>/dev/null)"
        rc=$((rc + 1))
        return 1
    fi

    if [ -n "$needle" ]; then
        if ! grep -q "$needle" /tmp/smoke_body.$$ 2>/dev/null; then
            fail "$path → HTTP $want but missing '$needle' in body"
            echo "       body: $(head -c 200 /tmp/smoke_body.$$ 2>/dev/null)"
            rc=$((rc + 1))
            return 1
        fi
    fi

    ok "$path → $status"
    return 0
}

# ---- Probes ----
# Order matters: hit the cheapest first so we fail fast. Each probe is allowed
# to fail independently — we keep going so the operator sees the full set of
# failures in one run rather than peeling them off one at a time.

info "Target: $BASE"

# 1. Liveness — cheapest possible ping; must return {"status":"ok"}.
_probe "/api/health" 200 '"ok"'

# 2. OpenAPI schema loads — catches startup-time router import failures
#    (a broken import inside any router module crashes uvicorn; if /docs
#    answers, every router loaded).
_probe "/openapi.json" 200

# 3. Public read-only endpoints — each belongs to a different router group,
#    so a 500 here localises the broken area. All are auth-free or tolerate
#    anon by returning []; we only check they don't 5xx.
for path in \
    "/api/news?limit=1" \
    "/api/sources/health" \
    "/api/analytics/system" \
    "/api/stock-hub/AAPL.US?limit=1" ; do
    # Accept 200 OR 401 (both mean the router loaded; auth is a separate
    # concern). Anything else = broken.
    status=$(curl -sS -o /dev/null -w "%{http_code}" \
        --max-time "$TIMEOUT" --noproxy '*' "${BASE}${path}" 2>/dev/null)
    if [[ "$status" =~ ^(200|401)$ ]]; then
        ok "$path → $status"
    else
        fail "$path → HTTP $status"
        rc=$((rc + 1))
    fi
done

# 4. Static SPA — the catch-all should hand back index.html (or at least a
#    2xx with some HTML). Only bother if this is a bundle-serving backend.
status=$(curl -sS -o /tmp/smoke_body.$$ -w "%{http_code}" \
    --max-time "$TIMEOUT" --noproxy '*' "${BASE}/" 2>/dev/null)
if [[ "$status" == "200" ]]; then
    if grep -q "<!doctype\|<html\|<!DOCTYPE" /tmp/smoke_body.$$ 2>/dev/null; then
        ok "/ (SPA root) → 200 + HTML"
    else
        warn "/ → 200 but body is not HTML (backend may not be serving the SPA)"
    fi
else
    warn "/ → $status (OK if nginx is in front of the backend)"
fi

rm -f /tmp/smoke_body.$$ /tmp/smoke_err.$$ 2>/dev/null

# ---- Result ----
echo ""
if [ "$rc" -eq 0 ]; then
    ok "All checks passed against $BASE"
    exit 0
else
    fail "$rc check(s) failed against $BASE"
    exit 1
fi
