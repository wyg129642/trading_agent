#!/usr/bin/env bash
# ============================================================
# promote.sh — fast-forward staging into production
# ============================================================
#
# Intended to be run from the STAGING worktree after you've
# verified new work on http://39.105.42.197:20301. It does NOT
# touch prod's running processes — it only moves git refs, so
# a human still has to decide when to flip prod by running
# `./start_web.sh deploy` inside /home/ygwang/trading_agent.
#
# Steps:
#   1. Refuse if run outside the staging worktree (safety).
#   2. Refuse if staging has uncommitted changes.
#   3. Fetch + fast-forward-check (staging must be strictly ahead of main).
#   4. Tag the staging HEAD with a dated release tag.
#   5. Fast-forward main → staging (in BOTH worktrees — prod worktree's
#      main is updated via `git -C` so the prod workspace is ready for
#      `./start_web.sh deploy`).
#   6. Push main + tag.
#   7. Print the deploy command for the human to copy-paste.
#
# Usage:
#   cd /home/ygwang/trading_agent_staging
#   ./scripts/promote.sh            # uses default tag: v$(date +%Y.%m.%d-%H%M)
#   ./scripts/promote.sh v2026.04.25-hotfix
# ============================================================

set -euo pipefail

# ---- Config ----
PROD_WORKTREE="/home/ygwang/trading_agent"
STAGING_WORKTREE="/home/ygwang/trading_agent_staging"
MAIN_BRANCH="main"
STAGING_BRANCH="staging"

# ---- Pretty output ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERR]${NC}   $*" >&2; }

# ---- Preconditions ----
HERE="$(cd "$(dirname "$0")/.." && pwd)"
if [ "$HERE" != "$STAGING_WORKTREE" ]; then
    err "promote.sh must be run from the staging worktree."
    err "  expected: $STAGING_WORKTREE"
    err "  actual:   $HERE"
    exit 1
fi

# Branch check
current_branch=$(git -C "$HERE" symbolic-ref --short HEAD 2>/dev/null || echo "")
if [ "$current_branch" != "$STAGING_BRANCH" ]; then
    err "staging worktree is on branch '$current_branch', expected '$STAGING_BRANCH'"
    exit 1
fi

# Working tree must be clean — no half-staged merges into prod.
if ! git -C "$HERE" diff --quiet || ! git -C "$HERE" diff --cached --quiet; then
    err "staging worktree has uncommitted changes. Commit or stash first."
    git -C "$HERE" status --short >&2
    exit 1
fi

# ---- Fetch ----
info "Fetching latest refs from origin..."
git -C "$HERE" fetch --tags --prune origin >/dev/null

# ---- Fast-forward precheck ----
# staging must be strictly ahead of main (no divergent commits) for the
# promotion to be a trivial fast-forward.
ahead=$(git -C "$HERE" rev-list --count "origin/${MAIN_BRANCH}..HEAD")
behind=$(git -C "$HERE" rev-list --count "HEAD..origin/${MAIN_BRANCH}")
if [ "$behind" -gt 0 ]; then
    err "main has ${behind} commit(s) that staging is missing. Rebase staging onto main first:"
    err "    git pull --rebase origin main"
    exit 1
fi
if [ "$ahead" -eq 0 ]; then
    warn "staging has no new commits since main. Nothing to promote."
    exit 0
fi

# Show the commits about to be promoted so the operator has a last chance
# to bail out before the merge + push.
info "Promoting $ahead commit(s) from staging → main:"
git -C "$HERE" log --oneline "origin/${MAIN_BRANCH}..HEAD"
echo ""
read -p "Proceed? [y/N] " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    warn "Aborted by operator."
    exit 0
fi

# ---- Tag ----
TAG="${1:-v$(date +%Y.%m.%d-%H%M)}"
if git -C "$HERE" rev-parse "$TAG" >/dev/null 2>&1; then
    err "tag '$TAG' already exists."
    exit 1
fi
info "Tagging staging HEAD as $TAG..."
git -C "$HERE" tag -a "$TAG" -m "promotion: $(git -C "$HERE" rev-parse --short HEAD) → prod"

# ---- Update prod worktree's main branch ----
# A bare `git push origin HEAD:main` would move origin/main but leave the
# prod worktree's checked-out main pointing at the old commit until the
# operator runs `git pull` there. Instead we fast-forward the prod
# worktree's branch directly so its working tree is ready to deploy.
if [ ! -d "$PROD_WORKTREE" ]; then
    err "prod worktree $PROD_WORKTREE does not exist."
    exit 1
fi
prod_branch=$(git -C "$PROD_WORKTREE" symbolic-ref --short HEAD 2>/dev/null || echo "")
if [ "$prod_branch" != "$MAIN_BRANCH" ]; then
    err "prod worktree is on branch '$prod_branch', expected '$MAIN_BRANCH' — refusing to promote."
    exit 1
fi
if ! git -C "$PROD_WORKTREE" diff --quiet || ! git -C "$PROD_WORKTREE" diff --cached --quiet; then
    err "prod worktree has uncommitted changes. Inspect and commit/stash before promoting."
    git -C "$PROD_WORKTREE" status --short >&2
    exit 1
fi

info "Fast-forwarding $MAIN_BRANCH in prod worktree to staging HEAD..."
# `git merge --ff-only` against the staging ref keeps this commit-free if
# the branch is truly a fast-forward.
git -C "$PROD_WORKTREE" fetch origin >/dev/null
git -C "$PROD_WORKTREE" merge --ff-only "refs/heads/${STAGING_BRANCH}"

# ---- Push ----
info "Pushing $MAIN_BRANCH and tag $TAG to origin..."
git -C "$PROD_WORKTREE" push origin "$MAIN_BRANCH"
git -C "$PROD_WORKTREE" push origin "$TAG"

ok  "Promotion complete."
echo ""
echo "Next step — deploy prod (runs migrations + rebuilds frontend + restarts backend):"
echo "  cd $PROD_WORKTREE && ./start_web.sh deploy"
echo ""
echo "To roll back:"
echo "  cd $PROD_WORKTREE && git reset --hard $(git -C "$PROD_WORKTREE" describe --tags --abbrev=0 "$TAG^" 2>/dev/null || echo '<prev-tag>')"
echo "  cd $PROD_WORKTREE && ./start_web.sh deploy"
