#!/usr/bin/env bash
# Idempotent installer for the Qwen3-Embedding-8B service on the jumpbox.
#
# Design goals (no sudo available on jumpbox):
#   - Everything under $HOME/embed/ — venv, logs, PID, supervisor socket.
#   - Model weights at $HOME/models/Qwen3-Embedding-8B/ (downloaded separately
#     via hf-mirror; this script does NOT re-download).
#   - supervisord runs user-space; @reboot crontab starts it on boot.
#   - API key generated on first install and written to
#     $PROJECT_ROOT/.env.secrets as TEI_API_KEY=... so the server-yaojinghe
#     side picks it up via config.py without any round trip.
#
# Usage (from server-yaojinghe, NOT from jumpbox):
#   bash ops/embed_jumpbox/install_embed_jumpbox.sh            # first run or refresh
#   REINSTALL_DEPS=1 bash ...                                  # force re-pip
#   DRY_RUN=1 bash ...                                         # preview actions
#
# After install:
#   ssh jumpbox 'tail -f ~/embed/logs/embed.log'
#   curl http://116.239.28.36:8080/health     # LAN access

set -euo pipefail

JUMPBOX=${JUMPBOX:-jumpbox}
# JUMPBOX_IP must be the LAN address, not the public 116.239.28.36 — from
# server-yaojinghe the public IP hairpins through the Xiaomi gateway whose
# admin panel occupies port 8080, intercepting our traffic. Inside-LAN:
JUMPBOX_IP=${JUMPBOX_IP:-192.168.31.224}
PROJECT_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SECRETS_FILE="$PROJECT_ROOT/.env.secrets"
EMBED_PORT=${EMBED_PORT:-8080}

echo "[embed] PROJECT_ROOT=$PROJECT_ROOT"

# ── 1. API key management ────────────────────────────────────
# TEI_API_KEY is the name pydantic-settings expects on server-yaojinghe.
# We write the same value under EMBED_API_KEY too so the jumpbox-side
# supervisord env can read it under the more descriptive name.
if [[ -z "${TEI_API_KEY:-}" ]]; then
  if [[ -f "$SECRETS_FILE" ]] && grep -q '^TEI_API_KEY=' "$SECRETS_FILE"; then
    TEI_API_KEY=$(grep '^TEI_API_KEY=' "$SECRETS_FILE" | tail -1 | cut -d= -f2-)
    echo "[embed] reusing existing TEI_API_KEY from $SECRETS_FILE"
  else
    TEI_API_KEY=$(head -c 32 /dev/urandom | xxd -p -c 32)
    echo "[embed] generated new TEI_API_KEY"
    mkdir -p "$(dirname "$SECRETS_FILE")"
    touch "$SECRETS_FILE"
    chmod 600 "$SECRETS_FILE"
    sed -i '/^TEI_API_KEY=/d' "$SECRETS_FILE"
    sed -i '/^TEI_BASE_URL=/d' "$SECRETS_FILE"
    echo "TEI_API_KEY=$TEI_API_KEY" >> "$SECRETS_FILE"
    echo "TEI_BASE_URL=http://${JUMPBOX_IP}:${EMBED_PORT}" >> "$SECRETS_FILE"
  fi
fi

EMBED_API_KEY="$TEI_API_KEY"

GITIGNORE="$PROJECT_ROOT/.gitignore"
if ! grep -q '^\.env\.secrets$' "$GITIGNORE" 2>/dev/null; then
  echo ".env.secrets" >> "$GITIGNORE"
fi

if [[ "${DRY_RUN:-}" == "1" ]]; then
  echo "[embed] DRY_RUN: would deploy server.py + supervisord to $JUMPBOX (GPU 2)"
  exit 0
fi

# ── 2. Reachability ──────────────────────────────────────────
echo "[embed] checking jumpbox reachability"
ssh -o BatchMode=yes -o ConnectTimeout=10 "$JUMPBOX" 'echo ok' > /dev/null

# ── 3. Bootstrap directories ─────────────────────────────────
ssh "$JUMPBOX" 'mkdir -p "$HOME/embed/run" "$HOME/embed/logs" "$HOME/embed/tmp" "$HOME/models"'

# ── 4. Copy server code + rendered supervisord.conf ──────────
echo "[embed] copying server.py"
scp -q "$PROJECT_ROOT/ops/embed_jumpbox/server.py" "$JUMPBOX:~/embed/server.py"

echo "[embed] rendering supervisord.conf"
TMP_CONF=$(mktemp)
trap 'rm -f "$TMP_CONF"' EXIT
HOME_REMOTE=$(ssh "$JUMPBOX" 'echo -n "$HOME"')
EMBED_API_KEY_ESCAPED=${EMBED_API_KEY//\//\\/}
sed \
  -e "s|\${HOME}|$HOME_REMOTE|g" \
  -e "s/\${EMBED_API_KEY}/$EMBED_API_KEY_ESCAPED/g" \
  "$PROJECT_ROOT/ops/embed_jumpbox/supervisord.conf.template" > "$TMP_CONF"
scp -q "$TMP_CONF" "$JUMPBOX:~/embed/supervisord.conf"

# ── 5. Create python venv + install deps ─────────────────────
ssh "$JUMPBOX" "REINSTALL_DEPS='${REINSTALL_DEPS:-}' bash -s" <<'REMOTE'
set -euo pipefail
cd "$HOME/embed"

if [[ -d venv && ! -f venv/bin/activate ]]; then
  echo "[embed] removing partial venv"
  rm -rf venv
fi

if [[ ! -d venv ]]; then
  echo "[embed] creating venv"
  if [[ ! -x "$HOME/.local/bin/virtualenv" ]] \
      && ! command -v virtualenv >/dev/null 2>&1; then
    echo "[embed] installing virtualenv (user-space)"
    pip install --user --break-system-packages \
      --index-url https://pypi.tuna.tsinghua.edu.cn/simple virtualenv
  fi
  VIRTUALENV_BIN=""
  if [[ -x "$HOME/.local/bin/virtualenv" ]]; then
    VIRTUALENV_BIN="$HOME/.local/bin/virtualenv"
  elif command -v virtualenv >/dev/null 2>&1; then
    VIRTUALENV_BIN=$(command -v virtualenv)
  fi
  "$VIRTUALENV_BIN" --python=python3.12 venv
fi

# shellcheck disable=SC1091
source venv/bin/activate

export PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
export PIP_DISABLE_PIP_VERSION_CHECK=1
python -m pip install --upgrade pip setuptools wheel

if [[ "${REINSTALL_DEPS:-}" == "1" ]] \
    || ! python -c "import transformers, fastapi, uvicorn, torch" 2>/dev/null; then
  echo "[embed] installing python deps (takes a few minutes for torch)"
  pip install --prefer-binary 'torch>=2.4,<2.7' 'transformers>=4.45'
  pip install \
    'fastapi>=0.110' 'uvicorn[standard]>=0.27' \
    'supervisor' 'accelerate'
fi
REMOTE

# ── 6. Ensure supervisord running; start/restart the program ──
ssh "$JUMPBOX" bash -s <<'REMOTE'
set -euo pipefail
cd "$HOME/embed"
source "$HOME/embed/venv/bin/activate"

PIDFILE="$HOME/embed/run/supervisord.pid"
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "[embed] supervisord already running, reloading"
  supervisorctl -c "$HOME/embed/supervisord.conf" reread >/dev/null 2>&1 || true
  supervisorctl -c "$HOME/embed/supervisord.conf" update >/dev/null 2>&1 || true
  supervisorctl -c "$HOME/embed/supervisord.conf" restart qwen3embed || true
else
  echo "[embed] starting supervisord"
  supervisord -c "$HOME/embed/supervisord.conf"
  sleep 2
  supervisorctl -c "$HOME/embed/supervisord.conf" status || true
fi

# ── 7. @reboot crontab (idempotent) ─────────────────────────
CRON_LINE="@reboot $HOME/embed/venv/bin/supervisord -c $HOME/embed/supervisord.conf"
current=$(crontab -l 2>/dev/null || true)
if ! echo "$current" | grep -Fq "$HOME/embed/supervisord.conf"; then
  echo "[embed] installing @reboot crontab entry"
  { echo "$current"; echo "$CRON_LINE"; } | crontab -
else
  echo "[embed] @reboot cron entry already present"
fi
REMOTE

# ── 8. Smoke test: wait for /health AND model_loaded ─────────
echo "[embed] waiting for /health (model load is ~60-120s on cold start)"
HEALTH=""
for i in {1..48}; do
  sleep 5
  HEALTH=$(curl -fsS --noproxy "*" "http://${JUMPBOX_IP}:${EMBED_PORT}/health" 2>/dev/null || true)
  if echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if d.get('model_loaded') else 1)" 2>/dev/null; then
    echo "[embed] model loaded (after $((i*5))s)"
    break
  fi
  if (( i % 6 == 0 )); then
    echo "[embed] still waiting ($((i*5))s) — latest health:"
    echo "$HEALTH" | head -c 200; echo
  fi
  if (( i == 48 )); then
    echo "ERROR: model did not load in 240s. Tail of embed.err:"
    ssh "$JUMPBOX" "tail -60 ~/embed/logs/embed.err 2>&1" || true
    exit 1
  fi
done

# ── 9. End-to-end embedding smoke test ───────────────────────
echo "[embed] smoke test /v1/embeddings"
DIM=$(curl -fsS --noproxy "*" \
  -H "Authorization: Bearer $TEI_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "http://${JUMPBOX_IP}:${EMBED_PORT}/v1/embeddings" \
  -d '{"input":["你好 NVDA 台积电"],"model":"qwen3-embed"}' \
  | python3 -c "import sys,json; print(len(json.load(sys.stdin)['data'][0]['embedding']))" 2>/dev/null || echo "ERROR")

if [[ "$DIM" == "4096" ]]; then
  echo "[embed] ✓ smoke test passed — embedding dim = 4096"
else
  echo "[embed] ✗ smoke test failed — got: $DIM"
  ssh "$JUMPBOX" "tail -30 ~/embed/logs/embed.err 2>&1" || true
  exit 1
fi

cat <<EOF

─────────────────────────────────────────────────────────────
 Deployment complete (venv + supervisord).

 Endpoint   : http://${JUMPBOX_IP}:${EMBED_PORT}/v1/embeddings
 Auth       : Bearer \$TEI_API_KEY (in $SECRETS_FILE)
 Model      : qwen3-embed (alias for Qwen3-Embedding-8B)
 GPU        : CUDA_VISIBLE_DEVICES=2 on $JUMPBOX
 Supervisor : ~/embed/supervisord.conf (auto-starts via @reboot crontab)

 Ops:
   ssh $JUMPBOX 'supervisorctl -c ~/embed/supervisord.conf status'
   ssh $JUMPBOX 'tail -f ~/embed/logs/embed.log'
   ssh $JUMPBOX 'supervisorctl -c ~/embed/supervisord.conf restart qwen3embed'
─────────────────────────────────────────────────────────────
EOF
