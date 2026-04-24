#!/usr/bin/env bash
# Idempotent installer for the Qwen3-ASR-1.7B service on the jumpbox.
#
# Design goals (no sudo available on jumpbox):
#   - Everything lives under $HOME/asr/ — venv, logs, PID, supervisor socket.
#   - Model weights at $HOME/models/Qwen3-ASR-1.7B/ (shared with other
#     ModelScope downloads so the TEI deploy script can piggyback if needed).
#   - supervisord runs in userspace; a @reboot crontab entry starts it on boot.
#   - The API key is generated on first install and written to
#     $PROJECT_ROOT/.env.secrets as ASR_API_KEY=... so the web server side
#     can pick it up without a round trip.
#
# Usage (from web server, NOT from jumpbox):
#   bash ops/asr_jumpbox/install_asr_jumpbox.sh                 # first run or refresh
#   REINSTALL_DEPS=1 bash ops/asr_jumpbox/install_asr_jumpbox.sh  # force re-pip
#   DRY_RUN=1 bash ops/asr_jumpbox/install_asr_jumpbox.sh       # preview actions
#
# After install:
#   ssh jumpbox 'tail -f ~/asr/logs/asr.log'
#   curl http://127.0.0.1:8760/health     # via tunnel

set -euo pipefail

JUMPBOX=${JUMPBOX:-jumpbox}
JUMPBOX_IP=${JUMPBOX_IP:-116.239.28.36}
PROJECT_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SECRETS_FILE="$PROJECT_ROOT/.env.secrets"
ASR_PORT=${ASR_PORT:-8760}

echo "[asr] PROJECT_ROOT=$PROJECT_ROOT"

# ── 1. API key management ────────────────────────────────────
if [[ -z "${ASR_API_KEY:-}" ]]; then
  if [[ -f "$SECRETS_FILE" ]] && grep -q '^ASR_API_KEY=' "$SECRETS_FILE"; then
    ASR_API_KEY=$(grep '^ASR_API_KEY=' "$SECRETS_FILE" | tail -1 | cut -d= -f2-)
    echo "[asr] reusing existing ASR_API_KEY from $SECRETS_FILE"
  else
    ASR_API_KEY=$(head -c 32 /dev/urandom | xxd -p -c 32)
    echo "[asr] generated new ASR_API_KEY"
    mkdir -p "$(dirname "$SECRETS_FILE")"
    touch "$SECRETS_FILE"
    chmod 600 "$SECRETS_FILE"
    # Two aliases for the same secret:
    #   ASR_API_KEY          — consumed by the jumpbox server.py via
    #                          supervisord environment= (see
    #                          supervisord.conf.template).
    #   ASR_SERVICE_API_KEY  — consumed by backend/app/config.py's
    #                          pydantic-settings field asr_service_api_key.
    # Writing both under one file keeps rotation atomic — bump both lines
    # together and neither side is caught out of sync.
    sed -i '/^ASR_API_KEY=/d' "$SECRETS_FILE"
    sed -i '/^ASR_SERVICE_API_KEY=/d' "$SECRETS_FILE"
    sed -i '/^ASR_SERVICE_URL=/d' "$SECRETS_FILE"
    echo "ASR_API_KEY=$ASR_API_KEY" >> "$SECRETS_FILE"
    echo "ASR_SERVICE_API_KEY=$ASR_API_KEY" >> "$SECRETS_FILE"
    echo "ASR_SERVICE_URL=http://127.0.0.1:$ASR_PORT" >> "$SECRETS_FILE"
  fi
fi

# Ensure the backend-side alias exists even on reuse (old installs only
# wrote ASR_API_KEY; add the pydantic-mapped name idempotently).
if [[ -f "$SECRETS_FILE" ]] && ! grep -q '^ASR_SERVICE_API_KEY=' "$SECRETS_FILE"; then
  echo "ASR_SERVICE_API_KEY=$ASR_API_KEY" >> "$SECRETS_FILE"
fi

# Ensure .env.secrets is git-ignored (defensive).
GITIGNORE="$PROJECT_ROOT/.gitignore"
if ! grep -q '^\.env\.secrets$' "$GITIGNORE" 2>/dev/null; then
  echo ".env.secrets" >> "$GITIGNORE"
fi

if [[ "${DRY_RUN:-}" == "1" ]]; then
  echo "[asr] DRY_RUN: would deploy server.py + supervisord to $JUMPBOX"
  exit 0
fi

# ── 2. Reachability ──────────────────────────────────────────
echo "[asr] checking jumpbox reachability"
ssh -o BatchMode=yes -o ConnectTimeout=10 "$JUMPBOX" 'echo ok' > /dev/null

# ── 3. Bootstrap directories on jumpbox ──────────────────────
ssh "$JUMPBOX" 'mkdir -p "$HOME/asr/run" "$HOME/asr/logs" "$HOME/asr/tmp" "$HOME/models"'

# ── 4. Copy server code + rendered supervisord.conf ──────────
echo "[asr] copying server.py"
scp -q "$PROJECT_ROOT/ops/asr_jumpbox/server.py" "$JUMPBOX:~/asr/server.py"

echo "[asr] rendering supervisord.conf"
# Render locally then ship — cleaner than eval on the far side.
TMP_CONF=$(mktemp)
trap 'rm -f "$TMP_CONF"' EXIT
HOME_REMOTE=$(ssh "$JUMPBOX" 'echo -n "$HOME"')
ASR_API_KEY_ESCAPED=${ASR_API_KEY//\//\\/}
sed \
  -e "s|\${HOME}|$HOME_REMOTE|g" \
  -e "s/\${ASR_API_KEY}/$ASR_API_KEY_ESCAPED/g" \
  "$PROJECT_ROOT/ops/asr_jumpbox/supervisord.conf.template" > "$TMP_CONF"
scp -q "$TMP_CONF" "$JUMPBOX:~/asr/supervisord.conf"
chmod 600 "$TMP_CONF" 2>/dev/null || true

# ── 5. Create python venv + install dependencies ─────────────
# Pass REINSTALL_DEPS through the SSH env so the remote heredoc can see it
# (cron on the far side otherwise has no env vars to inherit).
ssh "$JUMPBOX" "REINSTALL_DEPS='${REINSTALL_DEPS:-}' bash -s" <<'REMOTE'
set -euo pipefail
cd "$HOME/asr"

# A partial venv (from an interrupted install) is worse than none — the
# `[[ ! -d venv ]]` guard skips re-creation, but `source venv/bin/activate`
# will fail because ensurepip left no activate script. Detect and remove.
if [[ -d venv && ! -f venv/bin/activate ]]; then
  echo "[asr] removing partial venv (missing activate script)"
  rm -rf venv
fi

if [[ ! -d venv ]]; then
  echo "[asr] creating venv"
  # On Ubuntu 24.04 the stock system python lacks ensurepip (it lives in
  # the apt-only python3-venv package, which we can't install without sudo).
  # Use user-space virtualenv instead — same end result, no root required.
  # PEP-668 externally-managed-environment markers need --break-system-packages.
  if [[ ! -x "$HOME/.local/bin/virtualenv" ]] \
      && ! command -v virtualenv >/dev/null 2>&1; then
    echo "[asr] installing virtualenv (user-space)"
    pip install --user --break-system-packages \
      --index-url https://pypi.tuna.tsinghua.edu.cn/simple \
      virtualenv
  fi
  VIRTUALENV_BIN=""
  if [[ -x "$HOME/.local/bin/virtualenv" ]]; then
    VIRTUALENV_BIN="$HOME/.local/bin/virtualenv"
  elif command -v virtualenv >/dev/null 2>&1; then
    VIRTUALENV_BIN=$(command -v virtualenv)
  else
    echo "[asr] ERROR: virtualenv still not available after pip install"
    exit 1
  fi
  "$VIRTUALENV_BIN" --python=python3.12 venv
fi

# shellcheck disable=SC1091
source venv/bin/activate

# Pin every pip call to the Tsinghua mirror — the default pypi.org is
# unreliably reachable from the jumpbox's network (either firewalled or
# heavily rate-limited on large shards like torch). One central mirror
# keeps installs fast and deterministic.
export PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
export PIP_DISABLE_PIP_VERSION_CHECK=1
# virtualenv ≥ 20 only seeds pip; setuptools + wheel need explicit install.
python -m pip install --upgrade pip setuptools wheel

if [[ "${REINSTALL_DEPS:-}" == "1" ]] || ! python -c "import qwen_asr, fastapi, uvicorn" 2>/dev/null; then
  echo "[asr] installing python deps"
  # Torch from the Tsinghua mirror only. We intentionally do NOT add
  # download.pytorch.org as an extra index: transitive CUDA wheels
  # (notably nvidia-cudnn-cu12, 664 MB) download at ~50 kB/s from that
  # CDN in China and wedge the install for 30+ minutes. Tsinghua carries
  # the same wheels with identical hashes, and picks a CUDA variant
  # that's driver-compatible on Ubuntu 24.04 + A100.
  pip install --prefer-binary 'torch>=2.4,<2.7' torchaudio
  pip install \
    'qwen-asr' \
    'fastapi>=0.110' 'uvicorn[standard]>=0.27' 'python-multipart' \
    'soundfile' 'imageio-ffmpeg' \
    'supervisor' 'modelscope'
fi
REMOTE

# ── 6. Download model if missing ─────────────────────────────
ssh "$JUMPBOX" bash -s <<'REMOTE'
set -euo pipefail
MODEL_DIR="$HOME/models/Qwen3-ASR-1.7B"
if [[ -f "$MODEL_DIR/config.json" ]]; then
  echo "[asr] model already present at $MODEL_DIR"
else
  echo "[asr] downloading model to $MODEL_DIR"
  source "$HOME/asr/venv/bin/activate"
  modelscope download --model Qwen/Qwen3-ASR-1.7B --local_dir "$MODEL_DIR"
fi
REMOTE

# ── 7. Ensure supervisord is running; (re)start the program ──
ssh "$JUMPBOX" bash -s <<'REMOTE'
set -euo pipefail
cd "$HOME/asr"
source "$HOME/asr/venv/bin/activate"

PIDFILE="$HOME/asr/run/supervisord.pid"
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "[asr] supervisord already running, reloading"
  supervisorctl -c "$HOME/asr/supervisord.conf" reread >/dev/null 2>&1 || true
  supervisorctl -c "$HOME/asr/supervisord.conf" update >/dev/null 2>&1 || true
  supervisorctl -c "$HOME/asr/supervisord.conf" restart qwen3asr || true
else
  echo "[asr] starting supervisord"
  supervisord -c "$HOME/asr/supervisord.conf"
  # Give supervisord a moment to spawn the child.
  sleep 2
  supervisorctl -c "$HOME/asr/supervisord.conf" status || true
fi

# ── 8. Install @reboot cron entry (idempotent) ──────────────
CRON_LINE="@reboot $HOME/asr/venv/bin/supervisord -c $HOME/asr/supervisord.conf"
current=$(crontab -l 2>/dev/null || true)
if ! echo "$current" | grep -Fq "$HOME/asr/supervisord.conf"; then
  echo "[asr] installing @reboot crontab entry"
  { echo "$current"; echo "$CRON_LINE"; } | crontab -
else
  echo "[asr] @reboot cron entry already present"
fi
REMOTE

# ── 9. Smoke test: wait for /health AND model_loaded ─────────
# 60 s is enough for uvicorn to come up but not for a cold model load
# (~10-15 s on A100). We poll for up to 5 minutes so the check is
# meaningful for both fast reinstalls and cold-from-scratch deployments.
echo "[asr] waiting for service to respond on jumpbox /health"
HEALTH=""
for i in $(seq 1 150); do
  HEALTH=$(ssh "$JUMPBOX" "curl -sS --max-time 3 http://127.0.0.1:$ASR_PORT/health 2>/dev/null" || true)
  if [[ -n "$HEALTH" ]] && echo "$HEALTH" | grep -q '"model_loaded":true'; then
    echo "[asr] /health OK:"
    echo "$HEALTH"
    break
  fi
  # Informative progress ping every ~20 s
  if (( i % 10 == 0 )) && [[ -n "$HEALTH" ]]; then
    echo "[asr] still waiting — latest /health: $HEALTH"
  fi
  sleep 2
  if [[ $i -eq 150 ]]; then
    echo "[asr] WARNING: model still not loaded after 5 min. Last /health: $HEALTH"
    ssh "$JUMPBOX" "tail -n 40 ~/asr/logs/asr.log ~/asr/logs/asr.err 2>/dev/null" || true
    exit 1
  fi
done

echo "[asr] install OK. API key saved to $SECRETS_FILE"
echo "[asr] Next step: start the SSH tunnel via ops/asr_tunnel/install_tunnel.sh"
