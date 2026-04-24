#!/usr/bin/env bash
# Deploy Qwen3-Embedding-8B as an OpenAI-compatible /v1/embeddings server on jumpbox.
#
# Runtime: vLLM (--task embed), not TEI.
#  - TEI 1.5 (the only tag reliably available from CN mirrors) predates
#    Qwen3-Embedding's tokenizer format → fails to parse tokenizer.json.
#  - TEI 1.7+ supports Qwen3 but is not on ghcr.nju.edu.cn or docker.1ms.run
#    mirrors, and direct ghcr.io is unreachable from this LAN.
#  - vLLM has first-class Qwen3-Embedding support and ships via 1ms.run mirror.
#  - The HTTP contract stays OpenAI-compatible (/v1/embeddings + Bearer auth),
#    so tei_client.py and the rest of the backend are unchanged.
#
# No-sudo variant: requires ygwang in the docker group. Uses docker's
# --restart=always for auto-start after host reboot. No systemd, no ufw.
#
# Idempotent: TEI_API_KEY is generated once and persisted to .env.secrets.
# Re-run to refresh the container or bump model/image versions.
#
# Usage:
#   bash scripts/deploy_jumpbox_tei.sh            # deploy
#   DRY_RUN=1 bash scripts/deploy_jumpbox_tei.sh  # preview

set -euo pipefail

JUMPBOX=${JUMPBOX:-jumpbox}
MODEL_NAME=${MODEL_NAME:-Qwen/Qwen3-Embedding-8B}
MODEL_LOCAL_NAME=$(basename "$MODEL_NAME")
MODEL_DIR_REL=${MODEL_DIR_REL:-models}
VLLM_IMG=${VLLM_IMG:-docker.m.daocloud.io/vllm/vllm-openai:v0.9.0}
GPU_DEVICE=${GPU_DEVICE:-0}
TEI_PORT=${TEI_PORT:-8080}
CONTAINER_NAME=${CONTAINER_NAME:-tei-embed}
SERVED_MODEL_NAME=${SERVED_MODEL_NAME:-qwen3-embed}
MAX_MODEL_LEN=${MAX_MODEL_LEN:-8192}
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
SECRETS_FILE="$PROJECT_ROOT/.env.secrets"

# ── TEI_API_KEY handling ─────────────────────────────────────
if [[ -z "${TEI_API_KEY:-}" ]]; then
  if [[ -f "$SECRETS_FILE" ]] && grep -q '^TEI_API_KEY=' "$SECRETS_FILE"; then
    TEI_API_KEY=$(grep '^TEI_API_KEY=' "$SECRETS_FILE" | tail -1 | cut -d= -f2-)
    echo "[deploy] reusing existing TEI_API_KEY from $SECRETS_FILE"
  else
    TEI_API_KEY=$(head -c 32 /dev/urandom | xxd -p -c 32)
    echo "[deploy] generated new TEI_API_KEY"
    mkdir -p "$(dirname "$SECRETS_FILE")"
    touch "$SECRETS_FILE"
    chmod 600 "$SECRETS_FILE"
    sed -i '/^TEI_API_KEY=/d' "$SECRETS_FILE"
    sed -i '/^TEI_BASE_URL=/d' "$SECRETS_FILE"
    echo "TEI_API_KEY=$TEI_API_KEY" >> "$SECRETS_FILE"
    echo "TEI_BASE_URL=http://116.239.28.36:$TEI_PORT" >> "$SECRETS_FILE"
  fi
fi

GITIGNORE="$PROJECT_ROOT/.gitignore"
if ! grep -q '^\.env\.secrets$' "$GITIGNORE" 2>/dev/null; then
  echo ".env.secrets" >> "$GITIGNORE"
  echo "[deploy] added .env.secrets to .gitignore"
fi

if [[ "${DRY_RUN:-}" == "1" ]]; then
  echo "[deploy] DRY_RUN: would deploy $MODEL_NAME via vLLM → $JUMPBOX GPU $GPU_DEVICE on :$TEI_PORT"
  exit 0
fi

# ── 1. Pre-flight ────────────────────────────────────────────
echo "[deploy] checking jumpbox reachability"
ssh -o BatchMode=yes -o ConnectTimeout=5 "$JUMPBOX" 'echo ok' > /dev/null

echo "[deploy] checking docker group membership"
ssh "$JUMPBOX" 'docker ps > /dev/null' \
  || { echo "ERROR: user cannot run docker (not in docker group?)"; exit 1; }

echo "[deploy] checking GPU $GPU_DEVICE"
ssh "$JUMPBOX" "nvidia-smi --query-gpu=index,name --format=csv,noheader | grep -q '^${GPU_DEVICE},'" \
  || { echo "ERROR: GPU $GPU_DEVICE not visible"; exit 1; }

# ── 2. Download model if missing ─────────────────────────────
MODEL_PATH_REMOTE="\$HOME/$MODEL_DIR_REL/$MODEL_LOCAL_NAME"
echo "[deploy] ensuring model at ~/$MODEL_DIR_REL/$MODEL_LOCAL_NAME"
ssh "$JUMPBOX" "mkdir -p \$HOME/$MODEL_DIR_REL"

if ! ssh "$JUMPBOX" "test -f $MODEL_PATH_REMOTE/config.json"; then
  echo "[deploy] installing huggingface-hub + hf-transfer (user-space, Tsinghua PyPI mirror)"
  ssh "$JUMPBOX" 'pip3 install --quiet --user --break-system-packages -i https://pypi.tuna.tsinghua.edu.cn/simple huggingface-hub hf-transfer' \
    || { echo "ERROR: pip install failed"; exit 1; }
  echo "[deploy] downloading $MODEL_NAME from HF mirror (~16 GB; resumable)"
  ssh "$JUMPBOX" "export HF_ENDPOINT=https://hf-mirror.com HF_HUB_ENABLE_HF_TRANSFER=1 PATH=\$HOME/.local/bin:\$PATH && \
    hf download $MODEL_NAME --local-dir $MODEL_PATH_REMOTE" \
    || { echo "ERROR: model download failed"; exit 1; }
else
  echo "[deploy] model already present"
fi

# ── 3. Pull vLLM image ───────────────────────────────────────
echo "[deploy] pulling vLLM image: $VLLM_IMG"
ssh "$JUMPBOX" "docker pull $VLLM_IMG" | tail -2

# ── 4. Stop any existing container ───────────────────────────
echo "[deploy] replacing container '$CONTAINER_NAME' if present"
ssh "$JUMPBOX" "docker rm -f $CONTAINER_NAME 2>/dev/null; true"

# ── 5. Launch with --restart=always ──────────────────────────
# --runtime=nvidia + NVIDIA_VISIBLE_DEVICES works reliably; --gpus flag has
# issues with the prestart hook on this host (libnvidia-ml load failure even
# though the lib is present at the standard path).
echo "[deploy] starting container $CONTAINER_NAME (vLLM) on GPU $GPU_DEVICE :$TEI_PORT"
ssh "$JUMPBOX" "docker run -d --restart=always --name $CONTAINER_NAME \
    --runtime=nvidia \
    -e NVIDIA_VISIBLE_DEVICES=$GPU_DEVICE \
    -e VLLM_API_KEY=$TEI_API_KEY \
    --ipc=host \
    -p $TEI_PORT:8000 \
    -v $MODEL_PATH_REMOTE:/model \
    $VLLM_IMG \
    --model /model \
    --task embed \
    --served-model-name $SERVED_MODEL_NAME \
    --host 0.0.0.0 \
    --port 8000 \
    --dtype float16 \
    --max-model-len $MAX_MODEL_LEN \
    --gpu-memory-utilization 0.85 \
    --trust-remote-code" > /dev/null

# ── 6. Wait for ready ────────────────────────────────────────
echo "[deploy] waiting for vLLM to become healthy (model load 60-180s on A100-40G)"
for i in {1..60}; do
  sleep 5
  if ssh "$JUMPBOX" "curl -fsS -H 'Authorization: Bearer $TEI_API_KEY' http://localhost:$TEI_PORT/v1/models" > /dev/null 2>&1; then
    echo "[deploy] vLLM is up (after $((i*5))s)"
    break
  fi
  if (( i % 6 == 0 )); then
    echo "[deploy] still waiting ($((i*5))s) — container state:"
    ssh "$JUMPBOX" "docker ps --filter name=$CONTAINER_NAME --format '{{.Status}}'"
  fi
  if (( i == 60 )); then
    echo "ERROR: vLLM didn't come up in 300s. Last logs:"
    ssh "$JUMPBOX" "docker logs --tail 40 $CONTAINER_NAME 2>&1"
    exit 1
  fi
done

# ── 7. Smoke test from server-yaojinghe ──────────────────────
echo "[deploy] smoke test"
DIM=$(curl -fsS --noproxy '116.239.28.36,localhost' \
  -H "Authorization: Bearer $TEI_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "http://116.239.28.36:$TEI_PORT/v1/embeddings" \
  -d "{\"input\":[\"你好 NVDA 台积电\"],\"model\":\"$SERVED_MODEL_NAME\"}" \
  | python3 -c "import sys,json; print(len(json.load(sys.stdin)['data'][0]['embedding']))" 2>/dev/null || echo "ERROR")

if [[ "$DIM" == "4096" ]]; then
  echo "[deploy] ✓ smoke test passed — embedding dim = 4096"
else
  echo "[deploy] ✗ smoke test failed — got: $DIM"
  echo "[deploy] container logs (last 40):"
  ssh "$JUMPBOX" "docker logs --tail 40 $CONTAINER_NAME 2>&1"
  exit 1
fi

cat <<EOF

─────────────────────────────────────────────────────────────
 Deployment complete (vLLM).

 Endpoint    : http://116.239.28.36:$TEI_PORT/v1/embeddings
 Auth        : Bearer \$TEI_API_KEY (in $SECRETS_FILE)
 Model name  : $SERVED_MODEL_NAME (in API requests)
 Container   : $CONTAINER_NAME (restart=always — auto-starts after boot)
 GPU         : device $GPU_DEVICE on $JUMPBOX

 Tail logs:
   ssh $JUMPBOX 'docker logs -f $CONTAINER_NAME'

 NOTE: ufw is NOT configured (you don't have sudo). Relying on
       API-key auth + LAN reachability. If jumpbox becomes
       public-facing, ask admin to restrict port $TEI_PORT.

 Next steps:
   1. source .env.secrets                                              # for ad-hoc curl
   2. python3 -m backend.app.services.kb_vector_ingest --coll alphapai/roadshows
   3. python3 -m scripts.kb_vector status
─────────────────────────────────────────────────────────────
EOF
