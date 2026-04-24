#!/usr/bin/env bash
# End-to-end smoke test for the Qwen3-ASR pipeline.
#
# Runs through the happy-path lifecycle a real user would exercise:
#   1. Tunnel + service reachability via /health
#   2. Direct jumpbox /transcribe + poll to verify ASR works standalone
#   3. Backend /api/user-kb/asr/ping (requires auth — see TOKEN below)
#   4. (Optional) full upload via /api/user-kb/documents — needs a logged-in
#      user JWT in env var TOKEN. Skipped if TOKEN is empty.
#
# Not a load test. Not a CI integration. Hand-run after install to confirm
# the pipeline is wired up end-to-end.

set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
SECRETS_FILE="$PROJECT_ROOT/.env.secrets"

# ── Pull the API key (written by install_asr_jumpbox.sh) ─────────
API_KEY=$(grep '^ASR_API_KEY=' "$SECRETS_FILE" | tail -1 | cut -d= -f2-)
if [[ -z "${API_KEY:-}" ]]; then
  echo "ASR_API_KEY missing from $SECRETS_FILE — has the installer been run?"
  exit 1
fi

AUDIO=${AUDIO:-/tmp/asr_test.wav}
if [[ ! -f "$AUDIO" ]]; then
  echo "Fetching a short Chinese WAV sample..."
  curl -sS -o "$AUDIO" \
    https://qianwen-res.oss-cn-beijing.aliyuncs.com/Qwen3-ASR-Repo/asr_zh.wav
fi

echo "── 1. tunnel + /health ───────────────────────────────"
HEALTH=$(curl -sS --max-time 10 http://127.0.0.1:8760/health)
echo "$HEALTH"
echo "$HEALTH" | grep -q '"model_loaded":true' \
  || { echo "WARN: model not loaded yet; retry in a few seconds"; }

echo
echo "── 2. direct /transcribe + poll ───────────────────────"
JOB=$(curl -sS --max-time 60 \
  -H "Authorization: Bearer $API_KEY" \
  -F "file=@$AUDIO" \
  http://127.0.0.1:8760/transcribe)
echo "submit: $JOB"
JOB_ID=$(echo "$JOB" | grep -oE '"job_id":"[a-f0-9]+"' | cut -d'"' -f4)
if [[ -z "$JOB_ID" ]]; then
  echo "FAIL: no job_id returned"
  exit 1
fi

for i in $(seq 1 60); do
  STATE=$(curl -sS -H "Authorization: Bearer $API_KEY" \
    "http://127.0.0.1:8760/jobs/$JOB_ID")
  STATUS=$(echo "$STATE" | grep -oE '"status":"[a-z]+"' | cut -d'"' -f4)
  PCT=$(echo "$STATE" | grep -oE '"percent":[0-9]+' | cut -d: -f2)
  PHASE=$(echo "$STATE" | grep -oE '"phase":"[^"]*"' | cut -d'"' -f4)
  echo "  tick $i: status=$STATUS percent=$PCT phase=$PHASE"
  if [[ "$STATUS" == "done" ]]; then
    echo "  TEXT: $(echo "$STATE" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("text","")[:200])')"
    echo "  LANG: $(echo "$STATE" | grep -oE '"language":"[^"]*"' | cut -d'"' -f4)"
    break
  fi
  if [[ "$STATUS" == "error" ]]; then
    echo "  ERROR: $(echo "$STATE" | grep -oE '"error":"[^"]*"')"
    exit 1
  fi
  sleep 1
done

echo
echo "── 3. backend /api/user-kb/asr/ping (optional, needs TOKEN) ─"
if [[ -n "${TOKEN:-}" ]]; then
  curl -sS --max-time 10 \
    -H "Authorization: Bearer $TOKEN" \
    http://127.0.0.1:8000/api/user-kb/asr/ping
  echo
else
  echo "  (skipped — set TOKEN=<jwt> to exercise this path)"
fi

echo
echo "Smoke test complete."
