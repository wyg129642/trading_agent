#!/usr/bin/env bash
# SSH tunnel: 127.0.0.1:8760 on this host -> 127.0.0.1:8760 on the jumpbox.
#
# The ASR service on the jumpbox binds only to loopback (see
# ops/asr_jumpbox/server.py), so this tunnel is the only access path.
# Ergo: if this tunnel is down, the user-kb audio upload path fails fast
# rather than stalling.
#
# Supervision strategy (no systemd --user on this host, no sudo):
#   - ``flock -n`` on a lock file guarantees single-instance.
#   - This script is scheduled by cron every minute; a missed invocation
#     (ours still running, flock busy) exits 0 immediately.
#   - Inside, we loop ssh forever with a 5 s backoff. A brief network blip
#     repairs itself in < 10 s; a full reboot relies on the cron tick.
#   - ssh itself has ServerAliveInterval=30 / ServerAliveCountMax=3 so it
#     detects a dead connection within ~100 s and drops out of the inner
#     call, triggering the retry.

set -u

LOCK_FILE=${ASR_TUNNEL_LOCK:-/home/ygwang/trading_agent/logs/asr_tunnel.lock}
LOG_FILE=${ASR_TUNNEL_LOG:-/home/ygwang/trading_agent/logs/asr_tunnel.log}
LOCAL_PORT=${ASR_TUNNEL_LOCAL_PORT:-8760}
REMOTE_PORT=${ASR_TUNNEL_REMOTE_PORT:-8760}
JUMPBOX=${JUMPBOX:-jumpbox}

mkdir -p "$(dirname "$LOG_FILE")" "$(dirname "$LOCK_FILE")"

# Open FD 9 on the lock file. flock -n returns non-zero if held elsewhere;
# in that case another invocation is already running, so we exit quietly.
exec 9>"$LOCK_FILE"
flock -n 9 || exit 0

# From here on, log everything.
exec >>"$LOG_FILE" 2>&1

# Rotate past 10 MiB so we never fill the disk on a runaway log.
if [[ -f "$LOG_FILE" ]] && [[ "$(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)" -gt 10485760 ]]; then
  mv -f "$LOG_FILE" "${LOG_FILE}.1"
  # Re-open after rotation — the old fd still points at the moved file.
  exec >>"$LOG_FILE" 2>&1
fi

echo "[$(date -Is)] tunnel starting: localhost:${LOCAL_PORT} -> ${JUMPBOX}:127.0.0.1:${REMOTE_PORT}"

# Track whether the previous iteration was an error so we don't spam logs
# when the tunnel is stable and only exits cleanly on host reboot.
while true; do
  ssh -N \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o ConnectTimeout=10 \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=accept-new \
    -L "127.0.0.1:${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT}" \
    "$JUMPBOX"
  rc=$?
  echo "[$(date -Is)] ssh exited rc=${rc}, reconnecting in 5s"
  sleep 5
done
