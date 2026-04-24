#!/usr/bin/env bash
# Install + start the supervised SSH tunnel to the jumpbox ASR service.
#
# Adds two guarantees:
#   1. An every-minute cron entry running asr_tunnel.sh (flock-guarded).
#   2. An immediate foreground kick so we don't wait up to 60 s after
#      install for the first cron firing.
#
# Idempotent. Safe to re-run.

set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
TUNNEL="$HERE/asr_tunnel.sh"
chmod +x "$TUNNEL"

# ── 1. Install the cron entry if missing ───────────────────
CRON_LINE="* * * * * $TUNNEL >/dev/null 2>&1"
current=$(crontab -l 2>/dev/null || true)
if ! echo "$current" | grep -Fq "$TUNNEL"; then
  echo "[tunnel] installing cron entry"
  { [[ -n "$current" ]] && echo "$current"; echo "$CRON_LINE"; } | crontab -
else
  echo "[tunnel] cron entry already installed"
fi

# ── 2. Kick off an instance now (nohup detaches so this script can exit) ──
# Re-exec via nohup+setsid so the tunnel keeps running after install_tunnel
# itself terminates. flock inside asr_tunnel.sh prevents double-starts if
# the cron tick also fires during the race.
echo "[tunnel] starting tunnel (detached)"
nohup setsid "$TUNNEL" >/dev/null 2>&1 < /dev/null &

# ── 3. Wait for the local port to bind ────────────────────
echo "[tunnel] waiting for localhost:8760 to bind"
for i in $(seq 1 20); do
  if ss -tln 2>/dev/null | grep -q '127.0.0.1:8760 '; then
    echo "[tunnel] local port 8760 bound. Test with: curl http://127.0.0.1:8760/health"
    exit 0
  fi
  sleep 1
done

echo "[tunnel] WARNING: port did not bind within 20 s"
echo "[tunnel] Check: tail -n 50 /home/ygwang/trading_agent/logs/asr_tunnel.log"
exit 1
