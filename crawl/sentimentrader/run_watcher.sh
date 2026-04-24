#!/bin/bash
# Launch the SentimenTrader scraper in a long-running "fetch once per day" loop.
# Detaches via nohup so the process survives logout. Stops a previous instance
# if it's still running, so re-running is safe (idempotent).
#
#   ./run_watcher.sh        # start (or restart) the daily watcher
#   ./run_watcher.sh stop   # stop the watcher
#   ./run_watcher.sh status # print pid + last lines of the log
#
# Data source: https://sentimentrader.com (paid subscription).
# Credentials are read from crawl/sentimentrader/credentials.json or env vars
# SENTIMENTRADER_EMAIL / SENTIMENTRADER_PASSWORD.
# ---------------------------------------------------------------------------

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

PID_FILE="$HERE/watcher.pid"
LOG_FILE="$HERE/../../logs/sentimentrader.log"
mkdir -p "$(dirname "$LOG_FILE")"

_is_alive() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || echo)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

case "${1:-start}" in
  stop)
    if _is_alive; then
      pid="$(cat "$PID_FILE")"
      echo "stopping watcher pid=$pid"
      kill "$pid" || true
      rm -f "$PID_FILE"
    else
      echo "watcher not running"
    fi
    ;;

  status)
    if _is_alive; then
      echo "watcher running, pid=$(cat "$PID_FILE")"
    else
      echo "watcher NOT running"
    fi
    echo "--- last 20 log lines ---"
    tail -n 20 "$LOG_FILE" 2>/dev/null || echo "(no log yet)"
    ;;

  start|*)
    if _is_alive; then
      echo "watcher already running (pid=$(cat "$PID_FILE")) — restarting"
      kill "$(cat "$PID_FILE")" || true
      rm -f "$PID_FILE"
      sleep 1
    fi
    # Run one fetch synchronously first so the user sees immediate results,
    # then fall into the daily loop. Interval 86400s ≈ 24h.
    nohup python3 scraper.py --watch --interval 86400 >>"$LOG_FILE" 2>&1 &
    echo $! >"$PID_FILE"
    echo "watcher started, pid=$(cat "$PID_FILE"), log=$LOG_FILE"
    ;;
esac
