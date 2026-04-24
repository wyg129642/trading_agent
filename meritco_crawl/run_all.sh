#!/bin/bash
# Sequential full-crawl runner: type=2 → type=3 → type=1.
# - Waits for any currently-running crawler to finish first.
# - Migrates legacy progress.json → progress_type2.json between runs.
# - Serializes types to avoid triggering anti-bot on the same account.

set -u
cd "$(dirname "$0")"

LOG="data/run_all.log"
mkdir -p data
echo "[$(date '+%F %T')] ==== run_all started ====" | tee -a "$LOG"

wait_for_current() {
    if [ -f data/crawler.pid ]; then
        CURRENT_PID=$(cat data/crawler.pid)
        if ps -p "$CURRENT_PID" > /dev/null 2>&1; then
            echo "[$(date '+%F %T')] waiting for existing crawler PID=$CURRENT_PID..." | tee -a "$LOG"
            while ps -p "$CURRENT_PID" > /dev/null 2>&1; do
                sleep 30
            done
            echo "[$(date '+%F %T')] previous crawler finished" | tee -a "$LOG"
        fi
    fi
}

run_type() {
    local T=$1
    echo "" | tee -a "$LOG"
    echo "[$(date '+%F %T')] ==== starting type=$T ====" | tee -a "$LOG"

    # Migrate legacy progress.json if it belongs to this type
    if [ -f data/progress.json ] && [ ! -f "data/progress_type${T}.json" ]; then
        OWNER=$(python3 -c "import json; print(json.load(open('data/progress.json')).get('forum_type',''))" 2>/dev/null)
        if [ "$OWNER" = "$T" ]; then
            mv data/progress.json "data/progress_type${T}.json"
            echo "[$(date '+%F %T')] migrated progress.json -> progress_type${T}.json" | tee -a "$LOG"
        fi
    fi

    # Run crawler for this type (foreground, PID tracked)
    python3 crawler.py --type "$T" --delay 2 >> "data/crawl_type${T}.stdout.log" 2>&1 &
    NEW_PID=$!
    echo $NEW_PID > data/crawler.pid
    echo "[$(date '+%F %T')] launched crawler --type $T, PID=$NEW_PID" | tee -a "$LOG"

    # Wait for this specific crawl to finish
    wait "$NEW_PID"
    RC=$?
    echo "[$(date '+%F %T')] type=$T exited with code $RC" | tee -a "$LOG"

    if [ $RC -ne 0 ] && [ $RC -ne 130 ]; then
        # 130 = SIGINT (ok); other non-zero = token expired or error
        echo "[$(date '+%F %T')] type=$T FAILED (exit $RC). Halting queue." | tee -a "$LOG"
        echo "[$(date '+%F %T')] Check data/crawl.log. If token expired, update credentials.json and re-run run_all.sh." | tee -a "$LOG"
        return $RC
    fi
    return 0
}

# Step 1: wait for type=2 (already running)
wait_for_current

# Migrate the type=2 progress file (current runner wrote to legacy path)
if [ -f data/progress.json ] && [ ! -f data/progress_type2.json ]; then
    mv data/progress.json data/progress_type2.json
    echo "[$(date '+%F %T')] migrated progress.json -> progress_type2.json" | tee -a "$LOG"
fi

# Step 2: type=3 (久谦 original research)
run_type 3 || exit $?

# Step 3: type=1 (meeting activities)
run_type 1 || exit $?

echo "" | tee -a "$LOG"
echo "[$(date '+%F %T')] ==== ALL THREE TYPES COMPLETE ====" | tee -a "$LOG"
echo "[$(date '+%F %T')] Summary:" | tee -a "$LOG"
for T in 2 3 1; do
    P="data/progress_type${T}.json"
    if [ -f "$P" ]; then
        python3 -c "
import json
p = json.load(open('$P'))
total = p.get('total_items', 0)
done = len(p.get('completed_detail_ids', []))
print(f'  type=$T: {done}/{total} details, {p.get(\"last_list_page\",0)} list pages')
" | tee -a "$LOG"
    fi
done
