#!/bin/bash
# Quick status check for the meritco crawler.
cd "$(dirname "$0")"

if [ -f data/crawler.pid ]; then
    PID=$(cat data/crawler.pid)
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "RUNNING: PID=$PID, elapsed=$(ps -p $PID -o etime= | tr -d ' ')"
    else
        echo "NOT RUNNING (stale PID file: $PID)"
    fi
else
    echo "NO PID FILE"
fi

echo ""
echo "--- progress ---"
if [ -f data/progress.json ]; then
    python3 -c "
import json
p = json.load(open('data/progress.json'))
total = p.get('total_items', 0)
done = len(p.get('completed_detail_ids', []))
pct = 100 * done / total if total else 0
print(f'  type         = {p.get(\"forum_type\")}')
print(f'  list pages   = {p.get(\"last_list_page\", 0)}')
print(f'  details      = {done}/{total} ({pct:.1f}%)')
print(f'  started      = {p.get(\"started_at\", \"\")[:19]}')
print(f'  last update  = {p.get(\"last_update\", \"\")[:19]}')
"
fi

echo ""
echo "--- disk usage ---"
du -sh data/lists data/details 2>/dev/null

echo ""
echo "--- last 5 log lines ---"
tail -5 data/crawl.log 2>/dev/null
