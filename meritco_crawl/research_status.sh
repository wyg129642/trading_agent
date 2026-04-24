#!/bin/bash
# Quick status check for the /research/ crawler.
set -u
D="$(dirname "$0")/data/research"

echo "=== Meritco /research/ crawler status ==="
if [ -f "$D/crawler.pid" ]; then
    PID=$(cat "$D/crawler.pid")
    if ps -p "$PID" >/dev/null 2>&1; then
        ET=$(ps -p "$PID" -o etime= | tr -d ' ')
        echo "  process:    RUNNING pid=$PID  elapsed=$ET"
    else
        echo "  process:    STOPPED (stale pid=$PID)"
    fi
else
    echo "  process:    NOT STARTED (no pid file)"
fi

LIST_PAGES=$(find "$D/lists" -name 'page_*.json' 2>/dev/null | wc -l)
LIST_MENUS=$(find "$D/lists" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
ORGS=$(ls "$D/orgs"/*.json 2>/dev/null | wc -l)
DETAILS=$(ls "$D/details"/*.json 2>/dev/null | wc -l)
PDFS=$(ls "$D/pdfs"/*.pdf 2>/dev/null | wc -l)
PDF_ERRS=$(ls "$D/pdfs"/*.err.json 2>/dev/null | wc -l)

echo ""
echo "  list menus: $LIST_MENUS / 20"
echo "  list pages: $LIST_PAGES"
echo "  orgs saved: $ORGS"
echo "  details:    $DETAILS"
echo "  pdfs:       $PDFS  (err: $PDF_ERRS)"

# approximate size
if [ -d "$D" ]; then
    SIZE=$(du -sh "$D" 2>/dev/null | awk '{print $1}')
    echo "  disk:       $SIZE"
fi

echo ""
echo "--- last 10 log lines ---"
tail -10 "$D/crawl.log" 2>/dev/null

if [ -f "$D/progress.json" ]; then
    echo ""
    echo "--- per-menu list progress ---"
    python3 -c "
import json
p = json.load(open('$D/progress.json'))
pages = p.get('completed_list_pages', {})
for mc in sorted(pages, key=lambda m: pages[m], reverse=True):
    print(f'  {mc:40s}  {pages[mc]} pages')
" 2>/dev/null
fi
