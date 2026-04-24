#!/bin/bash
# 一屏显示 mndj 下载进度，推荐：watch -n 10 bash mndj_status.sh
cd /home/ygwang/trading_agent/jinmen-full-pdf-mndj-report

echo "=== $(date '+%Y-%m-%d %H:%M:%S') ==="
echo

echo "[running process]"
pgrep -af 'download_mndj_pdfs\.py|run_mndj_all\.sh|run_mndj_backfill\.sh' 2>/dev/null \
    | awk '{$2=$2; print "  " $0}' \
    | head -20
if ! pgrep -f 'download_mndj_pdfs\.py' >/dev/null 2>&1; then
    echo "  (no python downloader running)"
fi
echo

echo "[progress files]  tag                  last_scanned    downloaded    skipped_404"
for f in _progress_*.json; do
    [ -f "$f" ] || continue
    tag="${f#_progress_}"; tag="${tag%.json}"
    python3 - "$f" "$tag" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))
tag = sys.argv[2]
print(f"  {tag:<20} {p['last_scanned_id']:>12}   {p.get('downloaded',0):>10}   {p.get('skipped_404',0):>10}")
PY
done
echo

echo "[pdf_full @ /mnt/share/ygwang/pdf_full]"
CNT=$(ls /mnt/share/ygwang/pdf_full 2>/dev/null | wc -l)
SIZE=$(du -sh /mnt/share/ygwang/pdf_full 2>/dev/null | cut -f1)
echo "  files: $CNT    size: $SIZE"
# 迁移过渡期：顺带汇报残留本地量
LEGACY_CNT=$(ls /home/ygwang/crawl_data/pdf_full 2>/dev/null | wc -l)
echo "  legacy(/home/ygwang/crawl_data/pdf_full): $LEGACY_CNT files"
echo

echo "[latest log (tqdm stripped)]"
tail -c 6000 /home/ygwang/trading_agent/logs/mndj_backfill.log 2>/dev/null \
    | tr '\r' '\n' \
    | grep -v '^$' \
    | tail -15 \
    | sed 's/^/  /'
