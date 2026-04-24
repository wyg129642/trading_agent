#!/bin/bash
# 串行执行：补 26mid → 补 default → run_mndj_backfill.sh
# pdf_full/ 里已存在的文件会被自动跳过（不重复），
# progress JSON 保证各 tag 断点续传（不漏）。
cd /home/ygwang/trading_agent
set -u

LOG="logs/mndj_backfill.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG"; }

log "================ run_mndj_all START ================"

log "--- phase 1/3: resume 26mid (start=26717251 end=27000000) ---"
python3 download_mndj_pdfs.py --start 26717251 --end 27000000 --tag 26mid --consec-404-limit 0 \
    >> "$LOG" 2>&1
log "--- phase 1/3: 26mid done ---"

log "--- phase 2/3: resume default (start=27000001 end=27065000) ---"
python3 download_mndj_pdfs.py --start 27000001 --end 27065000 --tag default --consec-404-limit 0 \
    >> "$LOG" 2>&1
log "--- phase 2/3: default done ---"

log "--- phase 3/3: run_mndj_backfill.sh (25x → 24x → ... → 18-19x) ---"
bash run_mndj_backfill.sh
log "--- phase 3/3: backfill done ---"

log "================ run_mndj_all END ================"
