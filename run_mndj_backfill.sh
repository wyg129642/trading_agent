#!/bin/bash
cd /home/ygwang/trading_agent
set -u

echo "[$(date +%H:%M:%S)] 等待 26x 扫完 (26000001-26999999) ..." >> logs/mndj_backfill.log
while pgrep -f "download_mndj_pdfs.py.*--tag 26x" > /dev/null; do
    sleep 60
done
echo "[$(date +%H:%M:%S)] 26x done, 开始低 ID 段回扫" >> logs/mndj_backfill.log

for seg in "25000001 25999999 25x" \
           "24000001 24999999 24x" \
           "23000001 23999999 23x" \
           "22000001 22999999 22x" \
           "21000001 21999999 21x" \
           "20000001 20999999 20x" \
           "18688000 19999999 18-19x"; do
    read START END TAG <<< "$seg"
    echo "[$(date +%H:%M:%S)] === tag=$TAG  start=$START  end=$END ===" >> logs/mndj_backfill.log
    python3 download_mndj_pdfs.py --start "$START" --end "$END" --tag "$TAG" --consec-404-limit 0 \
        >> logs/mndj_backfill.log 2>&1
    echo "[$(date +%H:%M:%S)] === tag=$TAG done ===" >> logs/mndj_backfill.log
done
echo "[$(date +%H:%M:%S)] ALL SEGMENTS DONE" >> logs/mndj_backfill.log
