#!/bin/bash
# Continuous burst — keep parsing until backlog drains. One shard per process.
# Holds the cron lock so */30 cron skips while we're running.
set -e
cd /home/ygwang/trading_agent_staging
export PATH=/home/ygwang/jdk17/bin:/usr/bin:/bin
export PYTHONPATH=.
PY=/home/ygwang/miniconda3/envs/agent/bin/python3

ROLE="$1"   # "main" or "jinmen_shard_$N" with $2=N
shift

if [ "$ROLE" = "main" ]; then
    while true; do
        $PY -u scripts/extract_pdf_texts.py --workers 16 --batch-size 32 || true
        # Stop if no targets have backlog (cheap exit when nothing to do)
        sleep 5
    done
elif [ "$ROLE" = "shard" ]; then
    SHARD=$1
    while true; do
        $PY -u scripts/extract_pdf_texts.py \
            --platform jinmen --collection oversea_reports \
            --batch-size 32 --workers 1 \
            --id-mod 8 --id-rem $SHARD || true
        sleep 2
    done
fi
