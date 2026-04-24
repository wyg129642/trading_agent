#!/usr/bin/env bash
# orphan_hourly.sh — 每小时跑一次 flag_orphans.py.
# 配合 daily_catchup.sh 的每日两轮, 覆盖"工作日持续产生的新 orphan"场景
# (gangtise chief 下午每小时产生 ~10-20 个幽灵条目).
#
# 策略:
#   每小时跑一轮 apply → candidate_count +=1
#   相邻两小时扫到的同一 orphan 会使 candidate_count >= 2 → 升级为 _orphan=True
#   返回到平台可见的条目会: 第一次扫不到 → candidate_count=1 (未升级), 下一次
#   扫到 → 依然有 candidate=1 (本轮未+1). 无负作用.
#
# Suggested cron (每小时的 15 分):
#   15 * * * * cd /home/ygwang/trading_agent && bash crawl/orphan_hourly.sh \
#     >> logs/orphan_hourly_$(date +\%Y\%m\%d).log 2>&1
set -u
cd "$(dirname "$0")"
PY=/home/ygwang/miniconda3/envs/agent/bin/python
LOG_DIR="../logs/orphan_hourly"
mkdir -p "$LOG_DIR"
STAMP=$(date +%Y%m%d_%H)

for plat in gangtise jinmen; do
  LOG="$LOG_DIR/${plat}_${STAMP}.log"
  echo "[$(date +%H:%M:%S)] scanning $plat orphans..."
  $PY flag_orphans.py --platform "$plat" --apply >>"$LOG" 2>&1 || true
  summary=$(grep -E "newly_flagged|orphan=" "$LOG" 2>/dev/null | tail -6 | tr '\n' ' | ')
  echo "  [$plat] $summary"
done
