#!/usr/bin/env bash
# daily_catchup.sh — run once a day (cron) to catch items the realtime watchers missed.
#
# Why: the realtime watchers (--watch --resume --since-hours 24 --interval 60) can
# miss items when a publication burst pushes unseen items past page 1 before the
# watcher scans. Once an item ages out of the 24h --since-hours window without being
# ingested, it is never seen again. This script does a forced re-sweep of the last
# 36h (so yesterday is fully covered) for every scraper configured in ALL_SCRAPERS.
#
# Usage:
#   bash crawl/daily_catchup.sh            # run full catchup (all platforms)
#   bash crawl/daily_catchup.sh <platform> # run only one platform (alphapai/jinmen/...)
#
# Suggested cron (05:30 CST, after brokerage overnight publish window ends):
#   30 5 * * * cd /home/ygwang/trading_agent && bash crawl/daily_catchup.sh \
#     >> logs/daily_catchup_$(date +\%Y\%m\%d).log 2>&1
#
# The script is idempotent — running twice only re-writes existing docs via --force.

set -u
cd "$(dirname "$0")"
PY=/home/ygwang/miniconda3/envs/agent/bin/python
LOG_DIR="../logs/daily_catchup"
mkdir -p "$LOG_DIR"
STAMP=$(date +%Y%m%d_%H%M%S)
PLATFORM_FILTER="${1:-all}"

# Common args for catchup mode: 36h window, force re-ingest, no --watch/--resume
# (--resume early-stops at top_dedup_id which is exactly what we want to bypass)
COMMON="--since-hours 36 --force"

# Each row: "platform_dir | platform_filter_key | extra_args | max_items | log_name"
declare -a ROWS=(
  "alphapai_crawl|alphapai|--category roadshow --page-size 100|300|alphapai_roadshow"
  "alphapai_crawl|alphapai|--category comment  --page-size 100|700|alphapai_comment"
  "alphapai_crawl|alphapai|--category report   --page-size 100|400|alphapai_report"
  # wechat 微信社媒爬取已停用 (2026-04-24) — 已入库保留, 不再回填.
  # "alphapai_crawl|alphapai|--category wechat   --page-size 100|800|alphapai_wechat"
  "jinmen|jinmen|--page-size 50|400|jinmen_meetings"
  "jinmen|jinmen|--reports --page-size 50|500|jinmen_reports"
  "jinmen|jinmen|--oversea-reports --page-size 50|200|jinmen_oversea"
  "meritco_crawl|meritco|--type 2,3|200|meritco_forum"
  "third_bridge|thirdbridge||100|thirdbridge"
  "funda|funda|--category post|100|funda_post"
  "funda|funda|--category earnings_report|100|funda_earnings_report"
  "funda|funda|--category earnings_transcript|100|funda_earnings_transcript"
  "gangtise|gangtise|--type summary --skip-pdf --page-size 50|400|gangtise_summary"
  "gangtise|gangtise|--type research --skip-pdf --page-size 50|500|gangtise_research"
  "gangtise|gangtise|--type chief --skip-pdf --page-size 50|200|gangtise_chief"
  # AceCamp (2026-04-24 封控事故后调整):
  # - events: 已被平台移除, 不再抓
  # - opinions: 走 opinion_info 独立端点不吃 article quota 池, 保留 detail
  # - 日 catchup 上限下调 (200→120 / 200→80), 给其它路径留 quota
  # 2026-04-28 (二次): 用户要求 1/10 速率重启, max 也×1/10 (120→12 / 80→8).
  # 2026-04-29: --skip-detail 移除. 之前的 list-only 写库会把付费内容的"提纲式
  #   summary" 当 content_md 入库, StockHub 上一堆"信息不全"的截断卡 (用户反馈
  #   案例: "黄金再次新高的逻辑及后市展望"). dump_article 在 skip_detail 路径
  #   现在硬不写, 这里同步去掉 flag 让 catchup 走完整 detail 路径; quota 烧光
  #   有 SoftCooldown (10003/10040 自动 30min 静默) + tripwire (15 连空抛
  #   SessionDead, scraper 退出) 兜住.
  "AceCamp|acecamp|--type articles|12|acecamp_articles"
  "AceCamp|acecamp|--type opinions|8|acecamp_opinions"
  "alphaengine|alphaengine|--category all|300|alphaengine_all"
)

launched=()
skipped=()
for row in "${ROWS[@]}"; do
  IFS='|' read -r dir plat extra maxn log_name <<< "$row"
  if [[ "$PLATFORM_FILTER" != "all" && "$PLATFORM_FILTER" != "$plat" ]]; then
    skipped+=("$log_name")
    continue
  fi
  if [[ ! -d "$dir" ]]; then
    echo "[$(date +%H:%M:%S)] SKIP $log_name (dir missing)"
    continue
  fi
  # 平台级停爬闸门: crawl/<dir>/DISABLED 文件存在就跳过 (用户主动关闭该平台).
  if [[ -f "$dir/DISABLED" ]]; then
    echo "[$(date +%H:%M:%S)] SKIP $log_name ($dir/DISABLED exists)"
    continue
  fi
  LOG="$LOG_DIR/${log_name}_${STAMP}.log"
  (
    cd "$dir" || exit 1
    # shellcheck disable=SC2086
    exec $PY scraper.py $COMMON --max "$maxn" $extra
  ) >"$LOG" 2>&1 &
  pid=$!
  launched+=("$log_name (pid=$pid, log=$LOG)")
  echo "[$(date +%H:%M:%S)] LAUNCHED $log_name → pid $pid"
done

echo ""
echo "=== launched ${#launched[@]} catchup jobs ==="
printf '  %s\n' "${launched[@]}"
if (( ${#skipped[@]} )); then
  echo "=== skipped ${#skipped[@]} (platform filter) ==="
fi

# Wait for all to finish
wait
echo ""
echo "=== all catchup jobs finished at $(date) ==="

# Summarize each log
for row in "${ROWS[@]}"; do
  IFS='|' read -r dir plat extra maxn log_name <<< "$row"
  if [[ "$PLATFORM_FILTER" != "all" && "$PLATFORM_FILTER" != "$plat" ]]; then continue; fi
  LOG="$LOG_DIR/${log_name}_${STAMP}.log"
  [[ -f "$LOG" ]] || continue
  # Extract the final result line(s) for each scraper
  summary=$(grep -E "完成.*新增|本轮汇总|当前.*总数|抓取完成|写入成功|最终统计|Error: |SessionDead" "$LOG" 2>/dev/null | tail -3 | tr '\n' ' | ')
  echo "  [$log_name] $summary"
done

# ── 孤儿标记 (crawl/flag_orphans.py) ─────────────────────────────────────
# 每日 catchup 结束后扫一遍 "DB 今日, 平台 today 列表却看不到" 的孤儿条目,
# 打 `_orphan=True`. 后端 /api/platform-info/.../daily-counts 会按
# `_orphan: {$ne: True}` 排除. 采用 2 次连续确认, 第一轮是 candidate,
# 第二轮才真正标记 — 避免平台临时抽风误杀.
#
# 仅对已实现的平台跑 (见 flag_orphans.py::IMPLEMENTED).
echo ""
echo "=== orphan flagging @ $(date +%H:%M:%S) ==="
for plat in gangtise jinmen alphapai; do
  if [[ "$PLATFORM_FILTER" != "all" && "$PLATFORM_FILTER" != "$plat" ]]; then
    continue
  fi
  LOG="$LOG_DIR/orphan_${plat}_${STAMP}.log"
  # Round 1: 标为 candidate (候选)
  $PY flag_orphans.py --platform "$plat" --apply >>"$LOG" 2>&1 || true
  # 间隔 30s, 给平台列表波动一个缓冲
  sleep 30
  # Round 2: 仍然是孤儿 → 落 _orphan=True
  $PY flag_orphans.py --platform "$plat" --apply >>"$LOG" 2>&1 || true
  summary=$(grep -E "newly_flagged|orphan=" "$LOG" 2>/dev/null | tail -6 | tr '\n' ' | ')
  echo "  [orphan/$plat] $summary"
done
