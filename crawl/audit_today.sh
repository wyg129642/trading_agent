#!/usr/bin/env bash
# 全平台 "今日对齐" 审计 + 回填.
#
# 每个爬虫都有自己的 `--today` 统计 (平台 vs DB). 当 watcher `--resume` top_id
# 漏抓时, 这个脚本负责精确定位缺失 IDs 并单条 force 补抓.
#
# 每 15 分钟跑一次 (cron):
#   */15 * * * * /home/ygwang/trading_agent/crawl/audit_today.sh >> /home/ygwang/trading_agent/logs/audit_today.log 2>&1
#
# 手动跑:
#   bash crawl/audit_today.sh                # 跑所有已实现的 backfill
#   bash crawl/audit_today.sh --dry-run      # 只报告缺失, 不写 DB

set -u
cd "$(dirname "$0")"

PY=/home/ygwang/miniconda3/envs/agent/bin/python
DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then DRY_RUN="--dry-run"; fi

timestamp() { date +"%Y-%m-%d %H:%M:%S"; }

run_one() {
    local name="$1"; local cwd="$2"; local script="$3"; shift 3
    local args=("$@")
    echo "━━━━ [$(timestamp)] $name ━━━━"
    if [[ ! -f "$cwd/$script" ]]; then
        echo "  [skip] $cwd/$script 不存在"
        return
    fi
    ( cd "$cwd" && $PY -u "$script" "${args[@]}" $DRY_RUN ) 2>&1 | tail -20
    echo ""
}

# AlphaPai — report / roadshow / comment (report 用 sweep-today).
# wechat 微信社媒爬取已停用 (2026-04-24) — 已入库保留, 不再做当日审计补漏.
for cat in report roadshow comment; do
    run_one "alphapai_${cat}" alphapai_crawl backfill_today_reports.py \
            --category "$cat" --skip-pdf --throttle 0.4
done

# Gangtise — summary / research / chief 全扫
for type in summary research chief; do
    run_one "gangtise_${type}" gangtise backfill_today.py \
            --type "$type" --skip-pdf --throttle 0.4
done

echo "━━━━ [$(timestamp)] 审计完成 ━━━━"
