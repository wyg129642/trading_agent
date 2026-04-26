#!/bin/bash
# 一键启动 1 年历史数据回填
#
# 覆盖 7 个平台 (third_bridge token 过期, 跳过):
#   · alphapai (4 分类 serial)      · jinmen (纪要 / 国内研报 / 外资研报 3 进程)
#   · meritco (type 2+3 合并)        · funda (3 分类 serial)
#   · gangtise (3 分类 serial)       · acecamp (articles+events serial, skip-detail)
#
# 每个进程:
#   --since-hours 8760  (1 年 = 365×24)
#   --throttle-base 5 --throttle-jitter 3     (慢节流, 避免冲击 watcher)
#   --burst-size 0 --daily-cap 0              (不拦截, 一跑到底)
#   --skip-pdf                                  (PDF 太大, backfill 阶段不下)
#   NOT --watch / NOT --resume                  (一次性扫, 不停在 checkpoint)
#
# 用法:
#   bash scripts/start_crawler_backfill_1year.sh         # 启动 (若已在跑会报错退出)
#   bash scripts/start_crawler_backfill_1year.sh --force # kill 现有 backfill 再启
#   bash scripts/start_crawler_backfill_1year.sh --stop  # 只 kill 不重启
#   bash scripts/start_crawler_backfill_1year.sh --status
#
# 日志: logs/weekend_backfill/*.log (crawler_monitor 看板自动识别该路径)

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY=/home/ygwang/miniconda3/envs/agent/bin/python3
LOGS=$REPO/logs/weekend_backfill

SINCE_HOURS=8760  # 1 年
COMMON=(--since-hours $SINCE_HOURS
        --throttle-base 5 --throttle-jitter 3
        --burst-size 0 --daily-cap 0)

log() { echo "[$(date +%H:%M:%S)] $*"; }
err() { echo "[$(date +%H:%M:%S)] ERROR: $*" >&2; }

# 所有 backfill 任务定义 (逐条含: 名字, cwd 子目录, scraper 额外参数, 日志名)
# 日志名要对齐 crawler_monitor.py 的 _BACKFILL_LOG_MAP, 看板才能自动选它
TASKS=(
    "alphapai|alphapai_crawl|--category all --skip-pdf|alphapai.log"
    "jinmen_meetings|jinmen||jinmen.log"
    "jinmen_reports|jinmen|--reports --skip-pdf|jinmen_reports.log"
    "jinmen_oversea|jinmen|--oversea-reports --skip-pdf|jinmen_oversea_reports.log"
    "meritco|meritco_crawl|--type 2,3 --skip-pdf|meritco.log"
    "funda|funda|--category all|funda.log"
    "gangtise|gangtise|--type all --skip-pdf|gangtise_summary.log"
    "acecamp|AceCamp|--type all --skip-detail|acecamp.log"
)

count_backfill() {
    ps -ef | grep "scraper\.py" | grep -v grep | grep -v -- "--watch" | wc -l
}

list_backfill_pids() {
    ps -ef | grep "scraper\.py" | grep -v grep | grep -v -- "--watch" | awk '{print $2}'
}

cmd_status() {
    log "=== backfill 状态 ==="
    local n=$(count_backfill)
    log "backfill 进程: $n"
    if [ $n -gt 0 ]; then
        ps -eo pid,etime,args | grep "scraper\.py" | grep -v grep | grep -v -- "--watch" \
            | awk '{printf "  PID=%-7s  etime=%-10s  %s\n", $1, $2, substr($0, index($0,"scraper"))}'
    fi
    if [ -d $LOGS ]; then
        log ""
        log "=== 日志大小 ==="
        ls -la --time-style=+"%H:%M:%S" $LOGS 2>/dev/null | grep "\.log$" || true
    fi
}

cmd_stop() {
    log "=== 停止 backfill ==="
    local pids=$(list_backfill_pids)
    if [ -z "$pids" ]; then
        log "没有 backfill 在跑"
        return
    fi
    log "kill backfill PIDs: $pids"
    echo "$pids" | xargs -r kill -TERM 2>/dev/null || true
    sleep 2
    echo "$pids" | xargs -r kill -KILL 2>/dev/null || true
    log "已停止"
}

cmd_start() {
    local force=0
    [ "${1:-}" = "--force" ] && force=1

    log "=== 启动 1 年回填 (since-hours=$SINCE_HOURS) ==="

    # 0. Mongo 已迁远程 (2026-04-23), 本地 crawl_data 容器不再需要;
    #    依赖 ta-postgres-dev / ta-redis-dev 由 start_web.sh infra 托管,不在此检查。

    # 1. 检查现有 backfill
    local existing=$(count_backfill)
    if [ $existing -gt 0 ]; then
        if [ $force -eq 1 ]; then
            log "现有 $existing 个 backfill, --force 先 kill"
            cmd_stop
            sleep 1
        else
            err "已经有 $existing 个 backfill 在跑, 用 --force 重启或 --stop 停止"
            cmd_status
            exit 1
        fi
    fi

    mkdir -p $LOGS

    # 2. 启动所有任务
    log "启动 ${#TASKS[@]} 个 backfill 进程"
    for task in "${TASKS[@]}"; do
        IFS='|' read -r name subdir extra_args logname <<< "$task"
        local cwd=$REPO/crawl/$subdir
        local logfile=$LOGS/$logname
        # shellcheck disable=SC2086
        (cd $cwd && nohup $PY -u scraper.py \
            $extra_args "${COMMON[@]}" \
            > $logfile 2>&1 & disown)
        log "  ✓ $name  (log: $logname)"
    done

    # 3. 等 3s + 统计
    sleep 3
    local n=$(count_backfill)
    log ""
    log "=== 启动完成 ==="
    log "backfill 进程数: $n / ${#TASKS[@]}"
    if [ $n -lt ${#TASKS[@]} ]; then
        err "有 $((${#TASKS[@]} - n)) 个进程启动失败, 检查日志:"
        ls -la $LOGS 2>/dev/null
    fi
    log ""
    log "看板:   http://127.0.0.1:8080"
    log "日志:   tail -f $LOGS/*.log"
    log "停止:   bash scripts/start_crawler_backfill_1year.sh --stop"
}

case "${1:-}" in
    --stop)   cmd_stop ;;
    --status) cmd_status ;;
    --force)  cmd_start --force ;;
    *)        cmd_start ;;
esac
