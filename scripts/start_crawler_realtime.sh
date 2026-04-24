#!/bin/bash
# 一键启动爬虫实时监控
#
# 功能:
#   1. 启动 crawler_monitor.py (web :8080, 飞书监听 + 健康告警)
#   2. 通过 /api/start-all?mode=realtime 拉起所有平台的 watcher
#
# 用法:
#   bash scripts/start_crawler_realtime.sh          # 启动
#   bash scripts/start_crawler_realtime.sh --stop   # 停止所有 watcher + monitor
#   bash scripts/start_crawler_realtime.sh --status # 查看状态
#
# 服务器重启后直接跑此脚本即可; 会自动清理残留 PID 重新拉起。

set -euo pipefail

REPO=/home/ygwang/trading_agent
PY=/home/ygwang/miniconda3/envs/agent/bin/python3
MON_LOG=$REPO/logs/crawler_monitor.log
MON_PID=$REPO/logs/crawler_monitor.pid
PORT=8080

# Clash 代理会拦截 127.0.0.1, 本地 curl 必须 --noproxy '*'
CURL="curl -s --noproxy *"

log() { echo "[$(date +%H:%M:%S)] $*"; }
err() { echo "[$(date +%H:%M:%S)] ERROR: $*" >&2; }

cmd_status() {
    log "=== 实时监控状态 ==="
    if [ -f "$MON_PID" ] && kill -0 "$(cat $MON_PID)" 2>/dev/null; then
        log "monitor: 运行中 PID=$(cat $MON_PID)"
    else
        log "monitor: 未运行"
    fi
    local rt=$(ps -ef | grep "scraper\.py --watch" | grep -v grep | wc -l)
    local bf=$(ps -ef | grep "scraper\.py" | grep -v grep | grep -v -- "--watch" | wc -l)
    log "watcher (realtime): $rt 进程"
    log "backfill (一次性): $bf 进程"
    log "web 看板: http://127.0.0.1:$PORT"
}

cmd_stop() {
    log "=== 停止实时监控 ==="
    # 1. 先 kill watcher (所有带 --watch 的 scraper, 不动 backfill)
    local pids=$(ps -ef | grep "scraper\.py --watch" | grep -v grep | awk '{print $2}')
    if [ -n "$pids" ]; then
        log "kill watcher PIDs: $pids"
        echo "$pids" | xargs -r kill -TERM 2>/dev/null || true
        sleep 2
        echo "$pids" | xargs -r kill -KILL 2>/dev/null || true
    fi
    # 2. kill monitor
    if [ -f "$MON_PID" ]; then
        local mp=$(cat $MON_PID)
        if kill -0 "$mp" 2>/dev/null; then
            log "kill monitor PID=$mp"
            kill -TERM $mp 2>/dev/null || true
            sleep 2
            kill -KILL $mp 2>/dev/null || true
        fi
        rm -f $MON_PID
    fi
    # 还有些 monitor 可能没 PID 文件, 按命令行兜底
    local stray=$(ps -ef | grep "crawler_monitor\.py" | grep -v grep | awk '{print $2}')
    [ -n "$stray" ] && { log "kill stray monitor: $stray"; echo "$stray" | xargs -r kill -KILL 2>/dev/null || true; }
    log "已停止"
}

cmd_start() {
    log "=== 启动实时监控 ==="

    # 0. 检查 docker MongoDB 是否在跑 (crawl_data 容器)
    if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^crawl_data$'; then
        err "docker 容器 crawl_data 没在跑! 先执行: docker start crawl_data"
        err "或参考 CRAWLERS.md §11 新建容器"
        exit 1
    fi
    log "✓ MongoDB (crawl_data) 容器就绪"

    mkdir -p $REPO/logs

    # 1. 如有旧 monitor 先 kill 干净 (避免端口冲突)
    if [ -f "$MON_PID" ]; then
        local mp=$(cat $MON_PID)
        if kill -0 "$mp" 2>/dev/null; then
            log "kill 旧 monitor PID=$mp"
            kill -TERM $mp 2>/dev/null || true
            sleep 2
            kill -KILL $mp 2>/dev/null || true
        fi
    fi
    # 兜底按命令行 kill
    local stray=$(ps -ef | grep "crawler_monitor\.py" | grep -v grep | awk '{print $2}')
    [ -n "$stray" ] && echo "$stray" | xargs -r kill -KILL 2>/dev/null || true

    # 2. 启动 monitor (nohup + disown)
    log "启动 crawler_monitor.py (port $PORT)"
    cd $REPO/crawl
    nohup $PY -u crawler_monitor.py --web --port $PORT \
        >> $MON_LOG 2>&1 &
    local new_pid=$!
    disown
    echo $new_pid > $MON_PID
    log "monitor PID=$new_pid"

    # 3. 等待 HTTP 就绪 (最多 20s)
    local ok=0
    for i in $(seq 1 20); do
        if $CURL -o /dev/null -w "%{http_code}" http://127.0.0.1:$PORT/ 2>/dev/null | grep -q "^200$"; then
            ok=1; break
        fi
        sleep 1
    done
    [ $ok -eq 1 ] || { err "monitor HTTP 20s 内没响应, 查看 $MON_LOG"; exit 1; }
    log "✓ HTTP 就绪"

    # 4. 触发 start-all realtime (kill 旧 watcher + spawn 新 watcher)
    log "POST /api/start-all?mode=realtime"
    local resp=$($CURL -X POST "http://127.0.0.1:$PORT/api/start-all?mode=realtime" || echo '{}')
    echo "  $resp" | head -c 200; echo ""

    # 5. 等 3s 让 watcher 稳住再统计
    sleep 3
    local n=$(ps -ef | grep "scraper\.py --watch" | grep -v grep | wc -l)
    log "✓ 已启动 $n 个 watcher"
    log ""
    log "浏览器打开: http://127.0.0.1:$PORT"
    log "查看进度:  tail -f $MON_LOG"
    log "停止:     bash scripts/start_crawler_realtime.sh --stop"
}

case "${1:-}" in
    --stop)   cmd_stop ;;
    --status) cmd_status ;;
    *)        cmd_start ;;
esac
