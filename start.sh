#!/bin/bash
# AI Trading Agent — Start Script
# Usage:
#   ./start.sh              — Run in foreground (Ctrl+C to stop gracefully)
#   ./start.sh daemon       — Run as background daemon
#   ./start.sh test         — Test 1 source (default)
#   ./start.sh test 3      — Test 3 sources
#   ./start.sh test all    — Test all sources
#   ./start.sh status       — Show database statistics
#   ./start.sh tokens       — Show token usage and costs (today)
#   ./start.sh tokens 7     — Show token usage for last 7 days
#   ./start.sh stop         — Stop the background daemon
#  python run.py --test-mode deprecated function 
cd "$(dirname "$0")"
mkdir -p logs

# Set proxy for international sites (engine uses trust_env to route selectively)
export HTTP_PROXY="http://127.0.0.1:7890"
export HTTPS_PROXY="http://127.0.0.1:7890"
export ALL_PROXY="http://127.0.0.1:7890"
export NO_PROXY="localhost,127.0.0.1,.local"

case "${1:-run}" in
    run)
        echo "Starting AI Trading Agent (foreground)..."
        echo "Press Ctrl+C to stop gracefully."
        python run.py
        ;;
    daemon)
        if [ -f .agent.pid ] && kill -0 "$(cat .agent.pid)" 2>/dev/null; then
            echo "Agent already running (PID $(cat .agent.pid)). Stop first with: ./start.sh stop"
            exit 1
        fi
        echo "Starting AI Trading Agent as daemon..."
        nohup python run.py > logs/daemon.log 2>&1 &
        echo $! > .agent.pid
        echo "PID: $(cat .agent.pid)"
        echo "Log: logs/daemon.log"
        echo "  tail -f logs/daemon.log  — watch live output"
        echo "  ./start.sh stop          — stop the daemon"
        echo "  ./start.sh tokens        — check token usage"
        ;;
    test)
        # Usage: ./start.sh test         (test 1 source)
        #        ./start.sh test 3       (test 3 sources)
        #        ./start.sh test --all   (test all sources)
        TEST_ARG="${2:-1}"
        if [ "$TEST_ARG" = "--all" ] || [ "$TEST_ARG" = "all" ]; then
            python run.py --test all
        else
            python run.py --test "$TEST_ARG"
        fi
        ;;
    status)
        python run.py --status
        ;;
    tokens)
        DAYS="${2:-1}"
        python run.py --tokens --days "$DAYS"
        ;;
    stop)
        if [ -f .agent.pid ]; then
            PID=$(cat .agent.pid)
            if kill -0 "$PID" 2>/dev/null; then
                echo "Sending SIGTERM to PID $PID (graceful shutdown)..."
                kill "$PID"
                # Wait up to 15 seconds for graceful shutdown
                for i in $(seq 1 15); do
                    if ! kill -0 "$PID" 2>/dev/null; then
                        echo "Stopped gracefully."
                        rm -f .agent.pid
                        exit 0
                    fi
                    sleep 1
                done
                echo "Process still running after 15s, sending SIGKILL..."
                kill -9 "$PID" 2>/dev/null
                rm -f .agent.pid
                echo "Force-stopped."
            else
                echo "Process $PID not running."
                rm -f .agent.pid
            fi
        else
            echo "No daemon PID file found. Trying to find by process name..."
            PIDS=$(pgrep -f "python run.py" 2>/dev/null)
            if [ -n "$PIDS" ]; then
                echo "Found process(es): $PIDS"
                kill $PIDS 2>/dev/null
                echo "Sent SIGTERM."
            else
                echo "No running agent found."
            fi
        fi
        ;;
    *)
        echo "AI Trading Agent — Commands:"
        echo "  ./start.sh              Run in foreground"
        echo "  ./start.sh daemon       Run as background daemon"
        echo "  ./start.sh test         Test 1 source (default)"
        echo "  ./start.sh test 3       Test 3 sources"
        echo "  ./start.sh test all     Test all sources"
        echo "  ./start.sh status       Database statistics"
        echo "  ./start.sh tokens [N]   Token usage (last N days, default 1)"
        echo "  ./start.sh stop         Stop background daemon"
        exit 1
        ;;
esac
