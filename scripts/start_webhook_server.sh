#!/bin/bash
# Start GitHub Webhook Server for Code Graph Updates
#
# Usage:
#   ./scripts/start_webhook_server.sh           # Foreground
#   ./scripts/start_webhook_server.sh --daemon  # Background with nohup

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KOI_PROCESSOR="$(dirname "$SCRIPT_DIR")"

cd "$KOI_PROCESSOR"

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

# Load environment variables
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
fi

# Set Python path
export PYTHONPATH="$KOI_PROCESSOR:${PYTHONPATH:-}"

HOST="${WEBHOOK_HOST:-0.0.0.0}"
PORT="${WEBHOOK_PORT:-8360}"

# Check for daemon mode
if [ "${1:-}" = "--daemon" ]; then
    LOG_FILE="$KOI_PROCESSOR/logs/webhook_server.log"
    mkdir -p "$(dirname "$LOG_FILE")"

    echo "Starting webhook server in daemon mode..."
    echo "Logs: $LOG_FILE"

    nohup python -m uvicorn api.github_webhook:app \
        --host "$HOST" \
        --port "$PORT" \
        >> "$LOG_FILE" 2>&1 &

    echo "PID: $!"
    echo "To stop: kill $!"
else
    echo "Starting webhook server..."
    echo "Press Ctrl+C to stop"

    python -m uvicorn api.github_webhook:app \
        --host "$HOST" \
        --port "$PORT" \
        --reload
fi
