#!/bin/bash
# Start backend with correct virtual environment
# This script ensures the venv is always used correctly

set -e  # Exit on error

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${PROJECT_ROOT}/.venv/bin/python"
VENV_UVICORN="${PROJECT_ROOT}/.venv/bin/uvicorn"

# Verify venv exists
if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Virtual environment not found at: ${PROJECT_ROOT}/.venv"
    echo "   Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi

# Kill existing backend
pkill -f "uvicorn backend.api.main" || echo "No existing backend process found"
sleep 1

# Start backend using venv's uvicorn
cd "$PROJECT_ROOT"
echo "Starting backend using: $VENV_UVICORN"
nohup "$VENV_UVICORN" backend.api.main:app --host 0.0.0.0 --port 8000 > /tmp/fastapi.log 2>&1 &

echo "✓ Backend started (PID: $!)"
echo "  Logs: tail -f /tmp/fastapi.log"
