#!/bin/bash
# Run any Python script with the correct virtual environment
# Usage: ./scripts/run-python.sh path/to/script.py [args...]

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${PROJECT_ROOT}/.venv/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Virtual environment not found at: ${PROJECT_ROOT}/.venv"
    exit 1
fi

cd "$PROJECT_ROOT"
exec "$VENV_PYTHON" "$@"
