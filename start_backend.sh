#!/bin/bash
# Start FastAPI backend with correct module path
# This script ensures consistent startup and prevents module path errors

set -e  # Exit on error

cd "$(dirname "$0")"

# Activate virtual environment
if [ ! -d ".venv" ]; then
    echo "❌ Virtual environment not found. Run: python3 -m venv .venv && pip install -r requirements.txt"
    exit 1
fi

source .venv/bin/activate

# Check if port 8000 is already in use
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "⚠️  Port 8000 is already in use. Killing existing process..."
    lsof -ti:8000 | xargs kill -9 2>/dev/null || true
    sleep 2
fi

# Ensure analytics schema exists (idempotent)
echo "🔍 Checking analytics schema..."
if docker ps | grep -q cattlesaas-sqlserver; then
    cat "$(dirname "$0")/databricks/gold/sql/create_analytics_schema.sql" | \
        docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas >/dev/null 2>&1 && \
        echo "✅ Analytics schema ready" || \
        echo "⚠️  Could not verify analytics schema (continuing anyway)"
else
    echo "⚠️  SQL Server container not running - analytics queries may fail"
fi

# Create log directory
mkdir -p /tmp

echo "🚀 Starting FastAPI backend..."
echo "📝 Logs: /tmp/fastapi_debug.log"
echo "🌐 Server: http://localhost:8000"
echo "📚 API Docs: http://localhost:8000/docs"
echo ""

# Start with correct module path: backend.api.main:app (not backend.main:app)
.venv/bin/uvicorn backend.api.main:app \
    --reload \
    --host 0.0.0.0 \
    --port 8000 \
    2>&1 | tee /tmp/fastapi_debug.log
