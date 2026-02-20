#!/bin/bash
# Start both backend and frontend in background
# Logs are saved to /tmp/fastapi_debug.log and /tmp/frontend.log

set -e  # Exit on error

cd "$(dirname "$0")"

echo "🐄 Starting Endymion-AI Platform..."
echo ""

# Check Docker services
echo "🐳 Checking Docker services..."
if ! docker ps | grep -q endymion-ai; then
    echo "⚠️  Docker containers not running. Starting..."
    (cd docker && docker-compose up -d)
    sleep 5
fi

# Start backend in background
echo "🚀 Starting backend (FastAPI)..."
./start_backend.sh &
BACKEND_PID=$!
sleep 10

# Check if backend started successfully
if ! lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "❌ Backend failed to start. Check /tmp/fastapi_debug.log"
    exit 1
fi
echo "✅ Backend running on http://localhost:8000"

# Start frontend in background
echo "🚀 Starting frontend (Vite)..."
./start_frontend.sh &
FRONTEND_PID=$!
sleep 8

# Check if frontend started successfully
if ! lsof -Pi :3000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "❌ Frontend failed to start. Check /tmp/frontend.log"
    kill $BACKEND_PID 2>/dev/null || true
    exit 1
fi
echo "✅ Frontend running on http://localhost:3000"

echo ""
echo "✅ All services started successfully!"
echo ""
echo "📊 Access the application:"
echo "   - Frontend: http://localhost:3000"
echo "   - Backend API: http://localhost:8000"
echo "   - API Docs: http://localhost:8000/docs"
echo ""
echo "📝 Logs:"
echo "   - Backend: tail -f /tmp/fastapi_debug.log"
echo "   - Frontend: tail -f /tmp/frontend.log"
echo ""
echo "🛑 To stop: pkill -f 'uvicorn|vite'"
