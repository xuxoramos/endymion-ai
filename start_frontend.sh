#!/bin/bash
# Start Vite frontend dev server
# This script ensures consistent startup and logs to file

set -e  # Exit on error

cd "$(dirname "$0")/frontend"

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "❌ node_modules not found. Run: npm install"
    exit 1
fi

# Check if port 3000 is already in use
if lsof -Pi :3000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "⚠️  Port 3000 is already in use. Killing existing process..."
    lsof -ti:3000 | xargs kill -9 2>/dev/null || true
    sleep 2
fi

# Create log directory
mkdir -p /tmp

echo "🚀 Starting Vite frontend dev server..."
echo "📝 Logs: /tmp/frontend.log"
echo "🌐 Server: http://localhost:3000"
echo ""

# Start with logging
npm run dev 2>&1 | tee /tmp/frontend.log
