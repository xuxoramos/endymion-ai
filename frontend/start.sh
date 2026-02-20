#!/bin/bash

# Endymion-AI Frontend - Quick Start Script

set -e

echo "🐄 Endymion-AI Frontend Setup"
echo "=============================="
echo ""

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed"
    echo "   Please install Node.js 18+ from https://nodejs.org/"
    exit 1
fi

NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 18 ]; then
    echo "❌ Node.js version must be 18 or higher"
    echo "   Current version: $(node -v)"
    exit 1
fi

echo "✓ Node.js $(node -v) detected"
echo ""

# Check if backend is running
echo "Checking backend API..."
if curl -s http://localhost:8000/docs > /dev/null; then
    echo "✓ Backend API is running at http://localhost:8000"
else
    echo "⚠️  Backend API is not running at http://localhost:8000"
    echo "   Start it with: python backend/api/main.py"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
    echo "✓ Dependencies installed"
    echo ""
fi

# Start dev server
echo "🚀 Starting frontend dev server..."
echo ""
echo "The app will be available at:"
echo "   http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

npm run dev
