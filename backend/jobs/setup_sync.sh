#!/bin/bash

# ============================================
# Quick Setup Script for Sync Job
# ============================================
#
# Sets up and runs the Silver-to-SQL sync job.
#
# Usage:
#   ./setup_sync.sh

set -e

echo "============================================"
echo "Sync Job Setup"
echo "============================================"
echo ""

# ============================================
# Step 1: Install Dependencies
# ============================================

echo "Step 1: Installing Python dependencies..."
echo "--------------------------------------------"

pip install -q pyspark==3.5.0 delta-spark==3.0.0 sqlalchemy pyodbc

echo "✅ Dependencies installed"
echo ""

# ============================================
# Step 2: Create Database Tables
# ============================================

echo "Step 2: Creating database tables..."
echo "--------------------------------------------"
echo ""
echo "You need to create these tables in SQL Server:"
echo ""
echo "  - operational.sync_state"
echo "  - operational.sync_log"
echo "  - operational.sync_conflicts"
echo ""
echo "Table definitions are in: backend/models/sync.py"
echo ""
echo "Create tables using Alembic migration or manually."
echo ""
read -p "Press ENTER after tables are created..."
echo ""

# ============================================
# Step 3: Verify MinIO is Running
# ============================================

echo "Step 3: Verifying MinIO..."
echo "--------------------------------------------"

if curl -s http://localhost:9000/minio/health/live > /dev/null; then
    echo "✅ MinIO is running"
else
    echo "❌ MinIO is NOT running"
    echo ""
    echo "Start MinIO:"
    echo "  docker run -d -p 9000:9000 -p 9001:9001 \\"
    echo "    -e MINIO_ROOT_USER=minioadmin \\"
    echo "    -e MINIO_ROOT_PASSWORD=minioadmin \\"
    echo "    minio/minio server /data --console-address ':9001'"
    exit 1
fi
echo ""

# ============================================
# Step 4: Verify Silver Layer Exists
# ============================================

echo "Step 4: Checking Silver layer..."
echo "--------------------------------------------"
echo ""
echo "Silver layer should exist at: s3a://silver/cows_current"
echo ""
echo "If not, run Silver processing:"
echo "  cd databricks/silver"
echo "  python rebuild_cows.py"
echo ""
read -p "Press ENTER if Silver layer is ready..."
echo ""

# ============================================
# Step 5: Run First Sync
# ============================================

echo "Step 5: Running first sync..."
echo "--------------------------------------------"
echo ""

cd /home/xuxoramos/endymion-ai
python backend/jobs/sync_silver_to_sql.py

SYNC_EXIT_CODE=$?

if [ $SYNC_EXIT_CODE -ne 0 ]; then
    echo ""
    echo "❌ First sync FAILED"
    echo ""
    echo "Check the error message above."
    echo "Common issues:"
    echo "  - SQL Server not running"
    echo "  - Sync tables not created"
    echo "  - MinIO not accessible"
    echo "  - Silver layer empty"
    exit 1
fi

echo ""
echo "✅ First sync completed successfully!"
echo ""

# ============================================
# Step 6: Verify Sync Results
# ============================================

echo "Step 6: Verifying sync results..."
echo "--------------------------------------------"
echo ""
echo "Check these tables in SQL Server:"
echo ""
echo "  SELECT * FROM operational.sync_state;"
echo "  SELECT * FROM operational.sync_log ORDER BY started_at DESC;"
echo "  SELECT COUNT(*) FROM operational.cows;"
echo ""
read -p "Press ENTER after verifying results..."
echo ""

# ============================================
# Step 7: Start Scheduler
# ============================================

echo "Step 7: Starting sync scheduler..."
echo "--------------------------------------------"
echo ""
echo "The scheduler will run sync every 30 seconds."
echo ""
echo "To stop: Ctrl+C or kill -SIGTERM <pid>"
echo ""
read -p "Press ENTER to start scheduler..."
echo ""

python backend/jobs/sync_scheduler.py

# ============================================
# Done
# ============================================

echo ""
echo "============================================"
echo "Setup Complete!"
echo "============================================"
