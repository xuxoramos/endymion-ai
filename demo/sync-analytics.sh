#!/bin/bash
# Continuous Analytics Pipeline Sync Service
# Runs Bronze → Silver → Gold processing every 30 seconds
# This ensures weight updates and other events flow through analytics layers

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="${LOG_FILE:-/tmp/sync-analytics.log}"
SYNC_INTERVAL="${SYNC_INTERVAL:-10}"  # Seconds between sync cycles (reduced for faster weight updates)

# Use virtual environment Python
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Virtual environment not found at: $PROJECT_ROOT/.venv"
    echo "   Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

run_bronze_sync() {
    log "🔵 Checking for unpublished events..."
    
    # Quick check: count unpublished events before starting Spark
    local event_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas -h -1 \
        -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM operational.cow_events WHERE published_at IS NULL" 2>/dev/null | tr -d '[:space:]')
    
    # Check if we got a valid number
    if ! [[ "$event_count" =~ ^[0-9]+$ ]]; then
        log "⚠️  Could not check event count, proceeding with Bronze sync"
    elif [ "$event_count" -eq 0 ]; then
        log "⏭️  Skipping Bronze layer (no new events to process)"
        return 0
    else
        log "📊 Found $event_count unpublished event(s)"
    fi
    
    log "🔵 Starting Bronze layer ingestion..."
    cd "$PROJECT_ROOT/databricks/bronze"
    
    if "$VENV_PYTHON" ingest_from_sql.py --once 2>&1 | tee -a "$LOG_FILE"; then
        log "✅ Bronze layer synced successfully"
        return 0
    else
        log "⚠️  Bronze sync failed"
        return 1
    fi
}

run_silver_sync() {
    log "⚪ Starting Silver layer processing..."
    cd "$PROJECT_ROOT/databricks/silver"
    
    if "$VENV_PYTHON" resolve_cow_state.py 2>&1 | tee -a "$LOG_FILE"; then
        log "✅ Silver layer synced successfully"
        return 0
    else
        log "⚠️  Silver sync failed"
        return 1
    fi
}

run_gold_sync() {
    log "🟡 Starting Gold layer aggregation..."
    cd "$PROJECT_ROOT/databricks/gold"
    
    # OPTIMIZATION: Use unified runner for 4-6 minute speedup
    # Runs all Gold analytics in a single Spark session (eliminates startup overhead)
    if [ -f "gold_unified_runner.py" ]; then
        log "  ⚡ Using optimized unified runner (single Spark session)..."
        if "$VENV_PYTHON" gold_unified_runner.py 2>&1 | tee -a "$LOG_FILE"; then
            log "✅ Gold layer synced successfully (unified runner)"
            return 0
        else
            log "⚠️  Unified runner failed, falling back to individual scripts"
            # Fall through to individual script execution
        fi
    fi
    
    # FALLBACK: Run individual Gold scripts (original behavior)
    log "  📊 Running individual Gold scripts..."
    local gold_success=true
    local scripts_run=0
    
    # Only run implemented Gold scripts
    if [ -f "gold_herd_composition.py" ]; then
        log "    - Herd composition analytics..."
        if "$VENV_PYTHON" gold_herd_composition.py 2>&1 | tee -a "$LOG_FILE"; then
            log "    ✅ Herd composition updated"
            ((scripts_run++))
        else
            log "    ⚠️  Herd composition failed"
            gold_success=false
        fi
    fi
    
    # SKIP: gold_cow_weight_trends.py (placeholder - not implemented)
    # This script only prints "PLANNED" messages and requires silver_weights
    
    if [ -f "gold_cow_lifecycle.py" ]; then
        log "    - Cow lifecycle analytics..."
        if "$VENV_PYTHON" gold_cow_lifecycle.py 2>&1 | tee -a "$LOG_FILE"; then
            log "    ✅ Cow lifecycle updated"
            ((scripts_run++))
        else
            log "    ⚠️  Cow lifecycle failed"
            gold_success=false
        fi
    fi
    
    if [ -f "gold_daily_snapshots.py" ]; then
        log "    - Daily snapshots analytics..."
        if "$VENV_PYTHON" gold_daily_snapshots.py 2>&1 | tee -a "$LOG_FILE"; then
            log "    ✅ Daily snapshots updated"
            ((scripts_run++))
        else
            log "    ⚠️  Daily snapshots failed"
            gold_success=false
        fi
    fi
    
    # SKIP: gold_daily_sales.py (placeholder - not implemented)
    # This script only prints "PLANNED" messages and requires silver_sales
    
    if $gold_success; then
        log "✅ Gold layer synced successfully ($scripts_run individual script(s) executed)"
        return 0
    else
        return 1
    fi
}

run_full_sync() {
    log "🔄 Starting analytics pipeline sync cycle..."
    
    local start_time=$(date +%s)
    local bronze_success=false
    local silver_success=false
    local gold_success=false
    
    # Run Bronze
    if run_bronze_sync; then
        bronze_success=true
    fi
    
    # Run Silver (only if Bronze succeeded)
    if $bronze_success; then
        if run_silver_sync; then
            silver_success=true
        fi
    fi
    
    # Run Gold (only if Silver succeeded)
    if $silver_success; then
        if run_gold_sync; then
            gold_success=true
        fi
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if $gold_success; then
        log "✅ Full pipeline sync completed in ${duration}s"
    else
        log "⚠️  Pipeline sync completed with errors in ${duration}s"
    fi
}

# Trap signals for graceful shutdown
trap 'log "🛑 Analytics sync service stopping..."; exit 0' SIGTERM SIGINT

# Main sync loop
log "🚀 Analytics sync service starting (interval: ${SYNC_INTERVAL}s)..."
log "📂 Project root: $PROJECT_ROOT"
log "📝 Log file: $LOG_FILE"

while true; do
    run_full_sync
    log "⏳ Waiting ${SYNC_INTERVAL}s until next sync..."
    sleep "$SYNC_INTERVAL"
done
