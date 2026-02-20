#!/bin/bash
#
# Automatic Pipeline Sync - Runs Bronze → Silver → Gold automatically
# Designed for scheduled execution (cron/systemd timer)
#
# Features:
# - Smart skipping: Only runs if new events exist
# - Lock file: Prevents overlapping executions
# - Lightweight: Fast check before starting Spark
# - Logging: Detailed logs for debugging
#
# Usage:
#   ./demo/auto_sync_pipeline.sh
#
# Recommended Schedule:
#   */1 * * * *   # Every 1 minute (for demo/dev)
#   */5 * * * *   # Every 5 minutes (for production)
#

set -euo pipefail

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCK_FILE="/tmp/endymion_ai_sync.lock"
LOG_FILE="/tmp/endymion_ai_auto_sync.log"
MAX_LOG_SIZE=$((10 * 1024 * 1024))  # 10MB

VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

log_info() { log "INFO" "$@"; }
log_success() { log "SUCCESS" "$@"; }
log_warning() { log "WARNING" "$@"; }
log_error() { log "ERROR" "$@"; }

# Rotate log file if too large
rotate_log_if_needed() {
    if [ -f "$LOG_FILE" ]; then
        local size=$(stat -f%z "$LOG_FILE" 2>/dev/null || stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)
        if [ "$size" -gt "$MAX_LOG_SIZE" ]; then
            mv "$LOG_FILE" "${LOG_FILE}.old"
            log_info "Log file rotated (was ${size} bytes)"
        fi
    fi
}

# ============================================================================
# LOCKING MECHANISM
# ============================================================================

acquire_lock() {
    if [ -f "$LOCK_FILE" ]; then
        local pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            log_warning "Another sync is already running (PID: $pid). Skipping this cycle."
            return 1
        else
            log_warning "Stale lock file found. Removing and continuing."
            rm -f "$LOCK_FILE"
        fi
    fi
    
    echo $$ > "$LOCK_FILE"
    log_info "Lock acquired (PID: $$)"
    return 0
}

release_lock() {
    if [ -f "$LOCK_FILE" ]; then
        rm -f "$LOCK_FILE"
        log_info "Lock released"
    fi
}

# Ensure lock is released on exit
trap release_lock EXIT INT TERM

# ============================================================================
# PREFLIGHT CHECKS
# ============================================================================

check_prerequisites() {
    # Check virtual environment
    if [ ! -f "$VENV_PYTHON" ]; then
        log_error "Virtual environment not found at: $VENV_PYTHON"
        log_error "Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
        return 1
    fi
    
    # Check Docker services
    if ! docker ps | grep -q cattlesaas-sqlserver; then
        log_error "SQL Server container not running. Start with: docker-compose up -d"
        return 1
    fi
    
    if ! docker ps | grep -q cattlesaas-minio; then
        log_error "MinIO container not running. Start with: docker-compose up -d"
        return 1
    fi
    
    log_info "Prerequisites check passed"
    return 0
}

# ============================================================================
# SMART EVENT DETECTION
# ============================================================================

count_unpublished_events() {
    local count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas -h -1 \
        -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM operational.cow_events WHERE published_at IS NULL" \
        2>/dev/null | tr -d '[:space:]' || echo "0")
    
    if ! [[ "$count" =~ ^[0-9]+$ ]]; then
        count=0
    fi
    
    echo "$count"
}

# ============================================================================
# PIPELINE STAGES
# ============================================================================

run_bronze_layer() {
    log_info "🔵 Bronze Layer: Ingesting events from SQL Server to Delta Lake..."
    cd "$PROJECT_ROOT/databricks/bronze"
    
    # Use incremental mode for faster processing (only new events)
    if "$VENV_PYTHON" ingest_from_sql.py --once --incremental >> "$LOG_FILE" 2>&1; then
        log_success "Bronze layer ingestion completed"
        return 0
    else
        log_error "Bronze layer ingestion failed"
        return 1
    fi
}

run_silver_layer() {
    log_info "💎 Silver Layer: Resolving cow state (SCD Type 2)..."
    cd "$PROJECT_ROOT/databricks/silver"
    
    if "$VENV_PYTHON" resolve_cow_state.py >> "$LOG_FILE" 2>&1; then
        log_success "Silver layer processing completed"
        return 0
    else
        log_error "Silver layer processing failed"
        return 1
    fi
}

run_gold_layer() {
    log_info "🏆 Gold Layer: Generating analytics..."
    cd "$PROJECT_ROOT/databricks/gold"
    
    local success=true
    
    # Run all Gold analytics scripts
    if [ -f "gold_daily_snapshots.py" ]; then
        log_info "  - Running daily snapshots..."
        if "$VENV_PYTHON" gold_daily_snapshots.py >> "$LOG_FILE" 2>&1; then
            log_success "  Daily snapshots completed"
        else
            log_error "  Daily snapshots failed"
            success=false
        fi
    fi
    
    if [ -f "gold_herd_composition.py" ]; then
        log_info "  - Running herd composition..."
        if "$VENV_PYTHON" gold_herd_composition.py >> "$LOG_FILE" 2>&1; then
            log_success "  Herd composition completed"
        else
            log_error "  Herd composition failed"
            success=false
        fi
    fi
    
    if $success; then
        log_success "Gold layer analytics completed"
        return 0
    else
        log_error "Gold layer analytics completed with errors"
        return 1
    fi
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    rotate_log_if_needed
    
    log_info "=========================================="
    log_info "Endymion-AI Automatic Pipeline Sync"
    log_info "=========================================="
    
    # Acquire lock
    if ! acquire_lock; then
        exit 0  # Silently exit if another instance is running
    fi
    
    # Preflight checks
    if ! check_prerequisites; then
        log_error "Prerequisites check failed. Exiting."
        exit 1
    fi
    
    # Check for new events
    local unpublished_count=$(count_unpublished_events)
    log_info "Unpublished events: $unpublished_count"
    
    if [ "$unpublished_count" -eq 0 ]; then
        log_info "✅ No new events to process. Skipping pipeline execution."
        log_info "Next check will run according to schedule."
        exit 0
    fi
    
    log_info "🚀 Starting pipeline sync for $unpublished_count event(s)..."
    local start_time=$(date +%s)
    
    # Run pipeline stages
    local pipeline_success=true
    
    if ! run_bronze_layer; then
        pipeline_success=false
    fi
    
    if $pipeline_success && ! run_silver_layer; then
        pipeline_success=false
    fi
    
    if $pipeline_success && ! run_gold_layer; then
        pipeline_success=false
    fi
    
    # Calculate duration
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if $pipeline_success; then
        log_success "=========================================="
        log_success "✅ Pipeline sync completed successfully"
        log_success "Duration: ${duration}s"
        log_success "Events processed: $unpublished_count"
        log_success "=========================================="
        exit 0
    else
        log_error "=========================================="
        log_error "❌ Pipeline sync completed with errors"
        log_error "Duration: ${duration}s"
        log_error "Check logs for details: $LOG_FILE"
        log_error "=========================================="
        exit 1
    fi
}

# Run main function
main
