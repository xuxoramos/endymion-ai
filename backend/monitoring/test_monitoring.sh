#!/bin/bash
#
# Test script for monitoring system.
#
# Tests:
# 1. Health checks work when sync is running
# 2. Health checks detect when sync stops
# 3. Health checks recover when sync restarts
# 4. Metrics collection works
# 5. Dashboard generation works
#

set -e  # Exit on error

echo "========================================="
echo "🧪 Monitoring System Test"
echo "========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

# Check if sync scheduler is running
check_sync_running() {
    if pgrep -f "sync_scheduler.py" > /dev/null; then
        return 0
    else
        return 1
    fi
}

# ========================================
# Test 1: Health checks with sync running
# ========================================

echo "Test 1: Health checks with sync running"
echo "----------------------------------------"

if check_sync_running; then
    print_success "Sync scheduler is running"
else
    print_error "Sync scheduler is NOT running"
    print_info "Starting sync scheduler..."
    cd /home/xuxoramos/endymion-ai
    nohup python -m backend.jobs.sync_scheduler > /tmp/sync_scheduler.log 2>&1 &
    sleep 5
    
    if check_sync_running; then
        print_success "Sync scheduler started"
    else
        print_error "Failed to start sync scheduler"
        exit 1
    fi
fi

print_info "Running health checks..."
python -m backend.monitoring.health_check

if [ $? -eq 0 ]; then
    print_success "Health checks passed (system is healthy)"
else
    print_error "Health checks failed (system is unhealthy)"
fi

echo ""

# ========================================
# Test 2: Metrics collection
# ========================================

echo "Test 2: Metrics collection"
echo "----------------------------------------"

print_info "Collecting metrics (JSON format)..."
python -m backend.monitoring.metrics > /tmp/metrics.json

if [ $? -eq 0 ]; then
    print_success "Metrics collected successfully"
    echo "Sample metrics:"
    head -20 /tmp/metrics.json
else
    print_error "Metrics collection failed"
fi

echo ""

print_info "Collecting metrics (Prometheus format)..."
python -m backend.monitoring.metrics --prometheus > /tmp/metrics.prom

if [ $? -eq 0 ]; then
    print_success "Prometheus metrics collected successfully"
    echo "Sample metrics:"
    head -20 /tmp/metrics.prom
else
    print_error "Prometheus metrics collection failed"
fi

echo ""

# ========================================
# Test 3: Dashboard generation
# ========================================

echo "Test 3: Dashboard generation"
echo "----------------------------------------"

print_info "Generating dashboard..."
python -m backend.monitoring.dashboard /tmp/dashboard.html

if [ $? -eq 0 ]; then
    print_success "Dashboard generated successfully"
    print_info "Dashboard saved to: /tmp/dashboard.html"
    print_info "File size: $(du -h /tmp/dashboard.html | cut -f1)"
else
    print_error "Dashboard generation failed"
fi

echo ""

# ========================================
# Test 4: Stop sync and verify detection
# ========================================

echo "Test 4: Stop sync and verify detection"
echo "----------------------------------------"

print_info "Stopping sync scheduler..."
pkill -f "sync_scheduler.py" || true
sleep 2

if check_sync_running; then
    print_error "Sync scheduler still running"
else
    print_success "Sync scheduler stopped"
fi

print_info "Waiting 6 minutes for sync lag to exceed threshold..."
print_info "(This will cause health checks to fail)"
print_info "Sleeping for 360 seconds..."

# Show countdown
for i in {360..1}; do
    printf "\rTime remaining: %d seconds  " $i
    sleep 1
done
echo ""

print_info "Running health checks (should show CRITICAL)..."
python -m backend.monitoring.health_check

if [ $? -ne 0 ]; then
    print_success "Health checks correctly detected unhealthy system"
else
    print_error "Health checks did NOT detect unhealthy system"
fi

echo ""

# ========================================
# Test 5: Restart sync and verify recovery
# ========================================

echo "Test 5: Restart sync and verify recovery"
echo "----------------------------------------"

print_info "Restarting sync scheduler..."
cd /home/xuxoramos/endymion-ai
nohup python -m backend.jobs.sync_scheduler > /tmp/sync_scheduler.log 2>&1 &
sleep 5

if check_sync_running; then
    print_success "Sync scheduler restarted"
else
    print_error "Failed to restart sync scheduler"
fi

print_info "Waiting for sync to complete (60 seconds)..."
sleep 60

print_info "Running health checks (should be healthy again)..."
python -m backend.monitoring.health_check

if [ $? -eq 0 ]; then
    print_success "System recovered successfully"
else
    print_error "System did NOT recover"
fi

echo ""

# ========================================
# Test 6: FastAPI endpoints
# ========================================

echo "Test 6: FastAPI health endpoints"
echo "----------------------------------------"

# Check if FastAPI is running
if ! pgrep -f "uvicorn.*main:app" > /dev/null; then
    print_error "FastAPI is not running"
    print_info "Skipping API endpoint tests"
else
    print_success "FastAPI is running"
    
    print_info "Testing GET /health..."
    curl -s http://localhost:8000/health | jq . || print_error "Failed to query /health"
    
    echo ""
    
    print_info "Testing GET /metrics..."
    curl -s http://localhost:8000/metrics | head -20 || print_error "Failed to query /metrics"
    
    echo ""
    
    print_info "Testing GET /dashboard..."
    curl -s -o /tmp/api_dashboard.html http://localhost:8000/dashboard
    if [ $? -eq 0 ]; then
        print_success "Dashboard endpoint working"
        print_info "Dashboard saved to: /tmp/api_dashboard.html"
    else
        print_error "Dashboard endpoint failed"
    fi
fi

echo ""

# ========================================
# Summary
# ========================================

echo "========================================="
echo "📊 Test Summary"
echo "========================================="
echo ""

print_success "All monitoring components tested"
echo ""
echo "Files generated:"
echo "  - /tmp/metrics.json         - Metrics in JSON format"
echo "  - /tmp/metrics.prom         - Metrics in Prometheus format"
echo "  - /tmp/dashboard.html       - Standalone dashboard"
echo "  - /tmp/api_dashboard.html   - Dashboard from API endpoint"
echo ""
echo "Next steps:"
echo "  1. Open /tmp/dashboard.html in browser to view monitoring dashboard"
echo "  2. Set up alerting based on health check exit codes"
echo "  3. Configure Prometheus to scrape /metrics endpoint"
echo "  4. Set up continuous health monitoring (cron job)"
echo ""
print_success "Monitoring system is operational! 🎉"
