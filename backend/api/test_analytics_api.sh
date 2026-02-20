#!/bin/bash

# ============================================
# Test Analytics API Endpoints
# ============================================
#
# Tests the analytics API endpoints with curl commands.
#
# Prerequisites:
# 1. Backend API running (uvicorn backend.api.main:app --reload)
# 2. Gold layer tables populated (databricks/gold/rebuild_all.py)
# 3. MinIO running with gold bucket
#
# Usage:
#   ./test_analytics_api.sh [tenant_id]
#
# Example:
#   ./test_analytics_api.sh 550e8400-e29b-41d4-a716-446655440000

set -e

# ============================================
# Configuration
# ============================================

API_BASE="http://localhost:8000/api/v1/analytics"
TENANT_ID="${1:-550e8400-e29b-41d4-a716-446655440000}"

echo "============================================"
echo "Testing Analytics API"
echo "============================================"
echo "API Base: $API_BASE"
echo "Tenant ID: $TENANT_ID"
echo ""

# ============================================
# Test 1: Herd Composition (Latest)
# ============================================

echo "Test 1: Get herd composition (latest snapshot)"
echo "--------------------------------------------"
echo "GET /herd-composition?tenant_id=$TENANT_ID"
echo ""

curl -s -X GET \
  "$API_BASE/herd-composition?tenant_id=$TENANT_ID" \
  -H "Accept: application/json" | jq '.'

echo ""
echo ""

# ============================================
# Test 2: Herd Composition (Specific Date)
# ============================================

echo "Test 2: Get herd composition (specific date)"
echo "--------------------------------------------"
echo "GET /herd-composition?tenant_id=$TENANT_ID&date=2024-01-15"
echo ""

curl -s -X GET \
  "$API_BASE/herd-composition?tenant_id=$TENANT_ID&date=2024-01-15" \
  -H "Accept: application/json" | jq '.'

echo ""
echo ""

# ============================================
# Test 3: Cache Verification
# ============================================

echo "Test 3: Verify caching (should return cached=true on 2nd call)"
echo "--------------------------------------------"
echo "First call (cache MISS):"
echo ""

START_TIME=$(date +%s%N)
curl -s -X GET \
  "$API_BASE/herd-composition?tenant_id=$TENANT_ID" \
  -H "Accept: application/json" | jq '.cached, .as_of'
END_TIME=$(date +%s%N)
DURATION=$((($END_TIME - $START_TIME) / 1000000))
echo "Duration: ${DURATION}ms"
echo ""

sleep 1

echo "Second call (cache HIT):"
echo ""
START_TIME=$(date +%s%N)
curl -s -X GET \
  "$API_BASE/herd-composition?tenant_id=$TENANT_ID" \
  -H "Accept: application/json" | jq '.cached, .as_of'
END_TIME=$(date +%s%N)
DURATION=$((($END_TIME - $START_TIME) / 1000000))
echo "Duration: ${DURATION}ms (should be faster)"
echo ""
echo ""

# ============================================
# Test 4: Weight Trends (Not Implemented)
# ============================================

echo "Test 4: Weight trends (should return 501 Not Implemented)"
echo "--------------------------------------------"
COW_ID="00000000-0000-0000-0000-000000000001"
echo "GET /weight-trends/$COW_ID?tenant_id=$TENANT_ID"
echo ""

curl -s -X GET \
  "$API_BASE/weight-trends/$COW_ID?tenant_id=$TENANT_ID" \
  -H "Accept: application/json" | jq '.'

echo ""
echo ""

# ============================================
# Test 5: Sales Summary (Not Implemented)
# ============================================

echo "Test 5: Sales summary (should return 501 Not Implemented)"
echo "--------------------------------------------"
echo "GET /sales-summary?tenant_id=$TENANT_ID&start_date=2024-01-01&end_date=2024-01-31"
echo ""

curl -s -X GET \
  "$API_BASE/sales-summary?tenant_id=$TENANT_ID&start_date=2024-01-01&end_date=2024-01-31" \
  -H "Accept: application/json" | jq '.'

echo ""
echo ""

# ============================================
# Test 6: Clear Cache
# ============================================

echo "Test 6: Clear cache"
echo "--------------------------------------------"
echo "DELETE /cache"
echo ""

curl -s -X DELETE \
  "$API_BASE/cache" \
  -H "Accept: application/json" | jq '.'

echo ""
echo ""

# ============================================
# Test 7: Verify Cache Cleared
# ============================================

echo "Test 7: Verify cache cleared (should return cached=false)"
echo "--------------------------------------------"
echo "GET /herd-composition?tenant_id=$TENANT_ID"
echo ""

curl -s -X GET \
  "$API_BASE/herd-composition?tenant_id=$TENANT_ID" \
  -H "Accept: application/json" | jq '.cached, .as_of'

echo ""
echo ""

# ============================================
# Test 8: Error Handling - Invalid Tenant
# ============================================

echo "Test 8: Error handling (invalid tenant ID)"
echo "--------------------------------------------"
INVALID_TENANT="00000000-0000-0000-0000-000000000000"
echo "GET /herd-composition?tenant_id=$INVALID_TENANT"
echo ""

curl -s -X GET \
  "$API_BASE/herd-composition?tenant_id=$INVALID_TENANT" \
  -H "Accept: application/json" | jq '.'

echo ""
echo ""

# ============================================
# Test 9: Error Handling - Missing Tenant
# ============================================

echo "Test 9: Error handling (missing tenant ID)"
echo "--------------------------------------------"
echo "GET /herd-composition (no tenant_id)"
echo ""

curl -s -X GET \
  "$API_BASE/herd-composition" \
  -H "Accept: application/json" | jq '.'

echo ""
echo ""

# ============================================
# Summary
# ============================================

echo "============================================"
echo "Analytics API Test Summary"
echo "============================================"
echo "✅ Test 1: Herd composition (latest) - Expected: 200 OK with data"
echo "✅ Test 2: Herd composition (specific date) - Expected: 200 OK or 404"
echo "✅ Test 3: Cache verification - Expected: cached=false then cached=true"
echo "✅ Test 4: Weight trends - Expected: 501 Not Implemented"
echo "✅ Test 5: Sales summary - Expected: 501 Not Implemented"
echo "✅ Test 6: Clear cache - Expected: 200 OK"
echo "✅ Test 7: Verify cache cleared - Expected: cached=false"
echo "✅ Test 8: Invalid tenant - Expected: 404 Not Found"
echo "✅ Test 9: Missing tenant - Expected: 422 Validation Error"
echo ""
echo "Done!"
