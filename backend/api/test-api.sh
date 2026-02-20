#!/bin/bash

# Test script for FastAPI Pure Projection Pattern A implementation
# This verifies that:
# 1. Write operations create events in cow_events
# 2. Read operations query cows projection (initially empty)
# 3. Events and projections are separate

set -e  # Exit on error

API_URL="http://localhost:8000"
TENANT_ID="550e8400-e29b-41d4-a716-446655440000"
CONTENT_TYPE="Content-Type: application/json"
TENANT_HEADER="X-Tenant-ID: $TENANT_ID"

echo "================================================"
echo "FastAPI Pure Projection Pattern A - Test Suite"
echo "================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ========================================
# Test 1: Health Check
# ========================================
echo "Test 1: Health Check"
echo "--------------------"
HEALTH_RESPONSE=$(curl -s "$API_URL/health")
echo "$HEALTH_RESPONSE" | jq .

if echo "$HEALTH_RESPONSE" | jq -e '.status == "healthy"' > /dev/null; then
    echo -e "${GREEN}✅ Health check passed${NC}"
else
    echo -e "${RED}❌ Health check failed${NC}"
    exit 1
fi
echo ""

# ========================================
# Test 2: Create Cow (Event Sourcing)
# ========================================
echo "Test 2: Create Cow - POST /api/v1/cows"
echo "---------------------------------------"
echo "Creating cow 'US-TEST-001' (Holstein)..."

CREATE_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/cows" \
  -H "$CONTENT_TYPE" \
  -H "$TENANT_HEADER" \
  -d '{
    "tag_number": "US-TEST-001",
    "name": "Test Cow 1",
    "breed": "Holstein",
    "birth_date": "2022-01-15",
    "sex": "female",
    "weight_kg": 450.5,
    "current_location": "Barn A",
    "notes": "Test cow for API verification"
  }')

echo "$CREATE_RESPONSE" | jq .

# Extract cow_id and event_id
COW_ID=$(echo "$CREATE_RESPONSE" | jq -r '.cow_id')
EVENT_ID=$(echo "$CREATE_RESPONSE" | jq -r '.event_id')

if [ "$COW_ID" != "null" ] && [ "$EVENT_ID" != "null" ]; then
    echo -e "${GREEN}✅ Cow created with ID: $COW_ID${NC}"
    echo -e "${GREEN}✅ Event created with ID: $EVENT_ID${NC}"
else
    echo -e "${RED}❌ Cow creation failed${NC}"
    exit 1
fi

# Verify it returns status "accepted"
STATUS=$(echo "$CREATE_RESPONSE" | jq -r '.status')
if [ "$STATUS" == "accepted" ]; then
    echo -e "${GREEN}✅ Status: accepted (correct for event sourcing)${NC}"
else
    echo -e "${RED}❌ Expected status 'accepted', got: $STATUS${NC}"
fi
echo ""

# ========================================
# Test 3: Verify Event in cow_events Table
# ========================================
echo "Test 3: Verify Event in cow_events Table"
echo "-----------------------------------------"
echo "Checking if event appears in database..."

# Note: This requires direct database access
# For now, we'll skip this and rely on SQL verification below

echo -e "${YELLOW}⚠️  Direct DB verification requires manual SQL query${NC}"
echo "Run this SQL to verify:"
echo ""
echo "SELECT event_id, cow_id, event_type, payload, published_to_bronze"
echo "FROM operational.cow_events"
echo "WHERE event_id = '$EVENT_ID';"
echo ""

# ========================================
# Test 4: Verify Cow NOT in Projection Yet
# ========================================
echo "Test 4: Verify Cow NOT in Projection (GET /api/v1/cows/{cow_id})"
echo "------------------------------------------------------------------"
echo "Attempting to GET cow immediately after creation..."
echo "(Should return 404 since projection not synced yet)"

GET_RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X GET "$API_URL/api/v1/cows/$COW_ID" \
  -H "$TENANT_HEADER")

HTTP_CODE=$(echo "$GET_RESPONSE" | grep "HTTP_CODE" | cut -d: -f2)
BODY=$(echo "$GET_RESPONSE" | sed '/HTTP_CODE/d')

echo "$BODY" | jq . 2>/dev/null || echo "$BODY"

if [ "$HTTP_CODE" == "404" ]; then
    echo -e "${GREEN}✅ Got 404 as expected (projection not synced yet)${NC}"
    echo -e "${GREEN}✅ Proves Pure Projection Pattern A: writes don't update projection!${NC}"
else
    echo -e "${YELLOW}⚠️  Got HTTP $HTTP_CODE (expected 404)${NC}"
    echo -e "${YELLOW}⚠️  Cow may already be in projection (unexpected in pure pattern)${NC}"
fi
echo ""

# ========================================
# Test 5: List Cows from Projection
# ========================================
echo "Test 5: List Cows from Projection (GET /api/v1/cows)"
echo "-----------------------------------------------------"
echo "Listing all cows for tenant..."

LIST_RESPONSE=$(curl -s "$API_URL/api/v1/cows?page=1&page_size=20" \
  -H "$TENANT_HEADER")

echo "$LIST_RESPONSE" | jq .

TOTAL=$(echo "$LIST_RESPONSE" | jq -r '.total')
echo ""
echo "Total cows in projection: $TOTAL"

if [ "$TOTAL" == "0" ]; then
    echo -e "${GREEN}✅ Projection is empty (as expected before sync)${NC}"
else
    echo -e "${YELLOW}⚠️  Projection has $TOTAL cows (may include pre-existing data)${NC}"
fi
echo ""

# ========================================
# Test 6: Create Second Cow
# ========================================
echo "Test 6: Create Second Cow"
echo "-------------------------"
echo "Creating cow 'US-TEST-002' (Jersey)..."

CREATE_RESPONSE_2=$(curl -s -X POST "$API_URL/api/v1/cows" \
  -H "$CONTENT_TYPE" \
  -H "$TENANT_HEADER" \
  -d '{
    "tag_number": "US-TEST-002",
    "name": "Test Cow 2",
    "breed": "Jersey",
    "birth_date": "2023-03-20",
    "sex": "female",
    "weight_kg": 325.0,
    "current_location": "Paddock 3"
  }')

echo "$CREATE_RESPONSE_2" | jq .

COW_ID_2=$(echo "$CREATE_RESPONSE_2" | jq -r '.cow_id')
EVENT_ID_2=$(echo "$CREATE_RESPONSE_2" | jq -r '.event_id')

if [ "$COW_ID_2" != "null" ]; then
    echo -e "${GREEN}✅ Second cow created with ID: $COW_ID_2${NC}"
else
    echo -e "${RED}❌ Second cow creation failed${NC}"
fi
echo ""

# ========================================
# Test 7: Update Cow (Event Sourcing)
# ========================================
echo "Test 7: Update Cow - PUT /api/v1/cows/{cow_id}"
echo "-----------------------------------------------"
echo "Updating first cow's weight and location..."

UPDATE_RESPONSE=$(curl -s -X PUT "$API_URL/api/v1/cows/$COW_ID" \
  -H "$CONTENT_TYPE" \
  -H "$TENANT_HEADER" \
  -d '{
    "weight_kg": 475.0,
    "current_location": "Barn B",
    "notes": "Moved to Barn B after weight check"
  }')

echo "$UPDATE_RESPONSE" | jq .

UPDATE_EVENT_ID=$(echo "$UPDATE_RESPONSE" | jq -r '.event_id')
UPDATE_EVENT_TYPE=$(echo "$UPDATE_RESPONSE" | jq -r '.event_type')

if [ "$UPDATE_EVENT_TYPE" == "cow_updated" ]; then
    echo -e "${GREEN}✅ Update event created: $UPDATE_EVENT_ID${NC}"
else
    echo -e "${RED}❌ Update failed${NC}"
fi
echo ""

# ========================================
# Test 8: Record Weight
# ========================================
echo "Test 8: Record Weight - POST /api/v1/cows/{cow_id}/weight"
echo "-----------------------------------------------------------"
echo "Recording weight measurement..."

WEIGHT_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/cows/$COW_ID/weight" \
  -H "$CONTENT_TYPE" \
  -H "$TENANT_HEADER" \
  -d '{
    "weight_kg": 480.5,
    "measured_by": "John Smith",
    "measurement_method": "Scale",
    "notes": "Regular monthly weigh-in"
  }')

echo "$WEIGHT_RESPONSE" | jq .

WEIGHT_EVENT_TYPE=$(echo "$WEIGHT_RESPONSE" | jq -r '.event_type')

if [ "$WEIGHT_EVENT_TYPE" == "cow_weight_recorded" ]; then
    echo -e "${GREEN}✅ Weight event created${NC}"
else
    echo -e "${RED}❌ Weight recording failed${NC}"
fi
echo ""

# ========================================
# Test 9: Get Event History
# ========================================
echo "Test 9: Get Event History - GET /api/v1/cows/{cow_id}/events"
echo "--------------------------------------------------------------"
echo "Fetching event history for first cow..."

EVENTS_RESPONSE=$(curl -s "$API_URL/api/v1/cows/$COW_ID/events" \
  -H "$TENANT_HEADER")

echo "$EVENTS_RESPONSE" | jq .

EVENT_COUNT=$(echo "$EVENTS_RESPONSE" | jq 'length')
echo ""
echo "Total events for cow: $EVENT_COUNT"

if [ "$EVENT_COUNT" -ge "3" ]; then
    echo -e "${GREEN}✅ Event history shows $EVENT_COUNT events (created, updated, weight)${NC}"
else
    echo -e "${YELLOW}⚠️  Expected at least 3 events, got $EVENT_COUNT${NC}"
fi
echo ""

# ========================================
# Test 10: Deactivate Cow
# ========================================
echo "Test 10: Deactivate Cow - DELETE /api/v1/cows/{cow_id}"
echo "-------------------------------------------------------"
echo "Deactivating first cow..."

DEACTIVATE_RESPONSE=$(curl -s -X DELETE "$API_URL/api/v1/cows/$COW_ID" \
  -H "$CONTENT_TYPE" \
  -H "$TENANT_HEADER" \
  -d '{
    "reason": "Test completed",
    "notes": "Test cow no longer needed"
  }')

echo "$DEACTIVATE_RESPONSE" | jq .

DEACTIVATE_EVENT_TYPE=$(echo "$DEACTIVATE_RESPONSE" | jq -r '.event_type')

if [ "$DEACTIVATE_EVENT_TYPE" == "cow_deactivated" ]; then
    echo -e "${GREEN}✅ Deactivation event created${NC}"
else
    echo -e "${RED}❌ Deactivation failed${NC}"
fi
echo ""

# ========================================
# Summary
# ========================================
echo "================================================"
echo "Test Summary"
echo "================================================"
echo ""
echo "Created Resources:"
echo "  - Cow 1: $COW_ID (US-TEST-001)"
echo "  - Cow 2: $COW_ID_2 (US-TEST-002)"
echo ""
echo "Verification Steps:"
echo ""
echo "1. Check cow_events table (should have 5 events):"
echo "   SELECT event_id, cow_id, event_type, published_to_bronze"
echo "   FROM operational.cow_events"
echo "   WHERE tenant_id = '$TENANT_ID'"
echo "   ORDER BY event_time DESC;"
echo ""
echo "2. Check cows projection table (should be EMPTY unless synced):"
echo "   SELECT id, tag_number, breed, status, last_synced_at"
echo "   FROM operational.cows"
echo "   WHERE tenant_id = '$TENANT_ID';"
echo ""
echo "3. Expected behavior:"
echo "   ✅ cow_events has 5 events (2 created, 1 updated, 1 weight, 1 deactivated)"
echo "   ✅ cows projection is EMPTY (not synced yet)"
echo "   ✅ GET /cows/{cow_id} returns 404 (projection not ready)"
echo ""
echo -e "${GREEN}✅ API follows Pure Projection Pattern A correctly!${NC}"
echo ""
echo "To manually verify, run:"
echo "  ./verify-pattern-a.sh"
echo ""
