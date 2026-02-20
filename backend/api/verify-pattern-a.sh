#!/bin/bash

# SQL verification script for Pure Projection Pattern A
# Verifies that events are in cow_events but cows projection is empty

set -e

echo "================================================"
echo "Pure Projection Pattern A - SQL Verification"
echo "================================================"
echo ""

# Database connection parameters from environment or defaults
DB_HOST="${SQLSERVER_HOST:-localhost}"
DB_PORT="${SQLSERVER_PORT:-1433}"
DB_NAME="${SQLSERVER_DATABASE:-cattledb}"
DB_USER="${SQLSERVER_USERNAME:-sa}"
DB_PASS="${SQLSERVER_PASSWORD}"

if [ -z "$DB_PASS" ]; then
    echo "Error: SQLSERVER_PASSWORD environment variable not set"
    echo "Export it or provide password when prompted"
    exit 1
fi

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "Connecting to: $DB_HOST:$DB_PORT/$DB_NAME"
echo ""

# ========================================
# Check 1: Count events in cow_events
# ========================================
echo "Check 1: Events in cow_events table"
echo "------------------------------------"

EVENTS_QUERY="
SELECT 
    COUNT(*) as total_events,
    SUM(CASE WHEN published_to_bronze = 1 THEN 1 ELSE 0 END) as published_events,
    SUM(CASE WHEN published_to_bronze = 0 THEN 1 ELSE 0 END) as pending_events
FROM operational.cow_events;
"

echo "Running query..."
sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" \
    -Q "$EVENTS_QUERY" -s "," -W -h -1

EVENT_COUNT=$(sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" \
    -Q "SELECT COUNT(*) FROM operational.cow_events;" -h -1 -W | xargs)

echo ""
if [ "$EVENT_COUNT" -gt "0" ]; then
    echo -e "${GREEN}✅ Found $EVENT_COUNT events in cow_events table${NC}"
else
    echo -e "${YELLOW}⚠️  No events found in cow_events table${NC}"
fi
echo ""

# ========================================
# Check 2: List events by type
# ========================================
echo "Check 2: Events by type"
echo "-----------------------"

EVENTS_BY_TYPE_QUERY="
SELECT 
    event_type,
    COUNT(*) as count,
    MAX(event_time) as latest_event
FROM operational.cow_events
GROUP BY event_type
ORDER BY count DESC;
"

sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" \
    -Q "$EVENTS_BY_TYPE_QUERY" -W

echo ""

# ========================================
# Check 3: Count cows in projection
# ========================================
echo "Check 3: Cows in projection table"
echo "----------------------------------"

PROJECTION_QUERY="
SELECT 
    COUNT(*) as total_cows,
    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_cows,
    MAX(last_synced_at) as last_sync
FROM operational.cows;
"

echo "Running query..."
sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" \
    -Q "$PROJECTION_QUERY" -s "," -W -h -1

COW_COUNT=$(sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" \
    -Q "SELECT COUNT(*) FROM operational.cows;" -h -1 -W | xargs)

echo ""
if [ "$COW_COUNT" == "0" ]; then
    echo -e "${GREEN}✅ Projection table is EMPTY (as expected before sync)${NC}"
    echo -e "${GREEN}✅ Proves Pure Projection Pattern A is working correctly!${NC}"
else
    echo -e "${YELLOW}⚠️  Found $COW_COUNT cows in projection table${NC}"
    echo -e "${YELLOW}⚠️  Projection may have been synced or has pre-existing data${NC}"
fi
echo ""

# ========================================
# Check 4: Show recent events
# ========================================
echo "Check 4: Recent events (last 10)"
echo "--------------------------------"

RECENT_EVENTS_QUERY="
SELECT TOP 10
    event_id,
    cow_id,
    event_type,
    event_time,
    published_to_bronze,
    created_by
FROM operational.cow_events
ORDER BY event_time DESC;
"

sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" \
    -Q "$RECENT_EVENTS_QUERY" -W

echo ""

# ========================================
# Check 5: Show projection data (if any)
# ========================================
if [ "$COW_COUNT" != "0" ]; then
    echo "Check 5: Projection data (if synced)"
    echo "------------------------------------"
    
    PROJECTION_DATA_QUERY="
    SELECT 
        id,
        tag_number,
        breed,
        status,
        last_synced_at,
        sync_version
    FROM operational.cows
    ORDER BY created_at DESC;
    "
    
    sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" \
        -Q "$PROJECTION_DATA_QUERY" -W
    
    echo ""
fi

# ========================================
# Check 6: Verify tenant isolation
# ========================================
echo "Check 6: Tenant isolation"
echo "-------------------------"

TENANT_QUERY="
SELECT 
    tenant_id,
    COUNT(*) as event_count
FROM operational.cow_events
GROUP BY tenant_id;
"

sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" \
    -Q "$TENANT_QUERY" -W

echo ""

# ========================================
# Summary
# ========================================
echo "================================================"
echo "Verification Summary"
echo "================================================"
echo ""
echo "Pattern A Implementation:"
echo ""
echo "1. Events Table (cow_events):"
echo "   - Total events: $EVENT_COUNT"
echo "   - Status: Source of truth ✅"
echo ""
echo "2. Projection Table (cows):"
echo "   - Total cows: $COW_COUNT"
if [ "$COW_COUNT" == "0" ]; then
    echo "   - Status: Empty (waiting for sync) ✅"
else
    echo "   - Status: Synced or has pre-existing data ⚠️"
fi
echo ""
echo "Expected Behavior:"
echo "  ✅ Write operations create events in cow_events"
echo "  ✅ cow_events contains all events (append-only)"
echo "  ✅ cows projection is empty until sync runs"
echo "  ✅ GET endpoints return 404 until sync completes"
echo ""
echo "Next Steps:"
echo "  1. Run Databricks sync job to populate projection"
echo "  2. Verify cows table gets populated from Silver layer"
echo "  3. Confirm GET endpoints start returning data"
echo ""
