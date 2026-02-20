#!/bin/bash
#
# CQRS Projection Sync Script
#
# Syncs cow_events to cows projection table with deduplication.
# This ensures the frontend always displays current data.
#
# Usage:
#   ./sync-projection.sh              # Run once
#   ./sync-projection.sh --loop       # Run continuously every 5 seconds
#   ./sync-projection.sh --loop 10    # Run continuously every 10 seconds
#

INTERVAL=2
LOOP_MODE=false

# Parse arguments
if [ "$1" == "--loop" ]; then
    LOOP_MODE=true
    if [ -n "$2" ]; then
        INTERVAL=$2
    fi
fi

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

sync_projection() {
    # Sync with deduplication using ROW_NUMBER window function
    # Takes the latest event for each tag_number per tenant
    docker exec cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas -h -1 -Q "
    -- First sync: Insert new cows from cow_created events
    WITH LatestCreatedEvents AS (
        SELECT 
            cow_id,
            tenant_id,
            JSON_VALUE(payload, '\$.tag_number') as tag_number,
            JSON_VALUE(payload, '\$.name') as name,
            JSON_VALUE(payload, '\$.breed') as breed,
            JSON_VALUE(payload, '\$.sex') as sex,
            CAST(JSON_VALUE(payload, '\$.birth_date') as date) as birth_date,
            CAST(JSON_VALUE(payload, '\$.weight_kg') as decimal(10,2)) as weight_kg,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY JSON_VALUE(payload, '\$.tag_number'), tenant_id 
                ORDER BY created_at DESC
            ) as rn
        FROM operational.cow_events
        WHERE event_type = 'cow_created'
    )
    INSERT INTO operational.cows (
        cow_id, tenant_id, tag_number, name, breed, sex, birth_date, 
        weight_kg, status, created_at, updated_at, last_synced_at, sync_version
    )
    SELECT 
        cow_id, tenant_id, tag_number, name, breed, sex, birth_date,
        weight_kg, 'active' as status, created_at, created_at as updated_at,
        GETDATE() as last_synced_at, 1 as sync_version
    FROM LatestCreatedEvents
    WHERE rn = 1
    AND NOT EXISTS (
        SELECT 1 FROM operational.cows c 
        WHERE c.cow_id = LatestCreatedEvents.cow_id
    );

    -- Second sync: Update existing cows from cow_updated events
    WITH LatestUpdatedEvents AS (
        SELECT 
            cow_id,
            tenant_id,
            JSON_VALUE(payload, '\$.weight_kg') as weight_kg,
            JSON_VALUE(payload, '\$.status') as status,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY cow_id, tenant_id 
                ORDER BY created_at DESC
            ) as rn
        FROM operational.cow_events
        WHERE event_type = 'cow_updated'
    )
    UPDATE c
    SET 
        weight_kg = COALESCE(CAST(e.weight_kg as decimal(10,2)), c.weight_kg),
        status = COALESCE(e.status, c.status),
        updated_at = e.created_at,
        last_synced_at = GETDATE(),
        sync_version = c.sync_version + 1
    FROM operational.cows c
    INNER JOIN LatestUpdatedEvents e ON c.cow_id = e.cow_id AND c.tenant_id = e.tenant_id
    WHERE e.rn = 1 AND e.created_at > c.updated_at;

    -- Third sync: Update cows from cow_weight_recorded events
    WITH LatestWeightEvents AS (
        SELECT 
            cow_id,
            tenant_id,
            JSON_VALUE(payload, '\$.weight_kg') as weight_kg,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY cow_id, tenant_id 
                ORDER BY created_at DESC
            ) as rn
        FROM operational.cow_events
        WHERE event_type = 'cow_weight_recorded'
    )
    UPDATE c
    SET 
        weight_kg = CAST(e.weight_kg as decimal(10,2)),
        updated_at = e.created_at,
        last_synced_at = GETDATE(),
        sync_version = c.sync_version + 1
    FROM operational.cows c
    INNER JOIN LatestWeightEvents e ON c.cow_id = e.cow_id AND c.tenant_id = e.tenant_id
    WHERE e.rn = 1 AND e.created_at > c.updated_at;

    -- Fourth sync: Update cows from cow_deactivated events
    WITH LatestDeactivatedEvents AS (
        SELECT 
            cow_id,
            tenant_id,
            JSON_VALUE(payload, '\$.status') as status,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY cow_id, tenant_id 
                ORDER BY created_at DESC
            ) as rn
        FROM operational.cow_events
        WHERE event_type = 'cow_deactivated'
    )
    UPDATE c
    SET 
        status = COALESCE(e.status, 'inactive'),
        updated_at = e.created_at,
        last_synced_at = GETDATE(),
        sync_version = c.sync_version + 1
    FROM operational.cows c
    INNER JOIN LatestDeactivatedEvents e ON c.cow_id = e.cow_id AND c.tenant_id = e.tenant_id
    WHERE e.rn = 1 AND e.created_at > c.updated_at;

    -- Fifth sync: Update last_synced_at for ALL cows to show sync service is running
    UPDATE operational.cows
    SET last_synced_at = GETDATE()
    WHERE last_synced_at IS NULL 
       OR DATEDIFF(SECOND, last_synced_at, GETDATE()) > 5;
    " > /dev/null 2>&1
    
    # Count current cows
    COW_COUNT=$(docker exec cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas -h -1 -Q "SELECT COUNT(*) FROM operational.cows;" 2>/dev/null | grep -o '[0-9]\+' | head -1)
    
    # Count events
    EVENT_COUNT=$(docker exec cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas -h -1 -Q "SELECT COUNT(*) FROM operational.cow_events;" 2>/dev/null | grep -o '[0-9]\+' | head -1)
    
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${GREEN}[$TIMESTAMP]${NC} Synced: ${BLUE}${EVENT_COUNT} events${NC} → ${BLUE}${COW_COUNT} cows${NC}"
}

# Main execution
if [ "$LOOP_MODE" = true ]; then
    echo -e "${YELLOW}Starting continuous sync (interval: ${INTERVAL}s, press Ctrl+C to stop)${NC}"
    echo ""
    
    # Save PID for later termination
    echo $$ > /tmp/sync-projection.pid
    
    while true; do
        sync_projection
        sleep $INTERVAL
    done
else
    sync_projection
fi
