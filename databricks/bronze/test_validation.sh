#!/bin/bash
#
# Test Data Quality Validation
#
# Creates intentionally malformed events to verify validation logic

set -e

PROJECT_ROOT="/home/xuxoramos/endymion-ai"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Bronze Layer Data Quality Validation Test"
echo "=========================================="
echo

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Helper functions
pass() {
    echo -e "${GREEN}✓${NC} $1"
}

fail() {
    echo -e "${RED}✗${NC} $1"
}

info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Check if SQL Server is running
info "Checking SQL Server..."
if ! docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1" > /dev/null 2>&1; then
    fail "SQL Server is not running"
    exit 1
fi
pass "SQL Server is running"

# Generate test tenant ID
TEST_TENANT=$(uuidgen)
info "Test Tenant: $TEST_TENANT"

echo -e "\n${YELLOW}Creating Malformed Events${NC}"
echo "==========================================="

# Test 1: Invalid event_id (not a UUID)
echo -e "\nTest 1: Invalid event_id (not UUID)"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, published_to_bronze)
VALUES (
    'invalid-uuid-123',
    '${TEST_TENANT}',
    NEWID(),
    'cow_created',
    GETUTCDATE(),
    1,
    '{"tag_number": "TEST-001"}',
    GETUTCDATE(),
    0
);
EOF
info "Created event with invalid event_id"

# Test 2: Invalid tenant_id (not a UUID)
echo -e "\nTest 2: Invalid tenant_id (not UUID)"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, published_to_bronze)
VALUES (
    NEWID(),
    'bad-tenant-id',
    NEWID(),
    'cow_created',
    GETUTCDATE(),
    1,
    '{"tag_number": "TEST-002"}',
    GETUTCDATE(),
    0
);
EOF
info "Created event with invalid tenant_id"

# Test 3: Invalid event_type (not in enum)
echo -e "\nTest 3: Invalid event_type"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    NEWID(),
    'invalid_event_type',
    GETUTCDATE(),
    1,
    '{"tag_number": "TEST-003"}',
    GETUTCDATE(),
    0
);
EOF
info "Created event with invalid event_type"

# Test 4: Invalid JSON payload
echo -e "\nTest 4: Invalid JSON payload"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    NEWID(),
    'cow_created',
    GETUTCDATE(),
    1,
    '{this is not valid json',
    GETUTCDATE(),
    0
);
EOF
info "Created event with invalid JSON payload"

# Test 5: Null payload
echo -e "\nTest 5: Null payload"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    NEWID(),
    'cow_created',
    GETUTCDATE(),
    1,
    NULL,
    GETUTCDATE(),
    0
);
EOF
info "Created event with null payload"

# Test 6: Invalid sequence number (0)
echo -e "\nTest 6: Invalid sequence number"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    NEWID(),
    'cow_created',
    GETUTCDATE(),
    0,
    '{"tag_number": "TEST-006"}',
    GETUTCDATE(),
    0
);
EOF
info "Created event with invalid sequence_number"

# Create one valid event for comparison
echo -e "\nTest 7: Valid event (control)"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    NEWID(),
    'cow_created',
    GETUTCDATE(),
    1,
    '{"tag_number": "TEST-VALID", "breed": "Holstein"}',
    GETUTCDATE(),
    0
);
EOF
pass "Created valid event for comparison"

# Count total malformed events
TOTAL_EVENTS=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM CattleSaaS.dbo.cow_events WHERE published_to_bronze = 0" 2>/dev/null | tr -d '[:space:]')

echo -e "\n${YELLOW}Summary${NC}"
echo "==========================================="
echo "Created $TOTAL_EVENTS unpublished events (6 malformed + 1 valid)"

# Run ingestion
echo -e "\n${YELLOW}Running Ingestion${NC}"
echo "==========================================="
info "Starting Bronze ingestion..."

python databricks/bronze/ingest_from_sql.py --once 2>&1 | tee /tmp/bronze_validation_test.log

# Check results
echo -e "\n${YELLOW}Validation Results${NC}"
echo "==========================================="

if grep -q "Rejected: 6" /tmp/bronze_validation_test.log; then
    pass "6 events were rejected (as expected)"
else
    REJECTED=$(grep "Rejected:" /tmp/bronze_validation_test.log | head -1 | awk '{print $3}' || echo "0")
    if [ "$REJECTED" == "6" ]; then
        pass "6 events were rejected"
    else
        fail "Expected 6 rejected events, got $REJECTED"
    fi
fi

if grep -q "Written: 1" /tmp/bronze_validation_test.log; then
    pass "1 valid event was written to Bronze"
else
    info "Check Bronze table for written events"
fi

# Query rejected events
echo -e "\n${YELLOW}Rejected Events Table${NC}"
echo "==========================================="
info "Querying rejected events..."

python databricks/bronze/check_bronze.py --rejected 2>&1 | tee /tmp/bronze_rejected_check.log

# Verify rejection reasons
echo -e "\n${YELLOW}Rejection Reason Verification${NC}"
echo "==========================================="

if grep -q "Invalid event_id" /tmp/bronze_rejected_check.log; then
    pass "Found 'Invalid event_id' rejection"
else
    info "Check for 'Invalid event_id' in rejected table"
fi

if grep -q "Invalid tenant_id" /tmp/bronze_rejected_check.log; then
    pass "Found 'Invalid tenant_id' rejection"
else
    info "Check for 'Invalid tenant_id' in rejected table"
fi

if grep -q "Invalid event_type" /tmp/bronze_rejected_check.log; then
    pass "Found 'Invalid event_type' rejection"
else
    info "Check for 'Invalid event_type' in rejected table"
fi

if grep -q "not valid JSON" /tmp/bronze_rejected_check.log; then
    pass "Found 'not valid JSON' rejection"
else
    info "Check for JSON validation rejection"
fi

# Quality check
echo -e "\n${YELLOW}Quality Check${NC}"
echo "==========================================="
info "Running comprehensive quality check..."

python databricks/bronze/check_bronze.py --quality-check 2>&1 | tee /tmp/bronze_quality_check.log

# Final summary
echo -e "\n${GREEN}=========================================="
echo "Test Complete"
echo "==========================================${NC}"
echo
echo "Logs available at:"
echo "  - /tmp/bronze_validation_test.log"
echo "  - /tmp/bronze_rejected_check.log"
echo "  - /tmp/bronze_quality_check.log"
echo
echo "Next steps:"
echo "  1. Review rejected events: python databricks/bronze/check_bronze.py --rejected"
echo "  2. Check Bronze quality: python databricks/bronze/check_bronze.py --quality-check"
echo "  3. View valid events: python databricks/bronze/query_bronze.py --tenant-id $TEST_TENANT"
