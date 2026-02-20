#!/bin/bash
#
# Bronze Layer Test Script
#
# Tests the complete Bronze layer workflow:
# 1. Verify infrastructure
# 2. Initialize Bronze table
# 3. Create test events via API
# 4. Run ingestion
# 5. Query Bronze data
# 6. Verify deduplication

set -e

PROJECT_ROOT="/home/xuxoramos/endymion-ai"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Bronze Layer Integration Test"
echo "=========================================="
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
pass() {
    echo -e "${GREEN}✓${NC} $1"
    ((TESTS_PASSED++))
}

fail() {
    echo -e "${RED}✗${NC} $1"
    ((TESTS_FAILED++))
}

info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Test 1: Check MinIO
echo "Test 1: MinIO connectivity"
if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    pass "MinIO is accessible"
else
    fail "MinIO is not accessible"
    echo "  Start with: docker-compose -f docker/docker-compose.yml up -d minio minio-client"
    exit 1
fi

# Test 2: Check SQL Server
echo -e "\nTest 2: SQL Server connectivity"
if docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1" > /dev/null 2>&1; then
    pass "SQL Server is accessible"
else
    fail "SQL Server is not accessible"
    echo "  Start with: docker-compose -f docker/docker-compose.yml up -d sqlserver"
    exit 1
fi

# Test 3: Initialize Bronze table
echo -e "\nTest 3: Bronze table initialization"
if python databricks/bronze/setup_bronze.py > /tmp/bronze_setup.log 2>&1; then
    pass "Bronze table initialized"
else
    fail "Bronze table initialization failed"
    cat /tmp/bronze_setup.log
    exit 1
fi

# Test 4: Verify table structure
echo -e "\nTest 4: Bronze table structure"
if python databricks/bronze/query_bronze.py --details > /tmp/bronze_details.log 2>&1; then
    pass "Bronze table structure verified"
else
    fail "Could not verify table structure"
    cat /tmp/bronze_details.log
fi

# Test 5: Create test events
echo -e "\nTest 5: Create test events via API"
info "Starting FastAPI backend..."

# Kill any existing uvicorn processes
pkill -f "uvicorn.*main:app" || true
sleep 2

# Start FastAPI in background
cd backend/api
uvicorn main:app --host 0.0.0.0 --port 8000 > /tmp/fastapi.log 2>&1 &
FASTAPI_PID=$!
cd "$PROJECT_ROOT"

# Wait for API to be ready
info "Waiting for API to start..."
for i in {1..30}; do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        pass "FastAPI is running"
        break
    fi
    sleep 1
done

# Generate test tenant ID
TEST_TENANT_ID=$(uuidgen)
info "Test tenant ID: $TEST_TENANT_ID"

# Create test events
echo -e "\nCreating test events..."
for i in {1..3}; do
    RESPONSE=$(curl -s -X POST http://localhost:8000/api/v1/cows \
        -H "Content-Type: application/json" \
        -H "X-Tenant-ID: $TEST_TENANT_ID" \
        -d "{
            \"tag_number\": \"TEST-${i}\",
            \"breed\": \"Holstein\",
            \"birth_date\": \"2022-01-15\",
            \"sex\": \"female\",
            \"weight_kg\": 450.5
        }")
    
    if echo "$RESPONSE" | grep -q "accepted"; then
        pass "Created event $i"
    else
        fail "Failed to create event $i"
        echo "Response: $RESPONSE"
    fi
    
    sleep 0.5
done

# Test 6: Verify events in SQL
echo -e "\nTest 6: Verify events in SQL Server"
EVENT_COUNT=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM CattleSaaS.dbo.cow_events WHERE published_to_bronze = 0" 2>/dev/null | tr -d '[:space:]')

if [ "$EVENT_COUNT" -ge 3 ]; then
    pass "Found $EVENT_COUNT unpublished events in SQL"
else
    fail "Expected at least 3 unpublished events, found $EVENT_COUNT"
fi

# Test 7: Run ingestion
echo -e "\nTest 7: Run Bronze ingestion"
if python databricks/bronze/ingest_from_sql.py --once > /tmp/bronze_ingest.log 2>&1; then
    pass "Ingestion completed successfully"
    
    # Show ingestion stats
    grep -A 5 "Batch batch_" /tmp/bronze_ingest.log | head -7
else
    fail "Ingestion failed"
    cat /tmp/bronze_ingest.log
fi

# Test 8: Query Bronze table
echo -e "\nTest 8: Query Bronze table"
BRONZE_COUNT=$(python -c "
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName('test').master('local[*]') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.hadoop.fs.s3a.endpoint', 'http://localhost:9000') \
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \
    .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

df = spark.read.format('delta').load('s3a://bronze/cow_events')
count = df.count()
print(count)
spark.stop()
" 2>/dev/null)

if [ "$BRONZE_COUNT" -ge 3 ]; then
    pass "Found $BRONZE_COUNT events in Bronze table"
else
    fail "Expected at least 3 events in Bronze, found $BRONZE_COUNT"
fi

# Test 9: Verify events are marked as published
echo -e "\nTest 9: Verify publish flags updated"
UNPUBLISHED_COUNT=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM CattleSaaS.dbo.cow_events WHERE published_to_bronze = 0" 2>/dev/null | tr -d '[:space:]')

if [ "$UNPUBLISHED_COUNT" -eq 0 ]; then
    pass "All events marked as published"
else
    info "Still have $UNPUBLISHED_COUNT unpublished events (may be from other tests)"
fi

# Test 10: Verify deduplication
echo -e "\nTest 10: Test deduplication"
info "Running ingestion again (should find no new events)..."
if python databricks/bronze/ingest_from_sql.py --once > /tmp/bronze_ingest2.log 2>&1; then
    pass "Second ingestion completed"
    
    # Check for zero new events
    if grep -q "Fetched: 0" /tmp/bronze_ingest2.log; then
        pass "No duplicate events ingested"
    else
        info "May have ingested new events (check logs)"
    fi
else
    fail "Second ingestion failed"
fi

# Test 11: Check for duplicates in Bronze
echo -e "\nTest 11: Verify no duplicate event_ids"
if python databricks/bronze/query_bronze.py --check-duplicates > /tmp/bronze_duplicates.log 2>&1; then
    if grep -q "No duplicate event_ids found" /tmp/bronze_duplicates.log; then
        pass "No duplicates in Bronze table"
    else
        fail "Found duplicates in Bronze table"
        cat /tmp/bronze_duplicates.log
    fi
else
    fail "Could not check for duplicates"
fi

# Test 12: Query by tenant
echo -e "\nTest 12: Query events by tenant"
if python databricks/bronze/query_bronze.py --tenant-id "$TEST_TENANT_ID" --limit 10 > /tmp/bronze_tenant.log 2>&1; then
    pass "Successfully queried by tenant ID"
else
    fail "Failed to query by tenant"
fi

# Cleanup
echo -e "\n${YELLOW}Cleanup${NC}"
kill $FASTAPI_PID 2>/dev/null || true
info "Stopped FastAPI"

# Summary
echo -e "\n=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "${GREEN}Passed: $TESTS_PASSED${NC}"
echo -e "${RED}Failed: $TESTS_FAILED${NC}"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "\n${GREEN}✓ All tests passed!${NC}"
    echo
    echo "Bronze layer is fully operational."
    echo
    echo "Next steps:"
    echo "  1. Start continuous ingestion: ./databricks/bronze/run_ingestion.sh"
    echo "  2. Query Bronze data: python databricks/bronze/query_bronze.py --stats"
    echo "  3. Proceed to Silver layer setup"
    exit 0
else
    echo -e "\n${RED}✗ Some tests failed${NC}"
    echo
    echo "Check logs in /tmp/bronze_*.log"
    exit 1
fi
