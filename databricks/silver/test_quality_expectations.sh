#!/bin/bash
#
# Test Silver Layer Data Quality Expectations
#
# Creates events with INVALID data to verify expectations catch them:
# 1. Invalid breed (not in approved list)
# 2. Invalid birth date (too old)
# 3. Invalid weight (negative)
# 4. Invalid status (typo)
# 5. Missing required fields
#
# Verifies:
# - DROP expectations reject critical failures
# - WARN expectations log but allow non-critical issues
# - silver_quality_log captures all issues

set -e

PROJECT_ROOT="/home/xuxoramos/endymion-ai"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Silver Layer Quality Expectations Test"
echo "=========================================="
echo

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

pass() {
    echo -e "${GREEN}✓${NC} $1"
}

fail() {
    echo -e "${RED}✗${NC} $1"
}

info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

step() {
    echo -e "\n${BLUE}==>${NC} $1"
}

# Check prerequisites
step "Checking prerequisites"

if ! docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1" > /dev/null 2>&1; then
    fail "SQL Server is not running"
    exit 1
fi
pass "SQL Server is running"

# Generate test IDs
TEST_TENANT=$(uuidgen)
info "Test Tenant: $TEST_TENANT"

# Clean up previous test data
step "Cleaning up previous test data"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
DELETE FROM cow_events WHERE tenant_id = '${TEST_TENANT}';
EOF

# ============================================================================
# CREATE VALID COW (baseline)
# ============================================================================

step "Test 1: Valid cow (baseline)"
VALID_COW=$(uuidgen)

docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, created_by, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    '${VALID_COW}',
    'cow_created',
    GETUTCDATE(),
    1,
    '{"tag_number": "VALID-001", "breed": "Angus", "sex": "female", "birth_date": "2022-01-15", "weight_kg": 450.0}',
    GETUTCDATE(),
    'test_user',
    0
);
EOF
pass "Created valid cow event"

# ============================================================================
# CREATE COWS WITH WARN-LEVEL ISSUES (should be logged but written)
# ============================================================================

step "Test 2: Invalid breed (WARN - should be logged but allowed)"
INVALID_BREED_COW=$(uuidgen)

docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, created_by, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    '${INVALID_BREED_COW}',
    'cow_created',
    GETUTCDATE(),
    1,
    '{"tag_number": "WARN-001", "breed": "AlienBreed", "sex": "female", "birth_date": "2022-01-15", "weight_kg": 450.0}',
    GETUTCDATE(),
    'test_user',
    0
);
EOF
pass "Created cow with invalid breed"

step "Test 3: Invalid birth date (WARN - too old)"
INVALID_DATE_COW=$(uuidgen)

docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, created_by, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    '${INVALID_DATE_COW}',
    'cow_created',
    GETUTCDATE(),
    1,
    '{"tag_number": "WARN-002", "breed": "Hereford", "sex": "male", "birth_date": "1995-01-15", "weight_kg": 500.0}',
    GETUTCDATE(),
    'test_user',
    0
);
EOF
pass "Created cow with invalid birth date"

step "Test 4: Invalid weight (WARN - too high)"
INVALID_WEIGHT_COW=$(uuidgen)

docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, created_by, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    '${INVALID_WEIGHT_COW}',
    'cow_created',
    GETUTCDATE(),
    1,
    '{"tag_number": "WARN-003", "breed": "Charolais", "sex": "male", "birth_date": "2021-01-15", "weight_kg": 2500.0}',
    GETUTCDATE(),
    'test_user',
    0
);
EOF
pass "Created cow with invalid weight"

# ============================================================================
# CREATE COW WITH DROP-LEVEL ISSUES (should be REJECTED)
# ============================================================================

step "Test 5: Missing tag_number (DROP - should be rejected)"
MISSING_TAG_COW=$(uuidgen)

docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, created_by, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    '${MISSING_TAG_COW}',
    'cow_created',
    GETUTCDATE(),
    1,
    '{"breed": "Angus", "sex": "female", "birth_date": "2022-01-15", "weight_kg": 450.0}',
    GETUTCDATE(),
    'test_user',
    0
);
EOF
pass "Created cow with missing tag_number"

step "Test 6: Invalid status (DROP - should be rejected)"
INVALID_STATUS_COW=$(uuidgen)

docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' << EOF > /dev/null 2>&1
USE CattleSaaS;
INSERT INTO cow_events (event_id, tenant_id, cow_id, event_type, event_timestamp, sequence_number, payload, created_at, created_by, published_to_bronze)
VALUES (
    NEWID(),
    '${TEST_TENANT}',
    '${INVALID_STATUS_COW}',
    'cow_created',
    GETUTCDATE(),
    1,
    '{"tag_number": "DROP-001", "breed": "Angus", "sex": "female", "birth_date": "2022-01-15", "weight_kg": 450.0}',
    GETUTCDATE(),
    'test_user',
    0
);

-- Update to invalid status
UPDATE cow_events
SET event_type = 'cow_updated',
    sequence_number = 2,
    payload = '{"status": "INVALID_STATUS"}'
WHERE tenant_id = '${TEST_TENANT}' AND cow_id = '${INVALID_STATUS_COW}';
EOF
pass "Created cow with invalid status"

# Verify events in SQL
EVENT_COUNT=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM CattleSaaS.dbo.cow_events WHERE tenant_id = '${TEST_TENANT}'" 2>/dev/null | tr -d '[:space:]')

if [ "$EVENT_COUNT" == "6" ]; then
    pass "Verified 6 test events in SQL Server"
else
    fail "Expected 6 events, found $EVENT_COUNT"
fi

# ============================================================================
# INGEST TO BRONZE
# ============================================================================

step "Ingesting test events to Bronze layer"
python databricks/bronze/ingest_from_sql.py --once > /tmp/quality_test_bronze.log 2>&1
pass "Bronze ingestion complete"

# ============================================================================
# RUN SILVER RESOLUTION WITH EXPECTATIONS
# ============================================================================

step "Running Silver resolution with quality expectations"
python databricks/silver/resolve_cow_state.py --full-refresh > /tmp/quality_test_silver.log 2>&1

if [ $? -eq 0 ]; then
    pass "Silver resolution completed"
else
    fail "Silver resolution failed"
    cat /tmp/quality_test_silver.log
    exit 1
fi

# ============================================================================
# VERIFY EXPECTATIONS CAUGHT ISSUES
# ============================================================================

step "Verifying expectations caught quality issues"

python -c "
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

print('\n' + '='*80)
print('QUALITY TEST RESULTS')
print('='*80)

# Check Silver history
history_df = spark.read.format('delta').load('s3a://silver/cows_history')
test_cows = history_df.filter(history_df.tenant_id == '${TEST_TENANT}')

total_in_silver = test_cows.count()
print(f'\nRecords in Silver: {total_in_silver}')

# Check quality log
try:
    log_df = spark.read.format('delta').load('s3a://silver/quality_log')
    test_log = log_df.filter(log_df.tenant_id == '${TEST_TENANT}')
    
    failures_count = test_log.count()
    print(f'Quality issues logged: {failures_count}')
    
    if failures_count > 0:
        print('\n✓ Expectations detected quality issues')
        
        # Show what was caught
        print('\nIssues by Expectation:')
        test_log.groupBy('expectation_name', 'severity').count() \
            .orderBy('expectation_name').show(truncate=False)
        
        # Show specific failures
        print('\nDetailed Failures:')
        test_log.select('cow_id', 'expectation_name', 'severity', 'failure_reason') \
            .show(truncate=False)
    else:
        print('⚠️  No quality issues logged (unexpected)')
    
except Exception as e:
    print(f'⚠️  Quality log not found: {e}')

# Verify valid cow is in Silver
valid_cow = test_cows.filter(test_cows.cow_id == '${VALID_COW}')
if valid_cow.count() > 0:
    print(f'\n✓ Valid cow (${VALID_COW}) is in Silver')
else:
    print(f'\n✗ Valid cow missing from Silver')

# Verify WARN-level cows are in Silver (logged but allowed)
warn_breed = test_cows.filter(test_cows.cow_id == '${INVALID_BREED_COW}')
warn_date = test_cows.filter(test_cows.cow_id == '${INVALID_DATE_COW}')
warn_weight = test_cows.filter(test_cows.cow_id == '${INVALID_WEIGHT_COW}')

warn_count = warn_breed.count() + warn_date.count() + warn_weight.count()
print(f'\nWARN-level cows in Silver: {warn_count}/3')
if warn_count == 3:
    print('✓ WARN expectations logged but allowed records')
else:
    print('⚠️  Some WARN-level records missing')

# Verify DROP-level cows are NOT in Silver
drop_tag = test_cows.filter(test_cows.cow_id == '${MISSING_TAG_COW}')
drop_status = test_cows.filter(test_cows.cow_id == '${INVALID_STATUS_COW}')

drop_count = drop_tag.count() + drop_status.count()
print(f'\nDROP-level cows in Silver: {drop_count}/2')
if drop_count == 0:
    print('✓ DROP expectations rejected critical failures')
else:
    print('✗ DROP-level records should have been rejected')

spark.stop()
" 2>&1 | tee /tmp/quality_test_verify.log

# ============================================================================
# GENERATE QUALITY REPORT
# ============================================================================

step "Generating quality report"
python databricks/silver/quality_report.py > /tmp/quality_test_report.log 2>&1
cat /tmp/quality_test_report.log

# ============================================================================
# SUMMARY
# ============================================================================

echo -e "\n${GREEN}=========================================="
echo "Test Summary"
echo "==========================================${NC}"
echo
echo "Created test events:"
echo "  ✓ 1 valid cow (should be in Silver)"
echo "  ✓ 3 WARN-level issues (should be in Silver with warnings)"
echo "  ✓ 2 DROP-level issues (should be REJECTED)"
echo
echo "Verification:"
if grep -q "Expectations detected quality issues" /tmp/quality_test_verify.log; then
    echo -e "  ${GREEN}✓${NC} Expectations detected quality issues"
else
    echo -e "  ${RED}✗${NC} Expectations did not detect issues"
fi

if grep -q "WARN expectations logged but allowed records" /tmp/quality_test_verify.log; then
    echo -e "  ${GREEN}✓${NC} WARN expectations logged but allowed records"
else
    echo -e "  ${YELLOW}⚠${NC}  WARN expectations behavior unclear"
fi

if grep -q "DROP expectations rejected critical failures" /tmp/quality_test_verify.log; then
    echo -e "  ${GREEN}✓${NC} DROP expectations rejected critical failures"
else
    echo -e "  ${RED}✗${NC} DROP expectations did not reject as expected"
fi

echo
echo "Logs available at:"
echo "  - /tmp/quality_test_bronze.log (Bronze ingestion)"
echo "  - /tmp/quality_test_silver.log (Silver resolution)"
echo "  - /tmp/quality_test_verify.log (Verification)"
echo "  - /tmp/quality_test_report.log (Quality report)"
echo
echo -e "${GREEN}✓ Quality Expectations Test Complete!${NC}"
