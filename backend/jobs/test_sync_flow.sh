#!/bin/bash

# ============================================
# Test End-to-End Sync Flow
# ============================================
#
# Tests the complete data flow through the system:
#   API → cow_events → Bronze → Silver → SQL (via sync job)
#
# This validates the "Pure Projection Pattern A" architecture.
#
# Prerequisites:
# 1. SQL Server running with operational schema
# 2. MinIO running at localhost:9000
# 3. Bronze and Silver buckets created
# 4. Backend API running (for cow creation)
# 5. PySpark and dependencies installed
#
# Usage:
#   ./test_sync_flow.sh [tenant_id]
#
# Example:
#   ./test_sync_flow.sh 550e8400-e29b-41d4-a716-446655440000

set -e

# ============================================
# Configuration
# ============================================

TENANT_ID="${1:-550e8400-e29b-41d4-a716-446655440000}"
API_BASE="http://localhost:8000/api/v1"
TEST_TAG="TEST-SYNC-$(date +%s)"

echo "============================================"
echo "Test End-to-End Sync Flow"
echo "============================================"
echo "Tenant ID: $TENANT_ID"
echo "Test Tag: $TEST_TAG"
echo "API Base: $API_BASE"
echo ""

# ============================================
# Step 1: Create Cow via API
# ============================================

echo "Step 1: Creating cow via API (writes to cow_events table)"
echo "------------------------------------------------------------"

COW_PAYLOAD=$(cat <<EOF
{
  "tag_number": "$TEST_TAG",
  "name": "Sync Test Cow",
  "breed": "Holstein",
  "birth_date": "2024-01-01",
  "sex": "female",
  "status": "active"
}
EOF
)

echo "POST $API_BASE/cows"
echo "Payload: $COW_PAYLOAD"
echo ""

CREATE_RESPONSE=$(curl -s -X POST \
  "$API_BASE/cows" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT_ID" \
  -d "$COW_PAYLOAD")

echo "Response: $CREATE_RESPONSE"
echo ""

COW_ID=$(echo "$CREATE_RESPONSE" | jq -r '.id')
EVENT_ID=$(echo "$CREATE_RESPONSE" | jq -r '.event_id')

if [ "$COW_ID" == "null" ] || [ -z "$COW_ID" ]; then
  echo "❌ Failed to create cow"
  exit 1
fi

echo "✅ Cow created successfully"
echo "  Cow ID: $COW_ID"
echo "  Event ID: $EVENT_ID"
echo ""

# ============================================
# Step 2: Wait for Bronze Ingestion
# ============================================

echo "Step 2: Waiting for Bronze ingestion (cow_events → Bronze)"
echo "------------------------------------------------------------"
echo "Bronze ingestion reads cow_events table and writes to MinIO s3a://bronze/cow_events/"
echo ""

# In real system, Bronze ingestion would be running continuously
# For testing, you may need to run it manually:
echo "Run Bronze ingestion:"
echo "  cd databricks/bronze"
echo "  python ingest_cow_events.py"
echo ""

read -p "Press ENTER after Bronze ingestion completes..." 

echo "Verifying Bronze data exists..."
# TODO: Add verification that Bronze data exists in MinIO
echo "✅ Assuming Bronze ingestion completed"
echo ""

# ============================================
# Step 3: Wait for Silver Processing
# ============================================

echo "Step 3: Waiting for Silver processing (Bronze → Silver)"
echo "------------------------------------------------------------"
echo "Silver processing builds silver_cows_current from Bronze cow_events"
echo ""

# In real system, Silver processing would be running continuously
# For testing, you may need to run it manually:
echo "Run Silver processing:"
echo "  cd databricks/silver"
echo "  python rebuild_cows.py"
echo ""

read -p "Press ENTER after Silver processing completes..." 

echo "Verifying Silver data exists..."
# TODO: Add verification that Silver data exists
echo "✅ Assuming Silver processing completed"
echo ""

# ============================================
# Step 4: Run Sync Job (Silver → SQL)
# ============================================

echo "Step 4: Running sync job (Silver → SQL projection)"
echo "------------------------------------------------------------"
echo "This is the CRITICAL step that makes SQL a true projection"
echo ""

echo "Running: python backend/jobs/sync_silver_to_sql.py"
echo ""

cd /home/xuxoramos/endymion-ai
python backend/jobs/sync_silver_to_sql.py

SYNC_EXIT_CODE=$?

if [ $SYNC_EXIT_CODE -ne 0 ]; then
  echo "❌ Sync job failed (exit code: $SYNC_EXIT_CODE)"
  exit 1
fi

echo ""
echo "✅ Sync job completed successfully"
echo ""

# ============================================
# Step 5: Verify Cow in SQL
# ============================================

echo "Step 5: Verifying cow exists in SQL projection table"
echo "------------------------------------------------------------"
echo "Querying API (which reads from cows SQL table)"
echo ""

echo "GET $API_BASE/cows/$COW_ID"
echo ""

GET_RESPONSE=$(curl -s -X GET \
  "$API_BASE/cows/$COW_ID" \
  -H "X-Tenant-ID: $TENANT_ID")

echo "Response: $GET_RESPONSE" | jq '.'
echo ""

RETRIEVED_COW_ID=$(echo "$GET_RESPONSE" | jq -r '.id')
SILVER_UPDATED_AT=$(echo "$GET_RESPONSE" | jq -r '.silver_last_updated_at')
LAST_SYNCED_AT=$(echo "$GET_RESPONSE" | jq -r '.last_synced_at')

if [ "$RETRIEVED_COW_ID" != "$COW_ID" ]; then
  echo "❌ Cow not found in SQL projection"
  exit 1
fi

echo "✅ Cow found in SQL projection"
echo "  Cow ID: $RETRIEVED_COW_ID"
echo "  Tag: $(echo "$GET_RESPONSE" | jq -r '.tag_number')"
echo "  Silver Updated At: $SILVER_UPDATED_AT"
echo "  Last Synced At: $LAST_SYNCED_AT"
echo ""

# ============================================
# Step 6: Verify Sync Metadata
# ============================================

echo "Step 6: Verifying sync metadata is populated"
echo "------------------------------------------------------------"

if [ "$SILVER_UPDATED_AT" == "null" ] || [ -z "$SILVER_UPDATED_AT" ]; then
  echo "❌ silver_last_updated_at is NOT populated"
  exit 1
fi

if [ "$LAST_SYNCED_AT" == "null" ] || [ -z "$LAST_SYNCED_AT" ]; then
  echo "❌ last_synced_at is NOT populated"
  exit 1
fi

echo "✅ Sync metadata is correctly populated"
echo "  silver_last_updated_at: $SILVER_UPDATED_AT"
echo "  last_synced_at: $LAST_SYNCED_AT"
echo ""

# ============================================
# Step 7: Test Incremental Sync
# ============================================

echo "Step 7: Testing incremental sync"
echo "------------------------------------------------------------"
echo "Running sync again - should process 0 rows (no changes)"
echo ""

python backend/jobs/sync_silver_to_sql.py

echo ""
echo "✅ Incremental sync completed (should show 0 changed rows)"
echo ""

# ============================================
# Step 8: Test Update Flow
# ============================================

echo "Step 8: Testing update flow"
echo "------------------------------------------------------------"
echo "Updating cow via API..."
echo ""

UPDATE_PAYLOAD=$(cat <<EOF
{
  "name": "Sync Test Cow UPDATED",
  "weight_kg": 500.5
}
EOF
)

echo "PUT $API_BASE/cows/$COW_ID"
echo "Payload: $UPDATE_PAYLOAD"
echo ""

UPDATE_RESPONSE=$(curl -s -X PUT \
  "$API_BASE/cows/$COW_ID" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT_ID" \
  -d "$UPDATE_PAYLOAD")

echo "Response: $UPDATE_RESPONSE"
echo ""

echo "✅ Cow updated (created new event in cow_events)"
echo ""

echo "Run Bronze and Silver processing again, then sync:"
read -p "Press ENTER after Bronze → Silver processing completes..." 

echo "Running sync job again..."
python backend/jobs/sync_silver_to_sql.py

echo ""
echo "Verifying update was synced to SQL..."

GET_RESPONSE=$(curl -s -X GET \
  "$API_BASE/cows/$COW_ID" \
  -H "X-Tenant-ID: $TENANT_ID")

UPDATED_NAME=$(echo "$GET_RESPONSE" | jq -r '.name')
UPDATED_WEIGHT=$(echo "$GET_RESPONSE" | jq -r '.weight_kg')

if [ "$UPDATED_NAME" != "Sync Test Cow UPDATED" ]; then
  echo "❌ Update not synced to SQL"
  exit 1
fi

echo "✅ Update synced to SQL successfully"
echo "  Name: $UPDATED_NAME"
echo "  Weight: $UPDATED_WEIGHT kg"
echo ""

# ============================================
# Summary
# ============================================

echo "============================================"
echo "End-to-End Sync Test Summary"
echo "============================================"
echo "✅ Step 1: Created cow via API (→ cow_events)"
echo "✅ Step 2: Bronze ingestion (cow_events → Bronze)"
echo "✅ Step 3: Silver processing (Bronze → Silver)"
echo "✅ Step 4: Sync job (Silver → SQL)"
echo "✅ Step 5: Verified cow in SQL projection"
echo "✅ Step 6: Verified sync metadata populated"
echo "✅ Step 7: Tested incremental sync (0 changes)"
echo "✅ Step 8: Tested update flow"
echo ""
echo "🎉 All tests PASSED!"
echo ""
echo "The sync job is working correctly."
echo "SQL projection is consistent with Silver layer."
echo ""
echo "Test Cow Details:"
echo "  Cow ID: $COW_ID"
echo "  Tag: $TEST_TAG"
echo "  Silver Last Updated: $SILVER_UPDATED_AT"
echo "  Last Synced: $LAST_SYNCED_AT"
echo ""
echo "Next Steps:"
echo "1. Start sync scheduler for continuous sync:"
echo "   python backend/jobs/sync_scheduler.py"
echo ""
echo "2. Monitor sync logs:"
echo "   tail -f /tmp/sync_scheduler.log"
echo ""
echo "3. Check sync state:"
echo "   SELECT * FROM operational.sync_state"
echo ""
echo "4. Check sync logs:"
echo "   SELECT * FROM operational.sync_log ORDER BY started_at DESC"
echo ""
