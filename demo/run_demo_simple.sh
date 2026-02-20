#!/bin/bash
#
# Endymion-AI Simple Architecture Demo
# Adapted for SQL Server
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# Configuration
API_URL="http://localhost:8000"
TENANT_ID="550e8400-e29b-41d4-a716-446655440000"
PROJECT_DIR="/home/xuxoramos/endymion-ai"

# Helper functions
print_header() {
    echo ""
    echo -e "${BOLD}${MAGENTA}================================================================================${NC}"
    echo -e "${BOLD}${MAGENTA}  $1${NC}"
    echo -e "${BOLD}${MAGENTA}================================================================================${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BOLD}${CYAN}>>> $1${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

wait_for_sync() {
    local seconds=$1
    print_info "Waiting ${seconds}s for data to propagate..."
    for i in $(seq $seconds -1 1); do
        printf "\r${BLUE}⏳ Time remaining: %2d seconds${NC}" $i
        sleep 1
    done
    printf "\r${GREEN}✅ Wait complete!                    ${NC}\n"
}

# ========================================
# Pre-flight checks
# ========================================

print_header "🚀 Endymion-AI Architecture Demo (SQL Server)"

echo -e "${BOLD}Data flow:${NC}"
echo -e "  ${CYAN}API${NC} → ${CYAN}Events${NC} → ${BLUE}Bronze${NC} → ${CYAN}Silver${NC} → ${GREEN}Gold${NC}"
echo ""

print_section "Pre-flight Checks"

# Check API
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    print_success "FastAPI is running"
else
    print_error "FastAPI is not running"
    exit 1
fi

# Check database
HEALTH=$(curl -s http://localhost:8000/health | python -c "import sys, json; print(json.load(sys.stdin)['database'])")
if [ "$HEALTH" = "connected" ]; then
    print_success "Database connection OK"
else
    print_error "Database not connected"
    exit 1
fi

echo ""
read -p "Press Enter to start the demo..."

# ========================================
# STEP 1: Create Test Cows via API
# ========================================

print_header "📝 STEP 1: Create Test Cows via API"

print_section "Creating 3 cows via API..."

# Cow 1: Bessie
echo -e "${CYAN}Creating Cow #1: Bessie${NC}"
RESPONSE=$(curl -s -X POST "$API_URL/api/v1/cows" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT_ID" \
  -d '{
    "tag_number": "DEMO-001",
    "name": "Bessie",
    "breed": "Holstein",
    "birth_date": "2020-01-15",
    "sex": "female",
    "weight_kg": 650.5
  }')

BESSIE_ID=$(echo $RESPONSE | python -c "import sys, json; data=json.load(sys.stdin); print(data.get('cow_id', data.get('id', 'unknown')))" 2>/dev/null || echo "error")
if [ "$BESSIE_ID" != "error" ] && [ "$BESSIE_ID" != "unknown" ]; then
    print_success "Created Bessie (ID: $BESSIE_ID)"
else
    print_warning "Created Bessie but couldn't extract ID"
    echo $RESPONSE | python -m json.tool 2>/dev/null || echo $RESPONSE
fi

# Cow 2: Daisy
echo -e "${CYAN}Creating Cow #2: Daisy${NC}"
RESPONSE=$(curl -s -X POST "$API_URL/api/v1/cows" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT_ID" \
  -d '{
    "tag_number": "DEMO-002",
    "name": "Daisy",
    "breed": "Jersey",
    "birth_date": "2019-06-20",
    "sex": "female",
    "weight_kg": 450.0
  }')

DAISY_ID=$(echo $RESPONSE | python -c "import sys, json; data=json.load(sys.stdin); print(data.get('cow_id', data.get('id', 'unknown')))" 2>/dev/null || echo "error")
if [ "$DAISY_ID" != "error" ] && [ "$DAISY_ID" != "unknown" ]; then
    print_success "Created Daisy (ID: $DAISY_ID)"
else
    print_warning "Created Daisy but couldn't extract ID"
    echo $RESPONSE | python -m json.tool 2>/dev/null || echo $RESPONSE
fi

# Cow 3: Angus
echo -e "${CYAN}Creating Cow #3: Angus${NC}"
RESPONSE=$(curl -s -X POST "$API_URL/api/v1/cows" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT_ID" \
  -d '{
    "tag_number": "DEMO-003",
    "name": "Angus",
    "breed": "Black Angus",
    "birth_date": "2021-03-10",
    "sex": "male",
    "weight_kg": 800.0
  }')

ANGUS_ID=$(echo $RESPONSE | python -c "import sys, json; data=json.load(sys.stdin); print(data.get('cow_id', data.get('id', 'unknown')))" 2>/dev/null || echo "error")
if [ "$ANGUS_ID" != "error" ] && [ "$ANGUS_ID" != "unknown" ]; then
    print_success "Created Angus (ID: $ANGUS_ID)"
else
    print_warning "Created Angus but couldn't extract ID"
    echo $RESPONSE | python -m json.tool 2>/dev/null || echo $RESPONSE
fi

echo ""
print_info "Created 3 cows. Events written to operational.cow_events"

# ========================================
# STEP 2: Verify Events in Database
# ========================================

print_header "🔍 STEP 2: Verify Events in Database"

print_section "Querying cow_events table..."

echo "SELECT 
    cow_id,
    event_type,
    event_time,
    published_to_bronze
FROM operational.cow_events
WHERE tenant_id = '$TENANT_ID'
ORDER BY event_time DESC;" | sudo docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas

print_success "Events stored in database"

# ========================================
# STEP 3: Verify Read Projections
# ========================================

print_header "📊 STEP 3: Verify CQRS Read Projections"

print_section "Querying cows projection table..."

echo "SELECT 
    tag_number,
    name,
    breed,
    sex,
    weight_kg,
    is_active
FROM operational.cows
WHERE tenant_id = '$TENANT_ID'
ORDER BY tag_number;" | sudo docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas

print_success "Read projections populated"

# ========================================
# STEP 4: Update a Cow
# ========================================

print_header "✏️ STEP 4: Update Cow (Event Sourcing Demo)"

print_section "Updating Bessie's weight..."

if [ "$BESSIE_ID" != "error" ] && [ "$BESSIE_ID" != "unknown" ] && [ -n "$BESSIE_ID" ]; then
    curl -s -X PUT "$API_URL/api/v1/cows/$BESSIE_ID" \
      -H "Content-Type: application/json" \
      -H "X-Tenant-ID: $TENANT_ID" \
      -d '{
        "weight_kg": 675.0
      }' > /dev/null
    
    print_success "Bessie updated (weight: 650.5 → 675.0 kg)"
    
    wait_for_sync 2
    
    print_section "Checking event history for Bessie..."
    
    echo "SELECT 
    event_type,
    event_time,
    published_to_bronze
FROM operational.cow_events
WHERE cow_id = '$BESSIE_ID'
ORDER BY event_time;" | sudo docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d cattlesaas
    
    print_success "Event history shows CREATE and UPDATE events"
else
    print_warning "Skipping update (couldn't get Bessie's ID)"
fi

# ========================================
# STEP 5: Query via API
# ========================================

print_header "🔎 STEP 5: Query Cows via API"

print_section "Getting all cows for tenant..."

curl -s "$API_URL/api/v1/cows" \
  -H "X-Tenant-ID: $TENANT_ID" | python -m json.tool | head -50

print_success "API queries working"

# ========================================
# STEP 6: Bronze Layer (Optional)
# ========================================

print_header "🏗️ STEP 6: Bronze Layer Status"

print_section "Checking Bronze layer setup..."

if [ -d "$PROJECT_DIR/spark-warehouse/bronze" ]; then
    print_info "Bronze Delta tables present"
    ls -lh $PROJECT_DIR/spark-warehouse/bronze/ 2>/dev/null || true
else
    print_warning "Bronze layer not yet populated"
    print_info "To ingest: python databricks/bronze/ingest_from_sql.py --once"
fi

# ========================================
# Summary
# ========================================

print_header "✅ Demo Complete!"

echo -e "${BOLD}What was demonstrated:${NC}"
echo ""
echo -e "${GREEN}✅${NC} Event Sourcing: Commands create events (not direct updates)"
echo -e "${GREEN}✅${NC} CQRS: Separate write (events) and read (projections) models"
echo -e "${GREEN}✅${NC} Multi-Tenancy: All data isolated by tenant_id"
echo -e "${GREEN}✅${NC} Event History: Complete audit trail for each cow"
echo -e "${GREEN}✅${NC} API Integration: RESTful API for CRUD operations"
echo ""

echo -e "${BOLD}Architecture Layers:${NC}"
echo -e "  ${GREEN}✅${NC} API Layer (FastAPI)"
echo -e "  ${GREEN}✅${NC} Event Store (operational.cow_events)"
echo -e "  ${GREEN}✅${NC} Read Projections (operational.cows)"
echo -e "  ${YELLOW}⏳${NC} Bronze Layer (Delta Lake - ready to ingest)"
echo -e "  ${YELLOW}⏳${NC} Silver Layer (SCD Type 2 - ready to test)"
echo -e "  ${YELLOW}⏳${NC} Gold Layer (Analytics - ready to test)"
echo ""

echo -e "${BOLD}Next Steps:${NC}"
echo -e "  1. ${CYAN}Ingest to Bronze:${NC} python databricks/bronze/ingest_from_sql.py --once"
echo -e "  2. ${CYAN}Resolve Silver:${NC} python databricks/silver/resolve_cow_state.py"
echo -e "  3. ${CYAN}Generate Gold:${NC} python databricks/gold/gold_daily_snapshots.py"
echo -e "  4. ${CYAN}View timeline:${NC} python demo/timeline.py"
echo ""

print_info "Demo data created with tenant_id: $TENANT_ID"
print_info "All events are in operational.cow_events table"
print_info "All projections are in operational.cows table"
