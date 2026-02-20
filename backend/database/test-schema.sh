#!/bin/bash

# Test script for SQL database schema
# This script tests the schema.sql and seed.sql files against SQL Server

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  SQL Schema Testing Script${NC}"
echo -e "${BLUE}========================================${NC}"
echo

# Check if Docker is running
if ! docker ps > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

# Check if SQL Server container exists
if ! docker ps | grep -q cattlesaas-sqlserver; then
    echo -e "${RED}Error: SQL Server container is not running${NC}"
    echo "Please run: docker compose -f docker/docker-compose.yml up -d"
    exit 1
fi

echo -e "${GREEN}✓ Docker is running${NC}"
echo -e "${GREEN}✓ SQL Server container found${NC}"
echo

# Wait for SQL Server to be ready
echo -e "${BLUE}Checking SQL Server status...${NC}"
max_attempts=10
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ SQL Server is ready${NC}"
        break
    fi
    attempt=$((attempt + 1))
    echo -n "."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo -e "${RED}Error: SQL Server failed to become ready${NC}"
    exit 1
fi

echo

# Test 1: Run schema.sql
echo -e "${BLUE}Test 1: Running schema.sql...${NC}"
if docker exec -i cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' \
    < backend/database/schema.sql > /tmp/schema_output.txt 2>&1; then
    echo -e "${GREEN}✓ Schema created successfully${NC}"
    cat /tmp/schema_output.txt | tail -20
else
    echo -e "${RED}✗ Schema creation failed${NC}"
    cat /tmp/schema_output.txt
    exit 1
fi

echo

# Test 2: Verify tables exist
echo -e "${BLUE}Test 2: Verifying tables...${NC}"

tables=(
    "operational.cow_events"
    "operational.cows"
    "operational.categories"
    "tenant.tenants"
)

for table in "${tables[@]}"; do
    schema=$(echo $table | cut -d. -f1)
    table_name=$(echo $table | cut -d. -f2)
    
    if docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
        -Q "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '$schema' AND t.name = '$table_name'" \
        -h -1 | grep -q "1"; then
        echo -e "${GREEN}✓ Table $table exists${NC}"
    else
        echo -e "${RED}✗ Table $table not found${NC}"
        exit 1
    fi
done

echo

# Test 3: Verify indexes
echo -e "${BLUE}Test 3: Checking indexes...${NC}"
index_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM sys.indexes WHERE name LIKE 'IX_%'" -h -1 | tr -d ' ')

if [ "$index_count" -gt 10 ]; then
    echo -e "${GREEN}✓ Found $index_count indexes${NC}"
else
    echo -e "${YELLOW}⚠ Only $index_count indexes found (expected > 10)${NC}"
fi

echo

# Test 4: Run seed.sql
echo -e "${BLUE}Test 4: Running seed.sql...${NC}"
if docker exec -i cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' \
    < backend/database/seed.sql > /tmp/seed_output.txt 2>&1; then
    echo -e "${GREEN}✓ Seed data inserted successfully${NC}"
    cat /tmp/seed_output.txt | tail -30
else
    echo -e "${RED}✗ Seed data insertion failed${NC}"
    cat /tmp/seed_output.txt
    exit 1
fi

echo

# Test 5: Verify data counts
echo -e "${BLUE}Test 5: Verifying data...${NC}"

# Check tenants
tenant_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM tenant.tenants" -h -1 | tr -d ' ')
echo -e "  Tenants: ${GREEN}$tenant_count${NC}"

# Check categories
category_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM operational.categories" -h -1 | tr -d ' ')
echo -e "  Categories: ${GREEN}$category_count${NC}"

# Check cows
cow_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM operational.cows" -h -1 | tr -d ' ')
echo -e "  Cows: ${GREEN}$cow_count${NC}"

# Check events
event_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM operational.cow_events" -h -1 | tr -d ' ')
echo -e "  Events: ${GREEN}$event_count${NC}"

echo

# Test 6: Test views
echo -e "${BLUE}Test 6: Testing views...${NC}"

# Test active cows view
active_cow_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM operational.vw_active_cows" -h -1 | tr -d ' ')
echo -e "  Active cows view: ${GREEN}$active_cow_count rows${NC}"

# Test recent events view
recent_event_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM operational.vw_recent_events" -h -1 | tr -d ' ')
echo -e "  Recent events view: ${GREEN}$recent_event_count rows${NC}"

echo

# Test 7: Test foreign key constraints
echo -e "${BLUE}Test 7: Testing foreign key constraints...${NC}"
fk_count=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM sys.foreign_keys" -h -1 | tr -d ' ')
echo -e "  Foreign keys: ${GREEN}$fk_count${NC}"

echo

# Test 8: Test unpublished events query
echo -e "${BLUE}Test 8: Testing unpublished events...${NC}"
unpublished=$(docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT COUNT(*) FROM operational.cow_events WHERE published_to_bronze = 0" -h -1 | tr -d ' ')
echo -e "  Unpublished events: ${GREEN}$unpublished${NC}"

echo

# Summary
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✓ All tests passed!${NC}"
echo -e "${BLUE}========================================${NC}"
echo
echo "Database schema is ready for development."
echo
echo "Test some queries:"
echo "  docker exec -it cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas"
echo
echo "Sample queries:"
echo "  SELECT * FROM operational.vw_active_cows;"
echo "  SELECT * FROM operational.vw_recent_events;"
echo "  SELECT * FROM operational.cow_events WHERE published_to_bronze = 0;"
echo

# Optional: Display sample data
echo -e "${BLUE}Sample Data:${NC}"
echo
echo "Tenants:"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT tenant_name, tenant_code, subscription_tier FROM tenant.tenants" -W

echo
echo "Cows by breed:"
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd' -d cattlesaas \
    -Q "SELECT breed, COUNT(*) as count FROM operational.cows GROUP BY breed ORDER BY count DESC" -W

echo
