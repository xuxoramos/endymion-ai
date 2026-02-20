#!/bin/bash
#
# Bronze Layer Ingestion Runner
#
# Starts the Bronze ingestion job in continuous mode.
# Press Ctrl+C to stop.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=========================================="
echo "Bronze Layer Ingestion Runner"
echo "=========================================="
echo

# Check if MinIO is running
echo "Checking MinIO..."
if ! curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "✗ MinIO is not running"
    echo
    echo "Start MinIO with:"
    echo "  cd $PROJECT_ROOT"
    echo "  docker-compose -f docker/docker-compose.yml up -d minio minio-client"
    echo
    exit 1
fi
echo "✓ MinIO is running"

# Check if SQL Server is running
echo "Checking SQL Server..."
if ! docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1" > /dev/null 2>&1; then
    echo "✗ SQL Server is not running"
    echo
    echo "Start SQL Server with:"
    echo "  cd $PROJECT_ROOT"
    echo "  docker-compose -f docker/docker-compose.yml up -d sqlserver"
    echo
    exit 1
fi
echo "✓ SQL Server is running"

# Check if Bronze table exists
echo "Checking Bronze table..."
if ! python "$SCRIPT_DIR/query_bronze.py" --stats > /dev/null 2>&1; then
    echo "✗ Bronze table not initialized"
    echo
    echo "Initialize Bronze table with:"
    echo "  python $SCRIPT_DIR/setup_bronze.py"
    echo
    exit 1
fi
echo "✓ Bronze table exists"

echo
echo "Starting ingestion job..."
echo "(Press Ctrl+C to stop)"
echo

# Run ingestion in continuous mode
cd "$PROJECT_ROOT"
python "$SCRIPT_DIR/ingest_from_sql.py" --interval 10
