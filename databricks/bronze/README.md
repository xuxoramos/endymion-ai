# Bronze Layer - Event Ingestion

The Bronze layer ingests immutable events from SQL Server into Delta Lake on MinIO, forming the foundation of the medallion architecture.

## Architecture

```
SQL Server (cow_events)
         ↓
    [Ingestion Job]
         ↓
   MinIO (Bronze bucket)
         ↓
   Delta Lake Table
  (bronze_cow_events)
```

## Features

- **Event Sourcing**: Append-only immutable events
- **Deduplication**: Automatic handling of duplicate event_ids
- **Partitioning**: By tenant_id and partition_date (YYYY-MM-DD)
- **Atomic Publishing**: Events marked as published only after successful write
- **Change Data Feed**: Enabled for downstream consumption
- **Schema Enforcement**: Strong typing and validation
- **S3A Integration**: Hadoop AWS 3.3.1 + AWS SDK Bundle 1.11.901
- **PySpark**: Version 3.5.3 with explicit JAR configuration

## Table Schema

```
bronze_cow_events
├── event_id (String, PK)
├── tenant_id (String, Partition)
├── cow_id (String)
├── event_type (String)
├── event_time (Timestamp)              # Maps from SQL Server event_time
├── sequence_number (Integer)           # Set to 0 (SQL schema doesn't have this)
├── payload (String, JSON)
├── created_by (String)
├── created_at (Timestamp)
├── published_to_bronze (Boolean)
├── published_at (Timestamp)            # Maps from SQL Server published_to_bronze_at
├── ingested_at (Timestamp)
├── ingestion_batch_id (String)
└── partition_date (String, Partition)
```

## Setup

### 1. Start Infrastructure

```bash
# Start MinIO and SQL Server
cd /home/xuxoramos/endymion-ai
docker-compose -f docker/docker-compose.yml up -d minio minio-client sqlserver

# Verify services are running
docker ps
```

### 2. Initialize Bronze Table

```bash
# Create Delta table structure
python databricks/bronze/setup_bronze.py
```

This will:
- Create `bronze_cow_events` Delta table in MinIO
- Configure append-only mode
- Set up partitioning by tenant_id and date
- Enable Change Data Feed

### 3. Verify Setup

```bash
# Check table statistics
python databricks/bronze/query_bronze.py --stats

# Show table details
python databricks/bronze/query_bronze.py --details
```

## Usage

### Continuous Ingestion

The recommended way to run ingestion in development:

```bash
# Start continuous ingestion (polls every 10 seconds)
./databricks/bronze/run_ingestion.sh

# Or run directly
python databricks/bronze/ingest_from_sql.py
```

### One-Time Ingestion

For testing or manual backfills:

```bash
# Run once and exit
python databricks/bronze/ingest_from_sql.py --once

# Custom poll interval (seconds)
python databricks/bronze/ingest_from_sql.py --interval 30
```

### Querying Bronze Data

```bash
# Show recent events (default: 100)
python databricks/bronze/query_bronze.py

# Limit results
python databricks/bronze/query_bronze.py --limit 10

# Filter by tenant
python databricks/bronze/query_bronze.py --tenant-id <uuid>

# Show specific event details
python databricks/bronze/query_bronze.py --event-id <uuid>

# Check for duplicates
python databricks/bronze/query_bronze.py --check-duplicates

# Table statistics
python databricks/bronze/query_bronze.py --stats
```

## Testing Workflow

### 1. Create Events via API

```bash
# Start FastAPI backend
cd /home/xuxoramos/endymion-ai/backend/api
uvicorn main:app --reload

# Create cow (generates cow_created event)
curl -X POST http://localhost:8000/api/v1/cows \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $(uuidgen)" \
  -d '{
    "tag_number": "US-TEST-001",
    "breed": "Holstein",
    "birth_date": "2022-01-15",
    "sex": "female",
    "weight_kg": 450.5
  }'
```

### 2. Verify Events in SQL

```bash
# Check unpublished events
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT event_id, event_type, published_to_bronze FROM CattleSaaS.dbo.cow_events"
```

### 3. Run Ingestion

```bash
# Run one batch
python databricks/bronze/ingest_from_sql.py --once
```

Expected output:
```
✓ Batch batch_20260123_123456_1
  Fetched: 1
  Duplicates: 0
  Written: 1
  Marked: 1
  Duration: 2.34s
```

### 4. Query Bronze Table

```bash
# Show events
python databricks/bronze/query_bronze.py --limit 10

# Check statistics
python databricks/bronze/query_bronze.py --stats
```

### 5. Verify Deduplication

```bash
# Run ingestion again (should find no new events)
python databricks/bronze/ingest_from_sql.py --once

# Verify no duplicates in Bronze
python databricks/bronze/query_bronze.py --check-duplicates
```

## Configuration

Environment variables (optional):

```bash
# MinIO Configuration
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"

# SQL Server Configuration
export SQL_SERVER="localhost"
export SQL_DATABASE="CattleSaaS"
export SQL_USER="sa"
export SQL_PASSWORD="YourStrong!Passw0rd"
```

## Monitoring

### Ingestion Metrics

The ingestion job reports:
- Events fetched per batch
- Duplicate events skipped
- Events written to Bronze
- Events marked as published
- Batch duration

### Common Issues

**Issue: "MinIO connection failed"**
```bash
# Check MinIO is running
docker ps | grep minio

# Test MinIO endpoint
curl http://localhost:9000/minio/health/live

# Verify buckets exist
docker exec cattlesaas-minio-client mc ls localminio
```

**Issue: "Bronze table not found"**
```bash
# Run setup
python databricks/bronze/setup_bronze.py
```

**Issue: "SQL connection failed"**
```bash
# Check SQL Server
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1"
```

**Issue: "No events fetched"**
```bash
# Verify unpublished events exist
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT COUNT(*) FROM CattleSaaS.dbo.cow_events WHERE published_to_bronze = 0"
```

## Performance Tuning

### Batch Size

Adjust `BATCH_SIZE` in [ingest_from_sql.py](ingest_from_sql.py):

```python
BATCH_SIZE = 1000  # Events per batch
```

Recommendations:
- Development: 100-1000 events
- Production: 5000-10000 events

### Poll Interval

```bash
# Faster polling (5 seconds)
python databricks/bronze/ingest_from_sql.py --interval 5

# Slower polling (60 seconds)
python databricks/bronze/ingest_from_sql.py --interval 60
```

### Spark Configuration

Adjust in setup/ingestion scripts:

```python
.config("spark.sql.shuffle.partitions", "4")  # Increase for larger datasets
.config("spark.default.parallelism", "4")     # Match CPU cores
```

## Delta Lake Operations

### Optimize Table

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("OPTIMIZE bronze_cow_events")
```

### Vacuum Old Files

```python
# Remove files older than 7 days (retention period)
spark.sql("VACUUM bronze_cow_events RETAIN 168 HOURS")
```

### View History

```python
spark.sql("DESCRIBE HISTORY bronze_cow_events").show()
```

## Next Steps

After Bronze layer is operational:

1. **Silver Layer**: Transform and clean Bronze events
2. **Gold Layer**: Create business-level aggregations
3. **Monitoring**: Set up alerts for failed ingestions
4. **Orchestration**: Use Airflow/Prefect for production scheduling

## Files

- `setup_bronze.py` - Initialize Delta table
- `ingest_from_sql.py` - Ingestion job (SQL → Delta)
- `query_bronze.py` - Query and inspect Bronze data
- `run_ingestion.sh` - Continuous ingestion runner
- `README.md` - This file
