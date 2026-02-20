# Bronze Layer - Quick Start Guide

Complete setup and testing guide for the Bronze layer (SQL → Delta Lake).

## Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ with PySpark
- MinIO and SQL Server running

## 1. Start Infrastructure

```bash
cd /home/xuxoramos/endymion-ai

# Start MinIO and SQL Server
docker-compose -f docker/docker-compose.yml up -d minio minio-client sqlserver

# Verify services
docker ps

# Check MinIO buckets
docker exec cattlesaas-minio-client mc ls localminio
```

Expected output:
```
[2026-01-23 12:00:00 UTC]     0B bronze/
[2026-01-23 12:00:00 UTC]     0B silver/
[2026-01-23 12:00:00 UTC]     0B gold/
```

## 2. Install Python Dependencies

```bash
# Install required packages
pip install pyspark delta-spark pyarrow sqlalchemy pyodbc minio

# Or install from requirements.txt
pip install -r requirements.txt
```

## 3. Initialize Bronze Table

```bash
# Create Delta table in MinIO
python databricks/bronze/setup_bronze.py
```

Expected output:
```
✓ Spark session created
✓ MinIO connection successful - bucket 'bronze' exists
✓ Table created successfully
✓ Table properties configured
✓ Schema fields: 14
✓ Record count: 0

✓ Bronze layer setup completed successfully!
```

## 4. Run Integration Test (Recommended)

```bash
# Run automated test suite
./databricks/bronze/test_bronze.sh
```

This will:
- ✓ Verify infrastructure
- ✓ Create test events via API
- ✓ Run ingestion
- ✓ Verify data in Bronze
- ✓ Test deduplication
- ✓ Query by tenant

**OR** follow manual testing steps below...

## 5. Manual Testing

### Step 1: Start FastAPI Backend

```bash
cd backend/api
uvicorn main:app --reload
```

### Step 2: Create Test Events

```bash
# Generate tenant ID
TENANT_ID=$(uuidgen)
echo "Test Tenant: $TENANT_ID"

# Create cow (generates cow_created event)
curl -X POST http://localhost:8000/api/v1/cows \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT_ID" \
  -d '{
    "tag_number": "US-BRONZE-001",
    "breed": "Holstein",
    "birth_date": "2022-01-15",
    "sex": "female",
    "weight_kg": 450.5
  }'
```

Expected response:
```json
{
  "status": "accepted",
  "message": "Cow created - event pending processing",
  "event_type": "cow_created",
  "cow_id": "...",
  "event_id": "..."
}
```

### Step 3: Verify Events in SQL

```bash
# Check unpublished events
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT event_id, event_type, published_to_bronze FROM CattleSaaS.dbo.cow_events"
```

### Step 4: Run Ingestion (One-Time)

```bash
cd /home/xuxoramos/endymion-ai
python databricks/bronze/ingest_from_sql.py --once
```

Expected output:
```
✓ Batch batch_20260123_120000_1
  Fetched: 1
  Duplicates: 0
  Written: 1
  Marked: 1
  Duration: 2.34s
```

### Step 5: Query Bronze Table

```bash
# Show all events
python databricks/bronze/query_bronze.py

# Show statistics
python databricks/bronze/query_bronze.py --stats

# Query by tenant
python databricks/bronze/query_bronze.py --tenant-id $TENANT_ID
```

### Step 6: Verify Deduplication

```bash
# Run ingestion again (should find no new events)
python databricks/bronze/ingest_from_sql.py --once

# Check for duplicates
python databricks/bronze/query_bronze.py --check-duplicates
```

Expected output:
```
✓ No duplicate event_ids found
```

## 6. Start Continuous Ingestion

```bash
# Run ingestion every 10 seconds
./databricks/bronze/run_ingestion.sh

# Or with custom interval
python databricks/bronze/ingest_from_sql.py --interval 30
```

## Verification Checklist

- [ ] MinIO is accessible at http://localhost:9000
- [ ] Bronze bucket exists in MinIO
- [ ] Bronze table created successfully
- [ ] Events can be created via API
- [ ] Ingestion runs without errors
- [ ] Events appear in Bronze Delta table
- [ ] Events marked as published in SQL
- [ ] No duplicate event_ids in Bronze
- [ ] Query tools work correctly

## Common Commands

```bash
# Check Bronze table stats
python databricks/bronze/query_bronze.py --stats

# View recent events
python databricks/bronze/query_bronze.py --limit 10

# Check MinIO buckets
docker exec cattlesaas-minio-client mc ls localminio/bronze

# Check SQL unpublished events
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT COUNT(*) FROM CattleSaaS.dbo.cow_events WHERE published_to_bronze = 0"

# View ingestion logs
tail -f /tmp/bronze_ingest.log
```

## Troubleshooting

### "MinIO connection failed"

```bash
# Check MinIO status
docker ps | grep minio

# Restart MinIO
docker-compose -f docker/docker-compose.yml restart minio

# Test endpoint
curl http://localhost:9000/minio/health/live
```

### "Bronze table not found"

```bash
# Reinitialize table
python databricks/bronze/setup_bronze.py
```

### "SQL connection failed"

```bash
# Check SQL Server
docker logs cattlesaas-sqlserver

# Test connection
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1"
```

### "No events fetched"

```bash
# Create test event
curl -X POST http://localhost:8000/api/v1/cows \
  -H "X-Tenant-ID: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"tag_number":"TEST-001","breed":"Holstein","birth_date":"2022-01-15","sex":"female"}'

# Verify in SQL
docker exec cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT * FROM CattleSaaS.dbo.cow_events WHERE published_to_bronze = 0"
```

## Next Steps

After Bronze layer is operational:

1. **Monitor Ingestion**: Keep ingestion running in background
2. **Silver Layer**: Transform and clean Bronze events
3. **Gold Layer**: Create business aggregations
4. **Production Setup**: Configure orchestration (Airflow/Databricks Jobs)

## Files Reference

- `setup_bronze.py` - Initialize Delta table
- `ingest_from_sql.py` - SQL → Delta ingestion job
- `query_bronze.py` - Query and inspect Bronze data
- `run_ingestion.sh` - Continuous ingestion runner
- `test_bronze.sh` - Integration test suite
- `README.md` - Detailed documentation

## Support

For issues or questions:
1. Check logs in `/tmp/bronze_*.log`
2. Review detailed documentation: `databricks/bronze/README.md`
3. Verify all services are running: `docker ps`
