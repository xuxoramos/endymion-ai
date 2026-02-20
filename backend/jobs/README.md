# Sync Jobs - Quick Reference

## Overview

Background jobs that synchronize Silver layer (Delta Lake) to SQL projection tables.

This is the **CRITICAL** component in Pure Projection Pattern A that makes SQL a true projection.

## Files

```
backend/jobs/
├── sync_silver_to_sql.py       # Core sync logic (single run)
├── sync_scheduler.py            # Background scheduler (runs continuously)
├── test_sync_flow.sh            # End-to-end test script
├── SYNC_JOB_DOCUMENTATION.md    # Full documentation
└── README.md                    # This file
```

## Quick Start

### Run Sync Once

```bash
cd /home/xuxoramos/endymion-ai
python backend/jobs/sync_silver_to_sql.py
```

### Run Scheduler (Continuous)

```bash
python backend/jobs/sync_scheduler.py
```

**Stop**: `Ctrl+C` or `kill -SIGTERM <pid>`

### Run End-to-End Test

```bash
cd backend/jobs
./test_sync_flow.sh [tenant_id]
```

## What It Does

```
┌──────────────┐
│ Silver Layer │ ─┐
│ (Delta Lake) │  │
└──────────────┘  │
                  │ Sync Job
                  │ (30s interval)
                  │
┌──────────────┐  │
│ SQL Server   │ ←┘
│ (Projection) │
└──────────────┘
```

**Algorithm**:
1. Read sync watermark (last processed timestamp)
2. Query Silver: `WHERE last_updated_at > watermark`
3. UPSERT changed rows to SQL
4. Resolve conflicts (Silver wins)
5. Update watermark

## Key Features

✅ **Incremental Sync** - Only processes changed rows
✅ **Idempotent** - Safe to retry, run multiple times
✅ **Conflict Resolution** - Silver always wins
✅ **Batch Processing** - Handles large datasets
✅ **Error Handling** - Retry with exponential backoff
✅ **Monitoring** - Logs all sync runs and conflicts

## Configuration

Environment variables for scheduler:

```bash
SYNC_INTERVAL_SECONDS=30           # Sync every 30 seconds
MAX_CONSECUTIVE_FAILURES=5          # Stop after 5 failures
BACKOFF_MULTIPLIER=2.0              # Exponential backoff
MAX_BACKOFF_SECONDS=300             # Max 5 min backoff
HEALTH_CHECK_ENABLED=true           # Health monitoring
```

## Monitoring

### Check Sync State

```sql
SELECT * FROM operational.sync_state WHERE table_name = 'cows';
```

**Key fields**:
- `last_sync_watermark` - Resume point
- `total_rows_synced` - Lifetime counter
- `total_conflicts_resolved` - Conflict counter
- `last_sync_completed_at` - Last successful sync

### Check Recent Syncs

```sql
SELECT 
    started_at,
    status,
    rows_read,
    rows_inserted,
    rows_updated,
    conflicts_resolved,
    duration_seconds
FROM operational.sync_log
WHERE table_name = 'cows'
ORDER BY started_at DESC
LIMIT 10;
```

### Check Health File

```bash
cat /tmp/sync_scheduler_health.txt
```

### Check Conflicts

```sql
SELECT * FROM operational.sync_conflicts
ORDER BY detected_at DESC
LIMIT 10;
```

## Common Tasks

### Force Full Re-Sync

```sql
-- Reset watermark to NULL
UPDATE operational.sync_state 
SET last_sync_watermark = NULL 
WHERE table_name = 'cows';
```

Then run sync:
```bash
python backend/jobs/sync_silver_to_sql.py
```

### Run in Background

```bash
nohup python backend/jobs/sync_scheduler.py > /tmp/sync_scheduler.log 2>&1 &
```

### Check Scheduler Status

```bash
# Check if running
ps aux | grep sync_scheduler

# Check logs
tail -f /tmp/sync_scheduler.log

# Check health
cat /tmp/sync_scheduler_health.txt
```

### Stop Scheduler

```bash
# Graceful shutdown
pkill -SIGTERM -f sync_scheduler.py

# Force kill (not recommended)
pkill -9 -f sync_scheduler.py
```

## Troubleshooting

### No rows syncing

**Check**:
1. Is Silver layer being updated? (Run Silver processing)
2. Is watermark stuck? (Check `last_sync_watermark`)
3. Are there errors? (Check `sync_log` for failures)

**Fix**:
```bash
# Run Silver processing
cd databricks/silver
python rebuild_cows.py

# Then run sync
cd /home/xuxoramos/endymion-ai
python backend/jobs/sync_silver_to_sql.py
```

### Sync keeps failing

**Check logs**:
```bash
tail -f /tmp/sync_scheduler.log
```

**Check database**:
```sql
SELECT error_message, error_details 
FROM operational.sync_log 
WHERE status = 'failed' 
ORDER BY started_at DESC 
LIMIT 1;
```

**Common issues**:
- MinIO not running → Start MinIO
- SQL Server unreachable → Check connection
- Schema mismatch → Update sync job code
- PySpark errors → Check dependencies

### Too many conflicts

**Cause**: Something is writing directly to SQL projection

**Fix**:
1. Find what's writing to SQL (shouldn't happen!)
2. All writes MUST go through cow_events table
3. SQL projection is READ-ONLY

### SQL data is stale

**Check last sync**:
```sql
SELECT 
    last_sync_completed_at,
    DATEDIFF(second, last_sync_completed_at, GETUTCDATE()) AS seconds_ago
FROM operational.sync_state 
WHERE table_name = 'cows';
```

**If > 60 seconds ago**:
- Check scheduler is running
- Check for errors in sync_log
- Restart scheduler

## Database Tables

### sync_state

Tracks sync watermark and statistics.

```sql
CREATE TABLE operational.sync_state (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    table_name NVARCHAR(100) NOT NULL UNIQUE,
    silver_table_name NVARCHAR(100) NOT NULL,
    last_sync_watermark DATETIME2 NULL,
    total_rows_synced INT NOT NULL DEFAULT 0,
    total_conflicts_resolved INT NOT NULL DEFAULT 0,
    last_sync_started_at DATETIME2 NULL,
    last_sync_completed_at DATETIME2 NULL,
    last_sync_error NVARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
```

### sync_log

Logs each sync run.

```sql
CREATE TABLE operational.sync_log (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    table_name NVARCHAR(100) NOT NULL,
    silver_table_name NVARCHAR(100) NOT NULL,
    watermark_start DATETIME2 NULL,
    watermark_end DATETIME2 NULL,
    started_at DATETIME2 NOT NULL,
    completed_at DATETIME2 NULL,
    duration_seconds FLOAT NULL,
    status NVARCHAR(20) NOT NULL DEFAULT 'running',
    rows_read INT NOT NULL DEFAULT 0,
    rows_inserted INT NOT NULL DEFAULT 0,
    rows_updated INT NOT NULL DEFAULT 0,
    rows_skipped INT NOT NULL DEFAULT 0,
    conflicts_resolved INT NOT NULL DEFAULT 0,
    error_message NVARCHAR(MAX) NULL,
    error_details NVARCHAR(MAX) NULL,
    spark_read_time_seconds FLOAT NULL,
    sql_write_time_seconds FLOAT NULL
);
```

### sync_conflicts

Records conflicts where Silver overwrites SQL.

```sql
CREATE TABLE operational.sync_conflicts (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    table_name NVARCHAR(100) NOT NULL,
    record_id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    conflict_type NVARCHAR(50) NOT NULL,
    silver_value NVARCHAR(MAX) NULL,
    sql_value NVARCHAR(MAX) NULL,
    field_name NVARCHAR(100) NULL,
    resolution NVARCHAR(50) NOT NULL DEFAULT 'silver_wins',
    resolved_by_sync_log_id UNIQUEIDENTIFIER NULL,
    silver_timestamp DATETIME2 NULL,
    sql_timestamp DATETIME2 NULL,
    detected_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    resolved_at DATETIME2 NULL
);
```

## Performance

| Scenario | Rows | Duration | Notes |
|----------|------|----------|-------|
| Full sync | 1,000 | 10-15s | First run |
| Incremental (no changes) | 0 | <1s | Fast |
| Incremental (10 changes) | 10 | 2-3s | Typical |
| Incremental (1,000 changes) | 1,000 | 8-12s | Large update |

## Best Practices

✅ **DO**:
- Run exactly ONE scheduler instance
- Monitor sync_log for failures
- Keep sync interval at 30-60s
- Review conflicts regularly
- Test after schema changes

❌ **DON'T**:
- Write directly to SQL projection
- Run multiple schedulers
- Set interval too low (<10s)
- Ignore conflicts
- Skip testing

## Production Deployment

### Systemd Service

```bash
sudo systemctl start endymion-ai-sync
sudo systemctl enable endymion-ai-sync
sudo systemctl status endymion-ai-sync
```

### Docker

```bash
docker run -d \
  --name endymion-ai-sync \
  -e SYNC_INTERVAL_SECONDS=30 \
  endymion-ai-sync:latest
```

### Kubernetes

```bash
kubectl apply -f sync-deployment.yaml
kubectl logs -f deployment/endymion-ai-sync
```

## More Documentation

- **Full Docs**: [SYNC_JOB_DOCUMENTATION.md](SYNC_JOB_DOCUMENTATION.md)
- **Architecture**: [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
- **Silver Layer**: [../../databricks/silver/SILVER.md](../../databricks/silver/SILVER.md)

## Summary

The sync job is the **reconciliation loop** that makes SQL a true projection:

```
Silver (Source of Truth) ─[30s]→ SQL (Read Cache)
```

- **Incremental**: Only syncs changes
- **Idempotent**: Safe to retry
- **Conflict-Free**: Silver always wins
- **Monitored**: Full logging and metrics

This completes the Pure Projection Pattern A architecture! 🎉
