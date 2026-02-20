# Silver-to-SQL Sync Job Documentation

## Overview

The **Silver-to-SQL Sync Job** is the **CRITICAL** component that makes SQL projection tables consistent with the Silver layer. This is the "reconciliation loop" in the Pure Projection Pattern A architecture.

## Architecture Context

```
┌─────────────────────────────────────────────────────────────────┐
│                   Pure Projection Pattern A                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  WRITE PATH (Commands):                                          │
│    API → cow_events → Bronze → Silver → Gold                     │
│                                                                   │
│  READ PATH (Queries):                                            │
│    API ← cows (SQL projection) ← Silver (via SYNC JOB)          │
│                           ↑                                       │
│                    THIS COMPONENT                                │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Why This Sync Job Exists

**Problem**: API needs fast SQL reads, but data flows through Delta Lake (Bronze/Silver).

**Solution**: Sync job continuously copies Silver state → SQL projection.

**Key Principle**: **Silver is the source of truth**. SQL is just a read-optimized cache.

## Components

### 1. sync_silver_to_sql.py

Core sync logic that executes a single sync run.

**Key Functions**:
- `sync_silver_to_sql()` - Main entry point
- `read_changed_cows_from_silver()` - Query Silver for changes
- `upsert_cow_to_sql()` - UPSERT individual cow
- `log_conflict()` - Log when Silver overwrites SQL changes

**Algorithm**:
1. Read sync state (last watermark timestamp)
2. Query Silver: `WHERE last_updated_at > watermark`
3. For each changed cow:
   - If exists in SQL: UPDATE
   - If not exists: INSERT
   - If SQL has newer timestamp: CONFLICT (Silver wins)
4. Update watermark for next incremental sync

**Conflict Resolution**:
- **Silver ALWAYS wins**
- Manual SQL edits are **OVERWRITTEN**
- All conflicts logged to `sync_conflicts` table

### 2. sync_scheduler.py

Background daemon that runs sync job periodically.

**Features**:
- Runs every 30 seconds (configurable)
- Graceful shutdown (SIGTERM/SIGINT)
- Exponential backoff on failures
- Health check file for monitoring
- Prevents overlapping sync runs

**Configuration** (environment variables):
```bash
SYNC_INTERVAL_SECONDS=30           # Default: 30 seconds
MAX_CONSECUTIVE_FAILURES=5          # Stop after 5 failures
BACKOFF_MULTIPLIER=2.0              # Exponential backoff factor
MAX_BACKOFF_SECONDS=300             # Max 5 minutes backoff
HEALTH_CHECK_ENABLED=true           # Write health check file
```

### 3. Database Models (backend/models/sync.py)

**SyncState** - Tracks sync watermark and statistics
- `table_name` - Which table is being synced
- `last_sync_watermark` - Resume point for incremental sync
- `total_rows_synced` - Lifetime counter
- `total_conflicts_resolved` - Conflict counter

**SyncLog** - Logs each sync run
- `started_at`, `completed_at` - Timing
- `rows_read`, `rows_inserted`, `rows_updated` - Statistics
- `conflicts_resolved` - Conflicts in this run
- `error_message` - Failure details

**SyncConflict** - Records conflicts
- `record_id`, `tenant_id` - Which record
- `silver_value`, `sql_value` - Conflicting values
- `resolution` - Always "silver_wins"

## Data Flow

```
┌─────────────────────┐
│  Silver Layer       │
│  (Delta Lake)       │
│                     │
│  silver_cows_       │
│  current            │
│                     │
│  last_updated_at    │
│  = 2024-01-24       │
│    10:30:00         │
└──────────┬──────────┘
           │
           │ PySpark Query
           │ WHERE last_updated_at > watermark
           │
           ↓
┌─────────────────────┐
│  Sync Job           │
│  (This Component)   │
│                     │
│  - Read changes     │
│  - UPSERT to SQL    │
│  - Resolve conflicts│
│  - Update watermark │
└──────────┬──────────┘
           │
           │ SQLAlchemy UPSERT
           │
           ↓
┌─────────────────────┐
│  SQL Server         │
│  (Projection)       │
│                     │
│  cows table         │
│                     │
│  silver_last_       │
│  updated_at         │
│  last_synced_at     │
└─────────────────────┘
```

## Sync States

### Normal Operation

```
Watermark: 2024-01-24 10:00:00

Silver Changes:
  Cow A: updated at 10:05:00
  Cow B: updated at 10:10:00

Sync Run:
  1. Read cows WHERE last_updated_at > 10:00:00
  2. Found 2 cows
  3. UPSERT Cow A (action: updated)
  4. UPSERT Cow B (action: updated)
  5. New watermark: 10:10:00

Result: 2 updated, 0 conflicts
```

### First Sync (No Watermark)

```
Watermark: NULL

Sync Run:
  1. Read ALL cows from Silver
  2. UPSERT all cows to SQL
  3. Set watermark to MAX(last_updated_at)

Result: Full sync (all rows inserted)
```

### Conflict Detection

```
Watermark: 2024-01-24 10:00:00

Silver:
  Cow A: last_updated_at = 10:05:00

SQL:
  Cow A: silver_last_updated_at = 10:15:00  ← NEWER than Silver!

Analysis:
  SQL was updated at 10:15 but Silver only knows about 10:05
  Someone manually edited SQL (or concurrent sync issue)

Resolution:
  1. Log conflict to sync_conflicts table
  2. OVERWRITE SQL with Silver data (Silver wins)
  3. Set silver_last_updated_at = 10:05:00
  4. Increment conflicts_resolved counter

Result: Conflict resolved (Silver wins)
```

### Incremental Sync (No Changes)

```
Watermark: 2024-01-24 10:00:00

Silver Changes:
  (none)

Sync Run:
  1. Read cows WHERE last_updated_at > 10:00:00
  2. Found 0 cows
  3. Skip processing

Result: 0 rows processed (fast sync)
```

## Usage

### Run Sync Once (Manual)

```bash
cd /home/xuxoramos/endymion-ai
python backend/jobs/sync_silver_to_sql.py
```

**Output**:
```
Starting Silver-to-SQL sync
Created sync state: <uuid>
Reading changed cows from Silver (watermark: None)
Silver table has 150 total rows
Processing 150 changed cows...
Processing batch 1/1 (150 rows)
Sync completed successfully
  Rows read: 150
  Rows inserted: 150
  Rows updated: 0
  Rows skipped: 0
  Conflicts resolved: 0
  Duration: 5.23s
```

### Run Scheduler (Continuous)

```bash
cd /home/xuxoramos/endymion-ai
python backend/jobs/sync_scheduler.py
```

**Output**:
```
Sync Scheduler Starting
Sync interval: 30 seconds
Health check: True
Max consecutive failures: 5

Starting sync run...
Sync completed successfully: 5 inserted, 10 updated, 135 skipped, 0 conflicts
Scheduler status: Total syncs: 1, Success: 1, Failures: 0
Next sync in 30 seconds...
```

**Stop Gracefully**:
```bash
# Send SIGTERM
kill -SIGTERM <pid>

# Or use Ctrl+C
^C
```

### Run as Background Service

```bash
# Start in background
nohup python backend/jobs/sync_scheduler.py > /tmp/sync_scheduler.log 2>&1 &

# Check logs
tail -f /tmp/sync_scheduler.log

# Check health
cat /tmp/sync_scheduler_health.txt

# Stop
pkill -SIGTERM -f sync_scheduler.py
```

### Run End-to-End Test

```bash
cd /home/xuxoramos/endymion-ai/backend/jobs
./test_sync_flow.sh [tenant_id]
```

This test validates the complete flow:
1. Create cow via API (→ cow_events)
2. Bronze ingestion (cow_events → Bronze)
3. Silver processing (Bronze → Silver)
4. **Sync job (Silver → SQL)** ← Tests this component
5. Verify cow in SQL projection
6. Verify sync metadata populated

## Monitoring

### Check Sync State

```sql
-- Get current sync state
SELECT 
    table_name,
    last_sync_watermark,
    total_rows_synced,
    total_conflicts_resolved,
    last_sync_completed_at,
    last_sync_error
FROM operational.sync_state
WHERE table_name = 'cows';
```

### Check Recent Sync Logs

```sql
-- Get last 10 sync runs
SELECT 
    started_at,
    completed_at,
    status,
    rows_read,
    rows_inserted,
    rows_updated,
    rows_skipped,
    conflicts_resolved,
    duration_seconds
FROM operational.sync_log
WHERE table_name = 'cows'
ORDER BY started_at DESC
LIMIT 10;
```

### Check Conflicts

```sql
-- Get recent conflicts
SELECT 
    detected_at,
    record_id,
    tenant_id,
    conflict_type,
    silver_timestamp,
    sql_timestamp,
    resolution
FROM operational.sync_conflicts
WHERE table_name = 'cows'
ORDER BY detected_at DESC
LIMIT 10;
```

### Health Check

```bash
# Check scheduler health file
cat /tmp/sync_scheduler_health.txt

# Example output:
{
  "timestamp": "2024-01-24T10:30:00.123456",
  "status": {
    "running": true,
    "sync_in_progress": false,
    "last_sync_time": "2024-01-24T10:29:45.123456",
    "last_sync_status": "success",
    "consecutive_failures": 0,
    "total_syncs": 120,
    "total_successes": 118,
    "total_failures": 2,
    "current_backoff_seconds": 30,
    "next_sync_in_seconds": 15
  }
}
```

## Performance

### Typical Performance

| Scenario | Rows | Duration | Notes |
|----------|------|----------|-------|
| Full sync (first run) | 1,000 | 10-15s | Inserts all rows |
| Incremental (no changes) | 0 | <1s | Fast watermark check |
| Incremental (10 changes) | 10 | 2-3s | UPSERT 10 rows |
| Incremental (1,000 changes) | 1,000 | 8-12s | Batch processing |

### Optimization Tips

1. **Increase batch size** for large syncs:
   ```python
   BATCH_SIZE = 5000  # Default: 1000
   ```

2. **Tune Spark parallelism**:
   ```python
   .config("spark.sql.shuffle.partitions", "50")
   ```

3. **Add SQL indexes**:
   ```sql
   CREATE INDEX IX_cows_silver_updated ON cows(silver_last_updated_at);
   ```

4. **Monitor sync interval**:
   - Too short: Wastes resources on "no changes" syncs
   - Too long: SQL becomes stale
   - Recommended: 30-60 seconds

## Troubleshooting

### Issue: "No changed rows to sync" (always)

**Cause**: Silver table not being updated

**Solution**:
```bash
# Check Silver processing is running
cd databricks/silver
python rebuild_cows.py

# Verify Silver has data
spark-shell
val df = spark.read.format("delta").load("s3a://silver/cows_current")
df.show()
```

### Issue: "Too many conflicts"

**Cause**: Something is writing directly to SQL

**Solution**:
- Find what's writing to SQL (check application logs)
- Ensure all writes go through cow_events → Bronze → Silver
- SQL projection should be READ-ONLY

### Issue: "Sync keeps failing"

**Cause**: Various (network, credentials, schema mismatch)

**Solution**:
```bash
# Check logs
tail -f /tmp/sync_scheduler.log

# Check sync_log table
SELECT * FROM operational.sync_log 
WHERE status = 'failed' 
ORDER BY started_at DESC LIMIT 5;

# Look at error_message and error_details
```

### Issue: "SQL data is stale"

**Cause**: Sync not running or failing silently

**Solution**:
```bash
# Check scheduler is running
ps aux | grep sync_scheduler

# Check health file timestamp
stat /tmp/sync_scheduler_health.txt

# Check last sync time
SELECT last_sync_completed_at 
FROM operational.sync_state 
WHERE table_name = 'cows';

# Restart scheduler
pkill -SIGTERM -f sync_scheduler.py
python backend/jobs/sync_scheduler.py
```

### Issue: "Max consecutive failures"

**Cause**: Persistent error (MinIO down, SQL Server unreachable, etc.)

**Solution**:
1. Fix underlying issue (restart MinIO, check SQL connection)
2. Scheduler will auto-retry after backoff period
3. Or manually restart scheduler

## Production Deployment

### Systemd Service

Create `/etc/systemd/system/endymion-ai-sync.service`:

```ini
[Unit]
Description=Endymion-AI Silver-to-SQL Sync Scheduler
After=network.target minio.service sqlserver.service

[Service]
Type=simple
User=endymion-ai
WorkingDirectory=/home/endymion-ai/endymion-ai
Environment="SYNC_INTERVAL_SECONDS=30"
Environment="PYTHONPATH=/home/endymion-ai/endymion-ai"
ExecStart=/usr/bin/python3 backend/jobs/sync_scheduler.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Start**:
```bash
sudo systemctl daemon-reload
sudo systemctl start endymion-ai-sync
sudo systemctl enable endymion-ai-sync
sudo systemctl status endymion-ai-sync
```

### Docker Container

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

CMD ["python", "backend/jobs/sync_scheduler.py"]
```

**Run**:
```bash
docker build -t endymion-ai-sync .
docker run -d \
  --name endymion-ai-sync \
  -e SYNC_INTERVAL_SECONDS=30 \
  -v /data:/data \
  endymion-ai-sync
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: endymion-ai-sync
spec:
  replicas: 1  # Only run one sync instance
  selector:
    matchLabels:
      app: endymion-ai-sync
  template:
    metadata:
      labels:
        app: endymion-ai-sync
    spec:
      containers:
      - name: sync-scheduler
        image: endymion-ai-sync:latest
        env:
        - name: SYNC_INTERVAL_SECONDS
          value: "30"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
```

## Best Practices

### DO ✅

- Run exactly **ONE** scheduler instance (no concurrent syncs)
- Monitor sync_log table for failures
- Set up alerts for consecutive failures
- Test sync job after schema changes
- Use health check file for monitoring
- Keep sync interval reasonable (30-60s)
- Review conflict logs regularly

### DON'T ❌

- Write directly to SQL projection tables
- Run multiple sync schedulers concurrently
- Set sync interval too low (<10s)
- Ignore conflicts (they indicate bugs)
- Skip testing after Silver schema changes
- Disable conflict logging in production

## FAQ

**Q: What happens if sync scheduler crashes?**

A: Sync resumes from last watermark on restart. No data loss (idempotent).

**Q: Can I run sync scheduler on multiple servers?**

A: **NO**. Only run ONE instance. Multiple instances cause conflicts and race conditions.

**Q: How do I force a full re-sync?**

A: Reset watermark to NULL:
```sql
UPDATE operational.sync_state 
SET last_sync_watermark = NULL 
WHERE table_name = 'cows';
```

**Q: What if Silver and SQL schemas diverge?**

A: Sync job will fail. Update sync job code to handle new schema, then restart.

**Q: Can I skip syncing certain cows?**

A: Not recommended. Use tenant isolation or status filtering in Silver instead.

**Q: How do I monitor sync lag?**

A: Compare `last_sync_watermark` to current time:
```sql
SELECT 
    table_name,
    last_sync_watermark,
    DATEDIFF(second, last_sync_watermark, GETUTCDATE()) AS lag_seconds
FROM operational.sync_state;
```

## Related Documentation

- [Pure Projection Pattern A](../docs/ARCHITECTURE.md)
- [Silver Layer Documentation](../../databricks/silver/SILVER.md)
- [Event Sourcing Guide](../docs/EVENT_SOURCING.md)
- [Database Models](../models/README.md)

## Summary

The Silver-to-SQL sync job is **CRITICAL** for making SQL a true projection of Silver state.

**Key Points**:
- Runs every 30 seconds (configurable)
- Incremental sync using watermark
- Silver ALWAYS wins (conflict resolution)
- Idempotent and safe to retry
- Logs everything for auditing

**Success Criteria**:
✅ SQL projection stays consistent with Silver
✅ API reads always return current state (within 30-60s)
✅ No data loss during sync failures
✅ Conflicts are detected and logged
✅ Performance is acceptable (<15s per sync)

This component completes the data flow loop and enables fast SQL reads while maintaining Delta Lake as the source of truth.
