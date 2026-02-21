# Endymion-AI Architecture Demo

## 🎯 Overview

Interactive demonstration of the complete Endymion-AI data architecture, showcasing **Pure Projection Pattern A** (Event Sourcing + CQRS) in action.

## 🎬 What This Demo Shows

### Complete Data Flow

```
API → Events → Bronze → Silver → Gold → SQL → Analytics API
     (Outbox)  (Raw)   (Clean)  (Agg)  (Query) (Cached)
```

### Key Concepts Demonstrated

1. **Event Sourcing** - All changes stored as immutable events
2. **Bronze Layer** - Raw event storage for audit trail
3. **Silver Layer** - Cleaned state with history (SCD Type 2)
4. **SQL Projection** - Fast queries with eventual consistency
5. **Gold Layer** - Pre-computed analytics
6. **Monitoring** - Health checks, metrics, dashboard

## 📁 Files

```
demo/
├── run_demo.sh        # Main demo script (interactive)
├── timeline.py        # ASCII art timeline visualization
└── README.md          # This file
```

## 🚀 Quick Start

### Prerequisites

```bash
# 1. Activate virtual environment
cd /home/xuxoramos/endymion-ai
source .venv/bin/activate

# 2. Ensure Docker containers are running
cd docker
docker-compose up -d

# 3. Verify services
docker ps | grep -E "cattlesaas|minio"

# 4. Ensure Spark JARs are available
ls ~/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.1.jar
ls ~/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar
```

### Run the Complete Demo

```bash
cd /home/xuxoramos/endymion-ai
./demo/run_demo_all.sh
```

**Duration:** ~15 minutes (includes Spark initialization times)

The demo runs automatically through 8 phases:
1. **Teardown** - Clean environment (stop services, remove data)
2. **Infrastructure** - Start Docker, init database, setup Bronze/Silver layers
3. **API Creation** - Create 3 cows + 1 weight update via API
4. **Bronze Ingestion** - Ingest events from SQL Server to Delta Lake
5. **Silver Processing** - Resolve cow state with SCD Type 2
6. **Gold Analytics** - Generate daily snapshots and herd composition
7. **Visualization** - Display timeline and analytics dashboard
8. **Summary** - Show final status and next steps

### Important Timeouts

The demo includes proper timeouts for Spark initialization:
- **Bronze setup:** 180 seconds (Spark init + JAR downloads)
- **Silver setup:** 180 seconds (Schema creation + table initialization)
- **Bronze ingestion:** 120 seconds (Already initialized)
- **Silver processing:** 180 seconds (SCD Type 2 logic)
- **Gold analytics:** 240 seconds (Complex aggregations)

**Why these timeouts?** Spark initialization takes 30-90 seconds depending on:
- JVM startup time
- Maven JAR downloads (Hadoop AWS, Delta Lake)
- Network latency to MinIO
- System resources

## 📋 Demo Flow

### Phase 1: Environment Teardown
- Stops all running services (FastAPI, Frontend, Sync services)
- Removes Docker containers and volumes
- Ensures clean slate for demonstration

### Phase 2: Infrastructure Setup
1. Start Docker containers (SQL Server, MinIO)
2. Create database and schemas
3. Initialize database tables
4. **Setup Bronze layer** (Delta Lake, 180s timeout)
5. **Setup Silver layer** (Pre-create table to prevent race conditions, 180s timeout)
6. Start FastAPI backend
7. Start React frontend (Vite dev server)

**Key Changes:** Silver table is now pre-created to prevent `ProtocolChangedException` from concurrent writes.

### Phase 3: Create 3 Cows via API
- **Bessie** (DEMO-001) - Holstein heifer, 650.5 kg
- **Daisy** (DEMO-002) - Jersey heifer, 450.0 kg
- **Angus** (DEMO-003) - Black Angus bull, 800.0 kg
- **Update:** Bessie's weight 650.5 → 675.0 kg

Shows:
- curl commands with JSON payloads
- UUID extraction from API responses
- Event IDs returned (lowercase normalized)
- Tenant isolation (X-Tenant-ID header)

**Key Fix:** Demo script now uses full venv Python path for JSON parsing: `"$PROJECT_DIR/.venv/bin/python"` instead of bare `python` command.

### Phase 4: Bronze Layer Ingestion
```bash
timeout 120 python databricks/bronze/ingest_from_sql.py --once
```

Shows:
- Events fetched from SQL Server
- Delta Lake merge operation (dedupe by event_id)
- Partitioning by tenant_id and date
- UUID normalization to lowercase

**Note:** May encounter `ConcurrentAppendException` if analytics sync runs simultaneously. This is benign - no data loss occurs.

### Phase 5: Silver Layer Processing
```bash
timeout 180 python databricks/silver/resolve_cow_state.py
```

Shows:
- SCD Type 2 history tracking (__START_AT, __END_AT, __CURRENT)
- Data quality expectations (14 checks)
- Valid records written to Silver
- Current state view created

### Phase 6: Gold Layer Analytics

**Daily Snapshots:**
```bash
timeout 240 python databricks/gold/gold_daily_snapshots.py
```

**Herd Composition:**
```bash
timeout 240 python databricks/gold/gold_herd_composition.py
```

Shows:
- Pre-computed aggregations by breed/status/sex
- Long-format analytics (dimension_type, dimension_value, count, percentage)
- Partitioned by snapshot_date

**Key Fix:** Now uses `datetime.max.time()` (23:59:59) instead of `datetime.min.time()` (00:00:00) to include all cows created during the day.

### Phase 7: Analytics Visualization
- Visual timeline showing event flow
- Event store summary (SQL Server)
- Mock analytics dashboard (breed distribution, herd overview)
- Event activity log

### Phase 8: Final Summary
- End-to-end data flow verification
- Architecture layers status
- Demo artifacts and logs
- Next steps for interaction

## 🎨 Timeline Visualization

The `timeline.py` script generates a beautiful ASCII art visualization:

```
================================================================================
                   Data Flow Timeline: Cow <uuid>
================================================================================

[EVENT LAYER (Source of Truth)]
  📝 Outbox Table: events.cow_events

    1. cow_created
       Event ID: abc123...
       Timestamp: 10:30:15.123
       Version: 1
       Published: ✅ true

    2. cow_updated
       Event ID: def456...
       Timestamp: 10:32:45.678
       Version: 2
       Published: ✅ true

    ↓
    ↓ Outbox processor publishes to event bus
    ↓

[BRONZE LAYER (Raw Storage)]
  🗄️  Delta Table: s3://bronze/cow_events/
  Format: Parquet files (immutable)
  Purpose: Raw event storage, audit trail

    Stored: 2 event(s)

    ↓
    ↓ PySpark processes and deduplicates
    ↓

[SILVER LAYER (Cleaned State)]
  ✨ Delta Table: s3://silver/cows/
  Format: Parquet files with history
  Purpose: Event-sourced state, SCD Type 2

    Current State:
      Tag ID: US-002
      Breed: Angus
      Sex: male
      Status: active

    ↓
    ↓ Sync job reads changes and UPSERTs
    ↓

[SQL LAYER (Query Projection)]
  🗄️  SQL Server Table: operational.cows
  Purpose: Fast queries, API reads
  Consistency: Eventually consistent

    ✅ Synced
      Last Sync: 10:33:20.456
      Sync Lag: ✅ 2.34s Near real-time

    ↓
    ↓ Aggregation jobs process Silver
    ↓

[GOLD LAYER (Analytics)]
  📊 Delta Tables: s3://gold/herd_composition/, weight_trends/, etc.
  Purpose: Pre-computed analytics, dashboards
  Refresh: Scheduled (e.g., hourly)

================================================================================
                        Architecture Summary
================================================================================

Write Path (Commands):
  API → Events → Bronze → Silver → SQL

Read Path (Queries):
  API ← SQL Projection (operational queries)
  API ← Gold Tables (analytics queries)

Pattern:
  Pure Projection Pattern A (Event Sourcing + CQRS)

Key Benefits:
  ✅ Complete audit trail (all events stored)
  ✅ Time-travel queries (history in Silver)
  ✅ Fast reads (SQL projection)
  ✅ Scalable analytics (Gold aggregations)
  ✅ Eventually consistent (async sync)
```

### Standalone Timeline Usage

```bash
# Show timeline for all cows
python demo/timeline.py

# Show timeline for specific cow
python demo/timeline.py <cow-id>
```

## 🎓 Educational Value

This demo teaches:

1. **Event Sourcing Fundamentals**
   - Events as source of truth
   - Immutable append-only log
   - Event versioning

2. **CQRS Pattern**
   - Separate write and read models
   - Command side (Events)
   - Query side (SQL Projection)

3. **Medallion Architecture**
   - Bronze: Raw data
   - Silver: Cleaned data
   - Gold: Aggregated data

4. **Eventual Consistency**
   - Async sync patterns
   - Acceptable lag tolerance
   - Recovery mechanisms

5. **Data Lineage**
   - Track data through all layers
   - Audit trail
   - Time-travel queries

## 🔧 Troubleshooting

### FastAPI Not Running

```bash
cd /home/xuxoramos/endymion-ai
source .venv/bin/activate
uvicorn backend.api.main:app --reload
```

### Sync Scheduler Not Running

```bash
cd /home/xuxoramos/endymion-ai
source .venv/bin/activate
python -m backend.jobs.sync_scheduler &
```

### Database Connection Failed

```bash
# Check SQL Server container is running
sudo docker ps | grep cattlesaas-sqlserver

# Test connection
docker exec -it cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai -Q "SELECT 1;"
```

### Timeline Visualization Errors

```bash
# Install dependencies
pip install pyodbc

# Check database has data
docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai \
  -Q "SELECT COUNT(*) FROM events.cow_events;"
```

## 📊 Expected Output

### Successful Demo Run

```
🚀 Endymion-AI Architecture Demo
===============================================================================

✅ FastAPI is running
✅ Sync Scheduler is running
✅ Database connection OK

[... interactive steps ...]

🎉 Demo Complete!

What we demonstrated:
  ✅ Event Sourcing - All changes stored as events
  ✅ Bronze Layer - Raw event storage
  ✅ Silver Layer - Cleaned state with history
  ✅ SQL Projection - Fast queries
  ✅ Gold Layer - Pre-computed analytics
  ✅ Analytics API - Cached aggregations
  ✅ Monitoring - Health checks, metrics, dashboard

Demo successful! 🚀
```

## 🎯 Key Takeaways

After running this demo, you will understand:

1. **How events flow through the system** - From API to all layers
2. **Why we use multiple layers** - Each serves a specific purpose
3. **How eventual consistency works** - Async sync with acceptable lag
4. **How to query each layer** - Different access patterns
5. **How monitoring ensures reliability** - Health checks catch issues

## 📚 Related Documentation

- `backend/jobs/SYNC_JOB_DOCUMENTATION.md` - Sync job details
- `backend/monitoring/MONITORING.md` - Monitoring system
- `backend/api/README.md` - API documentation
- `docs/ARCHITECTURE.md` - Overall architecture

## 🎬 Demo Tips

1. **Take your time** - The demo is interactive, read each step carefully
2. **Open the dashboard** - View `demo/dashboard.html` in a browser during the demo
3. **Watch the logs** - Keep an eye on `/tmp/sync_scheduler.log`
4. **Experiment** - After the demo, try creating more cows or running queries
5. **Run multiple times** - Each run clears data for a fresh demonstration

## 🌟 Demo Highlights

### Visual Elements
- 🎨 Color-coded output (green/yellow/red/cyan)
- 📊 ASCII art timeline
- 🔄 Progress indicators
- ⏱️  Countdown timers

### Interactive Features
- Press Enter to progress
- See real-time data flow
- Query results formatted nicely
- Clear status messages

### Educational Flow
- Builds understanding step-by-step
- Shows cause and effect
- Demonstrates patterns in action
- Provides context at each stage

## 🚀 Running in Production

This demo shows a development setup. In production:

- **Events** → Kafka/Pulsar event bus
- **Bronze** → MinIO/S3 object storage
- **Silver** → Scheduled PySpark jobs
- **Sync** → Kubernetes CronJob
- **Monitoring** → Prometheus + Grafana
- **Alerting** → PagerDuty/Opsgenie

## 📞 Support

Questions? Check:
1. Run the demo with verbose logging
2. Check service status (FastAPI, sync scheduler, database)
3. View monitoring dashboard
4. Review documentation files

---

**Enjoy the demo! 🎉**

This demonstration showcases a production-ready event-sourced architecture with complete observability and monitoring. Perfect for understanding modern data engineering patterns!
