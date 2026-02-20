# Endymion-AI Developer Guide

Complete guide for developers working on the Endymion-AI event-sourced cattle management system.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Architecture Overview](#architecture-overview)
3. [Local Development](#local-development)
4. [Testing](#testing)
5. [Common Tasks](#common-tasks)
6. [Troubleshooting](#troubleshooting)

---

## 1. Getting Started

### Prerequisites

**Required Software:**
- Python 3.10+
- SQL Server 2022+ (local Docker or Azure SQL)
- Node.js 18+ (for frontend)
- Java 11+ (for PySpark)

**Optional:**
- Docker & Docker Compose (recommended)
- Azure Data Studio or SQL Server Management Studio (database GUI)
- VSCode with Python extension

**Python Packages:**
```bash
# Core dependencies
fastapi==0.104.1
uvicorn==0.24.0
pyodbc==5.1.0
pyspark==3.5.0
delta-spark==3.0.0

# See requirements.txt for complete list
```

### Installation Steps

#### Step 1: Clone and Setup Virtual Environment

```bash
# Clone repository
git clone https://github.com/yourusername/endymion-ai.git
cd endymion-ai

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

#### Step 2: Setup SQL Server

```bash
# Start SQL Server in Docker
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=StrongP@ssw0rd" \
    -p 1433:1433 --name endymion-sql -d mcr.microsoft.com/mssql/server:2022-latest

# Create database (update password to match your environment)
sqlcmd -S localhost -U sa -P 'StrongP@ssw0rd' -Q "CREATE DATABASE endymion_ai"

# Run schema migrations (T-SQL)
sqlcmd -S localhost -U sa -P 'StrongP@ssw0rd' -d endymion_ai -i backend/database/schema.sql
```

> 💡 Install the [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server) so `pyodbc` can connect locally.

**Schema Structure:**
```sql
-- Three schemas for separation of concerns
CREATE SCHEMA events;     -- Event store (source of truth)
CREATE SCHEMA operational; -- SQL projection (read models)
CREATE SCHEMA sync;        -- Sync job tracking
```

See: [backend/db/schema.sql](../backend/db/schema.sql)

#### Step 3: Setup Delta Lake

```bash
# Create Delta Lake directories
mkdir -p delta_lake/bronze
mkdir -p delta_lake/silver
mkdir -p delta_lake/gold

# Set permissions
chmod -R 755 delta_lake/
```

#### Step 4: Configure Environment

Create `.env` file:
```bash
# Database
DATABASE_URL=mssql+pyodbc://sa:StrongP%40ssw0rd@localhost:1433/endymion_ai?driver=ODBC+Driver+18+for+SQL+Server

# Delta Lake
DELTA_LAKE_PATH=/path/to/endymion_ai/delta_lake

# API
API_HOST=0.0.0.0
API_PORT=8000

# Sync
SYNC_INTERVAL_SECONDS=30
```

#### Step 5: Install Frontend

```bash
cd frontend
npm install
cd ..
```

### First Run

Start all services in separate terminals:

**Terminal 1: FastAPI Backend**
```bash
source .venv/bin/activate
python backend/api/main.py
```

**Terminal 2: Sync Scheduler**
```bash
source .venv/bin/activate
python backend/sync/sync_scheduler.py
```

**Terminal 3: Frontend**
```bash
cd frontend
npm run dev
```

### Verify Setup

#### 1. Check FastAPI
```bash
curl http://localhost:8000/docs
# Should return OpenAPI documentation
```

#### 2. Check Database
```bash
psql -d endymion_ai -c "SELECT COUNT(*) FROM events.cow_events;"
# Should return 0 (or event count)
```

#### 3. Check Frontend
Open browser: http://localhost:3000
- Should see Cattle List page
- Sync status bar should show green indicator

#### 4. Create Test Cow
```bash
curl -X POST http://localhost:8000/api/cows \
  -H "Content-Type: application/json" \
  -d '{
    "breed": "Holstein",
    "birth_date": "2024-01-15",
    "sex": "Female"
  }'
```

#### 5. Run Demo Script
```bash
./demo/run_demo.sh
```

**Expected Output:**
- 3 cows created (Bessie, Thunder, Daisy)
- Events flow through all layers
- Timeline visualization shows data flow
- No errors in any terminal

---

## 2. Architecture Overview

### System Design

Endymion-AI implements **Pure Projection Pattern A** (Event Sourcing + CQRS):

```mermaid
graph TD
    UI[Web UI] -->|Write| API[FastAPI]
    API -->|Emit Events| OUTBOX[SQL Outbox]
    OUTBOX -->|Publish| BRONZE[Bronze Layer]
    BRONZE -->|Process| SILVER[Silver Layer]
    SILVER -->|Sync Job| SQL[SQL Projection]
    SILVER -->|Aggregate| GOLD[Gold Layer]
    SQL -->|Read| API
    GOLD -->|Analytics| API
    API -->|Read| UI
```

See: [ARCHITECTURE.md](../ARCHITECTURE.md) for detailed diagrams.

### Layer Responsibilities

#### Event Store (SQL Server `operational.cow_events`)
- **Role**: Source of truth for writes
- **Characteristics**: Append-only, immutable
- **Used for**: Event sourcing, audit trail
- **Files**: [backend/db/models/events.py](../backend/db/models/events.py)

#### Bronze Layer (Delta Lake)
- **Role**: Raw event storage in data lake
- **Characteristics**: Immutable, partitioned by date
- **Used for**: Historical queries, replay
- **Files**: `delta_lake/bronze/`

#### Silver Layer (Delta Lake)
- **Role**: Canonical state with history (SCD Type 2)
- **Characteristics**: Current + historical versions
- **Used for**: Source of truth for reads, time-travel
- **Files**: `delta_lake/silver/`

#### SQL Projection (SQL Server `operational.cows`)
- **Role**: Optimized read model
- **Characteristics**: Indexed, query-optimized
- **Used for**: Fast operational queries
- **Files**: [backend/db/models/operational.py](../backend/db/models/operational.py)

#### Gold Layer (Delta Lake)
- **Role**: Pre-computed analytics
- **Characteristics**: Aggregated, business-focused
- **Used for**: Dashboards, reporting
- **Files**: `delta_lake/gold/`

### Data Flow Diagrams

#### Write Operation
```mermaid
sequenceDiagram
    participant UI
    participant API
    participant DB as SQL Outbox
    participant Bronze
    participant Silver
    participant Sync as Sync Job
    participant SQL as SQL Projection

    UI->>API: POST /cows
    API->>DB: INSERT event
    DB-->>API: event_id
    API-->>UI: 201 Created
    
    Note over DB,Bronze: Async processing
    DB->>Bronze: Publish event
    Bronze->>Silver: Process event
    
    loop Every 30s
        Sync->>Silver: Query new data
        Sync->>SQL: Upsert rows
    end
```

#### Read Operation
```mermaid
sequenceDiagram
    participant UI
    participant API
    participant SQL as SQL Projection
    participant Gold

    UI->>API: GET /cows
    API->>SQL: SELECT * FROM cows
    SQL-->>API: cow data
    API-->>UI: JSON response
    
    UI->>API: GET /analytics
    API->>Gold: Query aggregates
    Gold-->>API: analytics data
    API-->>UI: JSON response
```

### Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Frontend | React + Vite | User interface |
| API | FastAPI | REST endpoints |
| Event Store | SQL Server | Event sourcing |
| Bronze/Silver/Gold | PySpark + Delta Lake | Data lake layers |
| SQL Projection | SQL Server | Read model |
| Sync Job | Python (cron-like) | Keep SQL in sync |
| Monitoring | Prometheus + HTML | Observability |

---

## 3. Local Development

### Running Services

#### Development Mode (All Services)

**Option 1: Multiple Terminals**
```bash
# Terminal 1: API
source .venv/bin/activate
python backend/api/main.py

# Terminal 2: Sync
source .venv/bin/activate
python backend/sync/sync_scheduler.py

# Terminal 3: Frontend
cd frontend && npm run dev
```

**Option 2: Docker Compose (TODO)**
```bash
docker-compose up -d
```

#### Running Individual Components

**FastAPI with Auto-reload:**
```bash
uvicorn backend.api.main:app --reload --port 8000
```

**Sync Job (One-time):**
```bash
python backend/sync/sync_silver_to_sql.py
```

**Frontend (Production Build):**
```bash
cd frontend
npm run build
npm run preview
```

### Debugging Tips

#### 1. Enable Debug Logging

**FastAPI:**
```python
# backend/api/main.py
import logging
logging.basicConfig(level=logging.DEBUG)
```

**Sync Job:**
```python
# backend/sync/sync_scheduler.py
LOG_LEVEL = "DEBUG"
```

#### 2. Use Interactive Python Shell

```python
# Test database connection
from backend.db.connection import get_db
db = next(get_db())

# Query events
from backend.db.models.events import CowEvent
events = db.query(CowEvent).all()

# Check sync state
from backend.db.models.sync import SyncState
sync_state = db.query(SyncState).first()
print(sync_state.last_sync_completed_at)
```

#### 3. VSCode Launch Configuration

Create `.vscode/launch.json`:
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "FastAPI",
      "type": "python",
      "request": "launch",
      "module": "uvicorn",
      "args": ["backend.api.main:app", "--reload"],
      "jinja": true
    },
    {
      "name": "Sync Scheduler",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/backend/sync/sync_scheduler.py",
      "console": "integratedTerminal"
    }
  ]
}
```

### Useful SQL Queries

#### Event Store Queries

**View all events:**
```sql
SELECT 
    event_id,
    aggregate_id,
    event_type,
    event_timestamp,
    published
FROM events.cow_events
ORDER BY event_timestamp DESC
LIMIT 10;
```

**Find unpublished events:**
```sql
SELECT COUNT(*) as unpublished_count
FROM events.cow_events
WHERE published = FALSE;
```

**Events for specific cow:**
```sql
SELECT 
    event_type,
    event_timestamp,
    data->>'breed' as breed,
    published
FROM events.cow_events
WHERE aggregate_id = '<cow_id>'
ORDER BY event_timestamp;
```

**Event type distribution:**
```sql
SELECT 
    event_type,
    COUNT(*) as count,
    COUNT(*) FILTER (WHERE published = TRUE) as published,
    COUNT(*) FILTER (WHERE published = FALSE) as pending
FROM events.cow_events
GROUP BY event_type;
```

#### SQL Projection Queries

**All active cows:**
```sql
SELECT 
    cow_id,
    breed,
    sex,
    birth_date,
    is_active,
    created_at,
    updated_at
FROM operational.cows
WHERE is_active = TRUE
ORDER BY created_at DESC;
```

**Cows by breed:**
```sql
SELECT 
    breed,
    COUNT(*) as count,
    AVG(EXTRACT(YEAR FROM AGE(birth_date))) as avg_age_years
FROM operational.cows
WHERE is_active = TRUE
GROUP BY breed
ORDER BY count DESC;
```

**Recently updated cows:**
```sql
SELECT *
FROM operational.cows
WHERE updated_at >= NOW() - INTERVAL '5 minutes'
ORDER BY updated_at DESC;
```

#### Sync Status Queries

**Current sync state:**
```sql
SELECT 
    table_name,
    last_watermark,
    last_sync_started_at,
    last_sync_completed_at,
    EXTRACT(EPOCH FROM (NOW() - last_sync_completed_at)) as lag_seconds,
    total_rows_synced,
    total_conflicts_resolved
FROM sync.sync_state;
```

**Sync history:**
```sql
SELECT 
    run_id,
    started_at,
    completed_at,
    rows_synced,
    conflicts_resolved,
    EXTRACT(EPOCH FROM (completed_at - started_at)) as duration_seconds
FROM sync.sync_runs
ORDER BY started_at DESC
LIMIT 20;
```

**Conflict details:**
```sql
SELECT 
    run_id,
    conflict_timestamp,
    silver_data,
    sql_data,
    resolution
FROM sync.sync_conflicts
ORDER BY conflict_timestamp DESC;
```

### Delta Lake Queries

#### Using PySpark

**Initialize Spark Session:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Endymion-AI-Debug") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

**Query Bronze Events:**
```python
bronze_df = spark.read.format("delta").load("delta_lake/bronze")
bronze_df.show(10)

# Filter by event type
bronze_df.filter(bronze_df.event_type == "cow_created").show()

# Count events
bronze_df.groupBy("event_type").count().show()
```

**Query Silver (Current State):**
```python
silver_df = spark.read.format("delta").load("delta_lake/silver")

# Current cows only
current_df = silver_df.filter(silver_df.is_current == True)
current_df.show()

# Full history
silver_df.orderBy("cow_id", "valid_from").show()
```

**Time-Travel Queries:**
```python
# Query Silver as of specific timestamp
historical_df = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-15 10:00:00") \
    .load("delta_lake/silver")

historical_df.show()

# Query by version
version_df = spark.read \
    .format("delta") \
    .option("versionAsOf", 5) \
    .load("delta_lake/silver")
```

**Query Gold Analytics:**
```python
gold_df = spark.read.format("delta").load("delta_lake/gold/herd_analytics")
gold_df.show()
```

---

## 4. Testing

### Unit Tests

**Run all unit tests:**
```bash
pytest tests/
```

**Run specific test file:**
```bash
pytest tests/test_events.py -v
```

**Run with coverage:**
```bash
pytest --cov=backend --cov-report=html
open htmlcov/index.html
```

**Example Unit Test:**
```python
# tests/test_events.py
import pytest
from backend.db.models.events import CowEvent
from uuid import uuid4

def test_cow_event_creation():
    event = CowEvent(
        event_id=uuid4(),
        aggregate_id=uuid4(),
        event_type="cow_created",
        data={"breed": "Holstein", "sex": "Female"}
    )
    assert event.event_type == "cow_created"
    assert event.data["breed"] == "Holstein"

def test_event_immutability():
    # Events should not be updateable
    # Add your test here
    pass
```

### Integration Tests

**Test Full Data Flow:**
```bash
# Run integration test script
./tests/integration/test_flow.sh
```

**Manual Integration Test:**
```bash
# 1. Create cow
COW_ID=$(curl -s -X POST http://localhost:8000/api/cows \
  -H "Content-Type: application/json" \
  -d '{"breed": "Test", "birth_date": "2024-01-15", "sex": "Female"}' \
  | jq -r '.cow_id')

# 2. Wait for sync (30s)
sleep 35

# 3. Verify in SQL projection
curl http://localhost:8000/api/cows/$COW_ID | jq .

# 4. Update breed
curl -X PUT http://localhost:8000/api/cows/$COW_ID/breed \
  -H "Content-Type: application/json" \
  -d '{"breed": "Updated"}'

# 5. Wait and verify
sleep 35
curl http://localhost:8000/api/cows/$COW_ID | jq .breed
```

### Running the Demo

**Full Demo Script:**
```bash
./demo/run_demo.sh
```

**Demo Sections:**
1. Pre-flight checks (FastAPI, sync scheduler, DB)
2. Data cleanup (TRUNCATE tables)
3. Generate monitoring dashboard
4. Create 3 cows via API
5. Show logs from each layer
6. Query each layer (Events/Bronze/Silver/SQL)
7. Update cow breed
8. Demonstrate eventual consistency
9. Query analytics API
10. Generate timeline visualization

**Quick Test:**
```bash
# Just create test data
./demo/run_demo.sh --quick

# Skip cleanup
./demo/run_demo.sh --no-cleanup
```

### Test Data Generation

**Generate test cows:**
```python
# tests/generate_test_data.py
import requests
from faker import Faker

fake = Faker()

breeds = ["Holstein", "Angus", "Hereford", "Simmental", "Jersey"]

for i in range(100):
    response = requests.post("http://localhost:8000/api/cows", json={
        "breed": fake.random_element(breeds),
        "birth_date": fake.date_between(start_date="-5y", end_date="today").isoformat(),
        "sex": fake.random_element(["Male", "Female"])
    })
    print(f"Created cow {i+1}: {response.json()['cow_id']}")
```

---

## 5. Common Tasks

### Add a New Event Type

**Step 1: Define Event Type**

Edit [backend/db/models/events.py](../backend/db/models/events.py):
```python
# Add to EventType enum
class EventType(str, Enum):
    COW_CREATED = "cow_created"
    COW_UPDATED = "cow_updated"
    COW_DEACTIVATED = "cow_deactivated"
    COW_VACCINATED = "cow_vaccinated"  # NEW
```

**Step 2: Create API Endpoint**

Add to [backend/api/routers/cows.py](../backend/api/routers/cows.py):
```python
@router.post("/cows/{cow_id}/vaccinate")
def vaccinate_cow(
    cow_id: str,
    vaccine_data: VaccineData,
    db: Session = Depends(get_db)
):
    # Emit cow_vaccinated event
    event = CowEvent(
        event_id=uuid4(),
        aggregate_id=UUID(cow_id),
        event_type=EventType.COW_VACCINATED,
        data={
            "vaccine_type": vaccine_data.vaccine_type,
            "administered_date": vaccine_data.date.isoformat()
        }
    )
    db.add(event)
    db.commit()
    return {"event_id": str(event.event_id)}
```

**Step 3: Handle in Silver Layer**

Update Silver processing logic (if needed):
```python
# backend/spark/process_silver.py
def process_cow_vaccinated(event):
    # Add vaccination record to cow history
    # Update is_current flags if needed
    pass
```

**Step 4: Update Sync Job**

If new fields added to SQL projection:
```sql
-- Add migration
ALTER TABLE operational.cows ADD COLUMN last_vaccination_date DATE;
```

**Step 5: Test**
```bash
curl -X POST http://localhost:8000/api/cows/<cow_id>/vaccinate \
  -H "Content-Type: application/json" \
  -d '{"vaccine_type": "Rabies", "date": "2024-01-20"}'
```

### Add a New Gold Analytics Table

**Step 1: Define Schema**

Create [backend/spark/gold/vaccination_stats.py](../backend/spark/gold/vaccination_stats.py):
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, min

def create_vaccination_stats(spark: SparkSession):
    # Read from Silver
    silver_df = spark.read.format("delta").load("delta_lake/silver")
    
    # Aggregate vaccination data
    stats_df = silver_df \
        .filter(col("vaccination_date").isNotNull()) \
        .groupBy("breed") \
        .agg(
            count("*").alias("vaccinated_count"),
            max("vaccination_date").alias("last_vaccination"),
            min("vaccination_date").alias("first_vaccination")
        )
    
    # Write to Gold
    stats_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("delta_lake/gold/vaccination_stats")
```

**Step 2: Schedule Processing**

Add to gold aggregation job:
```python
# Run after Silver processing
create_vaccination_stats(spark)
```

**Step 3: Create API Endpoint**

```python
@router.get("/analytics/vaccinations")
def get_vaccination_stats():
    spark = get_spark_session()
    df = spark.read.format("delta").load("delta_lake/gold/vaccination_stats")
    return df.toPandas().to_dict(orient="records")
```

### Rebuild Silver from Bronze

**Complete Rebuild:**
```bash
# Backup current Silver
mv delta_lake/silver delta_lake/silver_backup_$(date +%Y%m%d_%H%M%S)

# Rebuild from Bronze
python backend/spark/rebuild_silver.py
```

**Rebuild Script:**
```python
# backend/spark/rebuild_silver.py
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Rebuild-Silver") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0.0") \
    .getOrCreate()

# Read all Bronze events
bronze_df = spark.read.format("delta").load("delta_lake/bronze")

# Process events in order
events_sorted = bronze_df.orderBy("event_timestamp")

# Rebuild state
# ... (SCD Type 2 logic)

# Write to new Silver
events_sorted.write \
    .format("delta") \
    .mode("overwrite") \
    .save("delta_lake/silver")

print("Silver layer rebuilt successfully!")
```

### Reset the System

**Complete Reset (Development Only!):**

```bash
#!/bin/bash
# reset_system.sh

echo "⚠️  WARNING: This will delete ALL data!"
read -p "Are you sure? (type 'yes'): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

echo "Resetting system..."

# 1. Truncate database tables
psql -d endymion_ai <<EOF
TRUNCATE events.cow_events CASCADE;
TRUNCATE operational.cows CASCADE;
TRUNCATE sync.sync_state CASCADE;
TRUNCATE sync.sync_runs CASCADE;
TRUNCATE sync.sync_conflicts CASCADE;
EOF

# 2. Delete Delta Lake data
rm -rf delta_lake/bronze/*
rm -rf delta_lake/silver/*
rm -rf delta_lake/gold/*

# 3. Recreate directories
mkdir -p delta_lake/bronze
mkdir -p delta_lake/silver
mkdir -p delta_lake/gold

echo "✅ System reset complete!"
echo "Restart all services to begin fresh."
```

**Partial Reset (SQL Projection Only):**
```sql
TRUNCATE operational.cows CASCADE;
DELETE FROM sync.sync_state WHERE table_name = 'cows';
```

Then run sync job to rebuild from Silver.

---

## 6. Troubleshooting

### Sync Lag is High

**Symptoms:**
- Sync lag > 60 seconds
- UI shows yellow/red indicator
- Changes take long to appear

**Diagnosis:**
```sql
-- Check sync state
SELECT 
    table_name,
    last_sync_started_at,
    last_sync_completed_at,
    EXTRACT(EPOCH FROM (last_sync_completed_at - last_sync_started_at)) as sync_duration_seconds,
    EXTRACT(EPOCH FROM (NOW() - last_sync_completed_at)) as lag_seconds,
    total_rows_synced
FROM sync.sync_state;

-- Check recent runs
SELECT 
    started_at,
    completed_at,
    rows_synced,
    conflicts_resolved,
    EXTRACT(EPOCH FROM (completed_at - started_at)) as duration
FROM sync.sync_runs
ORDER BY started_at DESC
LIMIT 10;
```

**Solutions:**

1. **Increase Sync Frequency:**
   ```python
   # backend/sync/sync_scheduler.py
   SYNC_INTERVAL_SECONDS = 15  # Reduce from 30
   ```

2. **Check System Resources:**
   ```bash
   # CPU/Memory usage
   top
   
   # Disk I/O
   iostat -x 1
   ```

3. **Optimize SQL Queries:**
   ```sql
   -- Add indexes
   CREATE INDEX idx_cows_updated_at ON operational.cows(updated_at);
   
   -- Analyze tables
   ANALYZE operational.cows;
   ```

4. **Check Sync Job Logs:**
   ```bash
   tail -f logs/sync_scheduler.log
   ```

### Events Stuck in Outbox

**Symptoms:**
- `published = FALSE` count increasing
- Events not appearing in Bronze/Silver

**Diagnosis:**
```sql
-- Count unpublished events
SELECT COUNT(*) FROM events.cow_events WHERE published = FALSE;

-- Check oldest unpublished
SELECT 
    event_id,
    event_type,
    event_timestamp,
    EXTRACT(EPOCH FROM (NOW() - event_timestamp)) as age_seconds
FROM events.cow_events
WHERE published = FALSE
ORDER BY event_timestamp
LIMIT 10;
```

**Solutions:**

1. **Manual Publish (Emergency):**
   ```sql
   UPDATE events.cow_events
   SET published = TRUE
   WHERE published = FALSE
   AND event_timestamp < NOW() - INTERVAL '5 minutes';
   ```

2. **Check Bronze Write Permissions:**
   ```bash
   ls -la delta_lake/bronze/
   # Should be writable
   ```

3. **Restart Publishing Service:**
   ```bash
   # If using separate publisher
   systemctl restart endymion-ai-publisher
   ```

### Silver State Looks Wrong

**Symptoms:**
- Cow has incorrect attributes
- History missing
- `is_current` flags incorrect

**Diagnosis:**

```python
# Check Silver history for a cow
spark = get_spark_session()
silver_df = spark.read.format("delta").load("delta_lake/silver")

cow_history = silver_df.filter(
    silver_df.cow_id == "<cow_id>"
).orderBy("valid_from").show()

# Check event count vs Silver rows
bronze_df = spark.read.format("delta").load("delta_lake/bronze")
cow_events = bronze_df.filter(
    bronze_df.aggregate_id == "<cow_id>"
).count()

print(f"Events: {cow_events}, Silver rows: {cow_history.count()}")
```

**Solutions:**

1. **Rebuild Silver for Specific Cow:**
   ```python
   # Replay events for one cow
   python backend/spark/replay_cow.py --cow-id <cow_id>
   ```

2. **Check SCD Type 2 Logic:**
   ```python
   # Verify is_current flags
   current_count = silver_df.filter(
       (silver_df.cow_id == "<cow_id>") & 
       (silver_df.is_current == True)
   ).count()
   
   assert current_count == 1, "Should have exactly one current record"
   ```

3. **Full Silver Rebuild:**
   ```bash
   python backend/spark/rebuild_silver.py
   ```

### SQL Projection Diverged

**Symptoms:**
- Row counts don't match Silver
- Data inconsistencies between layers
- Conflicts detected

**Diagnosis:**

```sql
-- Compare counts
SELECT 'SQL' as layer, COUNT(*) as count FROM operational.cows
UNION ALL
SELECT 'Silver', COUNT(*) FROM (
    -- Would need to query Silver via API/Spark
    SELECT COUNT(*) FROM silver_current
) as silver;

-- Check conflicts
SELECT 
    conflict_timestamp,
    cow_id,
    silver_data->>'breed' as silver_breed,
    sql_data->>'breed' as sql_breed,
    resolution
FROM sync.sync_conflicts
ORDER BY conflict_timestamp DESC;
```

**Solutions:**

1. **Force Full Resync:**
   ```python
   # backend/sync/force_resync.py
   from backend.sync.sync_silver_to_sql import sync_silver_to_sql
   
   sync_silver_to_sql(force_full_sync=True)
   ```

2. **Rebuild SQL from Silver:**
   ```sql
   -- Backup current data
   CREATE TABLE operational.cows_backup AS 
   SELECT * FROM operational.cows;
   
   -- Truncate and resync
   TRUNCATE operational.cows;
   DELETE FROM sync.sync_state WHERE table_name = 'cows';
   ```
   
   Then run sync job.

3. **Investigate Specific Conflicts:**
   ```sql
   SELECT * FROM sync.sync_conflicts
   WHERE cow_id = '<cow_id>'
   ORDER BY conflict_timestamp DESC;
   ```

### General Debugging Checklist

- [ ] All services running (FastAPI, Sync, Frontend)
- [ ] Database accessible (`psql -d endymion_ai`)
- [ ] Delta Lake directories writable
- [ ] No Python errors in logs
- [ ] Event count increasing (`SELECT COUNT(*) FROM events.cow_events`)
- [ ] Sync job completing (`SELECT * FROM sync.sync_runs LIMIT 1`)
- [ ] Frontend polling working (Network tab in DevTools)
- [ ] No CORS errors in browser console

---

## Additional Resources

- [Architecture Diagram](../ARCHITECTURE.md)
- [API Documentation](./API.md)
- [Operations Guide](./OPERATIONS.md)
- [Frontend README](../frontend/README.md)
- [Monitoring Guide](../backend/monitoring/README.md)
- [Demo Scripts](../demo/README.md)

## Getting Help

1. Check logs: `tail -f logs/*.log`
2. Search issues: GitHub Issues
3. Ask in Slack: #endymion-ai-dev
4. Review tests: `tests/` directory

---

**Last Updated:** January 2026  
**Version:** 1.0.0  
**Maintainer:** Endymion-AI Team
