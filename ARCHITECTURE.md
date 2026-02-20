# Endymion-AI - Complete System Architecture

## 🎯 System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Endymion-AI - Production System                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         API Layer (FastAPI)                            │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐             │  │
│  │  │  /cows   │  │/analytics│  │  /health │  │ /metrics │             │  │
│  │  │  CRUD    │  │ DuckDB   │  │  Checks  │  │Prometheus│             │  │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘             │  │
│  └───────┼─────────────┼─────────────┼─────────────┼────────────────────┘  │
│          │             │             │             │                         │
│          ↓             │             │             │                         │
│  ┌──────────────────┐  │             │             │                         │
│  │  Write Commands  │  │             │             │                         │
│  │  (Event Sourcing)│  │             │             │                         │
│  └────────┬─────────┘  │             │             │                         │
│           │            │             │             │                         │
│           ↓            │             │             │                         │
│  ┌───────────────────────────────────────────────────────────────────┐      │
│  │                Event Store (SQL Server 2022)                       │      │
│  │  ┌──────────────────────────────────────────────────────────┐     │      │
│  │  │ operational.cow_events (Outbox Pattern)                  │     │      │
│  │  │ • event_id (UNIQUEIDENTIFIER)                            │     │      │
│  │  │ • event_type (cow_created, cow_updated, etc.)           │     │      │
│  │  │ • tenant_id (Multi-tenant isolation)                    │     │      │
│  │  │ • cow_id (Cow ID)                                        │     │      │
│  │  │ • payload (NVARCHAR(MAX) - JSON)                        │     │      │
│  │  │ • published_to_bronze_at (DATETIME2)                    │     │      │
│  │  │ • event_time (Immutable timestamp)                       │     │      │
│  │  └──────────────────────────────────────────────────────────┘     │      │
│  └────────┬──────────────────────────────────────────────────────────┘      │
│           │                                                                  │
│           ↓                                                                  │
│  ┌───────────────────────────────────────────────────────────────────┐      │
│  │              Bronze Layer (Delta Lake - s3://bronze/)              │      │
│  │  • Raw event storage                                               │      │
│  │  • Immutable Parquet files                                         │      │
│  │  • Complete audit trail                                            │      │
│  │  • Retention: Forever                                              │      │
│  └────────┬──────────────────────────────────────────────────────────┘      │
│           │                                                                  │
│           ↓                                                                  │
│  ┌───────────────────────────────────────────────────────────────────┐      │
│  │              Silver Layer (Delta Lake - s3://silver/)              │      │
│  │  • Cleaned and deduplicated state                                  │      │
│  │  • SCD Type 2 history (__START_AT/__END_AT/__CURRENT)             │      │
│  │  • Data quality expectations (14 checks: DROP + WARN)             │      │
│  │  • Quality log tracking (s3://silver/quality_log)                 │      │
│  │  • Time-travel queries enabled                                     │      │
│  │  • PySpark 3.5.3 + Hadoop AWS JARs                                │      │
│  └────────┬──────────────────────────────────────────────────────────┘      │
│           │                                                                  │
│           ├──────────────────────────────────────────────────────────┐      │
│           │                                                          │      │
│           ↓                                                          ↓      │
│  ┌────────────────────┐                                    ┌──────────────┐ │
│  │   Sync Job         │                                    │ Gold Layer   │ │
│  │  (30s interval)    │                                    │ (Aggregates) │ │
│  │  • Watermark-based │                                    │ • Herd comp. │ │
│  │  • Incremental     │                                    │ • Weight     │ │
│  │  • UPSERT          │                                    │ • Sales      │ │
│  │  • Conflict res.   │                                    │ • Analytics  │ │
│  └─────────┬──────────┘                                    └──────┬───────┘ │
│            │                                                      │         │
│            ↓                                                      │         │
│  ┌────────────────────────────────────────────────────────┐      │         │
│  │       SQL Projection (SQL Server 2022)                 │      │         │
│  │  ┌──────────────────────────────────────────────┐     │      │         │
│  │  │ operational.cows (Table)                      │     │      │         │
│  │  │ • cow_id (UNIQUEIDENTIFIER)                  │     │      │         │
│  │  │ • tag_number, name, breed, sex               │     │      │         │
│  │  │ • weight_kg, birth_date                      │     │      │         │
│  │  │ • status (active/inactive)                   │     │      │         │
│  │  │ • Payload JSON for API responses             │     │      │         │
│  │  │ • Nonclustered indexes                       │     │      │         │
│  │  └──────────────────────────────────────────────┘     │      │         │
│  └────────────┬───────────────────────────────────────────┘      │         │
│               │                                                   │         │
│               │ (Read Path)                                       │         │
│               └───────────────────────────────────────────────────┘         │
│                              ↑                                    ↑         │
│                              │                                    │         │
│  ┌──────────────────────────┴────────────────────────────────────┴───────┐ │
│  │                      API Read Endpoints                               │ │
│  │  GET /api/v1/cows         → operational.cows (SQL Server)            │ │
│  │  GET /api/v1/analytics/*  → DuckDB → Gold Delta Lake (direct query) │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Monitoring & Observability                        │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐   │   │
│  │  │  Health    │  │  Metrics   │  │ Dashboard  │  │  Alerting  │   │   │
│  │  │  Checks    │  │(Prometheus)│  │   (HTML)   │  │ (PagerDuty)│   │   │
│  │  │  • Sync lag│  │ • 13 metrics│  │ • Visual   │  │ • Critical │   │   │
│  │  │  • Backlog │  │ • Counters │  │ • Real-time│  │ • Email    │   │   │
│  │  │  • Fresh   │  │ • Gauges   │  │ • Tables   │  │ • Slack    │   │   │
│  │  │  • Consist.│  │ • Histogram│  │ • Graphs   │  │            │   │   │
│  │  │  • Failures│  │            │  │            │  │            │   │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘   │   │
│  │  GET /health    GET /metrics    GET /dashboard                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        Sync Tracking                                 │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │   │
│  │  │ sync_state   │  │  sync_logs   │  │sync_conflicts│              │   │
│  │  │ • Watermark  │  │  • History   │  │ • Resolution │              │   │
│  │  │ • Last sync  │  │  • Duration  │  │ • Fields     │              │   │
│  │  │ • Row counts │  │  • Status    │  │ • Values     │              │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🔄 Data Flow Sequences

### Write Operation (Create Cow)

```
1. Client → POST /api/v1/cows (JSON payload)
              ↓
2. FastAPI → Validate request (Pydantic)
              ↓
3. FastAPI → INSERT INTO events.cow_events
             • event_type: "cow_created"
             • published: false
             • aggregate_version: 1
              ↓
4. FastAPI → Return 201 Created (event_id)
              ↓
5. Outbox Processor → UPDATE published = true
              ↓
6. Event Bus → Publish to Bronze topic
              ↓
7. Bronze → Write to Delta Lake (s3://bronze/cow_events/)
              ↓
8. PySpark → Process Bronze → Silver
             • Deduplicate
             • Validate
             • Enrich
              ↓
9. Silver → Write to Delta Lake (s3://silver/cows/)
            • Insert new row with valid_from = now
              ↓
10. Sync Job (30s interval) → Read Silver changes
               • Query with watermark
               • Batch 1000 rows
                ↓
11. Sync Job → UPSERT into operational.cows
               • INSERT if new
               • UPDATE if exists (Silver wins)
                ↓
12. SQL Projection → Ready for queries
```

**Timeline:** 30-60 seconds from write to SQL projection

### Read Operation (List Cows)

```
1. Client → GET /api/v1/cows
              ↓
2. FastAPI → Query operational.cows
             • Filter by tenant_id
             • Apply pagination
             • Use indexes
              ↓
3. SQL Server → Return results (fast, indexed)
              ↓
4. FastAPI → Return 200 OK (JSON)
```

**Timeline:** < 100ms

### Analytics Query (Herd Composition)

```
1. Client → GET /api/v1/analytics/herd-composition
              ↓
2. FastAPI → Check cache (5s TTL)
              ↓ (cache miss)
3. DuckDB → Query Gold Delta Lake directly
             • delta_scan('s3://gold/herd_composition')
             • In-memory engine, optimized columnar queries
             • MinIO S3 backend (localhost:9000)
              ↓
4. FastAPI → Cache result (5s)
              ↓
5. FastAPI → Return 200 OK (JSON)
```

**Timeline:** 10-50ms (direct Gold Delta query)

**Note:** DuckDB queries Gold Delta Lake directly without projection.
This provides:
- Zero sync lag (queries canonical source directly)
- No projection overhead (eliminates SQL sync step)
- Mirrors Databricks SQL serverless warehouses pattern
- Gold Delta Lake remains single source of truth

### Update Operation (Change Breed)

```
1. Client → PUT /api/v1/cows/{id} (JSON patch)
              ↓
2. FastAPI → INSERT INTO events.cow_events
             • event_type: "cow_updated"
             • aggregate_version: 2
             • payload: {"breed": "Angus"}
              ↓
3. Event Processing → Same as Create (steps 5-12)
              ↓
4. Silver → Insert new history row
            • Old row: valid_to = now
            • New row: valid_from = now, valid_to = NULL
              ↓
5. Sync Job → UPDATE operational.cows
              ↓
6. SQL Projection → Shows updated breed
```

**Timeline:** 30-60 seconds for eventual consistency

## 📊 System Characteristics

### Throughput
- **Writes**: 1000+ events/second (append-only)
- **Reads**: 10,000+ queries/second (indexed SQL)
- **Sync**: 1000 rows/batch, 30s interval
- **Analytics**: Cached (60s), pre-computed

### Latency
- **Write ACK**: < 50ms (event stored)
- **Read Query**: < 100ms (SQL indexed)
- **Sync Lag**: 30-60s (eventual consistency)
- **Analytics (DuckDB)**: 10-50ms (direct Gold Delta queries)
- **Analytics (cached)**: 3-10ms (in-memory cache)

### Scalability
- **Events**: Infinite (append-only, never deleted)
- **SQL Projection**: Millions of rows (indexed)
- **Bronze/Silver/Gold**: Petabytes (Delta Lake)
- **Horizontal scaling**: Add more API instances

### Availability
- **API**: 99.9% (3 replicas, load balanced)
- **Database**: 99.95% (SQL Server HA)
- **Sync**: Self-healing (retries, backoff)
- **Monitoring**: Real-time health checks

### Consistency
- **Events**: Strongly consistent (ACID)
- **SQL Projection**: Eventually consistent (30-60s lag)
- **Analytics (DuckDB)**: Eventually consistent (queries Gold Delta directly)
- **Conflict Resolution**: Silver always wins

## 🔍 Component Details

### Event Store (SQL Server)
- **Purpose**: Immutable source of truth
- **Tables**: events.cow_events
- **Properties**: Append-only, versioned, timestamped
- **Retention**: Forever (audit trail)
- **Indexes**: aggregate_id, event_timestamp, published

### Bronze Layer (Delta Lake)
- **Purpose**: Raw event storage
- **Format**: Parquet files
- **Properties**: Immutable, partitioned by date
- **Retention**: Forever
- **Access**: PySpark queries

### Silver Layer (Delta Lake)
- **Purpose**: Cleaned state with history
- **Format**: Parquet files with SCD Type 2
- **Properties**: Current + historical rows
- **Retention**: Forever
- **Access**: PySpark queries, sync jobs

### SQL Projection (SQL Server)
- **Purpose**: Fast operational queries
- **Tables**: operational.cows
- **Properties**: Indexed, denormalized, tenant-isolated
- **Retention**: Active data only
- **Access**: API read endpoints

### Gold Layer (Delta Lake)
- **Purpose**: Pre-computed analytics and aggregations
- **Format**: Parquet files (aggregations)
- **Properties**: Optimized for analytical queries
- **Retention**: Based on business needs
- **Access**: DuckDB (FastAPI analytics endpoints), BI tools, Databricks SQL

### Sync Job
- **Purpose**: Keep SQL projection consistent with Silver
- **Pattern**: Watermark-based incremental sync
- **Frequency**: Every 30 seconds
- **Batch Size**: 1000 rows
- **Conflict Resolution**: Silver wins (last write)

### Monitoring
- **Health Checks**: 5 checks (lag, backlog, freshness, consistency, failures)
- **Metrics**: 13 Prometheus metrics
- **Dashboard**: HTML real-time visualization
- **Alerting**: Critical/Warning thresholds

## 🎯 Production Deployment

```
┌─────────────────────────────────────────────────────────────┐
│                   Kubernetes Cluster                         │
├─────────────────────────────────────────────────────────────┤
│  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │  API Pod   │  │  API Pod   │  │  API Pod   │            │
│  │ (FastAPI)  │  │ (FastAPI)  │  │ (FastAPI)  │            │
│  └────────────┘  └────────────┘  └────────────┘            │
│         ↑                                                    │
│    ┌────┴─────┐                                             │
│    │ Ingress  │ (nginx)                                     │
│    │ /health  │ (liveness/readiness probes)                 │
│    └──────────┘                                             │
│                                                              │
│  ┌────────────┐                                             │
│  │ Sync Job   │ (CronJob, every 30s)                        │
│  │ (scheduler)│                                             │
│  └────────────┘                                             │
│                                                              │
│  ┌────────────────────────────────────────────┐             │
│  │        Monitoring Stack                    │             │
│  │  ┌───────────┐  ┌───────────┐             │             │
│  │  │Prometheus │  │  Grafana  │             │             │
│  │  │(metrics)  │  │(dashboards)│             │             │
│  │  └───────────┘  └───────────┘             │             │
│  │  ┌────────────────────────────┐            │             │
│  │  │     Alertmanager           │            │             │
│  │  │  → PagerDuty/Slack         │            │             │
│  │  └────────────────────────────┘            │             │
│  └────────────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────────┘
```

## 📈 Metrics Dashboard

Grafana dashboard shows:
- **Sync Lag**: Real-time lag gauge
- **Event Rate**: Events per minute graph
- **Sync Duration**: Average sync time
- **Row Counts**: SQL vs Silver comparison
- **Failure Rate**: Sync failures percentage
- **Health Status**: Overall system health

## 🎉 Summary

Endymion-AI implements a **production-ready event-sourced architecture** with:

✅ Complete audit trail (immutable events)  
✅ Fast reads (SQL projection for operational data)  
✅ Scalable writes (append-only)  
✅ Time-travel queries (Silver history)  
✅ Pre-computed analytics (Gold Delta Lake)  
✅ DuckDB analytics (queries Gold directly, 10-50ms, mirrors Databricks SQL pattern)  
✅ Comprehensive monitoring (5 checks, 13 metrics)  
✅ Eventually consistent (30-60s lag for operational, near-real-time for analytics)  
✅ Self-healing (retry logic, graceful shutdown)  

**Ready for production deployment and Databricks migration! 🚀**

See `demo/DATABRICKS_MIGRATION.md` for cloud deployment guide (AWS/Azure/GCP).
