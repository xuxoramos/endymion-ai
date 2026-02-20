## Gold Layer - Pre-Computed Analytics

The **Gold layer** contains business-ready aggregations and analytics computed from Silver. This is the final layer in the medallion architecture optimized for BI tools, dashboards, and **fast API queries via SQL Server projection**.

### Key Principles

✅ **Fully Recomputable**: Gold can be completely rebuilt from Silver  
✅ **No Manual Overrides**: All data is computed, never manually entered  
✅ **Deterministic**: Same Silver → Same Gold (always)  
✅ **Includes Lineage**: Full traceability to Silver sources  
✅ **BI-Optimized**: Pre-aggregated for fast query performance  
✅ **SQL Projection**: Automatically projected to SQL Server analytics schema for FastAPI

---

## Architecture

```
Silver Layer (Source of Truth)
       ↓
    ┌──────────────────────────────────┐
    │  Gold Transformation Logic       │
    │  - Aggregations                  │
    │  - Window functions              │
    │  - Business calculations         │
    └──────────────────────────────────┘
       ↓
Gold Layer (Business Analytics - Delta Lake)
  ├─ herd_composition (Canonical)
  ├─ daily_snapshots (Canonical)
  ├─ cow_lifecycle (Canonical)
  ├─ daily_sales (planned)
  └─ weight_trends (planned)
       ↓
    ┌──────────────────────────────────┐
    │  SQL Server Projection           │
    │  (Disposable, Rebuilt from Gold) │
    └──────────────────────────────────┘
       ↓
SQL Server analytics.* schema (Fast API queries)
  ├─ analytics.herd_composition
  ├─ analytics.cow_lifecycle
  └─ analytics.daily_snapshots
       ↓
FastAPI (SQLAlchemy queries <100ms)
```

### SQL Server Projection

**What:** Gold Delta tables are automatically projected to SQL Server analytics schema  
**Why:** Enable fast (<100ms) API queries without heavyweight Spark sessions  
**How:** Unified Gold runner writes to both Delta Lake AND SQL Server via JDBC  
**When:** Every 10 seconds (sync-analytics.sh service)

**Key Benefits:**
- 99% faster API queries (21-76ms vs 2-10 seconds with Spark)
- 95% less memory (84MB vs 500MB-2GB)
- No JVM startup overhead
- Standard SQLAlchemy patterns
- Connection pooling
- Horizontal scaling via SQL Server read replicas

**Architecture Principle:**
```
Gold Delta Lake = Canonical Analytical Truth (never deleted, authoritative)
SQL Server analytics.* = Disposable Projection (can rebuild from Gold anytime)
```

---

## Implemented Gold Tables

### 1. gold_herd_composition

**Daily herd composition by breed, status, and sex**

**Schema:**
- `tenant_id`: Tenant identifier
- `snapshot_date`: Date of snapshot
- `breed`: Cow breed
- `status`: active/inactive
- `sex`: male/female
- `cow_count`: Number of cows
- `avg_weight_kg`: Average weight
- `total_weight_kg`: Total weight
- `_gold_table`, `_source_tables`, `_aggregation_type`: Lineage metadata

**Query Example:**
```python
# Get herd composition on 2026-01-24
composition = spark.read.format("delta") \
    .load("s3a://gold/herd_composition") \
    .filter("snapshot_date = '2026-01-24'")

# Breed breakdown for tenant
breed_breakdown = composition \
    .filter(f"tenant_id = '{tenant_id}'") \
    .groupBy("breed") \
    .sum("cow_count") \
    .show()
```

**Build:**
```bash
# Full refresh (all dates)
python databricks/gold/gold_herd_composition.py --full-refresh

# Specific date
python databricks/gold/gold_herd_composition.py --date 2026-01-24

# Last 7 days
python databricks/gold/gold_herd_composition.py --days 7
```

**Query via FastAPI:**
```bash
# Query from SQL Server analytics schema (fast!)
curl "http://localhost:8000/api/v1/analytics/herd-composition?tenant_id=550e8400-e29b-41d4-a716-446655440000"

# Response time: 21-76ms (uncached), 3-39ms (cached)
# {
#   "as_of": "2026-01-28T06:09:05.669752",
#   "cached": false,
#   "data": {
#     "snapshot_date": "2026-01-28",
#     "total_cows": 7,
#     "by_breed": [...],
#     "by_status": [...],
#     "by_sex": [...]
#   }
# }
```

**Query via PySpark (for BI/analysis):**
```python
# Get herd composition on 2026-01-24 from Delta Lake
composition = spark.read.format("delta") \
    .load("s3a://gold/herd_composition") \
    .filter("snapshot_date = '2026-01-24'")

# Breed breakdown for tenant
breed_breakdown = composition \
    .filter(f"tenant_id = '{tenant_id}'") \
    .groupBy("breed") \
    .sum("cow_count") \
    .show()
```

---

### 2. gold_daily_snapshots

**Daily cow count snapshots per tenant**

**Schema:**
- `tenant_id`: Tenant identifier
- `snapshot_date`: Date of snapshot
- `total_cows`: Total count
- `active_cows`: Active count
- `inactive_cows`: Inactive count
- `cows_created_today`: New cows
- `cows_deactivated_today`: Deactivated cows
- `net_change`: Net change (created - deactivated)
- `change_from_prev_day`: Day-over-day change

**Query Example:**
```python
# Get daily trends for tenant
daily_trends = spark.read.format("delta") \
    .load("s3a://gold/daily_snapshots") \
    .filter(f"tenant_id = '{tenant_id}'") \
    .orderBy("snapshot_date") \
    .select("snapshot_date", "total_cows", "net_change")

# Show last 30 days
daily_trends.filter("snapshot_date >= current_date() - 30").show()
```

**Build:**
```bash
# Full refresh
python databricks/gold/gold_daily_snapshots.py --full-refresh

# Last 30 days
python databricks/gold/gold_daily_snapshots.py --days 30
```

---

### 3. gold_cow_lifecycle

**Cow lifecycle analytics from creation to deactivation**

**Schema:**
- `tenant_id`: Tenant identifier
- `cow_id`: Cow identifier
- `created_at`: Creation timestamp
- `last_updated_at`: Last update timestamp
- `initial_tag_number`, `initial_breed`: Initial values
- `final_status`, `final_breed`, `final_weight_kg`: Final values
- `deactivation_reason`: Reason if deactivated
- `lifecycle_duration_days`: Days from creation to now/deactivation
- `state_change_count`: Number of state changes
- `lifecycle_stage`: new/growing/mature/aged/deactivated

**Query Example:**
```python
# Get lifecycle summary by tenant
lifecycle = spark.read.format("delta") \
    .load("s3a://gold/cow_lifecycle")

# Average lifecycle duration
avg_lifecycle = lifecycle \
    .groupBy("tenant_id") \
    .avg("lifecycle_duration_days") \
    .show()

# Deactivation reasons
deactivation_summary = lifecycle \
    .filter("final_status = 'inactive'") \
    .groupBy("tenant_id", "deactivation_reason") \
    .count() \
    .show()
```

**Build:**
```bash
# Full refresh
python databricks/gold/gold_cow_lifecycle.py --full-refresh

# Specific tenant
python databricks/gold/gold_cow_lifecycle.py --tenant <tenant_id>
```

---

## Planned Gold Tables

### 4. gold_daily_sales (Planned)

**Daily sales aggregations**

**Requires:** `silver_sales` table (not yet implemented)

**Expected Schema:**
- `tenant_id`: Tenant identifier
- `sale_date`: Sale date
- `total_sales_amount`: SUM(sale_price)
- `sale_count`: COUNT(*)
- `avg_sale_price`: AVG(sale_price)
- `breed`: Optional breakdown

**To Implement:**
1. Add `cow_sale` event type to Bronze
2. Create Silver sales resolution logic
3. Complete `gold_daily_sales.py`

---

### 5. gold_cow_weight_trends (Planned)

**30-day weight trends and gain calculations**

**Requires:** `silver_weights` table (not yet implemented)

**Expected Schema:**
- `tenant_id`: Tenant identifier
- `cow_id`: Cow identifier
- `measurement_date`: Date
- `avg_weight_30d`: 30-day rolling average
- `weight_gain_kg_per_day`: Daily weight gain
- `percentile_in_breed`: Weight ranking
- `trend_direction`: increasing/stable/decreasing

**To Implement:**
1. Add `cow_weight_measured` event type to Bronze
2. Create Silver weight history table
3. Complete `gold_cow_weight_trends.py`

---

## Reproducibility

### Rebuild All Tables

```bash
# Rebuild all Gold tables from Silver
python databricks/gold/rebuild_all.py

# Rebuild specific table
python databricks/gold/rebuild_all.py --table herd_composition

# Verify without rebuilding
python databricks/gold/rebuild_all.py --verify-only

# Test determinism (rebuild twice, compare)
python databricks/gold/rebuild_all.py --determinism-test herd_composition
```

### Test Script

```bash
# Complete test: build, verify, test determinism
./databricks/gold/test_gold_rebuild.sh
```

**Test Coverage:**
1. ✅ Initial build of all Gold tables
2. ✅ Table verification
3. ✅ Content inspection
4. ✅ Determinism test (rebuild twice, compare counts)
5. ✅ Single table rebuild

---

## Data Lineage

All Gold tables include lineage metadata:

| Column | Description |
|--------|-------------|
| `_gold_table` | Name of Gold table |
| `_source_tables` | Comma-separated Silver source tables |
| `_aggregation_type` | Type of aggregation performed |
| `_computation_timestamp` | When computed |
| `_computation_logic` | Brief description of logic |

**Query Lineage:**
```python
# Show lineage for herd_composition
df = spark.read.format("delta").load("s3a://gold/herd_composition")
df.select("_gold_table", "_source_tables", "_aggregation_type").distinct().show()
```

---

## BI Tool Integration

### Power BI

```python
# Connect to Gold layer
source = Spark.Contents("s3a://gold/daily_snapshots"),
data = source{[name="delta"]}[Data]
```

### Tableau

Use Delta Lake connector:
- Server: `localhost:9000`
- Path: `gold/herd_composition`
- Format: Delta

### Databricks SQL

```sql
CREATE TABLE gold_herd_composition
USING DELTA
LOCATION 's3a://gold/herd_composition';

SELECT 
    snapshot_date,
    breed,
    SUM(cow_count) as total_cows
FROM gold_herd_composition
WHERE tenant_id = '<tenant_id>'
GROUP BY snapshot_date, breed
ORDER BY snapshot_date DESC;
```

---

## Performance Optimization

### Partitioning

Gold tables are partitioned for query performance:

- `herd_composition`: Partitioned by `snapshot_date`
- `daily_snapshots`: Partitioned by `snapshot_date`
- `cow_lifecycle`: Partitioned by `tenant_id`

**Query Optimization:**
```python
# Efficient: Uses partition pruning
df = spark.read.format("delta") \
    .load("s3a://gold/daily_snapshots") \
    .filter("snapshot_date >= '2026-01-01'")

# Inefficient: Reads all partitions
df = spark.read.format("delta") \
    .load("s3a://gold/daily_snapshots") \
    .filter("total_cows > 100")
```

### Incremental Updates

For daily updates, use incremental mode instead of full refresh:

```bash
# Update only today's data
python databricks/gold/gold_daily_snapshots.py --date $(date +%Y-%m-%d)

# Update last 7 days
python databricks/gold/gold_daily_snapshots.py --days 7
```

---

## Monitoring

### Key Metrics

Track these metrics for Gold layer health:

1. **Rebuild Time**: Time to rebuild each table
2. **Row Counts**: Expected vs actual rows
3. **Freshness**: Age of most recent data
4. **Determinism**: Row count consistency across rebuilds
5. **Lineage Coverage**: % of rows with lineage metadata

### Queries

**Check Freshness:**
```python
df = spark.read.format("delta").load("s3a://gold/daily_snapshots")
latest = df.agg({"snapshot_date": "max"}).collect()[0][0]
print(f"Latest snapshot: {latest}")
```

**Verify Determinism:**
```bash
# Run twice, compare counts
python databricks/gold/rebuild_all.py > /tmp/build1.log
COUNT1=$(grep "row_count" /tmp/build1.log)

python databricks/gold/rebuild_all.py > /tmp/build2.log
COUNT2=$(grep "row_count" /tmp/build2.log)

diff /tmp/build1.log /tmp/build2.log
```

---

## Best Practices

### 1. Always Include Lineage
```python
from common import add_lineage_metadata

df = add_lineage_metadata(
    df,
    source_tables=["silver_cows_history"],
    aggregation_type="daily_snapshot",
    gold_table_name="gold_daily_snapshots",
    computation_logic="Daily cow counts using SCD Type 2"
)
```

### 2. Use SCD Type 2 for Point-in-Time Queries
```python
# Get state on specific date
snapshot_timestamp = datetime.combine(snapshot_date, datetime.min.time())

cows_on_date = silver_df.filter(
    (col("__START_AT") <= lit(snapshot_timestamp)) &
    ((col("__END_AT") > lit(snapshot_timestamp)) | (col("__END_AT").isNull()))
)
```

### 3. Partition for Performance
```python
write_gold_table(
    df,
    "s3a://gold/my_table",
    mode="overwrite",
    partition_by=["snapshot_date"]  # or ["tenant_id"]
)
```

### 4. Test Determinism
```bash
python databricks/gold/rebuild_all.py --determinism-test my_table
```

### 5. Document Computation Logic
```python
computation_logic = """
    Count cows by breed/status/sex on snapshot_date.
    Uses Silver SCD Type 2 to determine cows that existed
    where __START_AT <= date AND (__END_AT > date OR __END_AT IS NULL)
"""
```

---

## Troubleshooting

### Gold Row Count Doesn't Match Expectations

**Check Silver Source:**
```python
silver_df = spark.read.format("delta").load("s3a://silver/cows_history")
print(f"Silver rows: {silver_df.count()}")
print(f"Unique cows: {silver_df.select('cow_id').distinct().count()}")
```

**Check Date Range:**
```python
date_range = silver_df.agg(
    {"__START_AT": "min", "__START_AT": "max"}
).collect()[0]
print(f"Date range: {date_range}")
```

### Rebuild Takes Too Long

**Optimize:**
1. Increase Spark parallelism: `.config("spark.sql.shuffle.partitions", "200")`
2. Use incremental updates instead of full refresh
3. Partition Gold tables appropriately
4. Cache intermediate results: `df.cache()`

### Non-Deterministic Results

**Check for:**
1. Non-deterministic functions (e.g., `rand()`, `uuid()`)
2. Timestamp precision issues
3. Null handling inconsistencies
4. Floating-point arithmetic
5. Concurrent writes to Silver

---

## Files

- **common.py**: Shared utilities and configuration
- **gold_herd_composition.py**: Herd composition analytics
- **gold_daily_snapshots.py**: Daily count snapshots
- **gold_cow_lifecycle.py**: Cow lifecycle analytics
- **gold_daily_sales.py**: Sales analytics (planned)
- **gold_cow_weight_trends.py**: Weight trends (planned)
- **rebuild_all.py**: Rebuild all Gold tables
- **test_gold_rebuild.sh**: Test script
- **README.md**: This documentation

---

## Next Steps

### To Add Sales Analytics:

1. **Update Bronze Schema:**
   ```sql
   ALTER TABLE bronze_cow_events ADD COLUMN sale_price DECIMAL(10,2);
   ALTER TABLE bronze_cow_events ADD COLUMN buyer_id STRING;
   ```

2. **Add Event Type:**
   Add `cow_sale` to validation in `bronze/ingest_from_sql.py`

3. **Create Silver Sales Table:**
   Create `silver/resolve_sales.py` to aggregate sales from events

4. **Complete Gold Analytics:**
   Implement `gold_daily_sales.py` to aggregate from `silver_sales`

### To Add Weight Tracking:

1. **Update Bronze Schema:**
   ```sql
   ALTER TABLE bronze_cow_events ADD COLUMN weight_measurement DECIMAL(10,2);
   ALTER TABLE bronze_cow_events ADD COLUMN measurement_date DATE;
   ```

2. **Add Event Type:**
   Add `cow_weight_measured` to validation

3. **Create Silver Weights Table:**
   Create `silver/resolve_weights.py` for weight history

4. **Complete Gold Analytics:**
   Implement `gold_cow_weight_trends.py` with 30-day rolling averages

---

## Related Documentation

- [Silver Layer SCD Type 2](../silver/SCD_TYPE2.md)
- [Silver Expectations](../silver/README.md)
- [Bronze Validation](../bronze/VALIDATION.md)
- [Data Quality Best Practices](../../docs/DATA_QUALITY.md)
