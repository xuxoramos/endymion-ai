# Gold Layer Quick Reference

## Available Tables

### ✅ Implemented

| Table | Description | Source | Partition |
|-------|-------------|--------|-----------|
| gold_herd_composition | Daily breed/status/sex counts | silver_cows_history | snapshot_date |
| gold_daily_snapshots | Daily cow count trends | silver_cows_history | snapshot_date |
| gold_cow_lifecycle | Lifecycle from creation to deactivation | silver_cows_history | tenant_id |

### 📋 Planned

| Table | Description | Requires |
|-------|-------------|----------|
| gold_daily_sales | Daily sales aggregations | silver_sales |
| gold_cow_weight_trends | 30-day weight trends | silver_weights |

---

## Commands

### Build Tables

```bash
# Rebuild all Gold tables
python databricks/gold/rebuild_all.py

# Rebuild specific table
python databricks/gold/rebuild_all.py --table herd_composition

# Verify tables
python databricks/gold/rebuild_all.py --verify-only

# Test determinism
python databricks/gold/rebuild_all.py --determinism-test herd_composition
```

### Individual Tables

```bash
# Herd Composition
python databricks/gold/gold_herd_composition.py --full-refresh
python databricks/gold/gold_herd_composition.py --date 2026-01-24
python databricks/gold/gold_herd_composition.py --days 7

# Daily Snapshots
python databricks/gold/gold_daily_snapshots.py --full-refresh
python databricks/gold/gold_daily_snapshots.py --days 30

# Cow Lifecycle
python databricks/gold/gold_cow_lifecycle.py --full-refresh
python databricks/gold/gold_cow_lifecycle.py --tenant <tenant_id>
```

### Test

```bash
# Complete test suite
./databricks/gold/test_gold_rebuild.sh
```

---

## Query Examples

### Herd Composition

```python
from pyspark.sql import SparkSession

# Latest composition by breed
df = spark.read.format("delta").load("s3a://gold/herd_composition")
latest = df.orderBy("snapshot_date", ascending=False).limit(1).collect()[0]["snapshot_date"]

breed_counts = df.filter(f"snapshot_date = '{latest}'") \
    .groupBy("breed") \
    .sum("cow_count") \
    .orderBy("sum(cow_count)", ascending=False)

breed_counts.show()
```

### Daily Trends

```python
# Get 30-day trend for tenant
snapshots = spark.read.format("delta").load("s3a://gold/daily_snapshots")

trend = snapshots \
    .filter(f"tenant_id = '{tenant_id}'") \
    .filter("snapshot_date >= current_date() - 30") \
    .orderBy("snapshot_date") \
    .select("snapshot_date", "total_cows", "active_cows", "net_change")

trend.show(30)
```

### Lifecycle Analysis

```python
# Average lifecycle by deactivation reason
lifecycle = spark.read.format("delta").load("s3a://gold/cow_lifecycle")

avg_by_reason = lifecycle \
    .filter("final_status = 'inactive'") \
    .groupBy("deactivation_reason") \
    .avg("lifecycle_duration_days") \
    .orderBy("avg(lifecycle_duration_days)", ascending=False)

avg_by_reason.show()
```

---

## Schemas

### gold_herd_composition
```
tenant_id: string
snapshot_date: date
breed: string
status: string (active/inactive)
sex: string (male/female)
cow_count: int
avg_weight_kg: int
total_weight_kg: int
_gold_table: string
_source_tables: string
_aggregation_type: string
```

### gold_daily_snapshots
```
tenant_id: string
snapshot_date: date
total_cows: int
active_cows: int
inactive_cows: int
cows_created_today: int
cows_deactivated_today: int
net_change: int
change_from_prev_day: int
_gold_table: string
_source_tables: string
_aggregation_type: string
```

### gold_cow_lifecycle
```
tenant_id: string
cow_id: string
created_at: timestamp
last_updated_at: timestamp
initial_tag_number: string
initial_breed: string
sex: string
birth_date: date
final_status: string
final_breed: string
final_weight_kg: double
deactivation_reason: string
lifecycle_duration_days: int
state_change_count: int
lifecycle_stage: string (new/growing/mature/aged/deactivated)
_gold_table: string
_source_tables: string
_aggregation_type: string
```

---

## Lineage Metadata

All Gold tables include:

- **_gold_table**: Table name
- **_source_tables**: Silver sources (comma-separated)
- **_aggregation_type**: Type of aggregation
- **_computation_timestamp**: When computed
- **_computation_logic**: Brief description

Query lineage:
```python
df.select("_gold_table", "_source_tables", "_aggregation_type").distinct().show()
```

---

## Performance Tips

### 1. Use Partition Pruning
```python
# Good: Reads only relevant partitions
df.filter("snapshot_date >= '2026-01-01'")

# Bad: Reads all partitions
df.filter("total_cows > 100")
```

### 2. Incremental Updates
```bash
# Instead of full refresh:
python databricks/gold/gold_daily_snapshots.py --full-refresh

# Use incremental:
python databricks/gold/gold_daily_snapshots.py --days 1
```

### 3. Cache for Multiple Queries
```python
df = spark.read.format("delta").load("s3a://gold/herd_composition")
df.cache()

# Multiple queries use cached data
df.filter(...).show()
df.groupBy(...).show()

df.unpersist()
```

---

## Reproducibility Guarantees

✅ **Deterministic**: Same Silver → Same Gold (always)  
✅ **No Manual Overrides**: All data computed from Silver  
✅ **Full Lineage**: Traceable to Silver sources  
✅ **Tested**: Rebuild twice, verify identical results

Test determinism:
```bash
# Rebuild twice and compare
python databricks/gold/rebuild_all.py > /tmp/build1.log
python databricks/gold/rebuild_all.py > /tmp/build2.log
diff /tmp/build1.log /tmp/build2.log  # Should be identical
```

---

## Troubleshooting

### Issue: Gold counts don't match Silver

**Solution:**
```python
# Check Silver source
silver_df = spark.read.format("delta").load("s3a://silver/cows_history")
print(f"Silver rows: {silver_df.count()}")
print(f"Unique cows: {silver_df.select('cow_id').distinct().count()}")

# Check date range
silver_df.agg({"__START_AT": "min", "__START_AT": "max"}).show()
```

### Issue: Non-deterministic results

**Check for:**
- `rand()` or `uuid()` in computation
- Timestamp precision issues
- Concurrent writes to Silver
- Floating-point arithmetic

### Issue: Slow rebuild

**Optimize:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
df.repartition(10).write...
```

---

## Adding New Gold Tables

1. **Create script**: `databricks/gold/gold_my_table.py`
2. **Use common utilities**: Import from `common.py`
3. **Add lineage**: Use `add_lineage_metadata()`
4. **Register in common.py**: Add to `GOLD_TABLES` dict
5. **Update rebuild_all.py**: Add to rebuild logic
6. **Test**: Run `./databricks/gold/test_gold_rebuild.sh`
7. **Document**: Update `README.md`

Template:
```python
from common import (
    create_spark_session,
    add_lineage_metadata,
    write_gold_table,
    read_silver_cows_history
)

def compute_my_metric(spark):
    silver_df = read_silver_cows_history(spark)
    
    # Your aggregation logic
    result_df = silver_df.groupBy(...).agg(...)
    
    # Add lineage
    result_df = add_lineage_metadata(
        result_df,
        source_tables=["silver_cows_history"],
        aggregation_type="my_metric",
        gold_table_name="gold_my_table"
    )
    
    return result_df

def run_full_refresh(spark):
    df = compute_my_metric(spark)
    write_gold_table(df, "s3a://gold/my_table", mode="overwrite")
```

---

## Files

- `common.py` - Shared utilities
- `gold_herd_composition.py` - Herd composition analytics
- `gold_daily_snapshots.py` - Daily snapshots
- `gold_cow_lifecycle.py` - Lifecycle analytics
- `gold_daily_sales.py` - Sales (planned)
- `gold_cow_weight_trends.py` - Weight trends (planned)
- `rebuild_all.py` - Rebuild orchestrator
- `test_gold_rebuild.sh` - Test script
- `README.md` - Full documentation
- `GOLD_REFERENCE.md` - This quick reference
