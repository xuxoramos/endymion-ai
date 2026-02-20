"""
Gold Layer - Daily Snapshots

Pre-computed daily count snapshots:
- Total cows per tenant per day
- Active vs inactive counts
- Daily change metrics (new, deactivated)

SOURCE: silver_cows_history (SCD Type 2)
OUTPUT: gold_daily_snapshots

Optimized for BI dashboards showing daily trends.

Usage:
    # Build for all dates
    python databricks/gold/gold_daily_snapshots.py --full-refresh
    
    # Build for last 30 days
    python databricks/gold/gold_daily_snapshots.py --days 30
    
    # Build for specific date
    python databricks/gold/gold_daily_snapshots.py --date 2026-01-24
"""

import sys
import argparse
from datetime import datetime, timedelta, date
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, to_date, sum as spark_sum, when,
    countDistinct, lag
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, IntegerType, DateType, StructType, StructField
)

from common import (
    create_spark_session,
    add_lineage_metadata,
    write_gold_table,
    read_silver_cows_history,
    project_gold_to_sql,
    GOLD_TABLES
)

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# ANALYTICS COMPUTATION
# ============================================================================

def compute_daily_snapshot(
    spark: SparkSession,
    snapshot_date: date
) -> "DataFrame":
    """
    Compute daily cow counts for a specific date.
    
    Uses Silver SCD Type 2 to determine counts on snapshot_date.
    
    Metrics:
    - Total cows (active + inactive)
    - Active cows
    - Inactive cows
    - Cows created on this date
    - Cows deactivated on this date
    
    Args:
        spark: SparkSession
        snapshot_date: Date to compute snapshot for
    
    Returns:
        DataFrame with daily snapshot metrics per tenant
    """
    
    logger.info(f"Computing daily snapshot for {snapshot_date}")
    
    # Read Silver history
    silver_df = read_silver_cows_history(spark)
    
    # Use end of day (23:59:59.999999) for snapshot to capture all activity during the day
    snapshot_timestamp = datetime.combine(snapshot_date, datetime.max.time())
    
    # Cows that existed on snapshot_date
    cows_on_date = silver_df.filter(
        (col("__START_AT") <= lit(snapshot_timestamp)) &
        (
            (col("__END_AT") > lit(snapshot_timestamp)) |
            (col("__END_AT").isNull())
        )
    )
    
    # Daily counts by tenant and status
    daily_counts = cows_on_date.groupBy("tenant_id", "status").agg(
        countDistinct("cow_id").alias("cow_count")
    )
    
    # Pivot to get active and inactive counts
    daily_pivot = daily_counts.groupBy("tenant_id").pivot("status", ["active", "inactive"]).sum("cow_count")
    
    # Rename columns and handle nulls - check if columns exist first
    if "active" in daily_pivot.columns:
        daily_snapshot = daily_pivot.withColumnRenamed("active", "active_cows")
    else:
        daily_snapshot = daily_pivot.withColumn("active_cows", lit(0))
    
    if "inactive" in daily_snapshot.columns:
        daily_snapshot = daily_snapshot.withColumnRenamed("inactive", "inactive_cows")
    else:
        daily_snapshot = daily_snapshot.withColumn("inactive_cows", lit(0))
    
    # Fill nulls
    daily_snapshot = daily_snapshot.fillna(0, ["active_cows", "inactive_cows"])
    
    # Calculate total
    daily_snapshot = daily_snapshot.withColumn(
        "total_cows",
        col("active_cows") + col("inactive_cows")
    )
    
    # Count cows created on this date
    # (cows where __START_AT date matches snapshot_date AND __SEQUENCE_NUMBER = 1)
    created_today = silver_df.filter(
        (to_date("__START_AT") == lit(snapshot_date)) &
        (col("__SEQUENCE_NUMBER") == 1)
    ).groupBy("tenant_id").agg(
        countDistinct("cow_id").alias("cows_created_today")
    )
    
    # Count cows deactivated on this date
    # (cows where __START_AT date matches snapshot_date AND status changed to inactive)
    deactivated_today = silver_df.filter(
        (to_date("__START_AT") == lit(snapshot_date)) &
        (col("status") == "inactive") &
        (col("__EVENT_TYPE") == "cow_deactivated")
    ).groupBy("tenant_id").agg(
        countDistinct("cow_id").alias("cows_deactivated_today")
    )
    
    # Join all metrics - ensure columns exist even if joins return empty
    daily_snapshot = daily_snapshot \
        .join(created_today, "tenant_id", "left") \
        .join(deactivated_today, "tenant_id", "left")
    
    # Add snapshot_date first
    daily_snapshot = daily_snapshot.withColumn("snapshot_date", lit(snapshot_date))
    
    # Explicitly add columns if they don't exist (happens when no cows created/deactivated OR empty DataFrame)
    # Must do this BEFORE fillna because fillna won't add missing columns
    if "cows_created_today" not in daily_snapshot.columns:
        daily_snapshot = daily_snapshot.withColumn("cows_created_today", lit(0))
    else:
        # Fill nulls only if column exists
        daily_snapshot = daily_snapshot.fillna(0, ["cows_created_today"])
    
    if "cows_deactivated_today" not in daily_snapshot.columns:
        daily_snapshot = daily_snapshot.withColumn("cows_deactivated_today", lit(0))
    else:
        # Fill nulls only if column exists
        daily_snapshot = daily_snapshot.fillna(0, ["cows_deactivated_today"])
    
    # Calculate net change
    daily_snapshot = daily_snapshot.withColumn(
        "net_change",
        col("cows_created_today") - col("cows_deactivated_today")
    )
    
    # Add lineage metadata
    daily_snapshot = add_lineage_metadata(
        daily_snapshot,
        source_tables=["silver_cows_history"],
        aggregation_type="daily_snapshot",
        gold_table_name="gold_daily_snapshots",
        computation_logic=f"Daily cow counts on {snapshot_date} using SCD Type 2"
    )
    
    return daily_snapshot


def compute_daily_snapshots_range(
    spark: SparkSession,
    start_date: date,
    end_date: date
) -> "DataFrame":
    """
    Compute daily snapshots for a date range.
    
    More efficient than single-date computation for historical analysis.
    """
    
    logger.info(f"Computing daily snapshots from {start_date} to {end_date}")
    
    # Generate date list
    date_list = []
    current = start_date
    while current <= end_date:
        date_list.append(current)
        current += timedelta(days=1)
    
    logger.info(f"Processing {len(date_list)} dates")
    
    # Compute for each date and union
    all_snapshots = []
    
    for snapshot_date in date_list:
        snapshot = compute_daily_snapshot(spark, snapshot_date)
        all_snapshots.append(snapshot)
    
    # Union all
    from functools import reduce
    from pyspark.sql import DataFrame
    
    result = reduce(DataFrame.union, all_snapshots)
    
    # Add day-over-day change
    window = Window.partitionBy("tenant_id").orderBy("snapshot_date")
    
    result = result.withColumn(
        "prev_day_total",
        lag("total_cows", 1).over(window)
    )
    
    result = result.withColumn(
        "change_from_prev_day",
        when(col("prev_day_total").isNotNull(),
             col("total_cows") - col("prev_day_total")
        ).otherwise(0)
    )
    
    # Drop temporary column
    result = result.drop("prev_day_total")
    
    logger.info(f"Computed {result.count()} daily snapshots")
    
    return result


# ============================================================================
# MAIN WORKFLOW
# ============================================================================

def run_full_refresh(spark: SparkSession):
    """
    Full refresh: Compute daily snapshots for all dates since earliest Silver event.
    """
    
    logger.info("=" * 80)
    logger.info("FULL REFRESH - Daily Snapshots")
    logger.info("=" * 80)
    
    # Read Silver to determine date range
    silver_df = read_silver_cows_history(spark)
    
    if silver_df.rdd.isEmpty():
        logger.warning("No data in Silver - nothing to compute")
        return
    
    # Get earliest and latest event dates
    from pyspark.sql.functions import min as spark_min, max as spark_max
    
    date_range = silver_df.agg(
        spark_min(to_date("__START_AT")).alias("min_date"),
        spark_max(to_date("__START_AT")).alias("max_date")
    ).collect()[0]
    
    start_date = date_range["min_date"]
    end_date = date_range["max_date"]
    
    logger.info(f"Computing snapshots from {start_date} to {end_date}")
    
    # Compute snapshots
    snapshots_df = compute_daily_snapshots_range(spark, start_date, end_date)
    
    # Write to Gold
    table_info = GOLD_TABLES["daily_snapshots"]
    write_gold_table(
        snapshots_df,
        table_info["path"],
        mode="overwrite",
        partition_by=table_info["partitions"]
    )
    
    # Project to SQL Server analytics schema (for API queries)
    try:
        project_gold_to_sql(snapshots_df, "daily_snapshots", mode="overwrite")
        logger.info("✓ Projected to SQL Server: analytics.daily_snapshots")
    except Exception as e:
        logger.warning(f"⚠️  SQL projection failed (Gold Delta is still valid): {e}")
    
    logger.info("✓ Full refresh complete")


def run_for_date(spark: SparkSession, snapshot_date: date):
    """
    Compute daily snapshot for a specific date.
    """
    
    logger.info(f"Computing daily snapshot for {snapshot_date}")
    
    snapshot_df = compute_daily_snapshot(spark, snapshot_date)
    
    # Write to Gold
    table_info = GOLD_TABLES["daily_snapshots"]
    
    # Delete existing data for this date
    from delta import DeltaTable
    table_path = table_info["path"]
    
    if DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.delete(f"snapshot_date = '{snapshot_date}'")
        logger.info(f"Deleted existing data for {snapshot_date}")
    
    # Write new data
    write_gold_table(
        snapshot_df,
        table_path,
        mode="append",
        partition_by=table_info["partitions"]
    )
    
    # Project to SQL Server analytics schema (for API queries)
    # Skip projection if DataFrame is empty to avoid schema issues
    row_count = snapshot_df.count()
    if row_count > 0:
        try:
            project_gold_to_sql(snapshot_df, "daily_snapshots", mode="append")
            logger.info(f"✓ Projected {row_count} rows to SQL Server: analytics.daily_snapshots")
        except Exception as e:
            logger.warning(f"⚠️  SQL projection failed (Gold Delta is still valid): {e}")
    else:
        logger.info("ℹ️  Skipping SQL projection (no data for this date)")
    
    logger.info(f"✓ Daily snapshot computed for {snapshot_date}")


def run_for_days(spark: SparkSession, days: int):
    """
    Compute daily snapshots for last N days.
    """
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    
    logger.info(f"Computing snapshots for last {days} days ({start_date} to {end_date})")
    
    snapshots_df = compute_daily_snapshots_range(spark, start_date, end_date)
    
    # Write to Gold
    table_info = GOLD_TABLES["daily_snapshots"]
    
    # Delete existing data for this date range
    from delta import DeltaTable
    table_path = table_info["path"]
    
    if DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.delete(
            f"snapshot_date >= '{start_date}' AND snapshot_date <= '{end_date}'"
        )
        logger.info(f"Deleted existing data for {start_date} to {end_date}")
    
    # Write new data
    write_gold_table(
        snapshots_df,
        table_path,
        mode="append",
        partition_by=table_info["partitions"]
    )
    
    logger.info(f"✓ Daily snapshots computed for last {days} days")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Gold Layer - Daily Snapshots")
    parser.add_argument("--full-refresh", action="store_true", help="Compute for all dates")
    parser.add_argument("--date", help="Compute for specific date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="Compute for last N days")
    
    args = parser.parse_args()
    
    spark = create_spark_session("GoldDailySnapshots")
    
    try:
        if args.full_refresh:
            run_full_refresh(spark)
        elif args.date:
            snapshot_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            run_for_date(spark, snapshot_date)
        elif args.days:
            run_for_days(spark, args.days)
        else:
            # Default: compute for today
            run_for_date(spark, date.today())
    
    finally:
        spark.stop()
    
    logger.info("=" * 80)
    logger.info("Daily Snapshots Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
