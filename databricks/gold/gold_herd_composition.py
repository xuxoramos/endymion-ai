"""
Gold Layer - Herd Composition Analytics

Pre-computed analytics for herd composition:
- Count cows by breed
- Count by status (active/inactive)
- Count by sex
- Group by tenant and date snapshot

SOURCE: silver_cows_history (SCD Type 2)
OUTPUT: gold_herd_composition

This is FULLY RECOMPUTABLE from Silver with no manual overrides.

Usage:
    # Build for all dates
    python databricks/gold/gold_herd_composition.py --full-refresh
    
    # Build for specific date
    python databricks/gold/gold_herd_composition.py --date 2026-01-24
    
    # Build last 7 days
    python databricks/gold/gold_herd_composition.py --days 7
"""

import sys
import argparse
from datetime import datetime, timedelta, date
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, to_date, current_date, when, sum as spark_sum
)
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField

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
# SCHEMA
# ============================================================================

def get_herd_composition_schema():
    """Schema for gold_herd_composition table (long format for analytics API)"""
    return StructType([
        # Identifiers
        StructField("tenant_id", StringType(), False),
        StructField("snapshot_date", DateType(), False),
        
        # Dimension (long format - unpivoted)
        StructField("dimension_type", StringType(), False),  # breed, status, or sex
        StructField("dimension_value", StringType(), False),  # actual value
        
        # Metrics
        StructField("count", IntegerType(), False),
        StructField("percentage", IntegerType(), False),
        StructField("total_cows", IntegerType(), False),
        
        # Lineage metadata (added by common.add_lineage_metadata)
        StructField("_gold_table", StringType(), True),
        StructField("_source_tables", StringType(), True),
        StructField("_aggregation_type", StringType(), True),
    ])


# ============================================================================
# ANALYTICS COMPUTATION
# ============================================================================

def compute_herd_composition(
    spark: SparkSession,
    snapshot_date: date
) -> "DataFrame":
    """
    Compute herd composition for a specific date.
    
    Uses Silver SCD Type 2 data to determine which cows existed on snapshot_date.
    
    Algorithm:
    1. Filter Silver history to rows where snapshot_date is between __START_AT and __END_AT
    2. This gives us the state of each cow on that specific date
    3. Group by tenant, breed, status, sex
    4. Count cows and compute aggregations
    
    Args:
        spark: SparkSession
        snapshot_date: Date to compute composition for
    
    Returns:
        DataFrame with herd composition metrics
    """
    
    logger.info(f"Computing herd composition for {snapshot_date}")
    
    # Read Silver history
    silver_df = read_silver_cows_history(spark)
    
    # Filter to cows that existed on snapshot_date
    # A cow existed on snapshot_date if:
    #   date(__START_AT) <= snapshot_date AND (date(__END_AT) >= snapshot_date OR __END_AT IS NULL)
    # Use end of day (23:59:59) to include all events that happened during snapshot_date
    
    snapshot_end = datetime.combine(snapshot_date, datetime.max.time())
    
    cows_on_date = silver_df.filter(
        (col("__START_AT") <= lit(snapshot_end)) &
        (
            (col("__END_AT") > lit(snapshot_end)) |
            (col("__END_AT").isNull())
        )
    )
    
    logger.info(f"Found {cows_on_date.count()} cow states on {snapshot_date}")
    
    # Get total count for percentage calculations
    total_cows = cows_on_date.count()
    
    # Compute composition by breed
    by_breed = cows_on_date.groupBy("tenant_id", "breed").agg(
        count("*").alias("count")
    ).withColumn("dimension_type", lit("breed")) \
     .withColumn("dimension_value", col("breed")) \
     .withColumn("snapshot_date", lit(snapshot_date)) \
     .withColumn("total_cows", lit(total_cows)) \
     .withColumn("percentage", (col("count") / lit(total_cows) * 100).cast(IntegerType())) \
     .select("tenant_id", "snapshot_date", "dimension_type", "dimension_value", "count", "percentage", "total_cows")
    
    # Compute composition by status
    by_status = cows_on_date.groupBy("tenant_id", "status").agg(
        count("*").alias("count")
    ).withColumn("dimension_type", lit("status")) \
     .withColumn("dimension_value", col("status")) \
     .withColumn("snapshot_date", lit(snapshot_date)) \
     .withColumn("total_cows", lit(total_cows)) \
     .withColumn("percentage", (col("count") / lit(total_cows) * 100).cast(IntegerType())) \
     .select("tenant_id", "snapshot_date", "dimension_type", "dimension_value", "count", "percentage", "total_cows")
    
    # Compute composition by sex
    by_sex = cows_on_date.groupBy("tenant_id", "sex").agg(
        count("*").alias("count")
    ).withColumn("dimension_type", lit("sex")) \
     .withColumn("dimension_value", col("sex")) \
     .withColumn("snapshot_date", lit(snapshot_date)) \
     .withColumn("total_cows", lit(total_cows)) \
     .withColumn("percentage", (col("count") / lit(total_cows) * 100).cast(IntegerType())) \
     .select("tenant_id", "snapshot_date", "dimension_type", "dimension_value", "count", "percentage", "total_cows")
    
    # Union all dimensions into single DataFrame
    composition = by_breed.union(by_status).union(by_sex)
    
    # Add lineage metadata
    composition = add_lineage_metadata(
        composition,
        source_tables=["silver_cows_history"],
        aggregation_type="herd_composition",
        gold_table_name="gold_herd_composition",
        computation_logic=f"Count cows by breed/status/sex on {snapshot_date} using SCD Type 2"
    )
    
    return composition


def compute_herd_composition_range(
    spark: SparkSession,
    start_date: date,
    end_date: date
) -> "DataFrame":
    """
    Compute herd composition for a date range.
    
    This is more efficient than calling compute_herd_composition for each date
    when building historical snapshots.
    """
    
    logger.info(f"Computing herd composition from {start_date} to {end_date}")
    
    # Generate list of dates
    date_list = []
    current = start_date
    while current <= end_date:
        date_list.append(current)
        current += timedelta(days=1)
    
    logger.info(f"Processing {len(date_list)} dates")
    
    # Compute composition for each date and union
    all_compositions = []
    
    for snapshot_date in date_list:
        composition = compute_herd_composition(spark, snapshot_date)
        all_compositions.append(composition)
    
    # Union all DataFrames
    from functools import reduce
    from pyspark.sql import DataFrame
    
    result = reduce(DataFrame.union, all_compositions)
    
    logger.info(f"Computed herd composition: {result.count()} total records")
    
    return result


# ============================================================================
# MAIN WORKFLOW
# ============================================================================

def run_full_refresh(spark: SparkSession):
    """
    Full refresh: Compute herd composition for all dates since earliest Silver event.
    """
    
    logger.info("=" * 80)
    logger.info("FULL REFRESH - Herd Composition")
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
    
    logger.info(f"Computing composition from {start_date} to {end_date}")
    
    # Compute composition for date range
    composition_df = compute_herd_composition_range(spark, start_date, end_date)
    
    # Write to Gold
    table_info = GOLD_TABLES["herd_composition"]
    write_gold_table(
        composition_df,
        table_info["path"],
        mode="overwrite",
        partition_by=table_info["partitions"]
    )
    
    logger.info("✓ Full refresh complete")


def run_for_date(spark: SparkSession, snapshot_date: date):
    """
    Compute herd composition for a specific date.
    """
    
    logger.info(f"Computing herd composition for {snapshot_date}")
    
    composition_df = compute_herd_composition(spark, snapshot_date)
    
    # Write to Gold (append mode for single date)
    table_info = GOLD_TABLES["herd_composition"]
    
    # Delete existing data for this date first (to handle re-runs)
    from delta import DeltaTable
    table_path = table_info["path"]
    
    if DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.delete(f"snapshot_date = '{snapshot_date}'")
        logger.info(f"Deleted existing data for {snapshot_date}")
    
    # Write new data
    write_gold_table(
        composition_df,
        table_path,
        mode="append",
        partition_by=table_info["partitions"]
    )
    
    logger.info(f"✓ Herd composition computed for {snapshot_date}")


def run_for_days(spark: SparkSession, days: int):
    """
    Compute herd composition for last N days.
    """
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    
    logger.info(f"Computing herd composition for last {days} days ({start_date} to {end_date})")
    
    composition_df = compute_herd_composition_range(spark, start_date, end_date)
    
    # Write to Gold
    table_info = GOLD_TABLES["herd_composition"]
    
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
        composition_df,
        table_path,
        mode="append",
        partition_by=table_info["partitions"]
    )
    
    # Project to SQL Server analytics schema (for API queries)
    try:
        # For range updates, overwrite SQL table to keep it in sync
        project_gold_to_sql(composition_df, "herd_composition", mode="overwrite")
        logger.info(f"✓ Projected to SQL Server: analytics.herd_composition")
    except Exception as e:
        logger.warning(f"⚠️  SQL projection failed (Gold Delta is still valid): {e}")
    
    logger.info(f"✓ Herd composition computed for last {days} days")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Gold Layer - Herd Composition Analytics")
    parser.add_argument("--full-refresh", action="store_true", help="Compute for all dates")
    parser.add_argument("--date", help="Compute for specific date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="Compute for last N days")
    
    args = parser.parse_args()
    
    spark = create_spark_session("GoldHerdComposition")
    
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
    logger.info("Herd Composition Analytics Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
