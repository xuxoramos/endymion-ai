"""
Gold Layer - Cow Lifecycle Analytics

Pre-computed analytics for cow lifecycle:
- Time from creation to deactivation
- Deactivation reasons by tenant
- Average cow lifespan
- Lifecycle stage distributions

SOURCE: silver_cows_history (SCD Type 2)
OUTPUT: gold_cow_lifecycle

This is FULLY RECOMPUTABLE from Silver with no manual overrides.

Usage:
    # Build lifecycle analytics for all cows
    python databricks/gold/gold_cow_lifecycle.py --full-refresh
    
    # Build for specific tenant
    python databricks/gold/gold_cow_lifecycle.py --tenant <tenant_id>
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, lit, min as spark_min, max as spark_max,
    datediff, when, avg, sum as spark_sum, collect_list,
    first, last, row_number
)
from pyspark.sql.types import (
    StringType, IntegerType, DateType, TimestampType,
    StructType, StructField, DoubleType
)

from common import (
    create_spark_session,
    add_lineage_metadata,
    write_gold_table,
    read_silver_cows_history,
    GOLD_TABLES
)

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# ANALYTICS COMPUTATION
# ============================================================================

def compute_cow_lifecycle(spark: SparkSession) -> "DataFrame":
    """
    Compute lifecycle analytics for each cow.
    
    For each cow, extracts:
    - Creation date (first event timestamp)
    - Deactivation date (if deactivated)
    - Lifecycle duration (days)
    - Number of state changes
    - Deactivation reason
    - Final breed, weight, status
    
    Args:
        spark: SparkSession
    
    Returns:
        DataFrame with cow lifecycle metrics
    """
    
    logger.info("Computing cow lifecycle analytics")
    
    # Read Silver history
    silver_df = read_silver_cows_history(spark)
    
    # Define window partitioned by cow
    cow_window = Window.partitionBy("tenant_id", "cow_id").orderBy("__START_AT")
    
    # Get first and last event for each cow
    cow_lifecycle = silver_df.groupBy("tenant_id", "cow_id").agg(
        # Creation (first event)
        spark_min("__START_AT").alias("created_at"),
        first("tag_number").alias("initial_tag_number"),
        first("breed").alias("initial_breed"),
        first("sex").alias("sex"),
        first("birth_date").alias("birth_date"),
        
        # Deactivation (last event if status = inactive)
        spark_max("__START_AT").alias("last_updated_at"),
        last("status").alias("final_status"),
        last("breed").alias("final_breed"),
        last("weight_kg").alias("final_weight_kg"),
        last("deactivation_reason").alias("deactivation_reason"),
        
        # Lifecycle metrics
        count("*").alias("state_change_count"),
    )
    
    # Calculate lifecycle duration
    cow_lifecycle = cow_lifecycle.withColumn(
        "lifecycle_duration_days",
        when(
            col("final_status") == "inactive",
            datediff(col("last_updated_at"), col("created_at"))
        ).otherwise(
            datediff(lit(datetime.now()), col("created_at"))
        )
    )
    
    # Categorize lifecycle stage
    cow_lifecycle = cow_lifecycle.withColumn(
        "lifecycle_stage",
        when(col("final_status") == "inactive", "deactivated")
        .when(col("lifecycle_duration_days") < 30, "new")
        .when(col("lifecycle_duration_days") < 365, "growing")
        .when(col("lifecycle_duration_days") < 1825, "mature")
        .otherwise("aged")
    )
    
    # Add lineage metadata
    cow_lifecycle = add_lineage_metadata(
        cow_lifecycle,
        source_tables=["silver_cows_history"],
        aggregation_type="cow_lifecycle",
        gold_table_name="gold_cow_lifecycle",
        computation_logic="Track cow lifecycle from creation to deactivation using SCD Type 2"
    )
    
    logger.info(f"Computed lifecycle for {cow_lifecycle.count()} cows")
    
    return cow_lifecycle


def compute_lifecycle_summary(spark: SparkSession) -> "DataFrame":
    """
    Compute summary lifecycle statistics by tenant.
    
    Aggregates:
    - Average lifecycle duration
    - Count by deactivation reason
    - Count by lifecycle stage
    """
    
    logger.info("Computing lifecycle summary by tenant")
    
    lifecycle_df = compute_cow_lifecycle(spark)
    
    # Summary by tenant
    summary = lifecycle_df.groupBy("tenant_id").agg(
        count("*").alias("total_cows"),
        avg("lifecycle_duration_days").cast(IntegerType()).alias("avg_lifecycle_days"),
        spark_sum(
            when(col("final_status") == "inactive", 1).otherwise(0)
        ).alias("deactivated_count"),
        spark_sum(
            when(col("final_status") == "active", 1).otherwise(0)
        ).alias("active_count"),
        
        # Count by deactivation reason
        spark_sum(
            when(col("deactivation_reason") == "sold", 1).otherwise(0)
        ).alias("deactivated_sold"),
        spark_sum(
            when(col("deactivation_reason") == "deceased", 1).otherwise(0)
        ).alias("deactivated_deceased"),
        spark_sum(
            when(col("deactivation_reason") == "transferred", 1).otherwise(0)
        ).alias("deactivated_transferred"),
        spark_sum(
            when(col("deactivation_reason") == "culled", 1).otherwise(0)
        ).alias("deactivated_culled"),
        
        # Count by lifecycle stage
        spark_sum(
            when(col("lifecycle_stage") == "new", 1).otherwise(0)
        ).alias("stage_new"),
        spark_sum(
            when(col("lifecycle_stage") == "growing", 1).otherwise(0)
        ).alias("stage_growing"),
        spark_sum(
            when(col("lifecycle_stage") == "mature", 1).otherwise(0)
        ).alias("stage_mature"),
        spark_sum(
            when(col("lifecycle_stage") == "aged", 1).otherwise(0)
        ).alias("stage_aged"),
    )
    
    logger.info(f"Computed summary for {summary.count()} tenants")
    
    return summary


# ============================================================================
# MAIN WORKFLOW
# ============================================================================

def run_full_refresh(spark: SparkSession):
    """
    Full refresh: Compute lifecycle analytics for all cows.
    """
    
    logger.info("=" * 80)
    logger.info("FULL REFRESH - Cow Lifecycle")
    logger.info("=" * 80)
    
    # Compute lifecycle
    lifecycle_df = compute_cow_lifecycle(spark)
    
    # Write to Gold
    table_info = GOLD_TABLES["cow_lifecycle"]
    write_gold_table(
        lifecycle_df,
        table_info["path"],
        mode="overwrite",
        partition_by=table_info["partitions"]
    )
    
    logger.info("✓ Full refresh complete")


def run_for_tenant(spark: SparkSession, tenant_id: str):
    """
    Compute lifecycle analytics for a specific tenant.
    """
    
    logger.info(f"Computing lifecycle for tenant: {tenant_id}")
    
    lifecycle_df = compute_cow_lifecycle(spark)
    tenant_lifecycle = lifecycle_df.filter(col("tenant_id") == tenant_id)
    
    # Write to Gold
    table_info = GOLD_TABLES["cow_lifecycle"]
    
    # Delete existing data for this tenant
    from delta import DeltaTable
    table_path = table_info["path"]
    
    if DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.delete(f"tenant_id = '{tenant_id}'")
        logger.info(f"Deleted existing data for tenant {tenant_id}")
    
    # Write new data
    write_gold_table(
        tenant_lifecycle,
        table_path,
        mode="append",
        partition_by=table_info["partitions"]
    )
    
    logger.info(f"✓ Lifecycle computed for tenant {tenant_id}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Gold Layer - Cow Lifecycle Analytics")
    parser.add_argument("--full-refresh", action="store_true", help="Compute for all cows")
    parser.add_argument("--tenant", help="Compute for specific tenant")
    
    args = parser.parse_args()
    
    spark = create_spark_session("GoldCowLifecycle")
    
    try:
        if args.full_refresh:
            run_full_refresh(spark)
        elif args.tenant:
            run_for_tenant(spark, args.tenant)
        else:
            # Default: full refresh
            run_full_refresh(spark)
    
    finally:
        spark.stop()
    
    logger.info("=" * 80)
    logger.info("Cow Lifecycle Analytics Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
