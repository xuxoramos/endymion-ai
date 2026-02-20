"""
Bronze Layer Data Quality Check Tool.

Comprehensive tool to query and validate Bronze layer data quality:
- Row counts by tenant and event type
- Latest events inspection
- Gap detection in event_ids and sequence numbers
- Duplicate detection
- Rejected events analysis
- Data freshness and lag metrics

Usage:
    # Show overview
    python databricks/bronze/check_bronze.py
    
    # Show detailed analysis
    python databricks/bronze/check_bronze.py --detailed
    
    # Check specific tenant
    python databricks/bronze/check_bronze.py --tenant-id <uuid>
    
    # Show rejected events
    python databricks/bronze/check_bronze.py --rejected
    
    # Check for data quality issues
    python databricks/bronze/check_bronze.py --quality-check
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, min as spark_min, max as spark_max,
    avg, sum as spark_sum, current_timestamp, unix_timestamp,
    countDistinct, window, lag, lead
)
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip


# ========================================
# Configuration
# ========================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_BUCKET = "bronze"
BRONZE_TABLE_PATH = f"s3a://{BRONZE_BUCKET}/cow_events"
BRONZE_REJECTED_TABLE_PATH = f"s3a://{BRONZE_BUCKET}/cow_events_rejected"


# ========================================
# Spark Session
# ========================================

def create_spark_session():
    """Create Spark session configured for Delta Lake and MinIO."""
    builder = (
        SparkSession.builder
        .appName("Bronze Quality Check")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark


# ========================================
# Basic Statistics
# ========================================

def show_overview(spark: SparkSession):
    """Show high-level overview of Bronze table."""
    print("\n" + "=" * 60)
    print("Bronze Layer Overview")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        total_count = df.count()
        
        print(f"\nTotal events: {total_count:,}")
        
        if total_count == 0:
            print("Table is empty")
            return
        
        # Time range
        time_stats = df.select(
            spark_min("event_timestamp").alias("earliest"),
            spark_max("event_timestamp").alias("latest"),
            spark_min("ingested_at").alias("first_ingested"),
            spark_max("ingested_at").alias("last_ingested")
        ).collect()[0]
        
        print(f"\nEvent Time Range:")
        print(f"  Earliest: {time_stats.earliest}")
        print(f"  Latest: {time_stats.latest}")
        
        print(f"\nIngestion Time Range:")
        print(f"  First: {time_stats.first_ingested}")
        print(f"  Last: {time_stats.last_ingested}")
        
        # Calculate freshness
        if time_stats.last_ingested:
            freshness = (datetime.utcnow() - time_stats.last_ingested).total_seconds()
            print(f"  Freshness: {freshness:.0f} seconds ago")
        
        # Count distinct values
        distinct_stats = df.select(
            countDistinct("tenant_id").alias("tenants"),
            countDistinct("cow_id").alias("cows"),
            countDistinct("event_type").alias("event_types")
        ).collect()[0]
        
        print(f"\nDistinct Values:")
        print(f"  Tenants: {distinct_stats.tenants}")
        print(f"  Cows: {distinct_stats.cows}")
        print(f"  Event Types: {distinct_stats.event_types}")
        
    except Exception as e:
        print(f"✗ Error reading Bronze table: {e}")


def show_counts_by_tenant(spark: SparkSession, tenant_id: str = None):
    """Show row counts by tenant."""
    print("\n" + "=" * 60)
    print("Events by Tenant")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        if tenant_id:
            df = df.filter(col("tenant_id") == tenant_id)
        
        tenant_stats = (
            df.groupBy("tenant_id")
            .agg(
                count("*").alias("total_events"),
                countDistinct("cow_id").alias("cows"),
                countDistinct("event_type").alias("event_types"),
                spark_min("event_timestamp").alias("earliest_event"),
                spark_max("event_timestamp").alias("latest_event")
            )
            .orderBy(col("total_events").desc())
        )
        
        tenant_stats.show(20, truncate=False)
        
    except Exception as e:
        print(f"✗ Error: {e}")


def show_counts_by_event_type(spark: SparkSession):
    """Show row counts by event type."""
    print("\n" + "=" * 60)
    print("Events by Type")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        type_stats = (
            df.groupBy("event_type")
            .agg(
                count("*").alias("total_events"),
                countDistinct("tenant_id").alias("tenants"),
                countDistinct("cow_id").alias("cows")
            )
            .orderBy(col("total_events").desc())
        )
        
        type_stats.show(truncate=False)
        
    except Exception as e:
        print(f"✗ Error: {e}")


# ========================================
# Latest Events
# ========================================

def show_latest_events(spark: SparkSession, limit: int = 10, tenant_id: str = None):
    """Show most recent events."""
    print("\n" + "=" * 60)
    print(f"Latest {limit} Events")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        if tenant_id:
            df = df.filter(col("tenant_id") == tenant_id)
        
        latest = (
            df.orderBy(col("event_timestamp").desc())
            .limit(limit)
            .select(
                "event_id",
                "tenant_id",
                "cow_id",
                "event_type",
                "event_timestamp",
                "sequence_number",
                "ingested_at"
            )
        )
        
        latest.show(limit, truncate=False)
        
    except Exception as e:
        print(f"✗ Error: {e}")


# ========================================
# Gap Detection
# ========================================

def detect_sequence_gaps(spark: SparkSession, tenant_id: str = None):
    """Detect gaps in sequence numbers per cow."""
    print("\n" + "=" * 60)
    print("Sequence Number Gap Detection")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        if tenant_id:
            df = df.filter(col("tenant_id") == tenant_id)
        
        # Use window function to find gaps
        window_spec = Window.partitionBy("tenant_id", "cow_id").orderBy("sequence_number")
        
        gaps = (
            df.withColumn("prev_seq", lag("sequence_number").over(window_spec))
            .withColumn("gap", col("sequence_number") - col("prev_seq"))
            .filter(col("gap") > 1)
            .select(
                "tenant_id",
                "cow_id",
                "prev_seq",
                "sequence_number",
                "gap",
                "event_timestamp"
            )
            .orderBy(col("gap").desc())
        )
        
        gap_count = gaps.count()
        
        if gap_count == 0:
            print("\n✓ No sequence gaps detected")
        else:
            print(f"\n⚠ Found {gap_count} sequence gaps:")
            gaps.show(20, truncate=False)
        
    except Exception as e:
        print(f"✗ Error: {e}")


def detect_time_gaps(spark: SparkSession, gap_threshold_hours: int = 24):
    """Detect large time gaps between consecutive events."""
    print("\n" + "=" * 60)
    print(f"Time Gap Detection (>{gap_threshold_hours}h)")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        # Use window function to find time gaps
        window_spec = Window.partitionBy("tenant_id").orderBy("event_timestamp")
        
        gaps = (
            df.withColumn("prev_time", lag("event_timestamp").over(window_spec))
            .withColumn(
                "gap_hours",
                (unix_timestamp("event_timestamp") - unix_timestamp("prev_time")) / 3600
            )
            .filter(col("gap_hours") > gap_threshold_hours)
            .select(
                "tenant_id",
                "prev_time",
                "event_timestamp",
                "gap_hours"
            )
            .orderBy(col("gap_hours").desc())
        )
        
        gap_count = gaps.count()
        
        if gap_count == 0:
            print(f"\n✓ No time gaps >{gap_threshold_hours}h detected")
        else:
            print(f"\n⚠ Found {gap_count} time gaps:")
            gaps.show(20, truncate=False)
        
    except Exception as e:
        print(f"✗ Error: {e}")


# ========================================
# Duplicate Detection
# ========================================

def detect_duplicates(spark: SparkSession):
    """Detect duplicate event_ids."""
    print("\n" + "=" * 60)
    print("Duplicate Event ID Detection")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        duplicates = (
            df.groupBy("event_id")
            .count()
            .filter(col("count") > 1)
            .orderBy(col("count").desc())
        )
        
        dup_count = duplicates.count()
        
        if dup_count == 0:
            print("\n✓ No duplicate event_ids found")
        else:
            print(f"\n✗ Found {dup_count} duplicate event_ids:")
            duplicates.show(20, truncate=False)
        
    except Exception as e:
        print(f"✗ Error: {e}")


# ========================================
# Rejected Events Analysis
# ========================================

def analyze_rejected_events(spark: SparkSession):
    """Analyze rejected events."""
    print("\n" + "=" * 60)
    print("Rejected Events Analysis")
    print("=" * 60)
    
    try:
        # Try to read rejected table
        rejected_df = spark.read.format("delta").load(BRONZE_REJECTED_TABLE_PATH)
        
        total_rejected = rejected_df.count()
        print(f"\nTotal rejected events: {total_rejected:,}")
        
        if total_rejected == 0:
            print("✓ No rejected events")
            return
        
        # Count by rejection reason
        print("\nRejection Reasons:")
        (
            rejected_df.groupBy("rejection_reason")
            .count()
            .orderBy(col("count").desc())
            .show(truncate=False)
        )
        
        # Count by tenant
        print("\nRejected Events by Tenant:")
        (
            rejected_df.groupBy("tenant_id")
            .count()
            .orderBy(col("count").desc())
            .show(10, truncate=False)
        )
        
        # Show recent rejected events
        print("\nRecent Rejected Events:")
        (
            rejected_df.orderBy(col("rejected_at").desc())
            .limit(10)
            .select(
                "event_id",
                "tenant_id",
                "event_type",
                "rejection_reason",
                "rejected_at"
            )
            .show(10, truncate=False)
        )
        
    except Exception as e:
        print(f"ℹ No rejected events table found (this is normal if no rejections yet)")


# ========================================
# Data Quality Metrics
# ========================================

def calculate_quality_metrics(spark: SparkSession):
    """Calculate comprehensive data quality metrics."""
    print("\n" + "=" * 60)
    print("Data Quality Metrics")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        total_count = df.count()
        
        if total_count == 0:
            print("\nTable is empty")
            return
        
        # Null checks
        print("\nNull Value Analysis:")
        null_counts = df.select([
            spark_sum(col(c).isNull().cast("int")).alias(c)
            for c in ["event_id", "tenant_id", "cow_id", "event_type", "payload"]
        ]).collect()[0]
        
        for field in ["event_id", "tenant_id", "cow_id", "event_type", "payload"]:
            null_count = null_counts[field]
            null_pct = (null_count / total_count * 100) if total_count > 0 else 0
            status = "✓" if null_count == 0 else "✗"
            print(f"  {status} {field}: {null_count} nulls ({null_pct:.2f}%)")
        
        # Ingestion lag
        print("\nIngestion Lag Analysis:")
        lag_stats = df.select(
            avg(unix_timestamp("ingested_at") - unix_timestamp("event_timestamp")).alias("avg_lag"),
            spark_min(unix_timestamp("ingested_at") - unix_timestamp("event_timestamp")).alias("min_lag"),
            spark_max(unix_timestamp("ingested_at") - unix_timestamp("event_timestamp")).alias("max_lag")
        ).collect()[0]
        
        print(f"  Average lag: {lag_stats.avg_lag:.1f} seconds")
        print(f"  Min lag: {lag_stats.min_lag:.1f} seconds")
        print(f"  Max lag: {lag_stats.max_lag:.1f} seconds")
        
        # Partition distribution
        print("\nPartition Distribution:")
        (
            df.groupBy("partition_date")
            .count()
            .orderBy("partition_date")
            .show(10, truncate=False)
        )
        
    except Exception as e:
        print(f"✗ Error: {e}")


# ========================================
# Main Execution
# ========================================

def main():
    """Main check workflow."""
    parser = argparse.ArgumentParser(description="Bronze layer quality check tool")
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Show detailed analysis"
    )
    parser.add_argument(
        "--tenant-id",
        type=str,
        help="Filter by specific tenant ID"
    )
    parser.add_argument(
        "--rejected",
        action="store_true",
        help="Analyze rejected events"
    )
    parser.add_argument(
        "--quality-check",
        action="store_true",
        help="Run comprehensive quality checks"
    )
    parser.add_argument(
        "--gaps",
        action="store_true",
        help="Detect sequence and time gaps"
    )
    parser.add_argument(
        "--latest",
        type=int,
        default=10,
        help="Number of latest events to show (default: 10)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Bronze Layer Quality Check")
    print("=" * 60)
    
    try:
        spark = create_spark_session()
        
        if args.rejected:
            # Show rejected events only
            analyze_rejected_events(spark)
        elif args.quality_check:
            # Comprehensive quality check
            show_overview(spark)
            calculate_quality_metrics(spark)
            detect_duplicates(spark)
            detect_sequence_gaps(spark, args.tenant_id)
            detect_time_gaps(spark)
            analyze_rejected_events(spark)
        elif args.gaps:
            # Gap detection only
            detect_sequence_gaps(spark, args.tenant_id)
            detect_time_gaps(spark)
        elif args.detailed:
            # Detailed analysis
            show_overview(spark)
            show_counts_by_tenant(spark, args.tenant_id)
            show_counts_by_event_type(spark)
            show_latest_events(spark, args.latest, args.tenant_id)
            detect_duplicates(spark)
        else:
            # Default: overview
            show_overview(spark)
            show_counts_by_tenant(spark, args.tenant_id)
            show_latest_events(spark, args.latest, args.tenant_id)
        
        spark.stop()
        return 0
        
    except Exception as e:
        print(f"\n✗ Check failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
