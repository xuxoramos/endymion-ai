"""
Query Bronze Layer - Verify events in Delta Lake.

Utility script to query and inspect events in the Bronze layer.

Usage:
    # Show all events
    python databricks/bronze/query_bronze.py
    
    # Show events for specific tenant
    python databricks/bronze/query_bronze.py --tenant-id <uuid>
    
    # Show recent events
    python databricks/bronze/query_bronze.py --limit 10
    
    # Show table statistics
    python databricks/bronze/query_bronze.py --stats
"""

import os
import sys
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min as spark_min, max as spark_max
from delta import configure_spark_with_delta_pip


# ========================================
# Configuration
# ========================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_BUCKET = "bronze"
BRONZE_TABLE_PATH = f"s3a://{BRONZE_BUCKET}/cow_events"


# ========================================
# Spark Session
# ========================================

def create_spark_session():
    """Create Spark session configured for Delta Lake and MinIO."""
    # Get the downloaded JAR paths
    import os
    ivy_jars = os.path.expanduser("~/.ivy2/jars")
    hadoop_aws_jar = f"{ivy_jars}/org.apache.hadoop_hadoop-aws-3.3.1.jar"
    aws_sdk_jar = f"{ivy_jars}/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar"
    delta_spark_jar = f"{ivy_jars}/io.delta_delta-spark_2.12-3.2.1.jar"
    delta_storage_jar = f"{ivy_jars}/io.delta_delta-storage-3.2.1.jar"
    
    builder = (
        SparkSession.builder
        .appName("Query Bronze Layer")
        .master("local[*]")
        # Add JARs for S3A and Delta support
        .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{delta_spark_jar},{delta_storage_jar}")
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
# Query Functions
# ========================================

def show_table_stats(spark: SparkSession):
    """Show table statistics."""
    print("\n" + "=" * 60)
    print("Bronze Table Statistics")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        total_count = df.count()
        print(f"\nTotal events: {total_count}")
        
        if total_count == 0:
            print("Table is empty")
            return
        
        # Count by tenant
        print("\nEvents by tenant:")
        df.groupBy("tenant_id").count().orderBy(col("count").desc()).show(truncate=False)
        
        # Count by event type
        print("\nEvents by type:")
        df.groupBy("event_type").count().orderBy(col("count").desc()).show(truncate=False)
        
        # Count by partition date
        print("\nEvents by date:")
        df.groupBy("partition_date").count().orderBy("partition_date").show(truncate=False)
        
        # Time range
        print("\nTime range:")
        time_stats = df.select(
            spark_min("event_timestamp").alias("earliest"),
            spark_max("event_timestamp").alias("latest")
        ).collect()[0]
        print(f"  Earliest: {time_stats.earliest}")
        print(f"  Latest: {time_stats.latest}")
        
        # Ingestion batches
        print("\nIngestion batches:")
        df.groupBy("ingestion_batch_id").agg(
            count("*").alias("events"),
            spark_min("ingested_at").alias("ingested_at")
        ).orderBy(col("ingested_at").desc()).show(10, truncate=False)
        
    except Exception as e:
        print(f"✗ Error querying table: {e}")


def show_events(spark: SparkSession, tenant_id: str = None, limit: int = 100):
    """Show events from Bronze table."""
    print("\n" + "=" * 60)
    print("Bronze Events")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        if tenant_id:
            df = df.filter(col("tenant_id") == tenant_id)
            print(f"\nFiltered to tenant: {tenant_id}")
        
        total = df.count()
        print(f"\nTotal events: {total}")
        
        if total == 0:
            print("No events found")
            return
        
        # Show events
        print(f"\nShowing up to {limit} events:\n")
        df.orderBy(col("event_timestamp").desc()).limit(limit).select(
            "event_id",
            "tenant_id",
            "cow_id",
            "event_type",
            "event_timestamp",
            "sequence_number",
            "partition_date"
        ).show(limit, truncate=False)
        
    except Exception as e:
        print(f"✗ Error querying events: {e}")


def show_event_details(spark: SparkSession, event_id: str):
    """Show detailed information for a specific event."""
    print("\n" + "=" * 60)
    print(f"Event Details: {event_id}")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        event_df = df.filter(col("event_id") == event_id)
        
        if event_df.count() == 0:
            print(f"\n✗ Event not found: {event_id}")
            return
        
        event = event_df.collect()[0]
        
        print(f"\nEvent ID: {event.event_id}")
        print(f"Tenant ID: {event.tenant_id}")
        print(f"Cow ID: {event.cow_id}")
        print(f"Event Type: {event.event_type}")
        print(f"Event Timestamp: {event.event_timestamp}")
        print(f"Sequence Number: {event.sequence_number}")
        print(f"\nPayload:")
        print(event.payload)
        print(f"\nCreated By: {event.created_by}")
        print(f"Created At: {event.created_at}")
        print(f"\nIngested At: {event.ingested_at}")
        print(f"Ingestion Batch: {event.ingestion_batch_id}")
        print(f"Partition Date: {event.partition_date}")
        
    except Exception as e:
        print(f"✗ Error querying event: {e}")


def show_table_details(spark: SparkSession):
    """Show Delta table metadata."""
    print("\n" + "=" * 60)
    print("Table Details")
    print("=" * 60)
    
    try:
        # Register table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze_cow_events
            USING DELTA
            LOCATION '{BRONZE_TABLE_PATH}'
        """)
        
        # Show details
        detail_df = spark.sql(f"DESCRIBE DETAIL bronze_cow_events")
        detail = detail_df.collect()[0]
        
        print(f"\nFormat: {detail.format}")
        print(f"Location: {detail.location}")
        print(f"Partition Columns: {detail.partitionColumns}")
        print(f"Number of Files: {detail.numFiles}")
        print(f"Size (bytes): {detail.sizeInBytes}")
        print(f"Properties:")
        for key, value in detail.properties.items():
            print(f"  {key}: {value}")
        
        # Show schema
        print("\nSchema:")
        spark.table("bronze_cow_events").printSchema()
        
    except Exception as e:
        print(f"✗ Error getting table details: {e}")


def verify_deduplication(spark: SparkSession):
    """Check for duplicate event_ids."""
    print("\n" + "=" * 60)
    print("Deduplication Check")
    print("=" * 60)
    
    try:
        df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        # Find duplicates
        duplicates = df.groupBy("event_id").count().filter(col("count") > 1)
        dup_count = duplicates.count()
        
        if dup_count == 0:
            print("\n✓ No duplicate event_ids found")
        else:
            print(f"\n✗ Found {dup_count} duplicate event_ids:")
            duplicates.show(truncate=False)
        
    except Exception as e:
        print(f"✗ Error checking duplicates: {e}")


# ========================================
# Main Execution
# ========================================

def main():
    """Main query workflow."""
    parser = argparse.ArgumentParser(description="Query Bronze layer")
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show table statistics"
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Show table details and schema"
    )
    parser.add_argument(
        "--tenant-id",
        type=str,
        help="Filter events by tenant ID"
    )
    parser.add_argument(
        "--event-id",
        type=str,
        help="Show details for specific event"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum events to show (default: 100)"
    )
    parser.add_argument(
        "--check-duplicates",
        action="store_true",
        help="Check for duplicate event_ids"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Bronze Layer Query Tool")
    print("=" * 60)
    
    try:
        spark = create_spark_session()
        
        if args.stats:
            show_table_stats(spark)
        elif args.details:
            show_table_details(spark)
        elif args.event_id:
            show_event_details(spark, args.event_id)
        elif args.check_duplicates:
            verify_deduplication(spark)
        else:
            show_events(spark, args.tenant_id, args.limit)
        
        spark.stop()
        return 0
        
    except Exception as e:
        print(f"\n✗ Query failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
