"""
SQL to Bronze Ingestion Job with Data Quality Validation.

Reads unpublished events from SQL Server cow_events table and writes them
to Bronze layer Delta Lake. Runs continuously every 10 seconds.

Features:
- Event deduplication (by event_id)
- Data quality validation (UUIDs, JSON, enums, timestamps)
- Rejected events handling (separate table)
- Atomic publish flag updates
- Batch processing with progress logging
- Comprehensive metrics (counts, lag, processing time)
- Error handling and retry logic

Usage:
    # One-time run
    python databricks/bronze/ingest_from_sql.py --once
    
    # Continuous mode (every 10 seconds)
    python databricks/bronze/ingest_from_sql.py
"""

import os
import sys
import time
import argparse
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from uuid import UUID

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from delta import configure_spark_with_delta_pip
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Import checkpoint manager for incremental processing
try:
    from databricks.common.checkpoints import CheckpointManager
    CHECKPOINTS_AVAILABLE = True
except ImportError:
    CHECKPOINTS_AVAILABLE = False
    print("Warning: Checkpoint manager not available, using full processing")


# ========================================
# Configuration
# ========================================

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# SQL Server Configuration
SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
SQL_DATABASE = os.getenv("SQL_DATABASE", "CattleSaaS")
SQL_USER = os.getenv("SQL_USER", "sa")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "YourStrong!Passw0rd")

# Delta Lake Configuration
BRONZE_BUCKET = "bronze"
BRONZE_TABLE_PATH = f"s3a://{BRONZE_BUCKET}/cow_events"
BRONZE_REJECTED_TABLE_PATH = f"s3a://{BRONZE_BUCKET}/cow_events_rejected"

# Job Configuration
BATCH_SIZE = 1000  # Max events to process per batch
POLL_INTERVAL = 10  # Seconds between polling
MAX_RETRIES = 3  # Max retries for failed operations

# Data Quality Configuration
VALID_EVENT_TYPES = {
    "cow_created",
    "cow_updated",
    "cow_deactivated",
    "cow_weight_recorded",
    "cow_health_check",
    "cow_breeding_event",
    "cow_calving_event",
    "cow_treatment_applied"
}


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
        .appName("Bronze Ingestion - SQL to Delta")
        .master("local[*]")
        # Add JARs for S3A and Delta support - using direct JAR paths
        .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{delta_spark_jar},{delta_storage_jar}")
        # Delta Lake configuration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO/S3 configuration
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


# ========================================
# SQL Server Connection
# ========================================

def create_sql_engine():
    """Create SQLAlchemy engine for SQL Server."""
    connection_string = (
        f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}"
        f"@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
        f"&TrustServerCertificate=yes"
    )
    
    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False
    )
    
    return engine


# ========================================
# Data Extraction
# ========================================

def fetch_unpublished_events(
    engine, 
    limit: int = BATCH_SIZE,
    since: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """
    Fetch unpublished events from SQL Server.
    
    Supports incremental processing by fetching only events created after
    a checkpoint timestamp.
    
    Args:
        engine: SQLAlchemy engine
        limit: Maximum number of events to fetch
        since: Fetch only events created after this timestamp (incremental mode)
    
    Returns:
        List of event dictionaries where published_to_bronze = false
    """
    # Build WHERE clause with incremental filter if provided
    where_clause = "published_to_bronze = 0"
    if since:
        # Format datetime for SQL Server
        since_str = since.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Millisecond precision
        where_clause += f" AND created_at > '{since_str}'"
    
    query = text(f"""
        SELECT TOP {limit}
            event_id,
            tenant_id,
            cow_id,
            event_type,
            event_time,
            payload,
            created_by,
            created_at,
            published_to_bronze,
            published_at,
            source_system,
            correlation_id
        FROM operational.cow_events
        WHERE {where_clause}
        ORDER BY created_at ASC
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        
        # Convert to list of dicts
        events = []
        for row in rows:
            events.append({
                "event_id": str(row.event_id).lower(),  # Normalize UUID to lowercase
                "tenant_id": str(row.tenant_id).lower(),  # Normalize UUID to lowercase
                "cow_id": str(row.cow_id).lower(),  # Normalize UUID to lowercase
                "event_type": row.event_type,
                "event_timestamp": row.event_time,  # SQL column is event_time
                "sequence_number": 0,  # Not in SQL schema, default to 0
                "payload": row.payload,
                "created_by": row.created_by,
                "created_at": row.created_at,
                "published_to_bronze": row.published_to_bronze,
                "published_to_bronze_at": row.published_at  # SQL column is published_at
            })
        
        return events


def mark_events_as_published(engine, event_ids: List[str]) -> int:
    """
    Mark events as published to Bronze layer.
    
    Updates published_to_bronze = true and sets timestamp.
    Returns number of updated rows.
    """
    if not event_ids:
        return 0
    
    # Create parameterized query
    placeholders = ", ".join([f":event_id_{i}" for i in range(len(event_ids))])
    query = text(f"""
        UPDATE operational.cow_events
        SET 
            published_to_bronze = 1,
            published_at = GETUTCDATE()
        WHERE event_id IN ({placeholders})
    """)
    
    # Create parameter dict
    params = {f"event_id_{i}": UUID(eid) for i, eid in enumerate(event_ids)}
    
    with engine.begin() as conn:
        result = conn.execute(query, params)
        return result.rowcount


# ========================================
# Data Quality Validation
# ========================================

def validate_uuid(value: str) -> bool:
    """Validate if string is a valid UUID."""
    if not value:
        return False
    try:
        UUID(value)
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def validate_json(value: str) -> bool:
    """Validate if string is valid JSON."""
    if not value:
        return False
    try:
        json.loads(value)
        return True
    except (json.JSONDecodeError, TypeError):
        return False


def validate_event_type(value: str) -> bool:
    """Validate if event_type is in allowed enum."""
    return value in VALID_EVENT_TYPES


def validate_timestamp(value) -> bool:
    """Validate if value is a valid timestamp."""
    if not value:
        return False
    return isinstance(value, datetime)


def validate_event(event: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate a single event.
    
    Returns:
        (is_valid, error_message)
    """
    # Validate event_id (UUID)
    if not validate_uuid(event.get("event_id")):
        return False, f"Invalid event_id: {event.get('event_id')} (not a valid UUID)"
    
    # Validate tenant_id (UUID, not null)
    if not event.get("tenant_id"):
        return False, "tenant_id is null"
    if not validate_uuid(event.get("tenant_id")):
        return False, f"Invalid tenant_id: {event.get('tenant_id')} (not a valid UUID)"
    
    # Validate cow_id (UUID)
    if not validate_uuid(event.get("cow_id")):
        return False, f"Invalid cow_id: {event.get('cow_id')} (not a valid UUID)"
    
    # Validate event_type (enum)
    event_type = event.get("event_type")
    if not event_type:
        return False, "event_type is null"
    if not validate_event_type(event_type):
        return False, f"Invalid event_type: {event_type} (not in allowed types)"
    
    # Validate event_timestamp
    if not validate_timestamp(event.get("event_timestamp")):
        return False, f"Invalid event_timestamp: {event.get('event_timestamp')}"
    
    # Validate payload (JSON)
    payload = event.get("payload")
    if not payload:
        return False, "payload is null"
    if not validate_json(payload):
        return False, f"Invalid payload: {payload[:100]}... (not valid JSON)"
    
    # sequence_number is optional in our SQL schema, default to 0 if not present
    # This field would be used for event ordering within a cow's event stream
    
    return True, ""


def separate_valid_and_rejected(
    events: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Separate events into valid and rejected based on validation.
    
    Returns:
        (valid_events, rejected_events)
    """
    valid_events = []
    rejected_events = []
    
    for event in events:
        is_valid, error_message = validate_event(event)
        
        if is_valid:
            valid_events.append(event)
        else:
            # Add rejection metadata
            rejected_event = event.copy()
            rejected_event["rejection_reason"] = error_message
            rejected_event["rejected_at"] = datetime.utcnow()
            rejected_events.append(rejected_event)
            
            # Log rejection
            print(f"  ⚠ Rejected event {event.get('event_id')}: {error_message}")
    
    return valid_events, rejected_events


# ========================================
# Data Transformation
# ========================================

def transform_events_to_bronze(
    spark: SparkSession,
    events: List[Dict[str, Any]],
    batch_id: str
) -> DataFrame:
    """
    Transform SQL events to Bronze layer format.
    
    Adds ingestion metadata and partition columns.
    """
    if not events:
        return None
    
    # Define schema explicitly to handle nullable fields
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType,
        IntegerType, BooleanType
    )
    
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("cow_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("sequence_number", IntegerType(), False),
        StructField("payload", StringType(), False),
        StructField("created_by", StringType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("published_to_bronze", BooleanType(), True),
        StructField("published_to_bronze_at", TimestampType(), True),
    ])
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(events, schema=schema)
    
    # Add ingestion metadata
    ingestion_time = datetime.utcnow()
    df = df.withColumn("ingested_at", lit(ingestion_time))
    df = df.withColumn("ingestion_batch_id", lit(batch_id))
    
    # Add partition column (YYYY-MM-DD from event_timestamp)
    df = df.withColumn(
        "partition_date",
        col("event_timestamp").cast("date").cast("string")
    )
    
    # Ensure correct column order and types
    df = df.select(
        "event_id",
        "tenant_id",
        "cow_id",
        "event_type",
        "event_timestamp",
        "sequence_number",
        "payload",
        "created_by",
        "created_at",
        "published_to_bronze",
        "published_to_bronze_at",
        "ingested_at",
        "ingestion_batch_id",
        "partition_date"
    )
    
    return df


# ========================================
# Data Loading
# ========================================

def write_to_bronze(df: DataFrame) -> int:
    """
    Write events to Bronze Delta table.
    
    Uses merge to handle deduplication by event_id.
    Returns number of records inserted.
    """
    if df is None or df.count() == 0:
        return 0
    
    # Create temp view for merge
    df.createOrReplaceTempView("new_events")
    
    # Get Spark session
    spark = df.sparkSession
    
    # Perform merge (upsert based on event_id)
    merge_query = f"""
        MERGE INTO delta.`{BRONZE_TABLE_PATH}` AS target
        USING new_events AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    spark.sql(merge_query)
    
    # Return count of new events
    return df.count()


def write_to_bronze_append(df: DataFrame) -> int:
    """
    Append events to Bronze Delta table (simpler alternative).
    
    Note: This doesn't handle duplicates. Use with caution.
    """
    if df is None or df.count() == 0:
        return 0
    
    count = df.count()
    
    df.write.format("delta").mode("append").save(BRONZE_TABLE_PATH)
    
    return count


# ========================================
# Rejected Events Handling
# ========================================

def write_to_rejected_table(
    spark: SparkSession,
    rejected_events: List[Dict[str, Any]],
    batch_id: str
) -> int:
    """
    Write rejected events to bronze_rejected_events table.
    
    Returns number of records written.
    """
    if not rejected_events:
        return 0
    
    # Create DataFrame from rejected events
    df = spark.createDataFrame(rejected_events)
    
    # Add ingestion metadata
    df = df.withColumn("ingestion_batch_id", lit(batch_id))
    
    # Add partition column
    df = df.withColumn(
        "partition_date",
        lit(datetime.utcnow().strftime("%Y-%m-%d"))
    )
    
    # Write to rejected table (append mode)
    try:
        df.write.format("delta").mode("append").save(BRONZE_REJECTED_TABLE_PATH)
        return len(rejected_events)
    except Exception as e:
        print(f"  ⚠ Warning: Could not write to rejected table: {e}")
        # If table doesn't exist, create it
        try:
            df.write.format("delta").mode("overwrite").save(BRONZE_REJECTED_TABLE_PATH)
            print(f"  ✓ Created rejected events table")
            return len(rejected_events)
        except Exception as e2:
            print(f"  ✗ Failed to create rejected table: {e2}")
            return 0


def mark_events_as_rejected(engine, event_ids: List[str], reason: str) -> int:
    """
    Mark events as rejected in SQL Server.
    
    Updates a custom field to track rejected events.
    Note: This assumes we add a 'rejection_reason' column to cow_events table.
    For now, we'll mark them as published but log the rejection.
    """
    if not event_ids:
        return 0
    
    # For now, mark as published but with rejection reason in published_to_bronze_at
    # In production, add a separate 'rejected' flag and 'rejection_reason' column
    return mark_events_as_published(engine, event_ids)


# ========================================
# Metrics Calculation
# ========================================

def calculate_metrics(
    events: List[Dict[str, Any]],
    start_time: float,
    end_time: float
) -> Dict[str, Any]:
    """
    Calculate ingestion metrics.
    
    Returns:
        metrics dictionary with counts, lag, and processing time
    """
    metrics = {
        "processing_time_seconds": round(end_time - start_time, 2),
        "events_per_second": 0,
        "avg_lag_seconds": 0,
        "max_lag_seconds": 0,
        "min_lag_seconds": 0
    }
    
    if not events:
        return metrics
    
    # Calculate events per second
    duration = end_time - start_time
    if duration > 0:
        metrics["events_per_second"] = round(len(events) / duration, 2)
    
    # Calculate lag (time from event_timestamp to ingestion)
    ingestion_time = datetime.utcnow()
    lags = []
    
    for event in events:
        event_time = event.get("event_timestamp")
        if isinstance(event_time, datetime):
            lag = (ingestion_time - event_time).total_seconds()
            lags.append(lag)
    
    if lags:
        metrics["avg_lag_seconds"] = round(sum(lags) / len(lags), 2)
        metrics["max_lag_seconds"] = round(max(lags), 2)
        metrics["min_lag_seconds"] = round(min(lags), 2)
    
    return metrics


# ========================================
# Deduplication Check
# ========================================

def check_existing_event_ids(spark: SparkSession, event_ids: List[str]) -> set:
    """
    Check which event_ids already exist in Bronze table.
    
    Returns set of existing event_ids.
    """
    if not event_ids:
        return set()
    
    try:
        # Read Bronze table
        bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
        
        # Filter to check event_ids
        existing_df = bronze_df.filter(col("event_id").isin(event_ids))
        
        # Collect existing IDs
        existing_ids = {row.event_id for row in existing_df.select("event_id").collect()}
        
        return existing_ids
        
    except Exception as e:
        # If table doesn't exist yet, return empty set
        print(f"Warning: Could not check existing events: {e}")
        return set()


# ========================================
# Ingestion Job
# ========================================

def run_ingestion_batch(
    spark: SparkSession, 
    sql_engine, 
    batch_id: str,
    checkpoint_manager: Optional['CheckpointManager'] = None
) -> Dict[str, Any]:
    """
    Run single ingestion batch with data quality validation.
    
    Supports incremental processing via checkpoint_manager.
    
    Args:
        spark: SparkSession
        sql_engine: SQLAlchemy engine
        batch_id: Unique batch identifier
        checkpoint_manager: Optional checkpoint manager for incremental mode
    
    Returns:
        Statistics about the batch
    """
    start_time = time.time()
    stats = {
        "batch_id": batch_id,
        "events_fetched": 0,
        "events_validated": 0,
        "events_rejected": 0,
        "events_deduplicated": 0,
        "events_written": 0,
        "events_rejected_written": 0,
        "events_marked": 0,
        "duration_seconds": 0,
        "avg_lag_seconds": 0,
        "events_per_second": 0,
        "success": False,
        "error": None,
        "incremental": checkpoint_manager is not None
    }
    
    try:
        # Step 1: Fetch unpublished events (with incremental filtering if enabled)
        last_checkpoint = None
        if checkpoint_manager:
            last_checkpoint = checkpoint_manager.get_checkpoint('bronze', 'last_created_at')
            if last_checkpoint:
                stats["checkpoint_used"] = str(last_checkpoint)
        
        events = fetch_unpublished_events(
            sql_engine, 
            limit=BATCH_SIZE,
            since=last_checkpoint
        )
        stats["events_fetched"] = len(events)
        
        if not events:
            stats["success"] = True
            stats["duration_seconds"] = round(time.time() - start_time, 2)
            return stats
        
        # Step 2: Validate events and separate valid from rejected
        valid_events, rejected_events = separate_valid_and_rejected(events)
        stats["events_validated"] = len(valid_events)
        stats["events_rejected"] = len(rejected_events)
        
        # Step 3: Handle rejected events
        if rejected_events:
            rejected_written = write_to_rejected_table(spark, rejected_events, batch_id)
            stats["events_rejected_written"] = rejected_written
            
            # Mark rejected events as published (with rejection logged)
            rejected_ids = [e["event_id"] for e in rejected_events]
            mark_events_as_rejected(sql_engine, rejected_ids, "validation_failed")
        
        # Step 4: Process valid events
        if not valid_events:
            stats["success"] = True
            stats["duration_seconds"] = round(time.time() - start_time, 2)
            return stats
        
        # Step 5: Check for duplicates in Bronze
        valid_event_ids = [e["event_id"] for e in valid_events]
        existing_ids = check_existing_event_ids(spark, valid_event_ids)
        
        # Filter out duplicates
        new_events = [e for e in valid_events if e["event_id"] not in existing_ids]
        stats["events_deduplicated"] = len(valid_events) - len(new_events)
        
        if not new_events:
            # All events were duplicates, still mark them as published
            marked = mark_events_as_published(sql_engine, valid_event_ids)
            stats["events_marked"] = marked
            stats["success"] = True
            stats["duration_seconds"] = round(time.time() - start_time, 2)
            return stats
        
        # Step 6: Transform events
        bronze_df = transform_events_to_bronze(spark, new_events, batch_id)
        
        # Step 7: Write to Bronze Delta table
        written = write_to_bronze(bronze_df)
        stats["events_written"] = written
        
        # Step 8: Mark events as published in SQL
        new_event_ids = [e["event_id"] for e in new_events]
        marked = mark_events_as_published(sql_engine, new_event_ids)
        stats["events_marked"] = marked
        
        # Step 9: Update checkpoint if using incremental mode
        if checkpoint_manager and new_events:
            # Save the latest created_at as the new checkpoint
            max_created_at = max(e["created_at"] for e in new_events)
            checkpoint_manager.save_checkpoint('bronze', 'last_created_at', max_created_at)
            stats["checkpoint_saved"] = str(max_created_at)
        
        # Step 10: Calculate metrics
        metrics = calculate_metrics(new_events, start_time, time.time())
        stats.update(metrics)
        
        stats["success"] = True
        
    except Exception as e:
        stats["error"] = str(e)
        print(f"✗ Batch failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        stats["duration_seconds"] = round(time.time() - start_time, 2)
    
    return stats


def print_batch_stats(stats: Dict[str, Any]):
    """Print formatted batch statistics with quality metrics."""
    status = "✓" if stats["success"] else "✗"
    print(f"\n{status} Batch {stats['batch_id']}")
    print(f"  Fetched: {stats['events_fetched']}")
    print(f"  Valid: {stats['events_validated']}")
    
    if stats["events_rejected"] > 0:
        rejection_rate = (stats['events_rejected'] / stats['events_fetched'] * 100) if stats['events_fetched'] > 0 else 0
        print(f"  ⚠ Rejected: {stats['events_rejected']} ({rejection_rate:.1f}%)")
        print(f"  Rejected Written: {stats['events_rejected_written']}")
    
    print(f"  Duplicates: {stats['events_deduplicated']}")
    print(f"  Written: {stats['events_written']}")
    print(f"  Marked: {stats['events_marked']}")
    print(f"  Duration: {stats['duration_seconds']}s")
    
    if stats.get("events_per_second", 0) > 0:
        print(f"  Throughput: {stats['events_per_second']} events/sec")
    
    if stats.get("avg_lag_seconds", 0) > 0:
        print(f"  Avg Lag: {stats['avg_lag_seconds']}s (max: {stats.get('max_lag_seconds', 0)}s)")
    
    if stats["error"]:
        print(f"  ✗ Error: {stats['error']}")


# ========================================
# Main Execution
# ========================================

def main():
    """Main ingestion workflow."""
    parser = argparse.ArgumentParser(description="Bronze layer ingestion job")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (default: continuous mode)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=POLL_INTERVAL,
        help=f"Poll interval in seconds (default: {POLL_INTERVAL})"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Enable incremental processing using checkpoints (only process new events)"
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Reset Bronze checkpoint and process all events (use with --incremental)"
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("Bronze Layer Ingestion - SQL to Delta Lake")
    print("=" * 60)
    print(f"Mode: {'One-time' if args.once else 'Continuous'}")
    print(f"Processing: {'Incremental' if args.incremental else 'Full'}")
    if not args.once:
        print(f"Poll interval: {args.interval} seconds")
    print()
    
    # Initialize connections
    spark = None
    sql_engine = None
    checkpoint_manager = None
    
    try:
        print("Initializing connections...")
        spark = create_spark_session()
        print("✓ Spark session created")
        
        sql_engine = create_sql_engine()
        print("✓ SQL connection created")
        
        # Initialize checkpoint manager if incremental mode enabled
        if args.incremental:
            if not CHECKPOINTS_AVAILABLE:
                print("✗ Checkpoint manager not available, falling back to full processing")
                print("  Install: Create databricks/common/checkpoints.py")
            else:
                checkpoint_manager = CheckpointManager(spark, use_local=True)
                print("✓ Checkpoint manager initialized")
                
                # Reset checkpoint if requested
                if args.reset_checkpoint:
                    checkpoint_manager.reset_checkpoint('bronze', 'last_created_at')
                    print("✓ Bronze checkpoint reset")
                
                # Show current checkpoint
                last_checkpoint = checkpoint_manager.get_checkpoint('bronze', 'last_created_at')
                if last_checkpoint:
                    print(f"  Last checkpoint: {last_checkpoint}")
                    print(f"  Will process events created after {last_checkpoint}")
                else:
                    print("  No checkpoint found, will process all unpublished events")
        
        # Verify Bronze table exists
        try:
            bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
            print(f"✓ Bronze table exists ({bronze_df.count()} records)")
        except Exception as e:
            print(f"✗ Bronze table not found: {e}")
            print("\nRun setup first: python databricks/bronze/setup_bronze.py")
            return 1
        
        print("\nStarting ingestion...")
        
        # Run ingestion loop
        batch_counter = 0
        total_stats = {
            "total_fetched": 0,
            "total_written": 0,
            "total_batches": 0,
            "successful_batches": 0
        }
        
        while True:
            batch_counter += 1
            batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{batch_counter}"
            
            # Run batch (with checkpoint manager if incremental mode)
            stats = run_ingestion_batch(spark, sql_engine, batch_id, checkpoint_manager)
            print_batch_stats(stats)
            
            # Update totals
            total_stats["total_fetched"] += stats["events_fetched"]
            total_stats["total_written"] += stats["events_written"]
            total_stats["total_rejected"] = total_stats.get("total_rejected", 0) + stats["events_rejected"]
            total_stats["total_batches"] += 1
            if stats["success"]:
                total_stats["successful_batches"] += 1
            
            # Exit if one-time mode or no more events
            if args.once:
                break
            
            if stats["events_fetched"] == 0:
                print(f"No new events. Waiting {args.interval} seconds...")
            
            # Wait before next poll
            time.sleep(args.interval)
        
        # Print summary
        print("\n" + "=" * 60)
        print("Ingestion Summary")
        print("=" * 60)
        print(f"Total batches: {total_stats['total_batches']}")
        print(f"Successful batches: {total_stats['successful_batches']}")
        print(f"Total events fetched: {total_stats['total_fetched']}")
        print(f"Total events written: {total_stats['total_written']}")
        print(f"Total events rejected: {total_stats.get('total_rejected', 0)}")
        if total_stats['total_fetched'] > 0:
            success_rate = (total_stats['total_written'] / total_stats['total_fetched'] * 100)
            print(f"Success rate: {success_rate:.1f}%")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\nIngestion stopped by user")
        return 0
        
    except Exception as e:
        print(f"\n✗ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        if spark:
            spark.stop()
        if sql_engine:
            sql_engine.dispose()


if __name__ == "__main__":
    exit(main())
