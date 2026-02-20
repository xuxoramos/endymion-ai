"""
Silver Layer - Cow State Resolution (SCD Type 2)

This is the SOURCE OF TRUTH for cow canonical state.

Resolves current and historical cow state from Bronze event stream by:
1. Reading all bronze_cow_events
2. Grouping by (tenant_id, cow_id)
3. Ordering by event_timestamp within each group
4. Applying events sequentially to build state transitions
5. Generating SCD Type 2 history with temporal columns

DETERMINISTIC ALGORITHM:
- Same events → same Silver state (guaranteed)
- Fully reproducible
- No randomness or non-deterministic operations

TABLES CREATED:
- silver_cows_history: SCD Type 2 with all state changes
- silver_cows_current: View of current state (__CURRENT = true)

Usage:
    # Initial load
    python databricks/silver/resolve_cow_state.py --full-refresh
    
    # Incremental update (process new events)
    python databricks/silver/resolve_cow_state.py
    
    # Rebuild entire history
    python databricks/silver/resolve_cow_state.py --rebuild
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, row_number, lag, lead, lit, when, struct, to_json,
    max as spark_max, min as spark_min, coalesce, explode, array
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, DoubleType, DateType, IntegerType
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

# Import expectations module
from expectations import (
    apply_expectations,
    write_quality_log,
    create_quality_log_table,
    SILVER_EXPECTATIONS
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ========================================
# Configuration
# ========================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Bronze Configuration
BRONZE_BUCKET = "bronze"
BRONZE_TABLE_PATH = f"s3a://{BRONZE_BUCKET}/cow_events"

# Silver Configuration
SILVER_BUCKET = "silver"
SILVER_HISTORY_TABLE_PATH = f"s3a://{SILVER_BUCKET}/cows_history"
SILVER_CURRENT_TABLE_PATH = f"s3a://{SILVER_BUCKET}/cows_current"


# ========================================
# Schema Definitions
# ========================================

def get_silver_history_schema():
    """
    Define schema for silver_cows_history table (SCD Type 2).
    
    Contains all cow attributes plus temporal tracking columns.
    """
    return StructType([
        # Primary keys
        StructField("tenant_id", StringType(), nullable=False),
        StructField("cow_id", StringType(), nullable=False),
        
        # Cow attributes (from events)
        StructField("tag_number", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("breed", StringType(), nullable=True),
        StructField("sex", StringType(), nullable=True),
        StructField("birth_date", DateType(), nullable=True),
        StructField("weight_kg", DoubleType(), nullable=True),
        StructField("color", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),  # active, inactive
        
        # Metadata
        StructField("category_id", StringType(), nullable=True),
        StructField("parent_cow_id", StringType(), nullable=True),
        StructField("deactivation_reason", StringType(), nullable=True),
        
        # SCD Type 2 temporal columns
        StructField("__START_AT", TimestampType(), nullable=False),
        StructField("__END_AT", TimestampType(), nullable=True),
        StructField("__CURRENT", BooleanType(), nullable=False),
        
        # Tracking columns
        StructField("__EVENT_ID", StringType(), nullable=False),
        StructField("__EVENT_TYPE", StringType(), nullable=False),
        StructField("__SEQUENCE_NUMBER", IntegerType(), nullable=False),
        StructField("__CREATED_BY", StringType(), nullable=True),
        StructField("__UPDATED_AT", TimestampType(), nullable=False),
        
        # Partition column
        StructField("partition_tenant_id", StringType(), nullable=False),
    ])


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
        .appName("Silver - Cow State Resolution")
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
        # Enable adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


# ========================================
# Event Application Logic
# ========================================

def apply_cow_created_event(state: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply cow_created event to create initial state.
    
    Args:
        state: Current state (should be empty/None)
        payload: Event payload with cow data
    
    Returns:
        New state after applying event
    """
    from datetime import datetime, date
    
    # Parse birth_date from string if needed
    birth_date = payload.get("birth_date")
    if isinstance(birth_date, str):
        try:
            birth_date = datetime.strptime(birth_date, "%Y-%m-%d").date()
        except ValueError as e:
            logger.warning(f"Invalid birth_date format '{birth_date}': {e}")
            birth_date = None
    
    return {
        "tag_number": payload.get("tag_number"),
        "name": payload.get("name"),
        "breed": payload.get("breed"),
        "sex": payload.get("sex"),
        "birth_date": birth_date,
        "weight_kg": payload.get("weight_kg"),
        "color": payload.get("color"),
        "status": "active",
        "category_id": payload.get("category_id"),
        "parent_cow_id": payload.get("parent_cow_id"),
        "deactivation_reason": None
    }


def apply_cow_updated_event(state: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply cow_updated event to merge new fields into state.
    
    Args:
        state: Current state
        payload: Event payload with updated fields
    
    Returns:
        New state after applying event
    """
    # Create new state by copying current state
    new_state = state.copy()
    
    # Merge updated fields (only non-null values)
    from datetime import datetime, date
    
    if "tag_number" in payload and payload["tag_number"] is not None:
        new_state["tag_number"] = payload["tag_number"]
    if "name" in payload and payload["name"] is not None:
        new_state["name"] = payload["name"]
    if "breed" in payload and payload["breed"] is not None:
        new_state["breed"] = payload["breed"]
    if "sex" in payload and payload["sex"] is not None:
        new_state["sex"] = payload["sex"]
    if "birth_date" in payload and payload["birth_date"] is not None:
        birth_date = payload["birth_date"]
        if isinstance(birth_date, str):
            try:
                birth_date = datetime.strptime(birth_date, "%Y-%m-%d").date()
            except ValueError as e:
                logger.warning(f"Invalid birth_date format '{birth_date}' in event: {e}")
                birth_date = None
        new_state["birth_date"] = birth_date
    if "weight_kg" in payload and payload["weight_kg"] is not None:
        new_state["weight_kg"] = payload["weight_kg"]
    if "color" in payload and payload["color"] is not None:
        new_state["color"] = payload["color"]
    if "category_id" in payload and payload["category_id"] is not None:
        new_state["category_id"] = payload["category_id"]
    if "parent_cow_id" in payload and payload["parent_cow_id"] is not None:
        new_state["parent_cow_id"] = payload["parent_cow_id"]
    
    return new_state


def apply_cow_deactivated_event(state: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply cow_deactivated event to mark cow as inactive.
    
    Args:
        state: Current state
        payload: Event payload with deactivation reason
    
    Returns:
        New state after applying event
    """
    new_state = state.copy()
    new_state["status"] = "inactive"
    new_state["deactivation_reason"] = payload.get("reason", "deactivated")
    
    return new_state


def apply_event(state: Dict[str, Any], event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply event to state based on event type.
    
    DETERMINISTIC: Same event + same state = same result
    
    Args:
        state: Current state (None if first event)
        event_type: Type of event
        payload: Event payload
    
    Returns:
        New state after applying event
    """
    if event_type == "cow_created":
        return apply_cow_created_event(state, payload)
    elif event_type == "cow_updated":
        return apply_cow_updated_event(state, payload)
    elif event_type == "cow_deactivated":
        return apply_cow_deactivated_event(state, payload)
    elif event_type == "cow_weight_recorded":
        # Weight recording is a type of update
        return apply_cow_updated_event(state, {"weight_kg": payload.get("weight_kg")})
    else:
        # Unknown event type - return state unchanged
        print(f"  ⚠ Unknown event type: {event_type} - state unchanged")
        return state


# ========================================
# State Resolution (PySpark Implementation)
# ========================================

def resolve_cow_states_spark(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Resolve cow states using PySpark operations (DETERMINISTIC).
    
    This is the core state resolution algorithm:
    1. Read all Bronze events
    2. Group by (tenant_id, cow_id)
    3. Order by event_timestamp
    4. Apply events sequentially using UDF
    5. Generate SCD Type 2 history
    
    Args:
        spark: SparkSession
        bronze_df: Bronze events DataFrame
    
    Returns:
        DataFrame with SCD Type 2 history
    """
    from pyspark.sql.functions import udf, pandas_udf
    from pyspark.sql.types import ArrayType, MapType
    import pandas as pd
    
    print("Resolving cow states from Bronze events...")
    
    # Register UDF for state application
    @pandas_udf(ArrayType(MapType(StringType(), StringType())))
    def resolve_states(event_types: pd.Series, payloads: pd.Series, 
                       event_timestamps: pd.Series, event_ids: pd.Series,
                       sequence_numbers: pd.Series, created_bys: pd.Series) -> pd.Series:
        """
        Pandas UDF to resolve states for a group of events.
        
        Returns array of states (one per event).
        """
        def process_group(types, payloads_list, timestamps, ids, seq_nums, creators):
            states = []
            current_state = None
            
            for i in range(len(types)):
                event_type = types.iloc[i]
                payload_str = payloads_list.iloc[i]
                
                # Parse payload
                try:
                    payload = json.loads(payload_str) if payload_str else {}
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON payload in event {i}: {e}")
                    payload = {}
                
                # Apply event to current state
                if current_state is None and event_type == "cow_created":
                    current_state = apply_cow_created_event({}, payload)
                elif current_state is not None:
                    current_state = apply_event(current_state, event_type, payload)
                
                # Store state (as dict with string values for PySpark)
                if current_state:
                    state_copy = {}
                    for k, v in current_state.items():
                        if v is not None:
                            state_copy[k] = str(v)
                        else:
                            state_copy[k] = None
                    states.append(state_copy)
                else:
                    states.append({})
            
            return states
        
        return pd.Series([process_group(event_types, payloads, event_timestamps, 
                                       event_ids, sequence_numbers, created_bys)])
    
    # Group events by tenant_id and cow_id, order by event_timestamp
    window_spec = Window.partitionBy("tenant_id", "cow_id").orderBy("event_timestamp", "sequence_number")
    
    # Collect events per cow and resolve states
    cow_events = (
        bronze_df
        .withColumn("row_num", row_number().over(window_spec))
        .groupBy("tenant_id", "cow_id")
        .agg(
            collect_list(struct(
                "event_timestamp", "event_type", "payload", "event_id",
                "sequence_number", "created_by", "row_num"
            )).alias("events")
        )
    )
    
    # Explode events and resolve states
    # Note: Using collect_list followed by explode maintains deterministic order
    from pyspark.sql.functions import collect_list, explode_outer, posexplode
    
    cow_states = (
        cow_events
        .select(
            "tenant_id",
            "cow_id",
            posexplode("events").alias("event_index", "event")
        )
        .select(
            "tenant_id",
            "cow_id",
            "event_index",
            col("event.event_timestamp").alias("event_timestamp"),
            col("event.event_type").alias("event_type"),
            col("event.payload").alias("payload"),
            col("event.event_id").alias("event_id"),
            col("event.sequence_number").alias("sequence_number"),
            col("event.created_by").alias("created_by")
        )
    )
    
    # Now we need to apply state transitions
    # Since we need sequential state building, we'll use a simpler approach:
    # Process in Python for each cow (deterministic)
    
    return cow_states


def resolve_states_python(spark: SparkSession, bronze_df: DataFrame) -> List[Dict[str, Any]]:
    """
    Resolve cow states using Python (FULLY DETERMINISTIC).
    
    This collects Bronze events and processes them in Python to ensure
    complete determinism and sequential state building.
    
    Returns:
        List of state history records
    """
    print("Collecting Bronze events...")
    
    # Collect all events (sorted deterministically)
    events = (
        bronze_df
        .orderBy("tenant_id", "cow_id", "event_timestamp", "sequence_number")
        .collect()
    )
    
    print(f"Processing {len(events)} events...")
    
    # Group events by (tenant_id, cow_id)
    cows_events = {}
    for event in events:
        key = (event.tenant_id, event.cow_id)
        if key not in cows_events:
            cows_events[key] = []
        cows_events[key].append(event)
    
    # Process each cow's events to build history
    history_records = []
    
    for (tenant_id, cow_id), cow_events in cows_events.items():
        current_state = None
        
        for i, event in enumerate(cow_events):
            # Parse payload
            try:
                payload = json.loads(event.payload) if event.payload else {}
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON payload in event {event.event_id}: {e}")
                payload = {}
            
            # Apply event to build new state
            if event.event_type == "cow_created":
                current_state = apply_cow_created_event({}, payload)
            elif current_state is not None:
                current_state = apply_event(current_state, event.event_type, payload)
            else:
                # Skip events before cow_created
                continue
            
            # Determine temporal boundaries
            start_at = event.event_timestamp
            
            # End time is the next event's timestamp (or None if last)
            if i < len(cow_events) - 1:
                end_at = cow_events[i + 1].event_timestamp
                is_current = False
            else:
                end_at = None
                is_current = True
            
            # Create history record
            record = {
                "tenant_id": tenant_id,
                "cow_id": cow_id,
                "tag_number": current_state.get("tag_number"),
                "name": current_state.get("name"),
                "breed": current_state.get("breed"),
                "sex": current_state.get("sex"),
                "birth_date": current_state.get("birth_date"),
                "weight_kg": current_state.get("weight_kg"),
                "color": current_state.get("color"),
                "status": current_state.get("status"),
                "category_id": current_state.get("category_id"),
                "parent_cow_id": current_state.get("parent_cow_id"),
                "deactivation_reason": current_state.get("deactivation_reason"),
                "__START_AT": start_at,
                "__END_AT": end_at,
                "__CURRENT": is_current,
                "__EVENT_ID": event.event_id,
                "__EVENT_TYPE": event.event_type,
                "__SEQUENCE_NUMBER": event.sequence_number,
                "__CREATED_BY": event.created_by,
                "__UPDATED_AT": datetime.utcnow(),
                "partition_tenant_id": tenant_id
            }
            
            history_records.append(record)
    
    print(f"Generated {len(history_records)} history records for {len(cows_events)} cows")
    
    return history_records


# ========================================
# Table Creation and Management
# ========================================

def create_silver_history_table(spark: SparkSession):
    """Create silver_cows_history Delta table if it doesn't exist."""
    print(f"Initializing Silver history table at: {SILVER_HISTORY_TABLE_PATH}")
    
    schema = get_silver_history_schema()
    
    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], schema)
    
    # Write as Delta table
    (
        empty_df.write
        .format("delta")
        .mode("ignore")  # Don't overwrite if exists
        .partitionBy("partition_tenant_id")
        .option("delta.enableChangeDataFeed", "true")
        .save(SILVER_HISTORY_TABLE_PATH)
    )
    
    print("✓ Silver history table initialized")


def create_silver_current_view(spark: SparkSession):
    """Create silver_cows_current view (current state only)."""
    print("\nCreating Silver current state view...")
    
    # Register history table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver_cows_history
        USING DELTA
        LOCATION '{SILVER_HISTORY_TABLE_PATH}'
    """)
    
    # Create or replace view for current state
    spark.sql("""
        CREATE OR REPLACE VIEW silver_cows_current AS
        SELECT 
            tenant_id,
            cow_id,
            tag_number,
            name,
            breed,
            sex,
            birth_date,
            weight_kg,
            color,
            status,
            category_id,
            parent_cow_id,
            deactivation_reason,
            __START_AT,
            __EVENT_ID,
            __EVENT_TYPE,
            __SEQUENCE_NUMBER,
            __CREATED_BY,
            __UPDATED_AT
        FROM silver_cows_history
        WHERE __CURRENT = true
    """)
    
    print("✓ Silver current view created")


def write_history_to_silver(spark: SparkSession, history_records: List[Dict[str, Any]], mode: str = "overwrite"):
    """
    Write history records to Silver history table.
    
    Applies data quality expectations before writing:
    - DROP: Rows failing critical expectations are rejected
    - WARN: Rows failing non-critical expectations are logged but written
    
    Args:
        spark: SparkSession
        history_records: List of history records
        mode: Write mode ('overwrite' or 'append')
    """
    if not history_records:
        print("No history records to write")
        return
    
    print(f"\nApplying data quality expectations to {len(history_records)} records...")
    
    # Create DataFrame from records
    history_df = spark.createDataFrame(history_records, schema=get_silver_history_schema())
    
    total_rows = history_df.count()
    run_timestamp = datetime.utcnow()
    
    # ========================================================================
    # APPLY EXPECTATIONS (DLT-STYLE QUALITY CHECKS)
    # ========================================================================
    
    print(f"\nApplying {len(SILVER_EXPECTATIONS)} expectations...")
    valid_df, failed_df, stats = apply_expectations(history_df)
    
    valid_count = valid_df.count()
    failed_count = failed_df.count()
    
    print(f"\n✓ Expectations applied:")
    print(f"  Valid records: {valid_count} ({(valid_count/total_rows*100):.1f}%)")
    print(f"  Failed DROP: {failed_count} ({(failed_count/total_rows*100):.1f}%)")
    
    # ========================================================================
    # WRITE QUALITY LOG
    # ========================================================================
    
    if failed_count > 0 or any(stats.values()):
        print(f"\nLogging quality issues to silver_quality_log...")
        write_quality_log(
            spark=spark,
            failed_df=failed_df,
            stats=stats,
            total_rows=total_rows,
            run_timestamp=run_timestamp
        )
    
    # ========================================================================
    # WRITE VALID DATA TO SILVER
    # ========================================================================
    
    if valid_count > 0:
        print(f"\nWriting {valid_count} valid records to Silver history table...")
        
        (
            valid_df.write
            .format("delta")
            .mode(mode)
            .save(SILVER_HISTORY_TABLE_PATH)
        )
        
        print(f"✓ Wrote {valid_count} records to Silver")
    else:
        print("⚠️  No valid records to write (all failed expectations)")
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    
    if failed_count > 0:
        print(f"\n⚠️  WARNING: {failed_count} records failed critical expectations and were NOT written")
        print("   Check silver_quality_log for details")


# ========================================
# Main Resolution Workflow
# ========================================

def run_full_refresh(spark: SparkSession):
    """
    Full refresh: Rebuild entire Silver layer from Bronze.
    
    This is DETERMINISTIC - same Bronze events always produce same Silver state.
    """
    print("\n" + "=" * 60)
    print("Full Refresh - Rebuilding Silver from Bronze")
    print("=" * 60)
    
    # Ensure quality log table exists
    create_quality_log_table(spark)
    
    # Read all Bronze events
    print("\nReading Bronze events...")
    bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
    
    event_count = bronze_df.count()
    print(f"Found {event_count} events in Bronze")
    
    if event_count == 0:
        print("No events to process")
        return
    
    # Resolve states (DETERMINISTIC)
    history_records = resolve_states_python(spark, bronze_df)
    
    # Initialize Silver table if needed
    create_silver_history_table(spark)
    
    # Write history (overwrite mode)
    write_history_to_silver(spark, history_records, mode="overwrite")
    
    # Create current view
    create_silver_current_view(spark)
    
    # Verify results
    verify_silver_tables(spark)
    
    print("\n✓ Full refresh completed")


def run_incremental_update(spark: SparkSession):
    """
    Incremental update: Process only new Bronze events.
    
    Reads events ingested since last Silver update and resolves affected cows.
    """
    print("\n" + "=" * 60)
    print("Incremental Update - Processing New Events")
    print("=" * 60)
    
    # Get last update timestamp from Silver
    try:
        silver_df = spark.read.format("delta").load(SILVER_HISTORY_TABLE_PATH)
        last_update = silver_df.agg(spark_max("__UPDATED_AT")).collect()[0][0]
        print(f"\nLast Silver update: {last_update}")
    except Exception as e:
        logger.info(f"Silver table not found or error reading: {e}")
        print("\nSilver table doesn't exist - running full refresh instead")
        run_full_refresh(spark)
        return
    
    # Read new Bronze events
    bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
    
    if last_update:
        new_events = bronze_df.filter(col("ingested_at") > last_update)
    else:
        new_events = bronze_df
    
    new_count = new_events.count()
    print(f"Found {new_count} new events")
    
    if new_count == 0:
        print("No new events to process")
        return
    
    # Get affected cows
    affected_cows = new_events.select("tenant_id", "cow_id").distinct().collect()
    print(f"Affected cows: {len(affected_cows)}")
    
    # For incremental, we need to rebuild history for affected cows
    # Get all events for affected cows
    cow_filters = [
        (col("tenant_id") == row.tenant_id) & (col("cow_id") == row.cow_id)
        for row in affected_cows
    ]
    
    if cow_filters:
        from functools import reduce
        affected_events = bronze_df.filter(reduce(lambda a, b: a | b, cow_filters))
        
        # Resolve states for affected cows
        history_records = resolve_states_python(spark, affected_events)
        
        # For each affected cow, delete old history and insert new
        for row in affected_cows:
            # Delete old history for this cow
            spark.sql(f"""
                DELETE FROM delta.`{SILVER_HISTORY_TABLE_PATH}`
                WHERE tenant_id = '{row.tenant_id}' AND cow_id = '{row.cow_id}'
            """)
        
        # Insert new history
        write_history_to_silver(spark, history_records, mode="append")
        
        print(f"\n✓ Updated {len(affected_cows)} cows")
    
    # Refresh current view
    create_silver_current_view(spark)


def verify_silver_tables(spark: SparkSession):
    """Verify Silver tables were created correctly."""
    print("\n" + "=" * 60)
    print("Silver Layer Verification")
    print("=" * 60)
    
    try:
        # Check history table
        history_df = spark.read.format("delta").load(SILVER_HISTORY_TABLE_PATH)
        total_records = history_df.count()
        current_records = history_df.filter(col("__CURRENT") == True).count()
        
        print(f"\nHistory Table:")
        print(f"  Total records: {total_records}")
        print(f"  Current records: {current_records}")
        
        # Show sample
        print("\nSample history records:")
        history_df.orderBy(col("__START_AT").desc()).limit(5).show(truncate=False)
        
        # Check current view
        print("\nCurrent State:")
        spark.sql("SELECT COUNT(*) as count FROM silver_cows_current").show()
        
    except Exception as e:
        print(f"✗ Verification failed: {e}")


# ========================================
# Main Execution
# ========================================

def main():
    """Main resolution workflow."""
    parser = argparse.ArgumentParser(description="Silver layer cow state resolution")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Full refresh: rebuild entire Silver from Bronze"
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Alias for --full-refresh"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify Silver tables only (no processing)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Silver Layer - Cow State Resolution (SCD Type 2)")
    print("=" * 60)
    print("\nSOURCE OF TRUTH - DETERMINISTIC ALGORITHM")
    
    try:
        spark = create_spark_session()
        
        if args.verify:
            verify_silver_tables(spark)
        elif args.full_refresh or args.rebuild:
            run_full_refresh(spark)
        else:
            # Default: incremental update
            run_incremental_update(spark)
        
        print("\n" + "=" * 60)
        print("✓ Silver layer resolution completed")
        print("=" * 60)
        
        spark.stop()
        return 0
        
    except Exception as e:
        print(f"\n✗ Resolution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
