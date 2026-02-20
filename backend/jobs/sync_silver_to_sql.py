"""
Silver-to-SQL Synchronization Job.

This is the CRITICAL job that makes SQL projection tables consistent with Silver.

Architecture: Pure Projection Pattern A
    Write: API → cow_events → Bronze → Silver
    Read:  API ← cows (SQL projection) ← Silver (via this sync job)

This job is the "reconciliation loop" that syncs Delta Lake state to SQL.

ALGORITHM:
1. Query silver_cows_current WHERE last_updated_at > last_sync_watermark
2. Read changed cows with PySpark
3. Connect to SQL database
4. For each cow:
   - UPSERT: UPDATE if exists, INSERT if new
   - Silver ALWAYS wins (conflict resolution)
   - Update silver_last_updated_at
   - Update last_synced_at
5. Update sync watermark for next incremental sync

CONFLICT RESOLUTION:
- Silver is the source of truth
- SQL manual edits are OVERWRITTEN
- All conflicts logged to sync_conflicts table

IDEMPOTENCY:
- Safe to run multiple times
- Safe to retry on failure
- Uses watermark for incremental processing
"""

import logging
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import UUID

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from sqlalchemy import create_engine, select, update, and_
from sqlalchemy.orm import Session, sessionmaker

# Add backend to path for imports
sys.path.insert(0, '/home/xuxoramos/endymion-ai')

from backend.database.connection import get_db_manager
from backend.models.cows import Cow
from backend.models.sync import SyncState, SyncLog, SyncConflict


# ========================================
# Logging Configuration
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ========================================
# Configuration
# ========================================

class SyncConfig:
    """Configuration for Silver-to-SQL sync."""
    
    # PySpark/Delta configuration
    MINIO_ENDPOINT = "http://localhost:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    
    # Silver source
    SILVER_BUCKET = "silver"
    SILVER_TABLE = "cows_current"
    SILVER_PATH = f"s3a://{SILVER_BUCKET}/{SILVER_TABLE}"
    
    # SQL target
    SQL_TABLE_NAME = "cows"
    SQL_SCHEMA = "operational"
    
    # Sync behavior
    BATCH_SIZE = 1000  # Process rows in batches
    CONFLICT_LOGGING_ENABLED = True
    
    # Safety limits
    MAX_ROWS_PER_SYNC = 10000  # Prevent runaway syncs
    STALE_THRESHOLD_SECONDS = 300  # 5 minutes


# ========================================
# PySpark Session Management
# ========================================

def create_spark_session() -> SparkSession:
    """
    Create PySpark session configured for MinIO and Delta Lake.
    
    Returns:
        Configured SparkSession
    """
    logger.info("Creating Spark session for sync job...")
    
    spark = (
        SparkSession.builder
        .appName("Endymion-AI-Silver-to-SQL-Sync")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO S3 Configuration
        .config("spark.hadoop.fs.s3a.endpoint", SyncConfig.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", SyncConfig.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SyncConfig.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    
    logger.info("✅ Spark session created")
    return spark


# ========================================
# Sync State Management
# ========================================

def get_or_create_sync_state(session: Session) -> SyncState:
    """
    Get existing sync state or create new one.
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        SyncState record
    """
    stmt = select(SyncState).where(
        SyncState.table_name == SyncConfig.SQL_TABLE_NAME
    )
    sync_state = session.execute(stmt).scalar_one_or_none()
    
    if sync_state is None:
        logger.info("Initializing sync state for first run")
        sync_state = SyncState(
            table_name=SyncConfig.SQL_TABLE_NAME,
            silver_table_name=SyncConfig.SILVER_TABLE,
            last_sync_watermark=None,  # Will sync all data on first run
            total_rows_synced=0,
            total_conflicts_resolved=0
        )
        session.add(sync_state)
        session.commit()
        logger.info(f"Created sync state: {sync_state.id}")
    
    return sync_state


def update_sync_state(
    session: Session,
    sync_state: SyncState,
    new_watermark: datetime,
    rows_synced: int,
    conflicts_resolved: int,
    error: Optional[str] = None
) -> None:
    """
    Update sync state after sync run.
    
    Args:
        session: SQLAlchemy session
        sync_state: SyncState record to update
        new_watermark: New watermark timestamp
        rows_synced: Number of rows synced in this run
        conflicts_resolved: Number of conflicts resolved
        error: Error message if sync failed
    """
    sync_state.last_sync_watermark = new_watermark
    sync_state.total_rows_synced += rows_synced
    sync_state.total_conflicts_resolved += conflicts_resolved
    sync_state.last_sync_completed_at = datetime.utcnow()
    sync_state.last_sync_error = error
    session.commit()
    
    logger.info(f"Updated sync state: watermark={new_watermark}, total_synced={sync_state.total_rows_synced}")


# ========================================
# Silver Data Reading
# ========================================

def read_changed_cows_from_silver(
    spark: SparkSession,
    watermark: Optional[datetime]
) -> Tuple[DataFrame, Optional[datetime]]:
    """
    Read cows that changed since last sync watermark.
    
    Args:
        spark: SparkSession
        watermark: Last sync watermark (None for full sync)
    
    Returns:
        Tuple of (DataFrame with changed cows, new watermark timestamp)
    """
    logger.info(f"Reading changed cows from Silver (watermark: {watermark})")
    
    start_time = time.time()
    
    try:
        # Read Silver Delta table
        df = spark.read.format("delta").load(SyncConfig.SILVER_PATH)
        
        total_rows = df.count()
        logger.info(f"Silver table has {total_rows} total rows")
        
        # Filter by watermark if provided (incremental sync)
        if watermark:
            df = df.filter(col("last_updated_at") > watermark.isoformat())
            changed_rows = df.count()
            logger.info(f"Found {changed_rows} changed rows since {watermark}")
        else:
            logger.info("No watermark - performing FULL sync")
            changed_rows = total_rows
        
        # Safety check
        if changed_rows > SyncConfig.MAX_ROWS_PER_SYNC:
            logger.warning(
                f"Too many rows to sync ({changed_rows} > {SyncConfig.MAX_ROWS_PER_SYNC}). "
                f"This may indicate a problem. Limiting to {SyncConfig.MAX_ROWS_PER_SYNC} rows."
            )
            df = df.limit(SyncConfig.MAX_ROWS_PER_SYNC)
        
        # Get new watermark (max last_updated_at from this batch)
        new_watermark = None
        if changed_rows > 0:
            max_timestamp_row = df.agg({"last_updated_at": "max"}).collect()[0]
            max_timestamp_str = max_timestamp_row["max(last_updated_at)"]
            if max_timestamp_str:
                new_watermark = datetime.fromisoformat(max_timestamp_str)
                logger.info(f"New watermark will be: {new_watermark}")
        
        read_time = time.time() - start_time
        logger.info(f"Read from Silver completed in {read_time:.2f}s")
        
        return df, new_watermark
    
    except Exception as e:
        logger.error(f"Failed to read from Silver: {e}", exc_info=True)
        raise


# ========================================
# SQL Upsert Logic
# ========================================

def upsert_cow_to_sql(
    session: Session,
    cow_data: Dict,
    sync_log_id: UUID
) -> Tuple[str, bool]:
    """
    Upsert a single cow to SQL projection table.
    
    CONFLICT RESOLUTION: Silver ALWAYS wins.
    If SQL record is newer, we overwrite it anyway and log a conflict.
    
    Args:
        session: SQLAlchemy session
        cow_data: Cow data from Silver (as dict)
        sync_log_id: ID of current sync log entry
    
    Returns:
        Tuple of (action taken, had_conflict)
        action is one of: 'inserted', 'updated', 'skipped'
    """
    cow_id = UUID(cow_data["id"])
    silver_updated_at = datetime.fromisoformat(cow_data["last_updated_at"])
    
    # Check if cow exists in SQL
    stmt = select(Cow).where(Cow.cow_id == cow_id)
    existing_cow = session.execute(stmt).scalar_one_or_none()
    
    if existing_cow is None:
        # INSERT new cow
        new_cow = Cow(
            cow_id=cow_id,
            tenant_id=UUID(cow_data["tenant_id"]),
            tag_number=cow_data["tag_number"],
            name=cow_data.get("name"),
            breed=cow_data["breed"],
            birth_date=datetime.fromisoformat(cow_data["birth_date"]).date(),
            sex=cow_data["sex"],
            dam_id=UUID(cow_data["dam_id"]) if cow_data.get("dam_id") else None,
            sire_id=UUID(cow_data["sire_id"]) if cow_data.get("sire_id") else None,
            status=cow_data["status"],
            weight_kg=cow_data.get("weight_kg"),
            last_weight_date=datetime.fromisoformat(cow_data["last_weight_date"]) if cow_data.get("last_weight_date") else None,
            current_location=cow_data.get("current_location"),
            notes=cow_data.get("notes"),
            silver_last_updated_at=silver_updated_at,
            last_synced_at=datetime.utcnow(),
            sync_version=1
        )
        session.add(new_cow)
        return 'inserted', False
    
    else:
        # Check for conflict
        had_conflict = False
        
        # If SQL was updated more recently than our watermark, it's a manual edit
        if existing_cow.silver_last_updated_at and existing_cow.silver_last_updated_at > silver_updated_at:
            had_conflict = True
            if SyncConfig.CONFLICT_LOGGING_ENABLED:
                log_conflict(
                    session=session,
                    cow_id=cow_id,
                    tenant_id=existing_cow.tenant_id,
                    silver_timestamp=silver_updated_at,
                    sql_timestamp=existing_cow.silver_last_updated_at,
                    sync_log_id=sync_log_id
                )
        
        # Check if we need to update (Silver data is newer)
        if existing_cow.silver_last_updated_at is None or silver_updated_at >= existing_cow.silver_last_updated_at:
            # UPDATE existing cow - Silver wins
            existing_cow.tag_number = cow_data["tag_number"]
            existing_cow.name = cow_data.get("name")
            existing_cow.breed = cow_data["breed"]
            existing_cow.birth_date = datetime.fromisoformat(cow_data["birth_date"]).date()
            existing_cow.sex = cow_data["sex"]
            existing_cow.dam_id = UUID(cow_data["dam_id"]) if cow_data.get("dam_id") else None
            existing_cow.sire_id = UUID(cow_data["sire_id"]) if cow_data.get("sire_id") else None
            existing_cow.status = cow_data["status"]
            existing_cow.weight_kg = cow_data.get("weight_kg")
            existing_cow.last_weight_date = datetime.fromisoformat(cow_data["last_weight_date"]) if cow_data.get("last_weight_date") else None
            existing_cow.current_location = cow_data.get("current_location")
            existing_cow.notes = cow_data.get("notes")
            existing_cow.silver_last_updated_at = silver_updated_at
            existing_cow.last_synced_at = datetime.utcnow()
            existing_cow.sync_version += 1
            
            return 'updated', had_conflict
        else:
            # Skip - SQL is already up-to-date
            return 'skipped', False


def log_conflict(
    session: Session,
    cow_id: UUID,
    tenant_id: UUID,
    silver_timestamp: datetime,
    sql_timestamp: datetime,
    sync_log_id: UUID
) -> None:
    """
    Log a sync conflict to sync_conflicts table.
    
    Args:
        session: SQLAlchemy session
        cow_id: ID of conflicting cow
        tenant_id: Tenant ID
        silver_timestamp: Timestamp from Silver
        sql_timestamp: Timestamp from SQL
        sync_log_id: ID of sync log entry
    """
    conflict = SyncConflict(
        table_name=SyncConfig.SQL_TABLE_NAME,
        record_id=cow_id,
        tenant_id=tenant_id,
        conflict_type="sql_newer",
        resolution="silver_wins",
        resolved_by_sync_log_id=sync_log_id,
        silver_timestamp=silver_timestamp,
        sql_timestamp=sql_timestamp,
        detected_at=datetime.utcnow(),
        resolved_at=datetime.utcnow()
    )
    session.add(conflict)
    
    logger.warning(
        f"CONFLICT: Cow {cow_id} - SQL timestamp {sql_timestamp} > Silver {silver_timestamp}. "
        f"Silver wins (overwriting SQL changes)."
    )


# ========================================
# Main Sync Logic
# ========================================

def sync_silver_to_sql() -> Dict:
    """
    Main sync function - synchronize Silver layer to SQL projection.
    
    This is the CRITICAL function that makes SQL a true projection.
    
    Returns:
        Dict with sync statistics
    """
    logger.info("=" * 60)
    logger.info("Starting Silver-to-SQL sync")
    logger.info("=" * 60)
    
    sync_start_time = time.time()
    spark = None
    
    # Statistics
    stats = {
        "status": "running",
        "rows_read": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_skipped": 0,
        "conflicts_resolved": 0,
        "duration_seconds": 0,
        "error": None
    }
    
    try:
        # Get database session
        db_manager = get_db_manager()
        session = db_manager.get_session()
        
        # Get or create sync state
        sync_state = get_or_create_sync_state(session)
        watermark_start = sync_state.last_sync_watermark
        
        # Create sync log entry
        sync_log = SyncLog(
            table_name=SyncConfig.SQL_TABLE_NAME,
            silver_table_name=SyncConfig.SILVER_TABLE,
            watermark_start=watermark_start,
            started_at=datetime.utcnow(),
            status="running"
        )
        session.add(sync_log)
        session.commit()
        logger.info(f"Created sync log: {sync_log.id}")
        
        # Update sync state to indicate sync started
        sync_state.last_sync_started_at = datetime.utcnow()
        session.commit()
        
        # Create Spark session
        spark = create_spark_session()
        
        # Read changed cows from Silver
        spark_read_start = time.time()
        changed_cows_df, new_watermark = read_changed_cows_from_silver(spark, watermark_start)
        spark_read_time = time.time() - spark_read_start
        
        # Convert to list of dicts for processing
        changed_cows = changed_cows_df.collect()
        stats["rows_read"] = len(changed_cows)
        
        if stats["rows_read"] == 0:
            logger.info("No changed rows to sync")
            sync_log.status = "completed"
            sync_log.completed_at = datetime.utcnow()
            sync_log.duration_seconds = time.time() - sync_start_time
            sync_log.spark_read_time_seconds = spark_read_time
            session.commit()
            
            stats["status"] = "completed"
            stats["duration_seconds"] = time.time() - sync_start_time
            return stats
        
        logger.info(f"Processing {stats['rows_read']} changed cows...")
        
        # Process cows in batches
        sql_write_start = time.time()
        batch_size = SyncConfig.BATCH_SIZE
        
        for i in range(0, len(changed_cows), batch_size):
            batch = changed_cows[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(changed_cows) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} rows)")
            
            for row in batch:
                cow_data = row.asDict()
                action, had_conflict = upsert_cow_to_sql(session, cow_data, sync_log.id)
                
                if action == 'inserted':
                    stats["rows_inserted"] += 1
                elif action == 'updated':
                    stats["rows_updated"] += 1
                elif action == 'skipped':
                    stats["rows_skipped"] += 1
                
                if had_conflict:
                    stats["conflicts_resolved"] += 1
            
            # Commit batch
            session.commit()
            logger.info(f"Batch {batch_num}/{total_batches} committed")
        
        sql_write_time = time.time() - sql_write_start
        
        # Update sync state with new watermark
        if new_watermark:
            update_sync_state(
                session=session,
                sync_state=sync_state,
                new_watermark=new_watermark,
                rows_synced=stats["rows_inserted"] + stats["rows_updated"],
                conflicts_resolved=stats["conflicts_resolved"]
            )
        
        # Update sync log with results
        sync_log.status = "completed"
        sync_log.completed_at = datetime.utcnow()
        sync_log.duration_seconds = time.time() - sync_start_time
        sync_log.watermark_end = new_watermark
        sync_log.rows_read = stats["rows_read"]
        sync_log.rows_inserted = stats["rows_inserted"]
        sync_log.rows_updated = stats["rows_updated"]
        sync_log.rows_skipped = stats["rows_skipped"]
        sync_log.conflicts_resolved = stats["conflicts_resolved"]
        sync_log.spark_read_time_seconds = spark_read_time
        sync_log.sql_write_time_seconds = sql_write_time
        session.commit()
        
        stats["status"] = "completed"
        stats["duration_seconds"] = time.time() - sync_start_time
        
        logger.info("=" * 60)
        logger.info("Sync completed successfully")
        logger.info(f"  Rows read: {stats['rows_read']}")
        logger.info(f"  Rows inserted: {stats['rows_inserted']}")
        logger.info(f"  Rows updated: {stats['rows_updated']}")
        logger.info(f"  Rows skipped: {stats['rows_skipped']}")
        logger.info(f"  Conflicts resolved: {stats['conflicts_resolved']}")
        logger.info(f"  Duration: {stats['duration_seconds']:.2f}s")
        logger.info("=" * 60)
        
        return stats
    
    except Exception as e:
        error_msg = str(e)
        error_details = traceback.format_exc()
        
        logger.error(f"Sync FAILED: {error_msg}")
        logger.error(error_details)
        
        stats["status"] = "failed"
        stats["error"] = error_msg
        stats["duration_seconds"] = time.time() - sync_start_time
        
        # Update sync log with error
        try:
            if 'sync_log' in locals():
                sync_log.status = "failed"
                sync_log.completed_at = datetime.utcnow()
                sync_log.duration_seconds = stats["duration_seconds"]
                sync_log.error_message = error_msg
                sync_log.error_details = error_details
                session.commit()
            
            # Update sync state with error
            if 'sync_state' in locals():
                sync_state.last_sync_error = error_msg
                session.commit()
        except:
            logger.error("Failed to log error to database")
        
        return stats
    
    finally:
        # Cleanup
        if spark:
            spark.stop()
            logger.info("Spark session stopped")
        
        if 'session' in locals():
            session.close()


# ========================================
# CLI Entry Point
# ========================================

if __name__ == "__main__":
    """Run sync job once."""
    result = sync_silver_to_sql()
    
    if result["status"] == "failed":
        sys.exit(1)
    else:
        sys.exit(0)
