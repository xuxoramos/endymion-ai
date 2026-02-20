"""
Sync tracking models for Silver-to-SQL synchronization.

These models track the state and history of synchronization between
the Silver layer (Delta tables) and SQL projection tables.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import String, Integer, Float, Text, Index
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, UUIDPrimaryKeyMixin


class SyncState(Base, UUIDPrimaryKeyMixin):
    """
    Tracks the current sync watermark for each projection table.
    
    The watermark is the timestamp of the last successfully processed record
    from the Silver layer. On next sync, we only process records newer than
    this watermark.
    
    This enables incremental syncing and prevents reprocessing of unchanged data.
    """
    
    __tablename__ = "sync_state"
    __table_args__ = (
        Index("UQ_sync_state_table_name", "table_name", unique=True),
        {"schema": "operational"}
    )
    
    # Which table is being synced
    table_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        doc="Name of the projection table being synced (e.g., 'cows')"
    )
    
    # Silver layer source
    silver_table_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Name of the Silver Delta table (e.g., 'silver_cows_current')"
    )
    
    # Sync watermark - the timestamp to resume from
    last_sync_watermark: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Timestamp of the last successfully synced record from Silver"
    )
    
    # Statistics
    total_rows_synced: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Total number of rows synced since tracking began"
    )
    
    total_conflicts_resolved: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Total number of conflicts resolved (Silver overwrites SQL)"
    )
    
    # Metadata
    last_sync_started_at: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Timestamp when the last sync started"
    )
    
    last_sync_completed_at: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Timestamp when the last sync completed successfully"
    )
    
    last_sync_error: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Error message from last sync failure (if any)"
    )
    
    created_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=datetime.utcnow,
        doc="Timestamp when sync tracking was initialized"
    )
    
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        doc="Timestamp when sync state was last updated"
    )
    
    def __repr__(self) -> str:
        return (
            f"<SyncState("
            f"table={self.table_name}, "
            f"watermark={self.last_sync_watermark}, "
            f"synced={self.total_rows_synced}"
            f")>"
        )


class SyncLog(Base, UUIDPrimaryKeyMixin):
    """
    Logs each sync run for auditing and monitoring.
    
    Records details about each synchronization attempt including:
    - Duration and performance metrics
    - Number of rows processed
    - Errors encountered
    - Conflict resolution details
    """
    
    __tablename__ = "sync_log"
    __table_args__ = (
        Index("IX_sync_log_table_name", "table_name"),
        Index("IX_sync_log_started_at", "started_at"),
        Index("IX_sync_log_status", "status"),
        {"schema": "operational"}
    )
    
    # What was synced
    table_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Name of the projection table synced"
    )
    
    silver_table_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Name of the Silver Delta table"
    )
    
    # Sync window
    watermark_start: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Starting watermark timestamp (exclusive)"
    )
    
    watermark_end: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Ending watermark timestamp (inclusive)"
    )
    
    # Timing
    started_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=datetime.utcnow,
        doc="Timestamp when sync started"
    )
    
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Timestamp when sync completed (success or failure)"
    )
    
    duration_seconds: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        doc="Duration of sync in seconds"
    )
    
    # Results
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="running",
        doc="Status: running, completed, failed"
    )
    
    rows_read: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of rows read from Silver"
    )
    
    rows_inserted: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of new rows inserted into SQL"
    )
    
    rows_updated: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of existing rows updated in SQL"
    )
    
    rows_skipped: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of rows skipped (already up-to-date)"
    )
    
    conflicts_resolved: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of conflicts where Silver overwrote SQL changes"
    )
    
    # Error details
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Error message if sync failed"
    )
    
    error_details: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Detailed error traceback"
    )
    
    # Performance metrics
    spark_read_time_seconds: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        doc="Time spent reading from Silver Delta table"
    )
    
    sql_write_time_seconds: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        doc="Time spent writing to SQL database"
    )
    
    def __repr__(self) -> str:
        return (
            f"<SyncLog("
            f"table={self.table_name}, "
            f"status={self.status}, "
            f"rows={self.rows_read}/{self.rows_inserted}+{self.rows_updated}"
            f")>"
        )


class SyncConflict(Base, UUIDPrimaryKeyMixin):
    """
    Records conflicts where Silver overwrites SQL changes.
    
    In Pure Projection Pattern A, Silver is the source of truth.
    Any manual changes to SQL projection tables are considered errors
    and will be overwritten by Silver.
    
    This table logs these conflicts for auditing and debugging.
    """
    
    __tablename__ = "sync_conflicts"
    __table_args__ = (
        Index("IX_sync_conflicts_table_name", "table_name"),
        Index("IX_sync_conflicts_record_id", "record_id"),
        Index("IX_sync_conflicts_detected_at", "detected_at"),
        {"schema": "operational"}
    )
    
    # Which record had a conflict
    table_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Name of the projection table"
    )
    
    record_id: Mapped[UUID] = mapped_column(
        UNIQUEIDENTIFIER,
        nullable=False,
        doc="ID of the conflicting record"
    )
    
    tenant_id: Mapped[UUID] = mapped_column(
        UNIQUEIDENTIFIER,
        nullable=False,
        doc="Tenant ID of the conflicting record"
    )
    
    # Conflict details
    conflict_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="Type of conflict: sql_newer, manual_edit, etc."
    )
    
    silver_value: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Value from Silver (winner)"
    )
    
    sql_value: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Value from SQL (overwritten)"
    )
    
    field_name: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        doc="Name of the field that conflicted"
    )
    
    # Resolution
    resolution: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="silver_wins",
        doc="How conflict was resolved (always 'silver_wins' for now)"
    )
    
    resolved_by_sync_log_id: Mapped[Optional[UUID]] = mapped_column(
        UNIQUEIDENTIFIER,
        nullable=True,
        doc="ID of the sync_log entry that resolved this conflict"
    )
    
    # Timestamps
    silver_timestamp: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Timestamp from Silver record"
    )
    
    sql_timestamp: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Timestamp from SQL record"
    )
    
    detected_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=datetime.utcnow,
        doc="Timestamp when conflict was detected"
    )
    
    resolved_at: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Timestamp when conflict was resolved"
    )
    
    def __repr__(self) -> str:
        return (
            f"<SyncConflict("
            f"table={self.table_name}, "
            f"record_id={self.record_id}, "
            f"type={self.conflict_type}"
            f")>"
        )
