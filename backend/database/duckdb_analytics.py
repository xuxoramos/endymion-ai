"""
DuckDB Analytics Repository

Queries Gold Delta Lake tables directly without SQL Server projection.
This mirrors Databricks SQL pattern where analytics query Delta tables directly.

Architecture:
    Gold Delta (canonical truth) → DuckDB queries → FastAPI

Advantages over SQL Server analytics.* projection:
- No sync delay (queries canonical Gold directly)
- No SQL projection step needed
- Faster analytics pipeline (eliminates Gold→SQL sync)
- Same query performance (~10-50ms)
- Mirrors Databricks SQL serverless warehouses

Usage:
    repo = DuckDBAnalyticsRepository()
    df = repo.get_herd_composition(tenant_id, date)
    data = df.to_dict('records')
"""

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Dict, Any
from uuid import UUID
import os

import duckdb
import pandas as pd


logger = logging.getLogger(__name__)


class DuckDBAnalyticsRepository:
    """
    Repository for querying Gold Delta tables with DuckDB.
    
    This replaces the SQL Server analytics.* projection schema.
    All queries are read-only and target Gold Delta Lake tables directly.
    """
    
    def __init__(self, s3_endpoint: str = None, s3_access_key: str = None, s3_secret_key: str = None):
        """
        Initialize DuckDB connection with Delta Lake and S3 support.
        
        Args:
            s3_endpoint: MinIO/S3 endpoint (default: localhost:9000)
            s3_access_key: S3 access key (default: minioadmin)
            s3_secret_key: S3 secret key (default: minioadmin)
        """
        # Use environment variables or defaults
        self.s3_endpoint = s3_endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.s3_access_key = s3_access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.s3_secret_key = s3_secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        
        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(database=':memory:', read_only=False)
        
        # Install and load extensions
        try:
            self.conn.execute("INSTALL delta")
            self.conn.execute("LOAD delta")
            logger.info("✅ DuckDB Delta extension loaded")
        except Exception as e:
            logger.warning(f"⚠️  Failed to load Delta extension: {e}")
            logger.warning("   Install with: duckdb -c 'INSTALL delta'")
        
        # Configure S3/MinIO access
        # Important: Must use CREATE SECRET for DuckDB >= 0.10
        self.conn.execute(f"""
            CREATE OR REPLACE SECRET minio_secret (
                TYPE S3,
                ENDPOINT '{self.s3_endpoint}',
                URL_STYLE 'path',
                USE_SSL false,
                KEY_ID '{self.s3_access_key}',
                SECRET '{self.s3_secret_key}'
            )
        """)
        
        logger.info(f"✅ DuckDB initialized for analytics (S3: {self.s3_endpoint})")
    
    def get_herd_composition(
        self,
        tenant_id: UUID,
        snapshot_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Get herd composition breakdown for a tenant and date.
        
        Queries: s3://gold/herd_composition Delta table
        
        Args:
            tenant_id: Tenant identifier
            snapshot_date: Snapshot date (defaults to latest if None)
            
        Returns:
            Pandas DataFrame with herd composition data
        """
        tenant_str = str(tenant_id).lower()
        
        # If no date provided, get latest
        if snapshot_date is None:
            latest_query = """
                SELECT MAX(snapshot_date) as max_date
                FROM delta_scan('s3://gold/herd_composition')
                WHERE tenant_id = ?
            """
            result = self.conn.execute(latest_query, [tenant_str]).fetchone()
            snapshot_date = result[0] if result and result[0] else date.today()
        
        # Query herd composition
        query = """
            SELECT 
                tenant_id,
                snapshot_date,
                dimension_type,
                dimension_value,
                count,
                percentage,
                total_cows,
                _computation_timestamp,
                _source_tables
            FROM delta_scan('s3://gold/herd_composition')
            WHERE tenant_id = ?
              AND snapshot_date = ?
            ORDER BY dimension_type, count DESC
        """
        
        df = self.conn.execute(query, [tenant_str, str(snapshot_date)]).df()
        
        logger.info(f"Queried herd composition: {len(df)} rows for {tenant_id} on {snapshot_date}")
        return df
    
    def get_cow_lifecycle(
        self,
        tenant_id: UUID,
        cow_id: UUID
    ) -> Optional[pd.DataFrame]:
        """
        Get lifecycle information for a specific cow.
        
        Queries: s3://gold/cow_lifecycle Delta table
        
        Args:
            tenant_id: Tenant identifier
            cow_id: Cow identifier
            
        Returns:
            Pandas DataFrame with single row or empty DataFrame if not found
        """
        tenant_str = str(tenant_id).lower()
        cow_str = str(cow_id).lower()
        
        query = """
            SELECT 
                tenant_id,
                cow_id,
                created_at,
                last_updated_at,
                lifecycle_duration_days,
                state_change_count,
                final_status,
                final_breed,
                final_weight_kg,
                deactivation_reason,
                lifecycle_stage,
                __CREATED_AT,
                __SOURCE_TABLES
            FROM delta_scan('s3://gold/cow_lifecycle')
            WHERE tenant_id = ?
              AND cow_id = ?
        """
        
        df = self.conn.execute(query, [tenant_str, cow_str]).df()
        
        if len(df) == 0:
            logger.info(f"No lifecycle data found for cow {cow_id}")
            return None
        
        logger.info(f"Queried cow lifecycle: {len(df)} rows for {cow_id}")
        return df
    
    def get_all_cow_lifecycles(
        self,
        tenant_id: UUID,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get lifecycle information for all cows in a herd.
        
        Args:
            tenant_id: Tenant identifier
            limit: Maximum number of records to return
            
        Returns:
            Pandas DataFrame with lifecycle data
        """
        tenant_str = str(tenant_id).lower()
        
        query = """
            SELECT 
                tenant_id,
                cow_id,
                created_at,
                last_updated_at,
                lifecycle_duration_days,
                state_change_count,
                final_status,
                final_breed,
                final_weight_kg,
                deactivation_reason,
                lifecycle_stage
            FROM delta_scan('s3://gold/cow_lifecycle')
            WHERE tenant_id = ?
            ORDER BY last_updated_at DESC
            LIMIT ?
        """
        
        df = self.conn.execute(query, [tenant_str, limit]).df()
        
        logger.info(f"Queried all cow lifecycles: {len(df)} rows for {tenant_id}")
        return df
    
    def get_daily_snapshot(
        self,
        tenant_id: UUID,
        snapshot_date: date
    ) -> Optional[pd.DataFrame]:
        """
        Get daily snapshot for a specific date.
        
        Queries: s3://gold/daily_snapshots Delta table
        
        Args:
            tenant_id: Tenant identifier
            snapshot_date: Date to retrieve
            
        Returns:
            Pandas DataFrame with single row or empty DataFrame if not found
        """
        tenant_str = str(tenant_id).lower()
        
        query = """
            SELECT 
                tenant_id,
                snapshot_date,
                total_cows,
                active_cows,
                inactive_cows,
                cows_created_today,
                cows_deactivated_today,
                avg_weight_kg,
                __CREATED_AT,
                __SOURCE_TABLES
            FROM delta_scan('s3://gold/daily_snapshots')
            WHERE tenant_id = ?
              AND snapshot_date = ?
        """
        
        df = self.conn.execute(query, [tenant_str, str(snapshot_date)]).df()
        
        if len(df) == 0:
            logger.info(f"No snapshot found for {tenant_id} on {snapshot_date}")
            return None
        
        logger.info(f"Queried daily snapshot: {snapshot_date}")
        return df
    
    def get_daily_snapshots_range(
        self,
        tenant_id: UUID,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Get daily snapshots for a date range.
        
        Args:
            tenant_id: Tenant identifier
            start_date: Start of date range
            end_date: End of date range
            
        Returns:
            Pandas DataFrame with snapshots
        """
        tenant_str = str(tenant_id).lower()
        
        query = """
            SELECT 
                tenant_id,
                snapshot_date,
                total_cows,
                active_cows,
                inactive_cows,
                cows_created_today,
                cows_deactivated_today,
                avg_weight_kg
            FROM delta_scan('s3://gold/daily_snapshots')
            WHERE tenant_id = ?
              AND snapshot_date >= ?
              AND snapshot_date <= ?
            ORDER BY snapshot_date ASC
        """
        
        df = self.conn.execute(query, [tenant_str, str(start_date), str(end_date)]).df()
        
        logger.info(f"Queried daily snapshots: {len(df)} rows from {start_date} to {end_date}")
        return df
    
    def get_latest_snapshot_date(self, tenant_id: UUID) -> Optional[date]:
        """
        Get the most recent snapshot date for a tenant.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            Latest snapshot date or None if no data exists
        """
        tenant_str = str(tenant_id).lower()
        
        query = """
            SELECT MAX(snapshot_date) as max_date
            FROM delta_scan('s3://gold/herd_composition')
            WHERE tenant_id = ?
        """
        
        result = self.conn.execute(query, [tenant_str]).fetchone()
        
        if result and result[0]:
            return result[0]
        return None
    
    def get_herd_stats(self, tenant_id: UUID) -> Dict[str, Any]:
        """
        Get summary statistics for a herd.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            Dictionary with herd statistics
        """
        latest_date = self.get_latest_snapshot_date(tenant_id)
        
        if latest_date is None:
            return {
                'snapshot_date': None,
                'total_cows': 0,
                'active_cows': 0,
                'inactive_cows': 0
            }
        
        snapshot_df = self.get_daily_snapshot(tenant_id, latest_date)
        
        if snapshot_df is None or len(snapshot_df) == 0:
            return {
                'snapshot_date': latest_date,
                'total_cows': 0,
                'active_cows': 0,
                'inactive_cows': 0
            }
        
        row = snapshot_df.iloc[0]
        return {
            'snapshot_date': row['snapshot_date'],
            'total_cows': int(row['total_cows']) if pd.notna(row['total_cows']) else 0,
            'active_cows': int(row['active_cows']) if pd.notna(row['active_cows']) else 0,
            'inactive_cows': int(row['inactive_cows']) if pd.notna(row['inactive_cows']) else 0
        }
    
    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")
    
    def __del__(self):
        """Ensure connection is closed on cleanup."""
        self.close()


# ============================================================================
# Singleton Instance (for FastAPI dependency injection)
# ============================================================================

_duckdb_analytics_instance: Optional[DuckDBAnalyticsRepository] = None


def get_duckdb_analytics() -> DuckDBAnalyticsRepository:
    """
    Get or create singleton DuckDB analytics repository.
    
    Used as FastAPI dependency:
        @app.get("/analytics/...")
        async def endpoint(repo: DuckDBAnalyticsRepository = Depends(get_duckdb_analytics)):
            ...
    """
    global _duckdb_analytics_instance
    
    if _duckdb_analytics_instance is None:
        _duckdb_analytics_instance = DuckDBAnalyticsRepository()
    
    return _duckdb_analytics_instance


def reset_duckdb_analytics():
    """Reset singleton (useful for testing)."""
    global _duckdb_analytics_instance
    
    if _duckdb_analytics_instance:
        _duckdb_analytics_instance.close()
        _duckdb_analytics_instance = None
