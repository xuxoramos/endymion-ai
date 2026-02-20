"""
Analytics data access layer.

Provides SQL-based queries for analytics endpoints using SQLAlchemy.
Replaces PySpark/Delta Lake queries with fast SQL Server queries.

Architecture:
    Gold Delta (canonical truth) → SQL Server analytics.* (projection) → This module → FastAPI
"""

import logging
from datetime import date, datetime
from typing import List, Optional, Dict, Any
from uuid import UUID

from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from backend.models.analytics import HerdComposition, CowLifecycle, DailySnapshot


logger = logging.getLogger(__name__)


class AnalyticsRepository:
    """
    Repository for analytics data access.
    
    All queries are read-only and target the analytics schema in SQL Server.
    """
    
    def __init__(self, session: Session):
        """
        Initialize repository with database session.
        
        Args:
            session: SQLAlchemy session
        """
        self.session = session
    
    def get_latest_snapshot_date(self, tenant_id: UUID) -> Optional[date]:
        """
        Get the most recent snapshot date for a tenant.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            Latest snapshot date or None if no data exists
        """
        result = self.session.query(func.max(HerdComposition.snapshot_date)) \
            .filter(HerdComposition.tenant_id == tenant_id) \
            .scalar()
        
        return result
    
    def get_herd_composition(
        self,
        tenant_id: UUID,
        snapshot_date: Optional[date] = None
    ) -> List[HerdComposition]:
        """
        Get herd composition breakdown for a tenant and date.
        
        Args:
            tenant_id: Tenant identifier
            snapshot_date: Snapshot date (defaults to latest if None)
            
        Returns:
            List of HerdComposition records
        """
        # If no date provided, get latest
        if snapshot_date is None:
            snapshot_date = self.get_latest_snapshot_date(tenant_id)
            if snapshot_date is None:
                return []
        
        # Query herd composition for tenant and date
        results = self.session.query(HerdComposition) \
            .filter(
                and_(
                    HerdComposition.tenant_id == tenant_id,
                    HerdComposition.snapshot_date == snapshot_date
                )
            ) \
            .all()
        
        return results
    
    def get_cow_lifecycle(
        self,
        tenant_id: UUID,
        cow_id: UUID
    ) -> Optional[CowLifecycle]:
        """
        Get lifecycle information for a specific cow.
        
        Args:
            tenant_id: Tenant identifier
            cow_id: Cow identifier
            
        Returns:
            CowLifecycle record or None if not found
        """
        result = self.session.query(CowLifecycle) \
            .filter(
                and_(
                    CowLifecycle.tenant_id == tenant_id,
                    CowLifecycle.cow_id == cow_id
                )
            ) \
            .first()
        
        return result
    
    def get_all_cow_lifecycles(
        self,
        tenant_id: UUID,
        limit: int = 100
    ) -> List[CowLifecycle]:
        """
        Get lifecycle information for all cows in a herd.
        
        Args:
            tenant_id: Tenant identifier
            limit: Maximum number of records to return
            
        Returns:
            List of CowLifecycle records
        """
        results = self.session.query(CowLifecycle) \
            .filter(CowLifecycle.tenant_id == tenant_id) \
            .order_by(CowLifecycle.last_event_date.desc()) \
            .limit(limit) \
            .all()
        
        return results
    
    def get_daily_snapshot(
        self,
        tenant_id: UUID,
        snapshot_date: date
    ) -> Optional[DailySnapshot]:
        """
        Get daily snapshot for a specific date.
        
        Args:
            tenant_id: Tenant identifier
            snapshot_date: Date to retrieve
            
        Returns:
            DailySnapshot record or None if not found
        """
        result = self.session.query(DailySnapshot) \
            .filter(
                and_(
                    DailySnapshot.tenant_id == tenant_id,
                    DailySnapshot.snapshot_date == snapshot_date
                )
            ) \
            .first()
        
        return result
    
    def get_daily_snapshots_range(
        self,
        tenant_id: UUID,
        start_date: date,
        end_date: date
    ) -> List[DailySnapshot]:
        """
        Get daily snapshots for a date range.
        
        Args:
            tenant_id: Tenant identifier
            start_date: Start of date range
            end_date: End of date range
            
        Returns:
            List of DailySnapshot records
        """
        results = self.session.query(DailySnapshot) \
            .filter(
                and_(
                    DailySnapshot.tenant_id == tenant_id,
                    DailySnapshot.snapshot_date >= start_date,
                    DailySnapshot.snapshot_date <= end_date
                )
            ) \
            .order_by(DailySnapshot.snapshot_date) \
            .all()
        
        return results
    
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
        
        snapshot = self.get_daily_snapshot(tenant_id, latest_date)
        
        if snapshot is None:
            return {
                'snapshot_date': latest_date,
                'total_cows': 0,
                'active_cows': 0,
                'inactive_cows': 0
            }
        
        return {
            'snapshot_date': snapshot.snapshot_date,
            'total_cows': snapshot.total_cows or 0,
            'active_cows': snapshot.active_cows or 0,
            'inactive_cows': snapshot.inactive_cows or 0
        }
