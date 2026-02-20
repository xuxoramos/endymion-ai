"""
SQLAlchemy models for analytics schema.

These models map to the SQL Server analytics schema tables that are
populated by the Gold layer projection. They are read-only from the
API perspective.

Architecture:
    Gold Delta (truth) → SQL Server analytics.* (disposable projection) → FastAPI
"""

from datetime import date
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Column, String, Integer, Date, Numeric, PrimaryKeyConstraint
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

from .base import Base


class HerdComposition(Base):
    """
    Analytics table: Herd composition breakdown by dimension.
    
    Projected from Gold layer's herd_composition Delta table.
    Contains pre-computed aggregations by breed, status, and sex.
    """
    
    __tablename__ = 'herd_composition'
    __table_args__ = (
        PrimaryKeyConstraint('tenant_id', 'snapshot_date', 'dimension_type', 'dimension_value'),
        {'schema': 'analytics'}
    )
    
    tenant_id = Column(UNIQUEIDENTIFIER, nullable=False)
    snapshot_date = Column(Date, nullable=False)
    dimension_type = Column(String(50), nullable=False)  # 'breed', 'status', 'sex'
    dimension_value = Column(String(100), nullable=False)  # e.g., 'Holstein', 'active', 'female'
    count = Column(Integer, nullable=False)
    percentage = Column(Numeric(5, 2))
    total_cows = Column(Integer, nullable=False)
    
    def __repr__(self):
        return f"<HerdComposition({self.snapshot_date}, {self.dimension_type}={self.dimension_value}, count={self.count})>"


class CowLifecycle(Base):
    """
    Analytics table: Individual cow lifecycle tracking.
    
    Projected from Gold layer's cow_lifecycle Delta table.
    One row per cow with complete lifecycle information.
    """
    
    __tablename__ = 'cow_lifecycle'
    __table_args__ = (
        PrimaryKeyConstraint('tenant_id', 'cow_id'),
        {'schema': 'analytics'}
    )
    
    tenant_id = Column(UNIQUEIDENTIFIER, nullable=False)
    cow_id = Column(String(50), nullable=False)
    initial_breed = Column(String(100))
    final_breed = Column(String(100))
    final_status = Column(String(50))
    sex = Column(String(20))
    birth_date = Column(Date)
    created_at = Column(Date)
    last_updated_at = Column(Date)
    lifecycle_duration_days = Column(Integer)
    state_change_count = Column(Integer)
    final_weight_kg = Column(Numeric(10, 2))
    deactivation_reason = Column(String(200))
    lifecycle_stage = Column(String(50))
    
    def __repr__(self):
        return f"<CowLifecycle({self.cow_id}, status={self.final_status})>"


class DailySnapshot(Base):
    """
    Analytics table: Daily herd snapshot aggregations.
    
    Projected from Gold layer's daily_snapshots Delta table.
    Contains daily totals and breakdowns.
    """
    
    __tablename__ = 'daily_snapshots'
    __table_args__ = (
        PrimaryKeyConstraint('tenant_id', 'snapshot_date'),
        {'schema': 'analytics'}
    )
    
    tenant_id = Column(UNIQUEIDENTIFIER, nullable=False)
    snapshot_date = Column(Date, nullable=False)
    total_cows = Column(Integer)
    active_cows = Column(Integer)
    inactive_cows = Column(Integer)
    
    def __repr__(self):
        return f"<DailySnapshot({self.snapshot_date}, total={self.total_cows})>"
