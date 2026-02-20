"""
Base model classes and mixins for SQLAlchemy ORM.

Provides common functionality for all models including:
- Tenant isolation mixin
- Timestamp tracking
- UUID primary keys
"""

from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""
    
    pass


class TenantMixin:
    """
    Mixin for multi-tenant tables.
    
    Adds tenant_id column and provides tenant isolation functionality.
    All queries should be filtered by tenant_id to ensure data isolation.
    """
    
    @declared_attr
    def tenant_id(cls) -> Mapped[UUID]:
        """Tenant ID for multi-tenant isolation."""
        return mapped_column(
            UNIQUEIDENTIFIER,
            nullable=False,
            index=True,
            doc="Tenant identifier for multi-tenant isolation"
        )
    
    def is_owned_by_tenant(self, tenant_id: UUID) -> bool:
        """
        Check if this record belongs to the specified tenant.
        
        Args:
            tenant_id: Tenant UUID to check
            
        Returns:
            True if record belongs to tenant, False otherwise
        """
        return self.tenant_id == tenant_id
    
    @classmethod
    def get_tenant_filter(cls, tenant_id: UUID):
        """
        Get SQLAlchemy filter expression for tenant isolation.
        
        Args:
            tenant_id: Tenant UUID to filter by
            
        Returns:
            SQLAlchemy filter expression
            
        Example:
            query = session.query(Model).filter(Model.get_tenant_filter(tenant_id))
        """
        return cls.tenant_id == tenant_id


class TimestampMixin:
    """
    Mixin for automatic timestamp tracking.
    
    Adds created_at and updated_at columns with automatic updates.
    """
    
    @declared_attr
    def created_at(cls) -> Mapped[datetime]:
        """Timestamp when record was created."""
        return mapped_column(
            DateTime,
            nullable=False,
            default=datetime.utcnow,
            doc="Timestamp when record was created (UTC)"
        )
    
    @declared_attr
    def updated_at(cls) -> Mapped[datetime]:
        """Timestamp when record was last updated."""
        return mapped_column(
            DateTime,
            nullable=False,
            default=datetime.utcnow,
            onupdate=datetime.utcnow,
            doc="Timestamp when record was last updated (UTC)"
        )


class UUIDPrimaryKeyMixin:
    """
    Mixin for UUID primary keys.
    
    Provides a UUID primary key that auto-generates on creation.
    """
    
    @declared_attr
    def id(cls) -> Mapped[UUID]:
        """UUID primary key."""
        return mapped_column(
            UNIQUEIDENTIFIER,
            primary_key=True,
            default=uuid4,
            doc="Unique identifier (UUID v4)"
        )


class ProjectionMixin:
    """
    Mixin for projection tables that are synced from external sources.
    
    Projection tables are READ-ONLY copies of data from another system
    (e.g., Databricks Silver layer). They should not be written to directly.
    """
    
    @declared_attr
    def silver_last_updated_at(cls) -> Mapped[Optional[datetime]]:
        """Timestamp when source record was last updated."""
        return mapped_column(
            DateTime,
            nullable=True,
            doc="Timestamp when record was last updated in Silver layer"
        )
    
    @declared_attr
    def last_synced_at(cls) -> Mapped[datetime]:
        """Timestamp when projection was last synchronized."""
        return mapped_column(
            DateTime,
            nullable=False,
            default=datetime.utcnow,
            doc="Timestamp when projection was last synced from source"
        )
    
    @declared_attr
    def sync_version(cls) -> Mapped[int]:
        """Version number for optimistic concurrency control."""
        return mapped_column(
            "sync_version",
            nullable=False,
            default=1,
            doc="Version number for sync tracking"
        )
    
    @classmethod
    def is_projection(cls) -> bool:
        """
        Indicates this is a projection table.
        
        Returns:
            Always returns True for projection tables.
            
        Warning:
            Projection tables should only be updated by sync jobs,
            not by direct application writes!
        """
        return True
    
    def is_stale(self, max_age_minutes: int = 60) -> bool:
        """
        Check if projection data is stale.
        
        Args:
            max_age_minutes: Maximum age in minutes before considered stale
            
        Returns:
            True if data hasn't been synced within max_age_minutes
        """
        if not self.last_synced_at:
            return True
        
        age = datetime.utcnow() - self.last_synced_at
        return age.total_seconds() > (max_age_minutes * 60)


class SoftDeleteMixin:
    """
    Mixin for soft delete functionality.
    
    Adds deleted_at column and helper methods for soft deletion.
    """
    
    @declared_attr
    def deleted_at(cls) -> Mapped[Optional[datetime]]:
        """Timestamp when record was soft-deleted."""
        return mapped_column(
            DateTime,
            nullable=True,
            doc="Timestamp when record was soft-deleted (NULL = active)"
        )
    
    def soft_delete(self) -> None:
        """Mark record as deleted without actually removing it."""
        self.deleted_at = datetime.utcnow()
    
    def restore(self) -> None:
        """Restore a soft-deleted record."""
        self.deleted_at = None
    
    @property
    def is_deleted(self) -> bool:
        """Check if record is soft-deleted."""
        return self.deleted_at is not None
    
    @classmethod
    def get_active_filter(cls):
        """Get SQLAlchemy filter for active (non-deleted) records."""
        return cls.deleted_at.is_(None)


# Common base classes combining mixins
class TenantBaseModel(Base, TenantMixin, TimestampMixin):
    """
    Base model for tenant-scoped entities with timestamps.
    
    Use this for most application models.
    """
    __abstract__ = True


class ProjectionBaseModel(Base, TenantMixin, ProjectionMixin, TimestampMixin):
    """
    Base model for projection tables.
    
    Use this for read-only projection tables synced from external sources.
    """
    __abstract__ = True
