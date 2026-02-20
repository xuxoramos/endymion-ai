"""
Cow model - Projection table (READ-ONLY).

This table is a PROJECTION synced from the Silver layer in Databricks.
It represents the current state of cows reconstructed from cow_events.

IMPORTANT: This table is READ-ONLY from the API perspective.
Do NOT write directly to this table! All writes must go through cow_events.
"""

from datetime import datetime, date
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import String, Date, Float, Index, CheckConstraint
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import Mapped, mapped_column

from .base import ProjectionBaseModel


class CowStatus(str):
    """Valid cow status values."""
    ACTIVE = "active"
    SOLD = "sold"
    DECEASED = "deceased"
    TRANSFERRED = "transferred"


class CowSex(str):
    """Valid cow sex values."""
    MALE = "male"
    FEMALE = "female"


class Cow(ProjectionBaseModel):
    """
    Cow projection table - current state view of cows.
    
    This table is a MATERIALIZED VIEW of cow state, reconstructed from
    cow_events by the Silver layer in Databricks.
    
    Architecture Pattern: Pure Projection Pattern A
    - Read-Only: API only reads from this table
    - Write Path: API → cow_events → Bronze → Silver → cows (this table)
    - Sync Direction: Silver layer writes to this table
    - Staleness: May lag behind cow_events by minutes
    
    Data Flow:
        cow_events → Bronze → Silver → cows (this table) → API reads
    
    WARNING: Never write directly to this table from the API!
    All cow operations must create events in cow_events table.
    This projection is maintained by the data pipeline.
    """
    
    __tablename__ = "cows"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'sold', 'deceased', 'transferred')",
            name="CK_cows_status"
        ),
        CheckConstraint(
            "sex IN ('male', 'female')",
            name="CK_cows_sex"
        ),
        Index("IX_cows_tenant_id", "tenant_id", "status"),
        Index("UQ_cows_tag_tenant", "tag_number", "tenant_id", unique=True),
        Index("IX_cows_breed", "breed", "tenant_id"),
        Index("IX_cows_birth_date", "birth_date", "tenant_id"),
        Index("IX_cows_dam_sire", "dam_id", "sire_id"),
        Index("IX_cows_last_synced", "last_synced_at"),
        {"schema": "operational"}
    )
    
    # Primary Key
    cow_id: Mapped[UUID] = mapped_column(
        UNIQUEIDENTIFIER,
        primary_key=True,
        default=uuid4,
        doc="Unique identifier for the cow (UUID)"
    )
    
    # Identity Information
    tag_number: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Unique tag/identifier for the cow within tenant"
    )
    
    name: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="Optional name for the cow"
    )
    
    # Biological Information
    breed: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Breed of the cow (e.g., Holstein, Jersey)"
    )
    
    birth_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        doc="Date the cow was born"
    )
    
    sex: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        doc="Sex of the cow (male/female)"
    )
    
    # Genealogy
    dam_id: Mapped[Optional[UUID]] = mapped_column(
        UNIQUEIDENTIFIER,
        nullable=True,
        doc="Reference to mother cow (dam)"
    )
    
    sire_id: Mapped[Optional[UUID]] = mapped_column(
        UNIQUEIDENTIFIER,
        nullable=True,
        doc="Reference to father cow (sire)"
    )
    
    # Current Status
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=CowStatus.ACTIVE,
        doc="Current status (active, sold, deceased, transferred)"
    )
    
    # Physical Attributes
    weight_kg: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        doc="Most recent weight in kilograms"
    )
    
    last_weight_date: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
        doc="Date of last weight recording"
    )
    
    # Location
    current_location: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="Current location/paddock of the cow"
    )
    
    # Metadata
    notes: Mapped[Optional[str]] = mapped_column(
        String(1000),
        nullable=True,
        doc="Additional notes about the cow"
    )
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"<Cow("
            f"cow_id={self.cow_id}, "
            f"tag={self.tag_number}, "
            f"breed={self.breed}, "
            f"status={self.status}, "
            f"projection={self.is_projection()}"
            f")>"
        )
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        age_str = self.get_age_display()
        return f"{self.tag_number} - {self.breed} ({age_str}, {self.status})"
    
    @property
    def age_days(self) -> int:
        """Calculate age in days."""
        if self.birth_date:
            return (date.today() - self.birth_date).days
        return 0
    
    @property
    def age_years(self) -> float:
        """Calculate age in years (decimal)."""
        return self.age_days / 365.25
    
    def get_age_display(self) -> str:
        """
        Get human-readable age string.
        
        Returns:
            Age formatted as "X years" or "X months" or "X days"
        """
        days = self.age_days
        
        if days < 30:
            return f"{days} days"
        elif days < 365:
            months = days // 30
            return f"{months} months"
        else:
            years = days // 365
            remaining_months = (days % 365) // 30
            if remaining_months > 0:
                return f"{years}y {remaining_months}m"
            return f"{years} years"
    
    @property
    def is_active(self) -> bool:
        """Check if cow is currently active."""
        return self.status == CowStatus.ACTIVE
    
    @property
    def is_female(self) -> bool:
        """Check if cow is female."""
        return self.sex == CowSex.FEMALE
    
    @property
    def is_male(self) -> bool:
        """Check if cow is male."""
        return self.sex == CowSex.MALE
    
    @property
    def has_genealogy(self) -> bool:
        """Check if cow has recorded parents."""
        return self.dam_id is not None or self.sire_id is not None
    
    @property
    def has_weight_data(self) -> bool:
        """Check if cow has weight recorded."""
        return self.weight_kg is not None and self.weight_kg > 0
    
    def to_dict(self, include_sync_info: bool = False) -> dict:
        """
        Convert cow to dictionary for serialization.
        
        Args:
            include_sync_info: Whether to include sync metadata
            
        Returns:
            Dictionary representation of the cow
        """
        result = {
            "cow_id": str(self.cow_id),
            "tenant_id": str(self.tenant_id),
            "tag_number": self.tag_number,
            "name": self.name,
            "breed": self.breed,
            "birth_date": self.birth_date.isoformat(),
            "sex": self.sex,
            "status": self.status,
            "dam_id": str(self.dam_id) if self.dam_id else None,
            "sire_id": str(self.sire_id) if self.sire_id else None,
            "weight_kg": self.weight_kg,
            "last_weight_date": self.last_weight_date.isoformat() if self.last_weight_date else None,
            "current_location": self.current_location,
            "notes": self.notes,
            "age_days": self.age_days,
            "age_display": self.get_age_display(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }
        
        if include_sync_info:
            result["sync_info"] = {
                "last_synced_at": self.last_synced_at.isoformat(),
                "sync_version": self.sync_version,
                "silver_last_updated_at": (
                    self.silver_last_updated_at.isoformat()
                    if self.silver_last_updated_at
                    else None
                ),
                "is_stale_5min": self.is_stale(max_age_minutes=5),
                "is_stale_15min": self.is_stale(max_age_minutes=15),
            }
        
        return result
    
    @classmethod
    def get_active_cows(cls, session, tenant_id: UUID):
        """
        Get all active cows for a tenant.
        
        Args:
            session: SQLAlchemy session
            tenant_id: Tenant identifier
            
        Returns:
            Query for active cows
        """
        return (
            session.query(cls)
            .filter(
                cls.tenant_id == tenant_id,
                cls.status == CowStatus.ACTIVE
            )
            .order_by(cls.tag_number.asc())
        )
    
    @classmethod
    def get_by_tag(cls, session, tag_number: str, tenant_id: UUID):
        """
        Get cow by tag number within tenant.
        
        Args:
            session: SQLAlchemy session
            tag_number: Tag number to search for
            tenant_id: Tenant identifier
            
        Returns:
            Cow instance or None
        """
        return (
            session.query(cls)
            .filter(
                cls.tag_number == tag_number,
                cls.tenant_id == tenant_id
            )
            .first()
        )
    
    @classmethod
    def get_by_breed(cls, session, breed: str, tenant_id: UUID):
        """
        Get all cows of a specific breed for a tenant.
        
        Args:
            session: SQLAlchemy session
            breed: Breed to filter by
            tenant_id: Tenant identifier
            
        Returns:
            Query for cows of specified breed
        """
        return (
            session.query(cls)
            .filter(
                cls.breed == breed,
                cls.tenant_id == tenant_id
            )
            .order_by(cls.tag_number.asc())
        )
    
    @classmethod
    def get_offspring(cls, session, parent_id: UUID, tenant_id: UUID):
        """
        Get all offspring of a cow (as dam or sire).
        
        Args:
            session: SQLAlchemy session
            parent_id: ID of parent cow
            tenant_id: Tenant identifier
            
        Returns:
            Query for offspring cows
        """
        return (
            session.query(cls)
            .filter(
                cls.tenant_id == tenant_id,
                (cls.dam_id == parent_id) | (cls.sire_id == parent_id)
            )
            .order_by(cls.birth_date.desc())
        )
    
    @classmethod
    def get_stale_projections(cls, session, max_age_minutes: int = 15):
        """
        Get projections that haven't been synced recently.
        
        Useful for monitoring data freshness and sync health.
        
        Args:
            session: SQLAlchemy session
            max_age_minutes: Maximum acceptable age in minutes
            
        Returns:
            Query for stale projections
        """
        threshold = datetime.utcnow() - timedelta(minutes=max_age_minutes)
        return (
            session.query(cls)
            .filter(cls.last_synced_at < threshold)
            .order_by(cls.last_synced_at.asc())
        )


# Import needed for get_stale_projections
from datetime import timedelta
