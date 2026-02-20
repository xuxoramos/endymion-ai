"""
CowEvent model - Event sourcing / outbox pattern.

This is the SOURCE OF TRUTH for all cow-related write operations.
Events in this table are published to the Bronze layer and flow through
the data pipeline.

IMPORTANT: This table is APPEND-ONLY. No updates or deletes allowed!
"""

from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean, Column, DateTime, String, Text, CheckConstraint, Index
)
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TenantMixin


class CowEventType(str, Enum):
    """
    Enumeration of valid cow event types.
    
    These represent all possible lifecycle events for a cow.
    """
    COW_CREATED = "cow_created"
    COW_UPDATED = "cow_updated"
    COW_DEACTIVATED = "cow_deactivated"
    COW_WEIGHT_RECORDED = "cow_weight_recorded"
    COW_HEALTH_EVENT = "cow_health_event"
    
    @classmethod
    def values(cls) -> list[str]:
        """Get list of all valid event type values."""
        return [e.value for e in cls]


class CowEvent(Base, TenantMixin):
    """
    Event sourcing table for cow lifecycle events.
    
    This is the SOURCE OF TRUTH for all cow-related operations.
    All writes to the system should create events in this table.
    
    Architecture Pattern: Event Sourcing / Outbox Pattern
    - Append-only: Events are never updated or deleted
    - Immutable: Once created, event data cannot change
    - Ordered: Events maintain temporal ordering via event_time
    - Published: Events are published to Bronze layer via CDC/batch
    
    Data Flow:
        API → cow_events → Bronze → Silver → Gold
    
    WARNING: Never update or delete records from this table!
    """
    
    __tablename__ = "cow_events"
    __table_args__ = (
        CheckConstraint(
            f"event_type IN {tuple(CowEventType.values())}",
            name="CK_cow_events_event_type"
        ),
        Index("IX_cow_events_tenant_id", "tenant_id", "event_time"),
        Index("IX_cow_events_cow_id", "cow_id", "event_time"),
        Index(
            "IX_cow_events_published",
            "published_to_bronze",
            "event_time",
            mssql_where="published_to_bronze = 0"
        ),
        Index("IX_cow_events_event_type", "event_type", "tenant_id", "event_time"),
        {"schema": "operational"}
    )
    
    # Primary Key
    event_id: Mapped[UUID] = mapped_column(
        UNIQUEIDENTIFIER,
        primary_key=True,
        default=uuid4,
        doc="Unique event identifier"
    )
    
    # Entity Reference
    cow_id: Mapped[UUID] = mapped_column(
        UNIQUEIDENTIFIER,
        nullable=False,
        doc="Reference to cow entity"
    )
    
    # Event Information
    event_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="Type of event (cow_created, cow_updated, etc.)"
    )
    
    payload: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc="JSON payload containing event data"
    )
    
    # Timestamps
    event_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        doc="When the event occurred (business time)"
    )
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        doc="When record was created (system time)"
    )
    
    # Publishing Status
    published_to_bronze: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        doc="Whether event has been published to Bronze layer"
    )
    
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        doc="When event was published to Bronze"
    )
    
    # Metadata
    created_by: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="User/system that created the event"
    )
    
    source_system: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        default="api",
        doc="Source system that generated the event"
    )
    
    correlation_id: Mapped[Optional[UUID]] = mapped_column(
        UNIQUEIDENTIFIER,
        nullable=True,
        doc="Correlation ID for tracing related events"
    )
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"<CowEvent("
            f"event_id={self.event_id}, "
            f"cow_id={self.cow_id}, "
            f"type={self.event_type}, "
            f"time={self.event_time}, "
            f"published={self.published_to_bronze}"
            f")>"
        )
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        status = "published" if self.published_to_bronze else "pending"
        return f"{self.event_type} for cow {self.cow_id} at {self.event_time} [{status}]"
    
    # Append-only enforcement
    def __setattr__(self, name: str, value: Any) -> None:
        """
        Override setattr to prevent modifications after creation.
        
        Once an event is created and persisted, it should never be modified.
        This enforces the immutability constraint.
        
        Raises:
            AttributeError: If trying to modify an existing persisted event
        """
        from sqlalchemy import inspect
        
        # Allow setting attributes during object creation
        # Check if instance is fully initialized by checking for _sa_instance_state
        if not hasattr(self, '_sa_instance_state'):
            # Object is being initialized, allow all sets
            super().__setattr__(name, value)
            return
            
        # Check if this event has been persisted (committed to database)
        state = inspect(self)
        if state.persistent or state.detached:
            # This is a persisted event - check if trying to modify existing data
            # Use getattr to properly handle SQLAlchemy lazy loading
            try:
                existing_value = getattr(self, name, None)
            except:
                existing_value = None
                
            if existing_value is not None and existing_value != value and name != '_sa_instance_state':
                # Allow updating publishing status (this is done by sync job)
                if name in ('published_to_bronze', 'published_at'):
                    super().__setattr__(name, value)
                else:
                    raise AttributeError(
                        f"CowEvent is immutable. Cannot modify '{name}' after creation. "
                        f"Events are append-only!"
                    )
        
        super().__setattr__(name, value)
    
    def mark_as_published(self) -> None:
        """
        Mark event as published to Bronze layer.
        
        This is the ONLY allowed modification to an event after creation.
        Should only be called by the sync/CDC job.
        """
        # Use normal assignment - __setattr__ has special handling for these fields
        self.published_to_bronze = True
        self.published_at = datetime.utcnow()
    
    @property
    def is_published(self) -> bool:
        """Check if event has been published to Bronze layer."""
        return self.published_to_bronze
    
    @property
    def is_pending(self) -> bool:
        """Check if event is waiting to be published."""
        return not self.published_to_bronze
    
    @property
    def age_seconds(self) -> float:
        """Get age of event in seconds since creation."""
        return (datetime.utcnow() - self.created_at).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert event to dictionary for serialization.
        
        Returns:
            Dictionary representation of the event
        """
        return {
            "event_id": str(self.event_id),
            "tenant_id": str(self.tenant_id),
            "cow_id": str(self.cow_id),
            "event_type": self.event_type,
            "payload": self.payload,
            "event_time": self.event_time.isoformat(),
            "created_at": self.created_at.isoformat(),
            "published_to_bronze": self.published_to_bronze,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "created_by": self.created_by,
            "source_system": self.source_system,
            "correlation_id": str(self.correlation_id) if self.correlation_id else None,
        }
    
    @classmethod
    def create_event(
        cls,
        tenant_id: UUID,
        cow_id: UUID,
        event_type: CowEventType,
        payload: str,
        event_time: Optional[datetime] = None,
        created_by: Optional[str] = None,
        correlation_id: Optional[UUID] = None,
    ) -> "CowEvent":
        """
        Factory method to create a new cow event.
        
        Args:
            tenant_id: Tenant identifier
            cow_id: Cow identifier
            event_type: Type of event
            payload: JSON string containing event data
            event_time: When event occurred (defaults to now)
            created_by: User/system creating the event
            correlation_id: Correlation ID for tracing
            
        Returns:
            New CowEvent instance (not yet persisted)
            
        Example:
            event = CowEvent.create_event(
                tenant_id=tenant_id,
                cow_id=cow_id,
                event_type=CowEventType.COW_CREATED,
                payload='{"breed": "Holstein", "tag_number": "US-123"}',
                created_by="user@example.com"
            )
            session.add(event)
            session.commit()
        """
        # Normalize UUIDs to lowercase strings per convention
        normalized_tenant_id = UUID(str(tenant_id).lower())
        normalized_cow_id = UUID(str(cow_id).lower())
        normalized_correlation_id = UUID(str(correlation_id).lower()) if correlation_id else None
        
        return cls(
            tenant_id=normalized_tenant_id,
            cow_id=normalized_cow_id,
            event_type=event_type.value if isinstance(event_type, CowEventType) else event_type,
            payload=payload,
            event_time=event_time or datetime.utcnow(),
            created_by=created_by,
            correlation_id=normalized_correlation_id,
        )
    
    @classmethod
    def get_unpublished_events(cls, session, limit: int = 100):
        """
        Get unpublished events that need to be synced to Bronze.
        
        Args:
            session: SQLAlchemy session
            limit: Maximum number of events to return
            
        Returns:
            Query for unpublished events ordered by event_time
        """
        return (
            session.query(cls)
            .filter(cls.published_to_bronze == False)
            .order_by(cls.event_time.asc())
            .limit(limit)
        )
    
    @classmethod
    def get_cow_history(cls, session, cow_id: UUID, tenant_id: UUID):
        """
        Get all events for a specific cow.
        
        Args:
            session: SQLAlchemy session
            cow_id: Cow identifier
            tenant_id: Tenant identifier
            
        Returns:
            Query for cow's event history ordered by event_time
        """
        return (
            session.query(cls)
            .filter(
                cls.cow_id == cow_id,
                cls.tenant_id == tenant_id
            )
            .order_by(cls.event_time.desc())
        )
