"""
SQLAlchemy models package.

Exports all models for convenient importing.
"""

from .base import (
    Base,
    TenantMixin,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
    ProjectionMixin,
    SoftDeleteMixin,
    TenantBaseModel,
    ProjectionBaseModel,
)

from .events import CowEvent, CowEventType

from .cows import Cow, CowStatus, CowSex

from .categories import Category


__all__ = [
    # Base classes and mixins
    "Base",
    "TenantMixin",
    "TimestampMixin",
    "UUIDPrimaryKeyMixin",
    "ProjectionMixin",
    "SoftDeleteMixin",
    "TenantBaseModel",
    "ProjectionBaseModel",
    
    # Event models
    "CowEvent",
    "CowEventType",
    
    # Projection models
    "Cow",
    "CowStatus",
    "CowSex",
    
    # Reference models
    "Category",
]
