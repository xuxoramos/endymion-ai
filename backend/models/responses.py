"""
Pydantic response models for API endpoints.

These models structure API responses, following the event-driven pattern
where write operations return event acknowledgments.
"""

from datetime import datetime, date
from typing import Optional, List, Dict, Any
from uuid import UUID

from pydantic import BaseModel, Field


class EventAcceptedResponse(BaseModel):
    """
    Response for write operations that create events.
    
    Following Pure Projection Pattern A:
    - Write operations create events in cow_events
    - They return immediately with event acknowledgment
    - The projection table is updated asynchronously by Databricks
    
    Clients should NOT expect the entity to be immediately available
    in the projection. Use GET with retry logic if needed.
    """
    
    status: str = Field(
        "accepted",
        description="Status of the request",
        examples=["accepted"]
    )
    
    event_id: UUID = Field(
        ...,
        description="Unique identifier of the created event"
    )
    
    cow_id: UUID = Field(
        ...,
        description="UUID of the cow entity"
    )
    
    event_type: str = Field(
        ...,
        description="Type of event created",
        examples=["cow_created", "cow_updated", "cow_deactivated"]
    )
    
    event_time: datetime = Field(
        ...,
        description="Server timestamp when event was created"
    )
    
    message: str = Field(
        ...,
        description="Human-readable message",
        examples=[
            "Cow creation event accepted. Changes will be reflected in queries after sync.",
            "Cow update event accepted. Changes will be reflected in queries after sync."
        ]
    )
    
    estimated_sync_time_seconds: int = Field(
        60,
        description="Estimated time until changes appear in queries (seconds)",
        examples=[30, 60, 120]
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "accepted",
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "cow_id": "123e4567-e89b-12d3-a456-426614174000",
                "event_type": "cow_created",
                "event_time": "2024-06-15T10:30:00Z",
                "message": "Cow creation event accepted. Changes will be reflected in queries after sync.",
                "estimated_sync_time_seconds": 60
            }
        }


class CowResponse(BaseModel):
    """
    Response model for cow queries.
    
    This represents the current state from the cows projection table.
    Data may lag behind recent events by the sync interval.
    """
    
    id: UUID = Field(..., description="Unique cow identifier")
    tenant_id: UUID = Field(..., description="Tenant identifier")
    
    # Identity
    tag_number: str = Field(..., description="Unique tag/identifier")
    name: Optional[str] = Field(None, description="Cow name")
    
    # Biology
    breed: str = Field(..., description="Breed")
    birth_date: date = Field(..., description="Birth date")
    sex: str = Field(..., description="Sex (male/female)")
    
    # Status
    status: str = Field(..., description="Current status")
    
    # Physical attributes
    weight_kg: Optional[float] = Field(None, description="Most recent weight (kg)")
    last_weight_date: Optional[datetime] = Field(None, description="Date of last weight")
    
    # Genealogy
    dam_id: Optional[UUID] = Field(None, description="Mother cow ID")
    sire_id: Optional[UUID] = Field(None, description="Father cow ID")
    
    # Location
    current_location: Optional[str] = Field(None, description="Current location")
    
    # Metadata
    notes: Optional[str] = Field(None, description="Notes")
    
    # Computed fields
    age_days: int = Field(..., description="Age in days")
    age_display: str = Field(..., description="Human-readable age")
    
    # Timestamps
    created_at: datetime = Field(..., description="When record was created")
    updated_at: datetime = Field(..., description="When record was last updated")
    
    # Sync metadata
    last_synced_at: datetime = Field(..., description="When projection was last synced")
    sync_version: int = Field(..., description="Sync version number")
    is_stale: bool = Field(..., description="Whether data may be outdated (>15 min)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "tenant_id": "789e4567-e89b-12d3-a456-426614174000",
                "tag_number": "US-001",
                "name": "Bessie",
                "breed": "Holstein",
                "birth_date": "2022-01-15",
                "sex": "female",
                "status": "active",
                "weight_kg": 475.5,
                "last_weight_date": "2024-06-10T10:00:00Z",
                "dam_id": None,
                "sire_id": None,
                "current_location": "Barn A",
                "notes": "Excellent milk producer",
                "age_days": 880,
                "age_display": "2y 5m",
                "created_at": "2022-01-15T08:00:00Z",
                "updated_at": "2024-06-10T10:00:00Z",
                "last_synced_at": "2024-06-15T10:25:00Z",
                "sync_version": 42,
                "is_stale": False
            }
        }


class CowListResponse(BaseModel):
    """
    Response model for listing cows with pagination.
    """
    
    items: List[CowResponse] = Field(..., description="List of cows")
    total: int = Field(..., description="Total number of cows matching filter")
    page: int = Field(..., description="Current page number (1-indexed)")
    page_size: int = Field(..., description="Number of items per page")
    total_pages: int = Field(..., description="Total number of pages")
    
    class Config:
        json_schema_extra = {
            "example": {
                "items": [
                    {
                        "id": "123e4567-e89b-12d3-a456-426614174000",
                        "tenant_id": "789e4567-e89b-12d3-a456-426614174000",
                        "tag_number": "US-001",
                        "breed": "Holstein",
                        "status": "active",
                        "age_display": "2y 5m"
                    }
                ],
                "total": 42,
                "page": 1,
                "page_size": 20,
                "total_pages": 3
            }
        }


class CowEventResponse(BaseModel):
    """
    Response model for cow event queries.
    
    Used to inspect the event history for audit or debugging.
    """
    
    event_id: UUID = Field(..., description="Event identifier")
    tenant_id: UUID = Field(..., description="Tenant identifier")
    cow_id: UUID = Field(..., description="Cow identifier")
    
    event_type: str = Field(..., description="Event type")
    payload: Dict[str, Any] = Field(..., description="Event payload (parsed JSON)")
    
    event_time: datetime = Field(..., description="When event occurred")
    created_at: datetime = Field(..., description="When event was recorded")
    
    published_to_bronze: bool = Field(..., description="Whether published to Bronze")
    published_at: Optional[datetime] = Field(None, description="When published")
    
    created_by: Optional[str] = Field(None, description="User who created event")
    source_system: str = Field(..., description="Source system")
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "tenant_id": "789e4567-e89b-12d3-a456-426614174000",
                "cow_id": "123e4567-e89b-12d3-a456-426614174000",
                "event_type": "cow_created",
                "payload": {
                    "tag_number": "US-001",
                    "breed": "Holstein",
                    "birth_date": "2022-01-15"
                },
                "event_time": "2024-06-15T10:30:00Z",
                "created_at": "2024-06-15T10:30:00.123Z",
                "published_to_bronze": True,
                "published_at": "2024-06-15T10:31:00Z",
                "created_by": "user@example.com",
                "source_system": "api"
            }
        }


class HealthResponse(BaseModel):
    """
    Health check response.
    """
    
    status: str = Field(..., description="Service status")
    timestamp: datetime = Field(..., description="Current server time")
    database: str = Field(..., description="Database connection status")
    version: str = Field(..., description="API version")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2024-06-15T10:30:00Z",
                "database": "connected",
                "version": "1.0.0"
            }
        }


class ErrorResponse(BaseModel):
    """
    Standard error response.
    """
    
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    timestamp: datetime = Field(..., description="When error occurred")
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "ValidationError",
                "message": "Birth date cannot be in the future",
                "details": {
                    "field": "birth_date",
                    "value": "2025-12-31"
                },
                "timestamp": "2024-06-15T10:30:00Z"
            }
        }
