"""
Events API router - Read-only access to cow events.

Provides endpoints to query the operational.cow_events table
for event history display in the UI.
"""

import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.api.dependencies import (
    get_db,
    get_tenant_id
)

# ========================================
# Setup
# ========================================

router = APIRouter(prefix="/events", tags=["events"])
logger = logging.getLogger(__name__)


# ========================================
# Response Models
# ========================================

class CowEvent(BaseModel):
    """Cow event from operational.cow_events table."""
    
    event_id: UUID = Field(description="Unique event identifier")
    cow_id: UUID = Field(description="Cow this event belongs to")
    tenant_id: UUID = Field(description="Tenant ID")
    event_type: str = Field(description="Type of event")
    payload: dict = Field(description="Event payload (JSON)")
    created_at: datetime = Field(description="When event was created")
    created_by: Optional[str] = Field(None, description="Who created the event")
    
    class Config:
        from_attributes = True


class EventsListResponse(BaseModel):
    """Response for list of events."""
    
    events: List[CowEvent] = Field(description="List of events")
    total: int = Field(description="Total number of events")
    cow_id: Optional[UUID] = Field(None, description="Filter by cow_id if provided")


# ========================================
# Endpoints
# ========================================

@router.get("/cow/{cow_id}", response_model=List[CowEvent])
async def get_cow_events(
    cow_id: UUID,
    tenant_id: UUID = Depends(get_tenant_id),
    limit: int = Query(50, ge=1, le=500, description="Max events to return"),
    db: Session = Depends(get_db)
):
    """
    Get all events for a specific cow.
    
    Returns events in reverse chronological order (newest first).
    """
    logger.info(f"Fetching events for cow {cow_id}, tenant {tenant_id}")
    
    try:
        # Query cow_events table
        query = text("""
            SELECT TOP (:limit)
                event_id,
                cow_id,
                tenant_id,
                event_type,
                payload,
                created_at,
                created_by
            FROM operational.cow_events
            WHERE cow_id = :cow_id 
                AND tenant_id = :tenant_id
            ORDER BY created_at DESC
        """)
        
        result = db.execute(
            query,
            {
                "cow_id": str(cow_id).lower(),  # Normalized to lowercase for consistency with Bronze
                "tenant_id": str(tenant_id).lower(),  # Normalized to lowercase
                "limit": limit
            }
        )
        
        events = []
        for row in result:
            # Parse JSON payload
            import json
            payload_data = json.loads(row.payload) if isinstance(row.payload, str) else row.payload
            
            events.append(CowEvent(
                event_id=UUID(row.event_id),
                cow_id=UUID(row.cow_id),
                tenant_id=UUID(row.tenant_id),
                event_type=row.event_type,
                payload=payload_data,
                created_at=row.created_at,
                created_by=row.created_by
            ))
        
        logger.info(f"Found {len(events)} events for cow {cow_id}")
        return events
        
    except Exception as e:
        logger.error(f"Error fetching events for cow {cow_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch events: {str(e)}")


@router.get("/recent", response_model=List[CowEvent])
async def get_recent_events(
    tenant_id: UUID = Depends(get_tenant_id),
    limit: int = Query(10, ge=1, le=100, description="Max events to return"),
    db: Session = Depends(get_db)
):
    """
    Get recent events across all cows for this tenant.
    
    Returns events in reverse chronological order (newest first).
    """
    logger.info(f"Fetching {limit} recent events for tenant {tenant_id}")
    
    try:
        query = text("""
            SELECT TOP (:limit)
                event_id,
                cow_id,
                tenant_id,
                event_type,
                payload,
                created_at,
                created_by
            FROM operational.cow_events
            WHERE tenant_id = :tenant_id
            ORDER BY created_at DESC
        """)
        
        result = db.execute(
            query,
            {
                "tenant_id": str(tenant_id),
                "limit": limit
            }
        )
        
        events = []
        for row in result:
            import json
            payload_data = json.loads(row.payload) if isinstance(row.payload, str) else row.payload
            
            events.append(CowEvent(
                event_id=UUID(row.event_id),
                cow_id=UUID(row.cow_id),
                tenant_id=UUID(row.tenant_id),
                event_type=row.event_type,
                payload=payload_data,
                created_at=row.created_at,
                created_by=row.created_by
            ))
        
        logger.info(f"Found {len(events)} recent events")
        return events
        
    except Exception as e:
        logger.error(f"Error fetching recent events: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch events: {str(e)}")
