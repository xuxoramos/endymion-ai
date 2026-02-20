"""
Cow management router - Event-driven CRUD endpoints.

Following Pure Projection Pattern A:
- Write operations create events (source of truth)
- Read operations query projections (materialized view)
- Eventual consistency between events and projections
"""

import json
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional
from uuid import UUID, uuid4
from math import ceil

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from backend.api.dependencies import (
    get_db,
    get_pagination,
    get_cow_filters,
    get_settings
)
from backend.api.auth import (
    get_current_tenant_id,
    get_current_user_email
)
from backend.models import CowEvent, CowEventType, Cow
from backend.models.requests import (
    CowCreate,
    CowUpdate,
    CowDeactivate,
    CowWeightRecord,
    CowHealthEvent
)
from backend.models.responses import (
    EventAcceptedResponse,
    CowResponse,
    CowListResponse,
    CowEventResponse
)


logger = logging.getLogger(__name__)
router = APIRouter()


# ========================================
# Helper Functions
# ========================================

def cow_to_response(cow: Cow) -> CowResponse:
    """
    Convert Cow model to CowResponse.
    
    Args:
        cow: Cow model instance
    
    Returns:
        CowResponse instance
    """
    return CowResponse(
        id=cow.cow_id,
        tenant_id=cow.tenant_id,
        tag_number=cow.tag_number,
        name=cow.name,
        breed=cow.breed,
        birth_date=cow.birth_date,
        sex=cow.sex,
        status=cow.status,
        weight_kg=cow.weight_kg,
        last_weight_date=cow.last_weight_date,
        dam_id=cow.dam_id,
        sire_id=cow.sire_id,
        current_location=cow.current_location,
        notes=cow.notes,
        age_days=cow.age_days,
        age_display=cow.get_age_display(),
        created_at=cow.created_at,
        updated_at=cow.updated_at,
        last_synced_at=cow.last_synced_at,
        sync_version=cow.sync_version,
        is_stale=cow.is_stale(max_age_minutes=15)
    )


# ========================================
# WRITE OPERATIONS (Create Events)
# ========================================

@router.post(
    "",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create Cow (Event Sourcing)",
    description="""
    Create a new cow by emitting a cow_created event.
    
    **Pattern A: Pure Projection**
    - ✅ Writes to `cow_events` table (source of truth)
    - ❌ Does NOT write to `cows` table
    - ⏱️ Projection updates after 60-120 seconds
    
    **Data Flow:**
    ```
    API → cow_events → Bronze → Silver → cows (projection)
    ```
    
    **Important:**
    - Returns immediately with event acknowledgment
    - Cow will NOT be in GET /cows until sync completes
    - Use returned `cow_id` to poll GET /cows/{cow_id}
    - Check `estimated_sync_time_seconds` for expected lag
    
    **Example:**
    ```bash
    curl -X POST http://localhost:8000/api/v1/cows \\
      -H "Content-Type: application/json" \\
      -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000" \\
      -d '{
        "tag_number": "US-001",
        "breed": "Holstein",
        "birth_date": "2022-01-15",
        "sex": "female"
      }'
    ```
    """
)
async def create_cow(
    cow_data: CowCreate,
    db: Session = Depends(get_db),
    tenant_id: UUID = Depends(get_current_tenant_id),
    user_email: str = Depends(get_current_user_email),
    settings: dict = Depends(get_settings)
):
    """
    Create a cow by emitting a cow_created event.
    
    This does NOT write to the cows table directly.
    The projection is updated asynchronously by Databricks.
    """
    logger.info(f"Creating cow for tenant {tenant_id}: {cow_data.tag_number}")
    
    # Check for duplicate tag number in projection
    # (Note: There's still a race condition, but this catches most duplicates)
    existing = Cow.get_by_tag(db, cow_data.tag_number, tenant_id)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cow with tag_number '{cow_data.tag_number}' already exists"
        )
    
    # Generate cow_id
    cow_id = uuid4()
    
    # Build event payload
    payload = {
        "tag_number": cow_data.tag_number,
        "name": cow_data.name,
        "breed": cow_data.breed,
        "birth_date": cow_data.birth_date.isoformat(),
        "sex": cow_data.sex,
        "dam_id": str(cow_data.dam_id) if cow_data.dam_id else None,
        "sire_id": str(cow_data.sire_id) if cow_data.sire_id else None,
        "weight_kg": cow_data.weight_kg,
        "current_location": cow_data.current_location,
        "notes": cow_data.notes,
        "status": "active"  # Initial status
    }
    
    # Create event (source of truth)
    event = CowEvent.create_event(
        tenant_id=tenant_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_CREATED,
        payload=json.dumps(payload),
        created_by=user_email
    )
    
    db.add(event)
    db.commit()
    db.refresh(event)
    
    logger.info(f"✅ Created cow_created event: {event.event_id}")
    
    # Return event acknowledgment
    return EventAcceptedResponse(
        status="accepted",
        event_id=event.event_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_CREATED.value,
        event_time=event.event_time,
        message=(
            f"Cow creation event accepted. "
            f"Cow '{cow_data.tag_number}' will appear in queries after sync. "
            f"Poll GET /api/v1/cows/{cow_id} to check availability."
        ),
        estimated_sync_time_seconds=settings.get("sync_interval_seconds", 60)
    )


@router.put(
    "/{cow_id}",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Update Cow (Event Sourcing)",
    description="""
    Update a cow by emitting a cow_updated event.
    
    **Pattern A: Pure Projection**
    - ✅ Writes to `cow_events` table (source of truth)
    - ❌ Does NOT update `cows` table directly
    - ⏱️ Changes reflect in projection after 60-120 seconds
    
    **Important:**
    - Only provided fields are included in the update event
    - Changes are NOT immediately visible in GET requests
    - Use event_id for audit trail
    """
)
async def update_cow(
    cow_id: UUID,
    cow_data: CowUpdate,
    db: Session = Depends(get_db),
    tenant_id: UUID = Depends(get_current_tenant_id),
    user_email: str = Depends(get_current_user_email),
    settings: dict = Depends(get_settings)
):
    """
    Update a cow by emitting a cow_updated event.
    
    This does NOT update the cows table directly.
    Changes are applied asynchronously by Databricks.
    """
    logger.info(f"Updating cow {cow_id} for tenant {tenant_id}")
    
    # Verify cow exists by checking event history (not projection)
    # This ensures we can update even if sync hasn't run yet
    cow_events = db.query(CowEvent).filter(
        CowEvent.cow_id == cow_id,
        CowEvent.tenant_id == tenant_id,
        CowEvent.event_type == CowEventType.COW_CREATED.value
    ).first()
    
    if not cow_events:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cow {cow_id} not found or not accessible to tenant"
        )
    
    # Build event payload with only provided fields
    payload = cow_data.model_dump(exclude_unset=True)
    
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields provided for update"
        )
    
    # Create event
    event = CowEvent.create_event(
        tenant_id=tenant_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_UPDATED,
        payload=json.dumps(payload),
        created_by=user_email
    )
    
    db.add(event)
    db.commit()
    db.refresh(event)
    
    logger.info(f"✅ Created cow_updated event: {event.event_id}")
    
    return EventAcceptedResponse(
        status="accepted",
        event_id=event.event_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_UPDATED.value,
        event_time=event.event_time,
        message=(
            f"Cow update event accepted. "
            f"Changes will be reflected in queries after sync."
        ),
        estimated_sync_time_seconds=settings.get("sync_interval_seconds", 60)
    )


@router.delete(
    "/{cow_id}",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Deactivate Cow (Logical Delete)",
    description="""
    Deactivate a cow by emitting a cow_deactivated event.
    
    **Pattern A: Pure Projection**
    - ✅ Creates cow_deactivated event (logical delete)
    - ❌ Does NOT delete from database (audit trail preserved)
    - ⏱️ Status change reflects after sync
    
    **Important:**
    - This is a logical delete (cow remains in database)
    - Cow will still appear in queries with status='inactive'
    - Event history is preserved for audit
    """
)
async def deactivate_cow(
    cow_id: UUID,
    deactivate_data: CowDeactivate,
    db: Session = Depends(get_db),
    tenant_id: UUID = Depends(get_current_tenant_id),
    user_email: str = Depends(get_current_user_email),
    settings: dict = Depends(get_settings)
):
    """
    Deactivate a cow by emitting a cow_deactivated event.
    
    This performs a logical delete (status change to inactive).
    """
    logger.info(f"Deactivating cow {cow_id} for tenant {tenant_id}")
    
    # Verify cow exists by checking event history (not projection)
    cow_events = db.query(CowEvent).filter(
        CowEvent.cow_id == cow_id,
        CowEvent.tenant_id == tenant_id,
        CowEvent.event_type == CowEventType.COW_CREATED.value
    ).first()
    
    if not cow_events:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cow {cow_id} not found or not accessible to tenant"
        )
    
    # Build event payload
    # Map reason to status value
    # Check if reason contains keywords to determine status
    reason_lower = deactivate_data.reason.lower()
    if "sold" in reason_lower or "sale" in reason_lower:
        cow_status = "sold"
    elif "deceased" in reason_lower or "died" in reason_lower or "death" in reason_lower:
        cow_status = "deceased"
    elif "transfer" in reason_lower:
        cow_status = "transferred"
    else:
        # Default to sold if no specific keyword found
        cow_status = "sold"
    
    payload = {
        "reason": deactivate_data.reason,
        "deactivation_date": (
            deactivate_data.deactivation_date or date.today()
        ).isoformat(),
        "notes": deactivate_data.notes,
        "status": cow_status
    }
    
    # Create event
    event = CowEvent.create_event(
        tenant_id=tenant_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_DEACTIVATED,
        payload=json.dumps(payload),
        created_by=user_email
    )
    
    db.add(event)
    db.commit()
    db.refresh(event)
    
    logger.info(f"✅ Created cow_deactivated event: {event.event_id}")
    
    return EventAcceptedResponse(
        status="accepted",
        event_id=event.event_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_DEACTIVATED.value,
        event_time=event.event_time,
        message=(
            f"Cow deactivation event accepted. "
            f"Status change will be reflected in queries after sync."
        ),
        estimated_sync_time_seconds=settings.get("sync_interval_seconds", 60)
    )


@router.post(
    "/{cow_id}/weight",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Record Cow Weight",
    description="Record a weight measurement by emitting a cow_weight_recorded event"
)
async def record_weight(
    cow_id: UUID,
    weight_data: CowWeightRecord,
    db: Session = Depends(get_db),
    tenant_id: UUID = Depends(get_current_tenant_id),
    user_email: str = Depends(get_current_user_email),
    settings: dict = Depends(get_settings)
):
    """Record a cow weight measurement."""
    logger.info(f"Recording weight for cow {cow_id}: {weight_data.weight_kg} kg")
    
    # Verify cow exists by checking event history (not projection)
    cow_events = db.query(CowEvent).filter(
        CowEvent.cow_id == cow_id,
        CowEvent.tenant_id == tenant_id,
        CowEvent.event_type == CowEventType.COW_CREATED.value
    ).first()
    
    if not cow_events:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cow {cow_id} not found"
        )
    
    # Build payload
    payload = {
        "weight_kg": weight_data.weight_kg,
        "measurement_date": (
            weight_data.measurement_date or datetime.utcnow()
        ).isoformat(),
        "measured_by": weight_data.measured_by,
        "measurement_method": weight_data.measurement_method,
        "notes": weight_data.notes
    }
    
    # Create event
    event = CowEvent.create_event(
        tenant_id=tenant_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_WEIGHT_RECORDED,
        payload=json.dumps(payload),
        created_by=user_email
    )
    
    db.add(event)
    db.commit()
    db.refresh(event)
    
    logger.info(f"✅ Created cow_weight_recorded event: {event.event_id}")
    
    return EventAcceptedResponse(
        status="accepted",
        event_id=event.event_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_WEIGHT_RECORDED.value,
        event_time=event.event_time,
        message="Weight recording event accepted",
        estimated_sync_time_seconds=settings.get("sync_interval_seconds", 60)
    )


@router.post(
    "/{cow_id}/health",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Record Health Event",
    description="Record a health event by emitting a cow_health_event"
)
async def record_health_event(
    cow_id: UUID,
    health_data: CowHealthEvent,
    db: Session = Depends(get_db),
    tenant_id: UUID = Depends(get_current_tenant_id),
    user_email: str = Depends(get_current_user_email),
    settings: dict = Depends(get_settings)
):
    """Record a cow health event."""
    logger.info(f"Recording health event for cow {cow_id}: {health_data.event_type}")
    
    # Verify cow exists by checking event history (not projection)
    cow_events = db.query(CowEvent).filter(
        CowEvent.cow_id == cow_id,
        CowEvent.tenant_id == tenant_id,
        CowEvent.event_type == CowEventType.COW_CREATED.value
    ).first()
    
    if not cow_events:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cow {cow_id} not found"
        )
    
    # Build payload
    payload = health_data.model_dump()
    if payload.get("event_date"):
        payload["event_date"] = payload["event_date"].isoformat()
    if payload.get("follow_up_date"):
        payload["follow_up_date"] = payload["follow_up_date"].isoformat()
    
    # Create event
    event = CowEvent.create_event(
        tenant_id=tenant_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_HEALTH_EVENT,
        payload=json.dumps(payload),
        created_by=user_email
    )
    
    db.add(event)
    db.commit()
    db.refresh(event)
    
    logger.info(f"✅ Created cow_health_event: {event.event_id}")
    
    return EventAcceptedResponse(
        status="accepted",
        event_id=event.event_id,
        cow_id=cow_id,
        event_type=CowEventType.COW_HEALTH_EVENT.value,
        event_time=event.event_time,
        message="Health event accepted",
        estimated_sync_time_seconds=settings.get("sync_interval_seconds", 60)
    )


# ========================================
# READ OPERATIONS (Query Projections)
# ========================================

@router.get(
    "",
    response_model=CowListResponse,
    summary="List Cows (Projection Query)",
    description="""
    List cows from the projection table.
    
    **Pattern A: Pure Projection**
    - ✅ Reads from `cows` projection table
    - ⏱️ Data may lag behind recent events (check `is_stale`)
    - 📄 Supports pagination and filtering
    
    **Important:**
    - Newly created cows may not appear immediately
    - Check `is_stale` flag on individual cows
    - Use filters to narrow results
    """
)
async def list_cows(
    db: Session = Depends(get_db),
    tenant_id: UUID = Depends(get_current_tenant_id),
    pagination: tuple[int, int] = Depends(get_pagination),
    filters: dict = Depends(get_cow_filters)
):
    """
    List cows from projection table with pagination and filtering.
    """
    offset, limit = pagination
    page = (offset // limit) + 1
    
    logger.info(f"Listing cows for tenant {tenant_id}, page {page}")
    
    # Build query
    query = db.query(Cow).filter(Cow.tenant_id == tenant_id)
    
    # Apply filters
    if filters.get("status"):
        query = query.filter(Cow.status == filters["status"])
    
    if filters.get("breed"):
        query = query.filter(Cow.breed == filters["breed"])
    
    if filters.get("min_age_days") is not None or filters.get("max_age_days") is not None:
        # Calculate birth date range from age
        today = date.today()
        
        if filters.get("max_age_days") is not None:
            min_birth_date = today - timedelta(days=filters["max_age_days"])
            query = query.filter(Cow.birth_date >= min_birth_date)
        
        if filters.get("min_age_days") is not None:
            max_birth_date = today - timedelta(days=filters["min_age_days"])
            query = query.filter(Cow.birth_date <= max_birth_date)
    
    # Get total count
    total = query.count()
    
    # Apply pagination and ordering
    cows = (
        query
        .order_by(Cow.tag_number.asc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    
    # Convert to response models
    items = [cow_to_response(cow) for cow in cows]
    
    return CowListResponse(
        items=items,
        total=total,
        page=page,
        page_size=limit,
        total_pages=ceil(total / limit) if total > 0 else 0
    )


@router.get(
    "/{cow_id}",
    response_model=CowResponse,
    summary="Get Cow by ID (Projection Query)",
    description="""
    Get a specific cow from the projection table.
    
    **Pattern A: Pure Projection**
    - ✅ Reads from `cows` projection table
    - ⏱️ Data may lag behind recent events
    - 🔍 Check `is_stale` flag for freshness
    
    **Important:**
    - Newly created cows return 404 until sync completes
    - Use polling with exponential backoff after writes
    - Check `last_synced_at` timestamp
    """
)
async def get_cow(
    cow_id: UUID,
    db: Session = Depends(get_db),
    tenant_id: UUID = Depends(get_current_tenant_id)
):
    """
    Get a specific cow from projection table.
    """
    logger.info(f"Getting cow {cow_id} for tenant {tenant_id}")
    
    cow = db.query(Cow).filter(
        Cow.cow_id == cow_id,
        Cow.tenant_id == tenant_id
    ).first()
    
    if not cow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Cow {cow_id} not found. "
                "If recently created, it may not be synced yet. "
                "Wait and retry after estimated sync time."
            )
        )
    
    return cow_to_response(cow)


@router.get(
    "/{cow_id}/events",
    response_model=List[CowEventResponse],
    summary="Get Cow Event History",
    description="""
    Get event history for a specific cow.
    
    This queries the source of truth (cow_events table) for audit trail.
    """
)
async def get_cow_events(
    cow_id: UUID,
    db: Session = Depends(get_db),
    tenant_id: UUID = Depends(get_current_tenant_id),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of events to return")
):
    """
    Get event history for a cow.
    """
    logger.info(f"Getting events for cow {cow_id}")
    
    events = (
        CowEvent.get_cow_history(db, cow_id, tenant_id)
        .limit(limit)
        .all()
    )
    
    return [
        CowEventResponse(
            event_id=e.event_id,
            tenant_id=e.tenant_id,
            cow_id=e.cow_id,
            event_type=e.event_type,
            payload=json.loads(e.payload),
            event_time=e.event_time,
            created_at=e.created_at,
            published_to_bronze=e.published_to_bronze,
            published_at=e.published_at,
            created_by=e.created_by,
            source_system=e.source_system
        )
        for e in events
    ]
