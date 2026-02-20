#!/usr/bin/env python3
"""
Simple event-to-projection sync for testing (without Databricks).

This bypasses the Bronze/Silver layers and syncs directly from
cow_events to the cows projection table. Useful for local testing.
"""

import sys
sys.path.insert(0, '/home/xuxoramos/endymion-ai')

import json
import logging
from datetime import datetime
from typing import Dict, Tuple
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database.connection import DatabaseManager
from backend.models.events import CowEvent, CowEventType
from backend.models.cows import Cow


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def rebuild_cow_from_events(session: Session, cow_id: UUID, tenant_id: UUID) -> Dict:
    """
    Rebuild cow state by replaying all events for a cow.
    
    This is the event sourcing pattern: replay events to get current state.
    """
    # Get all events for this cow, ordered by time
    stmt = select(CowEvent).where(
        CowEvent.cow_id == cow_id,
        CowEvent.tenant_id == tenant_id
    ).order_by(CowEvent.event_time)
    
    events = session.execute(stmt).scalars().all()
    
    if not events:
        return None
    
    # Start with empty state
    cow_state = {
        "cow_id": cow_id,
        "tenant_id": tenant_id,
    }
    
    # Replay each event to build current state
    for event in events:
        payload = json.loads(event.payload)
        
        if event.event_type == CowEventType.COW_CREATED.value:
            # Initial state from creation
            cow_state.update(payload)
            cow_state["status"] = "active"
            
        elif event.event_type == CowEventType.COW_UPDATED.value:
            # Merge updates
            cow_state.update(payload)
            
        elif event.event_type == CowEventType.COW_DEACTIVATED.value:
            # Map deactivated to 'sold' status (closest match)
            cow_state["status"] = "sold"
            if "reason" in payload:
                cow_state["deactivation_reason"] = payload["reason"]
    
    return cow_state


def sync_cow_to_projection(session: Session, cow_id: UUID, tenant_id: UUID) -> Tuple[str, bool]:
    """
    Sync a single cow from events to projection table.
    
    Returns:
        Tuple of (action, success) where action is 'inserted', 'updated', or 'skipped'
    """
    # Rebuild state from events
    cow_state = rebuild_cow_from_events(session, cow_id, tenant_id)
    
    if not cow_state:
        return 'skipped', False
    
    # Check if cow exists in projection
    existing_cow = session.query(Cow).filter(
        Cow.cow_id == cow_id,
        Cow.tenant_id == tenant_id
    ).first()
    
    if existing_cow is None:
        # Insert new cow
        try:
            new_cow = Cow(
                cow_id=cow_id,
                tenant_id=tenant_id,
                tag_number=cow_state["tag_number"],
                name=cow_state.get("name"),
                breed=cow_state["breed"],
                birth_date=datetime.fromisoformat(cow_state["birth_date"]).date() if isinstance(cow_state["birth_date"], str) else cow_state["birth_date"],
                sex=cow_state["sex"],
                dam_id=UUID(cow_state["dam_id"]) if cow_state.get("dam_id") else None,
                sire_id=UUID(cow_state["sire_id"]) if cow_state.get("sire_id") else None,
                status=cow_state.get("status", "active"),
                weight_kg=cow_state.get("weight_kg"),
                last_weight_date=datetime.fromisoformat(cow_state["last_weight_date"]).date() if cow_state.get("last_weight_date") else None,
                current_location=cow_state.get("current_location"),
                notes=cow_state.get("notes"),
                last_synced_at=datetime.utcnow(),
            )
            session.add(new_cow)
            return 'inserted', True
        except Exception as e:
            logger.error(f"Error inserting cow {cow_id}: {e}")
            return 'skipped', False
    else:
        # Update existing cow
        try:
            if cow_state.get("tag_number"):
                existing_cow.tag_number = cow_state["tag_number"]
            if cow_state.get("name"):
                existing_cow.name = cow_state["name"]
            if cow_state.get("breed"):
                existing_cow.breed = cow_state["breed"]
            if cow_state.get("status"):
                existing_cow.status = cow_state["status"]
            if cow_state.get("weight_kg"):
                existing_cow.weight_kg = cow_state["weight_kg"]
            if cow_state.get("current_location"):
                existing_cow.current_location = cow_state["current_location"]
            if cow_state.get("notes"):
                existing_cow.notes = cow_state["notes"]
            
            existing_cow.last_synced_at = datetime.utcnow()
            return 'updated', True
        except Exception as e:
            logger.error(f"Error updating cow {cow_id}: {e}")
            return 'skipped', False


def sync_events_to_projection():
    """
    Main sync function: sync all unpublished events to projection.
    """
    logger.info("=" * 70)
    logger.info("Starting Event-to-Projection Sync (Local Testing)")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    engine = db_manager.init_engine()
    session = Session(engine)
    
    try:
        # Get all unique cow_ids from events that haven't been published
        stmt = select(CowEvent.cow_id, CowEvent.tenant_id).distinct()
        unique_cows = session.execute(stmt).all()
        
        logger.info(f"Found {len(unique_cows)} unique cows to sync")
        
        stats = {
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        for cow_id, tenant_id in unique_cows:
            action, success = sync_cow_to_projection(session, cow_id, tenant_id)
            
            if success:
                stats[action] += 1
                logger.info(f"✅ Cow {cow_id}: {action}")
            else:
                stats['errors'] += 1
                logger.error(f"❌ Cow {cow_id}: failed")
        
        # Commit all changes
        session.commit()
        
        logger.info("=" * 70)
        logger.info("Sync completed!")
        logger.info(f"  Inserted: {stats['inserted']}")
        logger.info(f"  Updated:  {stats['updated']}")
        logger.info(f"  Skipped:  {stats['skipped']}")
        logger.info(f"  Errors:   {stats['errors']}")
        logger.info("=" * 70)
        
        # Mark events as published (optional)
        if stats['inserted'] + stats['updated'] > 0:
            logger.info("Marking events as published...")
            session.query(CowEvent).update(
                {"published_to_bronze": True, "published_at": datetime.utcnow()},
                synchronize_session=False
            )
            session.commit()
            logger.info("✅ Events marked as published")
        
        return stats
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    sync_events_to_projection()
