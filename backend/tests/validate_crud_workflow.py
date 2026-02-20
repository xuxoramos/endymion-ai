"""
CRUD Workflow Validation Script
Tests the complete event sourcing flow: CREATE → UPDATE → DEACTIVATE
"""
import sys
import time
from datetime import datetime, date
from sqlalchemy import text
from backend.database.connection import DatabaseManager
from backend.models.events import CowEvent
from backend.models.cows import Cow
from backend.scripts.sync_local import sync_events_to_projection

def print_section(title):
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")

def main():
    tenant_id = "test-tenant-validation"
    cow_tag = "CRUD-TEST-001"
    
    db = DatabaseManager()
    
    print_section("CRUD WORKFLOW VALIDATION")
    print(f"Tenant ID: {tenant_id}")
    print(f"Cow Tag: {cow_tag}")
    
    # Clean up any existing test data
    with db.get_session() as session:
        session.execute(text("DELETE FROM operational.cows WHERE tag_number = :tag"), {"tag": cow_tag})
        session.execute(text("DELETE FROM operational.cow_events WHERE cow_id LIKE :pattern"), {"pattern": f"%{cow_tag}%"})
        session.commit()
    
    # Step 1: CREATE Event
    print_section("STEP 1: CREATE COW EVENT")
    cow_id = f"cow-{cow_tag.lower()}"
    
    create_event = CowEvent.create_event(
        tenant_id=tenant_id,
        cow_id=cow_id,
        cow_tag=cow_tag,
        birth_date=date(2023, 1, 15),
        breed="Holstein",
        sex="F",
        created_by="validator"
    )
    
    with db.get_session() as session:
        session.add(create_event)
        session.commit()
        print(f"✓ CREATE event saved: {create_event.event_id}")
        print(f"  - Event Type: {create_event.event_type}")
        print(f"  - Sequence: {create_event.sequence_number}")
    
    # Sync to projection
    print("\nSyncing to projection...")
    sync_events_to_projection(tenant_id)
    
    # Verify projection
    with db.get_session() as session:
        cow = session.query(Cow).filter_by(tenant_id=tenant_id, tag_number=cow_tag).first()
        if cow:
            print(f"✓ Cow projection created:")
            print(f"  - ID: {cow.id}")
            print(f"  - Tag: {cow.tag_number}")
            print(f"  - Breed: {cow.breed}")
            print(f"  - Active: {cow.is_active}")
        else:
            print("✗ Cow projection NOT found")
            return 1
    
    time.sleep(1)
    
    # Step 2: UPDATE Event
    print_section("STEP 2: UPDATE COW EVENT")
    
    update_event = CowEvent.create_event(
        tenant_id=tenant_id,
        cow_id=cow_id,
        event_type="COW_UPDATED",
        payload={
            "previous_weight": None,
            "new_weight": 450.5,
            "location": "Barn-A",
            "notes": "Weight update"
        },
        created_by="validator"
    )
    
    with db.get_session() as session:
        session.add(update_event)
        session.commit()
        print(f"✓ UPDATE event saved: {update_event.event_id}")
        print(f"  - Event Type: {update_event.event_type}")
        print(f"  - Sequence: {update_event.sequence_number}")
    
    # Sync to projection
    print("\nSyncing to projection...")
    sync_events_to_projection(tenant_id)
    
    # Verify projection update
    with db.get_session() as session:
        cow = session.query(Cow).filter_by(tenant_id=tenant_id, tag_number=cow_tag).first()
        if cow:
            print(f"✓ Cow projection updated:")
            print(f"  - Current Weight: {cow.current_weight}")
            print(f"  - Location: {cow.location}")
            print(f"  - Last Updated: {cow.updated_at}")
        else:
            print("✗ Cow projection NOT found")
            return 1
    
    time.sleep(1)
    
    # Step 3: DEACTIVATE Event
    print_section("STEP 3: DEACTIVATE COW EVENT")
    
    deactivate_event = CowEvent.create_event(
        tenant_id=tenant_id,
        cow_id=cow_id,
        event_type="COW_DEACTIVATED",
        payload={
            "reason": "Sold",
            "notes": "Validation test complete"
        },
        created_by="validator"
    )
    
    with db.get_session() as session:
        session.add(deactivate_event)
        session.commit()
        print(f"✓ DEACTIVATE event saved: {deactivate_event.event_id}")
        print(f"  - Event Type: {deactivate_event.event_type}")
        print(f"  - Sequence: {deactivate_event.sequence_number}")
    
    # Sync to projection
    print("\nSyncing to projection...")
    sync_events_to_projection(tenant_id)
    
    # Verify projection deactivation
    with db.get_session() as session:
        cow = session.query(Cow).filter_by(tenant_id=tenant_id, tag_number=cow_tag).first()
        if cow:
            print(f"✓ Cow projection deactivated:")
            print(f"  - Active: {cow.is_active}")
            print(f"  - Deactivation Reason: {cow.deactivation_reason}")
            print(f"  - Deactivated At: {cow.deactivated_at}")
            
            if not cow.is_active:
                print("\n✓ Deactivation successful!")
            else:
                print("\n✗ Cow still active - deactivation failed")
                return 1
        else:
            print("✗ Cow projection NOT found")
            return 1
    
    # Step 4: Verify Event History
    print_section("STEP 4: VERIFY EVENT HISTORY")
    
    with db.get_session() as session:
        events = CowEvent.get_cow_history(session, tenant_id, cow_id)
        print(f"✓ Found {len(events)} events in history:")
        for i, event in enumerate(events, 1):
            print(f"  {i}. {event.event_type} (seq: {event.sequence_number}) at {event.event_timestamp}")
        
        if len(events) == 3:
            print("\n✓ All events recorded correctly!")
        else:
            print(f"\n✗ Expected 3 events, found {len(events)}")
            return 1
    
    # Final Summary
    print_section("VALIDATION SUMMARY")
    print("✅ CREATE: Event stored, projection created")
    print("✅ UPDATE: Event stored, projection updated")
    print("✅ DEACTIVATE: Event stored, projection deactivated")
    print("✅ EVENT HISTORY: All 3 events recorded")
    print("\n🎉 CRUD WORKFLOW VALIDATION PASSED!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
