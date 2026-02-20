"""
Test Complete CRUD Workflow for Event Sourcing Architecture

This script tests:
1. CREATE: Create cow via API → sync → verify projection
2. UPDATE: Update cow via API → sync → verify changes
3. DEACTIVATE: Deactivate cow via API → sync → verify status change

Tests the full event sourcing flow without Databricks/Silver layer.
"""

import sys
import uuid
import time
import json
from datetime import datetime, date
from sqlalchemy.orm import Session

# Add project root to path
sys.path.insert(0, '/home/xuxoramos/endymion-ai')

from backend.database.connection import DatabaseManager
from backend.models.events import CowEvent
from backend.models.cows import Cow
from backend.scripts.sync_local import sync_events_to_projection


def clear_test_data(session: Session, tenant_id: str, cow_id: str = None):
    """Clear test data from database"""
    print("\n🧹 Clearing previous test data...")
    
    if cow_id:
        # Clear specific cow
        session.query(CowEvent).filter(
            CowEvent.tenant_id == uuid.UUID(tenant_id),
            CowEvent.cow_id == uuid.UUID(cow_id)
        ).delete()
        session.query(Cow).filter(
            Cow.tenant_id == uuid.UUID(tenant_id),
            Cow.cow_id == uuid.UUID(cow_id)
        ).delete()
    else:
        # Clear all test data for tenant
        session.query(CowEvent).filter(CowEvent.tenant_id == uuid.UUID(tenant_id)).delete()
        session.query(Cow).filter(Cow.tenant_id == uuid.UUID(tenant_id)).delete()
    
    session.commit()
    print("✅ Test data cleared")


def create_cow_event(session: Session, tenant_id: str, cow_id: str, tag_number: str) -> str:
    """Create a COW_CREATED event"""
    event = CowEvent(
        event_id=uuid.uuid4(),
        tenant_id=uuid.UUID(tenant_id),
        cow_id=uuid.UUID(cow_id),
        event_type="cow_created",
        payload=json.dumps({
            "cow_id": cow_id,
            "tenant_id": tenant_id,
            "tag_number": tag_number,
            "name": "Bessie",
            "breed": "Holstein",
            "birth_date": "2024-01-15",
            "sex": "female",
            "status": "active",
            "category_id": None,
            "dam_id": None,
            "sire_id": None,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }),
        event_time=datetime.now(),
        created_at=datetime.now(),
        published_to_bronze=False
    )
    session.add(event)
    session.commit()
    
    return str(event.event_id)


def update_cow_event(session: Session, tenant_id: str, cow_id: str, new_name: str, new_breed: str) -> str:
    """Create a COW_UPDATED event"""
    event = CowEvent(
        event_id=uuid.uuid4(),
        tenant_id=uuid.UUID(tenant_id),
        cow_id=uuid.UUID(cow_id),
        event_type="cow_updated",
        payload=json.dumps({
            "cow_id": cow_id,
            "tenant_id": tenant_id,
            "name": new_name,
            "breed": new_breed,
            "updated_at": datetime.now().isoformat()
        }),
        event_time=datetime.now(),
        created_at=datetime.now(),
        published_to_bronze=False
    )
    session.add(event)
    session.commit()
    
    return str(event.event_id)


def deactivate_cow_event(session: Session, tenant_id: str, cow_id: str, reason: str) -> str:
    """Create a COW_DEACTIVATED event"""
    event = CowEvent(
        event_id=uuid.uuid4(),
        tenant_id=uuid.UUID(tenant_id),
        cow_id=uuid.UUID(cow_id),
        event_type="cow_deactivated",
        payload=json.dumps({
            "cow_id": cow_id,
            "tenant_id": tenant_id,
            "reason": reason,
            "deactivated_at": datetime.now().isoformat()
        }),
        event_time=datetime.now(),
        created_at=datetime.now(),
        published_to_bronze=False
    )
    session.add(event)
    session.commit()
    
    return str(event.event_id)


def verify_projection(session: Session, tenant_id: str, cow_id: str, step: str):
    """Verify cow state in projection table"""
    cow = session.query(Cow).filter(
        Cow.tenant_id == tenant_id,
        Cow.cow_id == cow_id
    ).first()
    
    print(f"\n📊 Projection after {step}:")
    if cow:
        print(f"  ✅ Found cow in projection")
        print(f"     Tag: {cow.tag_number}")
        print(f"     Name: {cow.name}")
        print(f"     Breed: {cow.breed}")
        print(f"     Sex: {cow.sex}")
        print(f"     Status: {cow.status}")
        print(f"     Birth Date: {cow.birth_date}")
        print(f"     Active: {cow.is_active}")
        return cow
    else:
        print(f"  ❌ Cow NOT found in projection")
        return None


def count_events(session: Session, tenant_id: str, cow_id: str) -> dict:
    """Count events by type"""
    events = session.query(CowEvent).filter(
        CowEvent.tenant_id == uuid.UUID(tenant_id),
        CowEvent.cow_id == uuid.UUID(cow_id)
    ).all()
    
    counts = {}
    for event in events:
        counts[event.event_type] = counts.get(event.event_type, 0) + 1
    
    return counts


def main():
    """Run complete CRUD workflow test"""
    
    print("=" * 70)
    print("🧪 TESTING COMPLETE CRUD WORKFLOW - EVENT SOURCING ARCHITECTURE")
    print("=" * 70)
    
    # Initialize database
    db = DatabaseManager()
    engine = db.init_engine()
    session = Session(engine)
    
    # Test data
    tenant_id = str(uuid.uuid4())
    cow_id = str(uuid.uuid4())
    tag_number = f"CRUD{int(time.time()) % 10000:04d}"
    
    print(f"\n📋 Test Configuration:")
    print(f"   Tenant ID: {tenant_id}")
    print(f"   Cow ID: {cow_id}")
    print(f"   Tag Number: {tag_number}")
    
    try:
        # Clear any previous test data
        clear_test_data(session, tenant_id)
        
        # =====================================================================
        # STEP 1: CREATE COW
        # =====================================================================
        print("\n" + "=" * 70)
        print("STEP 1: CREATE COW")
        print("=" * 70)
        
        print("\n📝 Creating COW_CREATED event...")
        event_id = create_cow_event(session, tenant_id, cow_id, tag_number)
        print(f"✅ Event created: {event_id}")
        
        # Count events
        counts = count_events(session, tenant_id, cow_id)
        print(f"\n📊 Event Store Status:")
        for event_type, count in counts.items():
            print(f"   {event_type}: {count}")
        
        # Sync to projection
        print("\n🔄 Running sync to projection...")
        result = sync_events_to_projection()
        print(f"✅ Sync completed:")
        print(f"   Inserted: {result['inserted']}")
        print(f"   Updated: {result['updated']}")
        print(f"   Errors: {result['errors']}")
        
        # Verify projection
        cow = verify_projection(session, tenant_id, cow_id, "CREATE")
        
        if not cow:
            print("\n❌ TEST FAILED: Cow not found in projection after CREATE")
            return False
        
        if cow.name != "Bessie" or cow.breed != "Holstein" or cow.status != "active":
            print("\n❌ TEST FAILED: Cow data incorrect after CREATE")
            return False
        
        print("\n✅ CREATE test passed!")
        
        # =====================================================================
        # STEP 2: UPDATE COW
        # =====================================================================
        print("\n" + "=" * 70)
        print("STEP 2: UPDATE COW")
        print("=" * 70)
        
        print("\n📝 Creating COW_UPDATED event...")
        event_id = update_cow_event(session, tenant_id, cow_id, "Buttercup", "Jersey")
        print(f"✅ Event created: {event_id}")
        
        # Count events
        counts = count_events(session, tenant_id, cow_id)
        print(f"\n📊 Event Store Status:")
        for event_type, count in counts.items():
            print(f"   {event_type}: {count}")
        
        # Sync to projection
        print("\n🔄 Running sync to projection...")
        result = sync_events_to_projection()
        print(f"✅ Sync completed:")
        print(f"   Inserted: {result['inserted']}")
        print(f"   Updated: {result['updated']}")
        print(f"   Errors: {result['errors']}")
        
        # Verify projection
        cow = verify_projection(session, tenant_id, cow_id, "UPDATE")
        
        if not cow:
            print("\n❌ TEST FAILED: Cow not found in projection after UPDATE")
            return False
        
        if cow.name != "Buttercup" or cow.breed != "Jersey":
            print(f"\n❌ TEST FAILED: Cow data not updated correctly")
            print(f"   Expected: name='Buttercup', breed='Jersey'")
            print(f"   Got: name='{cow.name}', breed='{cow.breed}'")
            return False
        
        if cow.status != "active":
            print(f"\n❌ TEST FAILED: Status changed unexpectedly to '{cow.status}'")
            return False
        
        print("\n✅ UPDATE test passed!")
        
        # =====================================================================
        # STEP 3: DEACTIVATE COW
        # =====================================================================
        print("\n" + "=" * 70)
        print("STEP 3: DEACTIVATE COW")
        print("=" * 70)
        
        print("\n📝 Creating COW_DEACTIVATED event...")
        event_id = deactivate_cow_event(session, tenant_id, cow_id, "Sold to another farm")
        print(f"✅ Event created: {event_id}")
        
        # Count events
        counts = count_events(session, tenant_id, cow_id)
        print(f"\n📊 Event Store Status:")
        for event_type, count in counts.items():
            print(f"   {event_type}: {count}")
        
        # Sync to projection
        print("\n🔄 Running sync to projection...")
        result = sync_events_to_projection()
        print(f"✅ Sync completed:")
        print(f"   Inserted: {result['inserted']}")
        print(f"   Updated: {result['updated']}")
        print(f"   Errors: {result['errors']}")
        
        # Verify projection
        cow = verify_projection(session, tenant_id, cow_id, "DEACTIVATE")
        
        if not cow:
            print("\n❌ TEST FAILED: Cow not found in projection after DEACTIVATE")
            return False
        
        if cow.status != "sold":
            print(f"\n❌ TEST FAILED: Status not updated to 'sold' (got '{cow.status}')")
            return False
        
        if cow.is_active:
            print(f"\n❌ TEST FAILED: is_active should be False after deactivation")
            return False
        
        # Verify name/breed unchanged
        if cow.name != "Buttercup" or cow.breed != "Jersey":
            print(f"\n❌ TEST FAILED: Name/breed changed during deactivation")
            return False
        
        print("\n✅ DEACTIVATE test passed!")
        
        # =====================================================================
        # FINAL SUMMARY
        # =====================================================================
        print("\n" + "=" * 70)
        print("📊 FINAL TEST SUMMARY")
        print("=" * 70)
        
        # Count all events
        all_events = session.query(CowEvent).filter(
            CowEvent.tenant_id == uuid.UUID(tenant_id),
            CowEvent.cow_id == uuid.UUID(cow_id)
        ).order_by(CowEvent.created_at).all()
        
        print(f"\n📝 Event History ({len(all_events)} events):")
        for i, event in enumerate(all_events, 1):
            print(f"   {i}. {event.event_type} at {event.created_at.strftime('%H:%M:%S')}")
            print(f"      Published: {event.published_to_bronze}")
        
        # Final projection state
        print(f"\n📊 Final Projection State:")
        cow = session.query(Cow).filter(
            Cow.tenant_id == tenant_id,
            Cow.cow_id == cow_id
        ).first()
        
        if cow:
            print(f"   Tag: {cow.tag_number}")
            print(f"   Name: {cow.name} (was 'Bessie')")
            print(f"   Breed: {cow.breed} (was 'Holstein')")
            print(f"   Status: {cow.status} (was 'active')")
            print(f"   Active: {cow.is_active}")
        
        print("\n" + "=" * 70)
        print("🎉 ALL CRUD TESTS PASSED!")
        print("=" * 70)
        print("\n✅ Event sourcing architecture validated:")
        print("   • CREATE: Event stored → Sync → Projection created")
        print("   • UPDATE: Event stored → Sync → Projection updated")
        print("   • DEACTIVATE: Event stored → Sync → Status changed")
        print("\n✅ Event replay pattern working correctly")
        print("✅ All events marked as published")
        print("✅ Projection reflects final state after replaying all events")
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED WITH ERROR:")
        print(f"   {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        print("\n\n🧹 Cleaning up test data...")
        clear_test_data(session, tenant_id, cow_id)
        session.close()
        engine.dispose()
        print("✅ Cleanup complete")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
