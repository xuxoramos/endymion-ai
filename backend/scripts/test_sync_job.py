#!/usr/bin/env python3
"""
Test the sync job by creating events and running the sync.
"""

import sys
sys.path.insert(0, '/home/xuxoramos/endymion-ai')

from uuid import uuid4
from backend.database.connection import DatabaseManager
from backend.models.events import CowEvent, CowEventType
from backend.models.cows import Cow
from sqlalchemy.orm import Session


def test_sync_flow():
    """Test creating events and checking if sync would work."""
    
    print("=" * 60)
    print("Testing Sync Job Flow")
    print("=" * 60)
    
    db_manager = DatabaseManager()
    engine = db_manager.init_engine()
    session = Session(engine)
    
    try:
        # Create test tenant and cow IDs
        tenant_id = uuid4()
        cow_id = uuid4()
        
        print(f"\n1. Creating test event...")
        print(f"   Tenant ID: {tenant_id}")
        print(f"   Cow ID: {cow_id}")
        
        # Create a cow_created event
        event = CowEvent.create_event(
            tenant_id=tenant_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"tag_number": "TEST001", "name": "Test Cow", "breed": "Holstein", "birth_date": "2022-01-15", "sex": "female", "status": "active"}',
            created_by="test_script"
        )
        
        session.add(event)
        session.commit()
        
        print(f"   ✅ Event created: {event.event_id}")
        
        # Check cow_events table
        print(f"\n2. Checking cow_events table...")
        events_count = session.query(CowEvent).filter(
            CowEvent.tenant_id == tenant_id
        ).count()
        print(f"   Events in table: {events_count}")
        
        # Check cows projection table (should be empty before sync)
        print(f"\n3. Checking cows projection table (before sync)...")
        cows_count = session.query(Cow).filter(
            Cow.tenant_id == tenant_id
        ).count()
        print(f"   Cows in projection: {cows_count}")
        
        if cows_count == 0:
            print(f"   ✅ Correct! Projection is empty before sync")
        else:
            print(f"   ⚠️  Warning: Projection already has data")
        
        print(f"\n4. Next step: Run the sync job")
        print(f"   Command: PYTHONPATH=/home/xuxoramos/endymion-ai python -m backend.jobs.sync_silver_to_sql")
        print(f"\n   Note: The sync job expects data in Silver layer (MinIO/Delta Lake)")
        print(f"   Since we only created events in SQL, the sync won't find anything.")
        print(f"\n   For a full test, we need to:")
        print(f"   - Either mock the Silver layer data")
        print(f"   - Or implement event-to-projection sync (bypass Silver)")
        
        print(f"\n5. Current architecture note:")
        print(f"   Events → [Databricks CDC] → Bronze → Silver → [Sync Job] → SQL Projection")
        print(f"   ^^^ We're here               Missing              ^^^ Currently testing")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    test_sync_flow()
