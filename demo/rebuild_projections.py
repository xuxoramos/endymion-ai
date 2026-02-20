#!/usr/bin/env python
"""
Manual projection builder - rebuilds cows projection from events
"""
import sys
import os
sys.path.insert(0, '/home/xuxoramos/endymion-ai')

from backend.database.connection import DatabaseManager
from sqlalchemy import text
import json

def rebuild_projections():
    """Rebuild cow projections from events"""
    db = DatabaseManager()
    
    print("🔄 Rebuilding projections from events...")
    
    with db.engine.connect() as conn:
        # Get all cow_created and cow_updated events
        result = conn.execute(text("""
            SELECT 
                cow_id,
                event_type,
                payload,
                event_time,
                tenant_id
            FROM operational.cow_events
            ORDER BY cow_id, event_time
        """))
        
        events = result.fetchall()
        print(f"📊 Found {len(events)} events")
        
        # Group by cow_id
        cows = {}
        for event in events:
            cow_id = str(event.cow_id)
            if cow_id not in cows:
                cows[cow_id] = []
            cows[cow_id].append(event)
        
        print(f"🐄 Processing {len(cows)} cows...")
        
        # Process each cow
        for cow_id, cow_events in cows.items():
            # Start with created event
            created_event = [e for e in cow_events if e.event_type == 'cow_created'][0]
            payload = json.loads(created_event.payload)
            
            # Apply updates
            for event in cow_events:
                if event.event_type == 'cow_updated':
                    update_payload = json.loads(event.payload)
                    payload.update(update_payload)
            
            # Upsert into cows projection
            conn.execute(text("""
                IF EXISTS (SELECT 1 FROM operational.cows WHERE cow_id = :cow_id)
                BEGIN
                    UPDATE operational.cows 
                    SET tag_number = :tag_number,
                        name = :name,
                        breed = :breed,
                        birth_date = :birth_date,
                        sex = :sex,
                        weight_kg = :weight_kg,
                        status = :status,
                        tenant_id = :tenant_id,
                        updated_at = GETDATE(),
                        last_synced_at = GETDATE()
                    WHERE cow_id = :cow_id
                END
                ELSE
                BEGIN
                    INSERT INTO operational.cows (
                        cow_id, tag_number, name, breed, birth_date, sex, 
                        weight_kg, status, tenant_id, created_at, updated_at, last_synced_at
                    )
                    VALUES (
                        :cow_id, :tag_number, :name, :breed, :birth_date, :sex,
                        :weight_kg, :status, :tenant_id, GETDATE(), GETDATE(), GETDATE()
                    )
                END
            """), {
                'cow_id': cow_id,
                'tag_number': payload.get('tag_number'),
                'name': payload.get('name'),
                'breed': payload.get('breed'),
                'birth_date': payload.get('birth_date'),
                'sex': payload.get('sex'),
                'weight_kg': payload.get('weight_kg'),
                'status': payload.get('status', 'active'),
                'tenant_id': str(created_event.tenant_id)
            })
            
            print(f"  ✅ {payload.get('name', 'Unknown')} ({payload.get('tag_number')})")
        
        conn.commit()
    
    print("\n✅ Projections rebuilt successfully!")
    print("\n📊 Current projections:")
    
    with db.engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                tag_number,
                name,
                breed,
                sex,
                weight_kg,
                status
            FROM operational.cows
            ORDER BY tag_number
        """))
        
        for row in result:
            print(f"  {row.tag_number}: {row.name} ({row.breed}, {row.sex}, {row.weight_kg}kg)")

if __name__ == '__main__':
    rebuild_projections()
