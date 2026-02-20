#!/usr/bin/env python3
"""
Initialize all database tables using SQLAlchemy models.
Run this after the database is created to set up all tables.
"""

import sys
sys.path.insert(0, '/home/xuxoramos/endymion-ai')

from backend.database.connection import DatabaseManager
from backend.models.base import Base
from backend.models.cows import Cow
from backend.models.categories import Category
from backend.models.events import CowEvent
from backend.models.sync import SyncState, SyncLog, SyncConflict


def init_tables():
    """Create all tables in the database."""
    print("Initializing database tables...")
    
    db_manager = DatabaseManager()
    engine = db_manager.init_engine()
    
    # Create all tables
    Base.metadata.create_all(engine)
    
    print("✅ All tables created successfully!")
    print("\nCreated tables:")
    print("  - operational.cow_events (event store)")
    print("  - operational.cows (projection)")
    print("  - operational.categories")
    print("  - operational.sync_state")
    print("  - operational.sync_log")
    print("  - operational.sync_conflict")
    
    engine.dispose()


if __name__ == "__main__":
    init_tables()
