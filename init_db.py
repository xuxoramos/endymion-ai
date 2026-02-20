#!/usr/bin/env python
"""
Initialize database schema.

Creates all tables defined in the SQLAlchemy models.
"""

from backend.database.connection import DatabaseManager

def main():
    """Create all database tables."""
    print("Initializing database schema...")
    
    db = DatabaseManager()
    db.init_engine()
    
    print("Creating tables...")
    db.create_all_tables()
    
    print("✅ Database schema initialized successfully!")

if __name__ == "__main__":
    main()
