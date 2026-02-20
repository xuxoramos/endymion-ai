"""
Background jobs for data synchronization and processing.

This package contains:
- sync_silver_to_sql: Sync Silver layer Delta tables to SQL projection
- sync_scheduler: Schedule and manage sync jobs
"""

__all__ = ["sync_silver_to_sql", "sync_scheduler"]
