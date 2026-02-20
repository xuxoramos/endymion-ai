"""
Sync Scheduler - Background service for periodic synchronization.

Runs the Silver-to-SQL sync job on a fixed interval (default: 30 seconds).

Features:
- Periodic execution with configurable interval
- Graceful shutdown handling (SIGTERM, SIGINT)
- Error handling and retry logic
- Prevents overlapping sync runs
- Health monitoring

Usage:
    # Run as daemon
    python sync_scheduler.py
    
    # Run with custom interval
    SYNC_INTERVAL_SECONDS=60 python sync_scheduler.py
    
    # Stop gracefully
    kill -SIGTERM <pid>
"""

import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Optional

# Add backend to path for imports
sys.path.insert(0, '/home/xuxoramos/endymion-ai')

from backend.jobs.sync_silver_to_sql import sync_silver_to_sql


# ========================================
# Logging Configuration
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/sync_scheduler.log')
    ]
)
logger = logging.getLogger(__name__)


# ========================================
# Configuration
# ========================================

class SchedulerConfig:
    """Configuration for sync scheduler."""
    
    # Sync interval
    SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "30"))
    
    # Error handling
    MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "5"))
    BACKOFF_MULTIPLIER = float(os.getenv("BACKOFF_MULTIPLIER", "2.0"))
    MAX_BACKOFF_SECONDS = int(os.getenv("MAX_BACKOFF_SECONDS", "300"))  # 5 minutes
    
    # Health monitoring
    HEALTH_CHECK_ENABLED = os.getenv("HEALTH_CHECK_ENABLED", "true").lower() == "true"
    HEALTH_CHECK_FILE = os.getenv("HEALTH_CHECK_FILE", "/tmp/sync_scheduler_health.txt")


# ========================================
# Scheduler State
# ========================================

class SchedulerState:
    """Tracks scheduler state."""
    
    def __init__(self):
        self.running = False
        self.shutdown_requested = False
        self.sync_in_progress = False
        self.last_sync_time: Optional[datetime] = None
        self.last_sync_status: Optional[str] = None
        self.consecutive_failures = 0
        self.total_syncs = 0
        self.total_successes = 0
        self.total_failures = 0
        self.current_backoff_seconds = SchedulerConfig.SYNC_INTERVAL_SECONDS
        self.lock = threading.Lock()
    
    def record_sync_start(self):
        """Record that a sync has started."""
        with self.lock:
            self.sync_in_progress = True
            self.total_syncs += 1
    
    def record_sync_success(self):
        """Record a successful sync."""
        with self.lock:
            self.sync_in_progress = False
            self.last_sync_time = datetime.utcnow()
            self.last_sync_status = "success"
            self.consecutive_failures = 0
            self.total_successes += 1
            # Reset backoff on success
            self.current_backoff_seconds = SchedulerConfig.SYNC_INTERVAL_SECONDS
    
    def record_sync_failure(self):
        """Record a failed sync."""
        with self.lock:
            self.sync_in_progress = False
            self.last_sync_time = datetime.utcnow()
            self.last_sync_status = "failure"
            self.consecutive_failures += 1
            self.total_failures += 1
            
            # Apply exponential backoff
            self.current_backoff_seconds = min(
                self.current_backoff_seconds * SchedulerConfig.BACKOFF_MULTIPLIER,
                SchedulerConfig.MAX_BACKOFF_SECONDS
            )
    
    def get_status(self) -> dict:
        """Get current scheduler status."""
        with self.lock:
            return {
                "running": self.running,
                "sync_in_progress": self.sync_in_progress,
                "last_sync_time": self.last_sync_time.isoformat() if self.last_sync_time else None,
                "last_sync_status": self.last_sync_status,
                "consecutive_failures": self.consecutive_failures,
                "total_syncs": self.total_syncs,
                "total_successes": self.total_successes,
                "total_failures": self.total_failures,
                "current_backoff_seconds": self.current_backoff_seconds,
                "next_sync_in_seconds": self.current_backoff_seconds
            }


# Global scheduler state
scheduler_state = SchedulerState()


# ========================================
# Signal Handlers
# ========================================

def signal_handler(signum, frame):
    """
    Handle shutdown signals gracefully.
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    signal_name = signal.Signals(signum).name
    logger.info(f"Received signal {signal_name} - initiating graceful shutdown")
    
    scheduler_state.shutdown_requested = True
    
    # Wait for current sync to complete if one is running
    if scheduler_state.sync_in_progress:
        logger.info("Waiting for current sync to complete...")
        while scheduler_state.sync_in_progress:
            time.sleep(1)
    
    logger.info("Shutdown complete")
    sys.exit(0)


# ========================================
# Health Check
# ========================================

def write_health_check():
    """
    Write health check file for external monitoring.
    
    File contains timestamp and scheduler status.
    External systems can monitor this file's age to detect hangs.
    """
    if not SchedulerConfig.HEALTH_CHECK_ENABLED:
        return
    
    try:
        status = scheduler_state.get_status()
        health_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": status
        }
        
        with open(SchedulerConfig.HEALTH_CHECK_FILE, 'w') as f:
            import json
            json.dump(health_data, f, indent=2)
    
    except Exception as e:
        logger.warning(f"Failed to write health check file: {e}")


# ========================================
# Sync Execution
# ========================================

def execute_sync() -> bool:
    """
    Execute a single sync run.
    
    Returns:
        True if sync succeeded, False if failed
    """
    logger.info("Starting sync run...")
    
    scheduler_state.record_sync_start()
    
    try:
        result = sync_silver_to_sql()
        
        if result["status"] == "completed":
            logger.info(
                f"Sync completed successfully: "
                f"{result['rows_inserted']} inserted, "
                f"{result['rows_updated']} updated, "
                f"{result['rows_skipped']} skipped, "
                f"{result['conflicts_resolved']} conflicts"
            )
            scheduler_state.record_sync_success()
            return True
        else:
            logger.error(f"Sync failed: {result.get('error', 'Unknown error')}")
            scheduler_state.record_sync_failure()
            return False
    
    except Exception as e:
        logger.error(f"Unexpected error during sync: {e}", exc_info=True)
        scheduler_state.record_sync_failure()
        return False


# ========================================
# Main Scheduler Loop
# ========================================

def run_scheduler():
    """
    Main scheduler loop.
    
    Runs sync job periodically until shutdown is requested.
    """
    logger.info("=" * 60)
    logger.info("Sync Scheduler Starting")
    logger.info("=" * 60)
    logger.info(f"Sync interval: {SchedulerConfig.SYNC_INTERVAL_SECONDS} seconds")
    logger.info(f"Health check: {SchedulerConfig.HEALTH_CHECK_ENABLED}")
    logger.info(f"Max consecutive failures: {SchedulerConfig.MAX_CONSECUTIVE_FAILURES}")
    logger.info("=" * 60)
    
    scheduler_state.running = True
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Registered signal handlers (SIGTERM, SIGINT)")
    
    # Main loop
    while not scheduler_state.shutdown_requested:
        # Check if we've exceeded max consecutive failures
        if scheduler_state.consecutive_failures >= SchedulerConfig.MAX_CONSECUTIVE_FAILURES:
            logger.error(
                f"Exceeded max consecutive failures ({SchedulerConfig.MAX_CONSECUTIVE_FAILURES}). "
                f"Pausing sync for {SchedulerConfig.MAX_BACKOFF_SECONDS} seconds..."
            )
            time.sleep(SchedulerConfig.MAX_BACKOFF_SECONDS)
            # Reset failure count to try again
            scheduler_state.consecutive_failures = 0
            continue
        
        # Execute sync
        try:
            success = execute_sync()
            
            # Write health check
            write_health_check()
            
            # Log status
            status = scheduler_state.get_status()
            logger.info(
                f"Scheduler status: "
                f"Total syncs: {status['total_syncs']}, "
                f"Success: {status['total_successes']}, "
                f"Failures: {status['total_failures']}, "
                f"Consecutive failures: {status['consecutive_failures']}"
            )
        
        except Exception as e:
            logger.error(f"Unexpected error in scheduler loop: {e}", exc_info=True)
            scheduler_state.record_sync_failure()
        
        # Sleep until next sync (using current backoff)
        next_sync_in = scheduler_state.current_backoff_seconds
        
        if scheduler_state.consecutive_failures > 0:
            logger.warning(
                f"Backing off due to failures. Next sync in {next_sync_in} seconds..."
            )
        else:
            logger.info(f"Next sync in {next_sync_in} seconds...")
        
        # Sleep in small increments to allow quick shutdown
        sleep_interval = 1  # Check for shutdown every second
        elapsed = 0
        while elapsed < next_sync_in and not scheduler_state.shutdown_requested:
            time.sleep(sleep_interval)
            elapsed += sleep_interval
    
    scheduler_state.running = False
    logger.info("Scheduler stopped")


# ========================================
# Entry Point
# ========================================

def main():
    """Main entry point."""
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        scheduler_state.shutdown_requested = True
    except Exception as e:
        logger.error(f"Fatal error in scheduler: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Scheduler shutdown complete")


if __name__ == "__main__":
    main()
