"""
Metrics Module for Sync Jobs.

Provides Prometheus-style metrics and simple JSON endpoint for monitoring.

Metrics:
- sync_lag_seconds: Time since last successful sync
- events_per_minute: Event creation rate
- silver_processing_time: Time to process events in Silver
- sql_projection_lag: Time between Silver update and SQL sync
- sync_duration_seconds: How long each sync takes
- sync_rows_processed: Number of rows synced
- sync_conflicts_total: Total conflicts resolved
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict

from sqlalchemy import select, func, text
from sqlalchemy.orm import Session

from backend.database.connection import get_db_manager
from backend.models.sync import SyncState, SyncLog


# ========================================
# Logging Configuration
# ========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ========================================
# Metrics Collection
# ========================================

class MetricsCollector:
    """
    Collects metrics for monitoring and alerting.
    
    Can export in:
    - Prometheus format (text)
    - JSON format
    """
    
    def __init__(self):
        self.metrics: Dict[str, float] = {}
        self.labels: Dict[str, Dict[str, str]] = {}
        self.timestamp = datetime.utcnow()
    
    def add_metric(
        self,
        name: str,
        value: float,
        help_text: str = "",
        metric_type: str = "gauge",
        labels: Optional[Dict[str, str]] = None
    ):
        """
        Add a metric.
        
        Args:
            name: Metric name
            value: Metric value
            help_text: Help text for metric
            metric_type: Type (gauge, counter, histogram)
            labels: Optional labels (key-value pairs)
        """
        self.metrics[name] = value
        self.labels[name] = {
            "help": help_text,
            "type": metric_type,
            "labels": labels or {}
        }
    
    def to_prometheus(self) -> str:
        """
        Export metrics in Prometheus text format.
        
        Returns:
            Prometheus-formatted metrics string
        """
        lines = []
        
        for name, value in self.metrics.items():
            label_info = self.labels.get(name, {})
            help_text = label_info.get("help", "")
            metric_type = label_info.get("type", "gauge")
            labels = label_info.get("labels", {})
            
            # Add HELP line
            if help_text:
                lines.append(f"# HELP {name} {help_text}")
            
            # Add TYPE line
            lines.append(f"# TYPE {name} {metric_type}")
            
            # Add metric line with labels
            if labels:
                label_str = ",".join([f'{k}="{v}"' for k, v in labels.items()])
                lines.append(f"{name}{{{label_str}}} {value}")
            else:
                lines.append(f"{name} {value}")
        
        return "\n".join(lines) + "\n"
    
    def to_json(self) -> Dict:
        """
        Export metrics in JSON format.
        
        Returns:
            Dictionary with metrics
        """
        return {
            "timestamp": self.timestamp.isoformat(),
            "metrics": {
                name: {
                    "value": value,
                    **self.labels.get(name, {})
                }
                for name, value in self.metrics.items()
            }
        }


# ========================================
# Metric Collectors
# ========================================

def collect_sync_lag_metric(session: Session, collector: MetricsCollector):
    """Collect sync lag metric."""
    try:
        stmt = select(SyncState).where(SyncState.table_name == "cows")
        sync_state = session.execute(stmt).scalar_one_or_none()
        
        if sync_state and sync_state.last_sync_completed_at:
            lag_seconds = (datetime.utcnow() - sync_state.last_sync_completed_at).total_seconds()
            collector.add_metric(
                name="endymion_ai_sync_lag_seconds",
                value=lag_seconds,
                help_text="Time since last successful sync",
                metric_type="gauge",
                labels={"table": "cows"}
            )
        else:
            # No sync completed yet
            collector.add_metric(
                name="endymion_ai_sync_lag_seconds",
                value=-1,  # Indicates no data
                help_text="Time since last successful sync",
                metric_type="gauge",
                labels={"table": "cows"}
            )
    
    except Exception as e:
        logger.error(f"Error collecting sync lag metric: {e}")


def collect_events_per_minute_metric(session: Session, collector: MetricsCollector):
    """Collect event creation rate metric."""
    try:
        # Count events created in last minute
        one_minute_ago = datetime.utcnow() - timedelta(minutes=1)
        
        query = text("""
            SELECT COUNT(*) as count
            FROM events.cow_events
            WHERE event_timestamp >= :since
        """)
        
        result = session.execute(query, {"since": one_minute_ago}).fetchone()
        count = result[0] if result else 0
        
        collector.add_metric(
            name="endymion_ai_events_per_minute",
            value=float(count),
            help_text="Number of events created per minute",
            metric_type="gauge",
            labels={"event_type": "cow_events"}
        )
    
    except Exception as e:
        logger.error(f"Error collecting events per minute metric: {e}")


def collect_sync_duration_metric(session: Session, collector: MetricsCollector):
    """Collect average sync duration metric."""
    try:
        # Get last 10 completed syncs
        stmt = (
            select(SyncLog)
            .where(SyncLog.table_name == "cows")
            .where(SyncLog.status == "completed")
            .order_by(SyncLog.started_at.desc())
            .limit(10)
        )
        recent_syncs = session.execute(stmt).scalars().all()
        
        if recent_syncs:
            avg_duration = sum(s.duration_seconds or 0 for s in recent_syncs) / len(recent_syncs)
            collector.add_metric(
                name="endymion_ai_sync_duration_seconds",
                value=avg_duration,
                help_text="Average sync duration (last 10 runs)",
                metric_type="gauge",
                labels={"table": "cows", "stat": "avg"}
            )
            
            # Also add last sync duration
            last_duration = recent_syncs[0].duration_seconds or 0
            collector.add_metric(
                name="endymion_ai_sync_duration_seconds_last",
                value=last_duration,
                help_text="Last sync duration",
                metric_type="gauge",
                labels={"table": "cows"}
            )
    
    except Exception as e:
        logger.error(f"Error collecting sync duration metric: {e}")


def collect_sync_rows_metric(session: Session, collector: MetricsCollector):
    """Collect rows synced metrics."""
    try:
        stmt = select(SyncState).where(SyncState.table_name == "cows")
        sync_state = session.execute(stmt).scalar_one_or_none()
        
        if sync_state:
            collector.add_metric(
                name="endymion_ai_sync_rows_total",
                value=float(sync_state.total_rows_synced),
                help_text="Total rows synced since tracking began",
                metric_type="counter",
                labels={"table": "cows"}
            )
            
            collector.add_metric(
                name="endymion_ai_sync_conflicts_total",
                value=float(sync_state.total_conflicts_resolved),
                help_text="Total conflicts resolved",
                metric_type="counter",
                labels={"table": "cows"}
            )
    
    except Exception as e:
        logger.error(f"Error collecting sync rows metric: {e}")


def collect_last_sync_rows_metric(session: Session, collector: MetricsCollector):
    """Collect last sync row counts."""
    try:
        stmt = (
            select(SyncLog)
            .where(SyncLog.table_name == "cows")
            .order_by(SyncLog.started_at.desc())
            .limit(1)
        )
        last_sync = session.execute(stmt).scalar_one_or_none()
        
        if last_sync:
            collector.add_metric(
                name="endymion_ai_sync_rows_read_last",
                value=float(last_sync.rows_read),
                help_text="Rows read in last sync",
                metric_type="gauge",
                labels={"table": "cows"}
            )
            
            collector.add_metric(
                name="endymion_ai_sync_rows_inserted_last",
                value=float(last_sync.rows_inserted),
                help_text="Rows inserted in last sync",
                metric_type="gauge",
                labels={"table": "cows"}
            )
            
            collector.add_metric(
                name="endymion_ai_sync_rows_updated_last",
                value=float(last_sync.rows_updated),
                help_text="Rows updated in last sync",
                metric_type="gauge",
                labels={"table": "cows"}
            )
            
            collector.add_metric(
                name="endymion_ai_sync_rows_skipped_last",
                value=float(last_sync.rows_skipped),
                help_text="Rows skipped in last sync",
                metric_type="gauge",
                labels={"table": "cows"}
            )
    
    except Exception as e:
        logger.error(f"Error collecting last sync rows metric: {e}")


def collect_sql_row_count_metric(session: Session, collector: MetricsCollector):
    """Collect SQL table row count."""
    try:
        query = text("SELECT COUNT(*) as count FROM operational.cows")
        result = session.execute(query).fetchone()
        count = result[0] if result else 0
        
        collector.add_metric(
            name="endymion_ai_sql_row_count",
            value=float(count),
            help_text="Number of rows in SQL projection table",
            metric_type="gauge",
            labels={"table": "cows", "schema": "operational"}
        )
    
    except Exception as e:
        logger.error(f"Error collecting SQL row count metric: {e}")


def collect_sync_failure_rate_metric(session: Session, collector: MetricsCollector):
    """Collect sync failure rate."""
    try:
        # Get last 20 sync runs
        stmt = (
            select(SyncLog)
            .where(SyncLog.table_name == "cows")
            .order_by(SyncLog.started_at.desc())
            .limit(20)
        )
        recent_syncs = session.execute(stmt).scalars().all()
        
        if recent_syncs:
            failures = sum(1 for s in recent_syncs if s.status == "failed")
            failure_rate = failures / len(recent_syncs) * 100
            
            collector.add_metric(
                name="endymion_ai_sync_failure_rate",
                value=failure_rate,
                help_text="Sync failure rate (last 20 runs, percentage)",
                metric_type="gauge",
                labels={"table": "cows"}
            )
            
            collector.add_metric(
                name="endymion_ai_sync_failures_total",
                value=float(failures),
                help_text="Number of failures in last 20 runs",
                metric_type="gauge",
                labels={"table": "cows"}
            )
    
    except Exception as e:
        logger.error(f"Error collecting sync failure rate metric: {e}")


# ========================================
# Main Metrics Collection
# ========================================

def collect_all_metrics() -> MetricsCollector:
    """
    Collect all metrics.
    
    Returns:
        MetricsCollector with all metrics
    """
    logger.info("Collecting metrics...")
    
    collector = MetricsCollector()
    
    # Get database session
    db_manager = get_db_manager()
    session = db_manager.get_session()
    
    try:
        # Collect all metrics
        collect_sync_lag_metric(session, collector)
        collect_events_per_minute_metric(session, collector)
        collect_sync_duration_metric(session, collector)
        collect_sync_rows_metric(session, collector)
        collect_last_sync_rows_metric(session, collector)
        collect_sql_row_count_metric(session, collector)
        collect_sync_failure_rate_metric(session, collector)
        
        logger.info(f"Collected {len(collector.metrics)} metrics")
        return collector
    
    finally:
        session.close()


# ========================================
# CLI Entry Point
# ========================================

if __name__ == "__main__":
    """Export metrics to stdout."""
    import sys
    import json
    
    collector = collect_all_metrics()
    
    # Check command line argument for format
    if len(sys.argv) > 1 and sys.argv[1] == "--prometheus":
        # Prometheus format
        print(collector.to_prometheus())
    else:
        # JSON format (default)
        print(json.dumps(collector.to_json(), indent=2))
