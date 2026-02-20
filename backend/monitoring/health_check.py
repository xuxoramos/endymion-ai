"""
Health Check Module for Sync Jobs.

Monitors:
- Sync lag (time since last successful sync)
- Event backlog (unpublished events in SQL outbox)
- Silver freshness (last updated timestamp in Silver layer)
- Data consistency (SQL vs Silver row counts)

Alerts if:
- Sync lag > 5 minutes (configurable)
- Event backlog > 1000 events
- Silver not updated in 10 minutes
- SQL/Silver row count mismatch > 10%
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum

from sqlalchemy import select, func, text
from sqlalchemy.orm import Session
from pyspark.sql import SparkSession

from backend.database.connection import get_db_manager
from backend.models.sync import SyncState, SyncLog


# ========================================
# Logging Configuration
# ========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ========================================
# Configuration
# ========================================

class HealthCheckConfig:
    """Configuration for health checks."""
    
    # Sync lag thresholds
    SYNC_LAG_WARNING_SECONDS = 120  # 2 minutes
    SYNC_LAG_CRITICAL_SECONDS = 300  # 5 minutes
    
    # Event backlog thresholds
    EVENT_BACKLOG_WARNING = 500
    EVENT_BACKLOG_CRITICAL = 1000
    
    # Silver freshness thresholds
    SILVER_FRESHNESS_WARNING_SECONDS = 300  # 5 minutes
    SILVER_FRESHNESS_CRITICAL_SECONDS = 600  # 10 minutes
    
    # Row count mismatch thresholds (percentage)
    ROW_COUNT_MISMATCH_WARNING = 5.0  # 5%
    ROW_COUNT_MISMATCH_CRITICAL = 10.0  # 10%
    
    # MinIO/Delta configuration
    MINIO_ENDPOINT = "http://localhost:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    SILVER_PATH = "s3a://silver/cows_current"


# ========================================
# Health Status Enum
# ========================================

class HealthStatus(str, Enum):
    """Health check status levels."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


# ========================================
# Health Check Result Models
# ========================================

class HealthCheckResult:
    """Result of a single health check."""
    
    def __init__(
        self,
        name: str,
        status: HealthStatus,
        message: str,
        value: Optional[float] = None,
        threshold: Optional[float] = None,
        details: Optional[Dict] = None
    ):
        self.name = name
        self.status = status
        self.message = message
        self.value = value
        self.threshold = threshold
        self.details = details or {}
        self.timestamp = datetime.utcnow()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "value": self.value,
            "threshold": self.threshold,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


class SystemHealthReport:
    """Overall system health report."""
    
    def __init__(self):
        self.checks: List[HealthCheckResult] = []
        self.timestamp = datetime.utcnow()
    
    def add_check(self, check: HealthCheckResult):
        """Add a health check result."""
        self.checks.append(check)
    
    @property
    def overall_status(self) -> HealthStatus:
        """Determine overall health status."""
        if not self.checks:
            return HealthStatus.UNKNOWN
        
        # If any check is CRITICAL, overall is CRITICAL
        if any(c.status == HealthStatus.CRITICAL for c in self.checks):
            return HealthStatus.CRITICAL
        
        # If any check is WARNING, overall is WARNING
        if any(c.status == HealthStatus.WARNING for c in self.checks):
            return HealthStatus.WARNING
        
        # All checks are HEALTHY
        return HealthStatus.HEALTHY
    
    @property
    def is_healthy(self) -> bool:
        """Check if system is healthy (no CRITICAL status)."""
        return self.overall_status != HealthStatus.CRITICAL
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "overall_status": self.overall_status.value,
            "is_healthy": self.is_healthy,
            "timestamp": self.timestamp.isoformat(),
            "checks": [c.to_dict() for c in self.checks]
        }


# ========================================
# Sync Lag Check
# ========================================

def check_sync_lag(session: Session) -> HealthCheckResult:
    """
    Check time since last successful sync.
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        HealthCheckResult
    """
    try:
        # Get sync state for cows table
        stmt = select(SyncState).where(SyncState.table_name == "cows")
        sync_state = session.execute(stmt).scalar_one_or_none()
        
        if sync_state is None:
            return HealthCheckResult(
                name="sync_lag",
                status=HealthStatus.UNKNOWN,
                message="Sync state not found (sync never run?)",
                value=None,
                threshold=HealthCheckConfig.SYNC_LAG_CRITICAL_SECONDS
            )
        
        # Calculate lag
        if sync_state.last_sync_completed_at is None:
            return HealthCheckResult(
                name="sync_lag",
                status=HealthStatus.WARNING,
                message="Sync never completed successfully",
                value=None,
                threshold=HealthCheckConfig.SYNC_LAG_CRITICAL_SECONDS,
                details={
                    "last_sync_error": sync_state.last_sync_error
                }
            )
        
        lag_seconds = (datetime.utcnow() - sync_state.last_sync_completed_at).total_seconds()
        
        # Determine status
        if lag_seconds >= HealthCheckConfig.SYNC_LAG_CRITICAL_SECONDS:
            status = HealthStatus.CRITICAL
            message = f"Sync lag is CRITICAL: {lag_seconds:.0f}s (threshold: {HealthCheckConfig.SYNC_LAG_CRITICAL_SECONDS}s)"
        elif lag_seconds >= HealthCheckConfig.SYNC_LAG_WARNING_SECONDS:
            status = HealthStatus.WARNING
            message = f"Sync lag is elevated: {lag_seconds:.0f}s (threshold: {HealthCheckConfig.SYNC_LAG_WARNING_SECONDS}s)"
        else:
            status = HealthStatus.HEALTHY
            message = f"Sync lag is healthy: {lag_seconds:.0f}s"
        
        return HealthCheckResult(
            name="sync_lag",
            status=status,
            message=message,
            value=lag_seconds,
            threshold=HealthCheckConfig.SYNC_LAG_CRITICAL_SECONDS,
            details={
                "last_sync_completed_at": sync_state.last_sync_completed_at.isoformat(),
                "total_rows_synced": sync_state.total_rows_synced,
                "total_conflicts_resolved": sync_state.total_conflicts_resolved
            }
        )
    
    except Exception as e:
        logger.error(f"Error checking sync lag: {e}", exc_info=True)
        return HealthCheckResult(
            name="sync_lag",
            status=HealthStatus.UNKNOWN,
            message=f"Error checking sync lag: {str(e)}",
            value=None,
            threshold=HealthCheckConfig.SYNC_LAG_CRITICAL_SECONDS
        )


# ========================================
# Event Backlog Check
# ========================================

def check_event_backlog(session: Session) -> HealthCheckResult:
    """
    Check number of unpublished events in cow_events table.
    
    These are events waiting to be processed by Bronze ingestion.
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        HealthCheckResult
    """
    try:
        # Query unpublished events
        # Note: Assuming cow_events has a 'published' or 'processed' flag
        # Adjust column names based on actual schema
        query = text("""
            SELECT COUNT(*) as backlog
            FROM events.cow_events
            WHERE published_at IS NULL
        """)
        
        result = session.execute(query).fetchone()
        backlog_count = result[0] if result else 0
        
        # Determine status
        if backlog_count >= HealthCheckConfig.EVENT_BACKLOG_CRITICAL:
            status = HealthStatus.CRITICAL
            message = f"Event backlog is CRITICAL: {backlog_count} events (threshold: {HealthCheckConfig.EVENT_BACKLOG_CRITICAL})"
        elif backlog_count >= HealthCheckConfig.EVENT_BACKLOG_WARNING:
            status = HealthStatus.WARNING
            message = f"Event backlog is elevated: {backlog_count} events (threshold: {HealthCheckConfig.EVENT_BACKLOG_WARNING})"
        else:
            status = HealthStatus.HEALTHY
            message = f"Event backlog is healthy: {backlog_count} events"
        
        return HealthCheckResult(
            name="event_backlog",
            status=status,
            message=message,
            value=float(backlog_count),
            threshold=float(HealthCheckConfig.EVENT_BACKLOG_CRITICAL),
            details={
                "unpublished_events": backlog_count
            }
        )
    
    except Exception as e:
        logger.error(f"Error checking event backlog: {e}", exc_info=True)
        return HealthCheckResult(
            name="event_backlog",
            status=HealthStatus.UNKNOWN,
            message=f"Error checking event backlog: {str(e)}",
            value=None,
            threshold=float(HealthCheckConfig.EVENT_BACKLOG_CRITICAL)
        )


# ========================================
# Silver Freshness Check
# ========================================

def check_silver_freshness() -> HealthCheckResult:
    """
    Check when Silver layer was last updated.
    
    Returns:
        HealthCheckResult
    """
    spark = None
    try:
        # Get Delta JAR paths
        import os
        ivy_jars = os.path.expanduser("~/.ivy2/jars")
        hadoop_aws_jar = f"{ivy_jars}/org.apache.hadoop_hadoop-aws-3.3.1.jar"
        aws_sdk_jar = f"{ivy_jars}/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar"
        delta_spark_jar = f"{ivy_jars}/io.delta_delta-spark_2.12-3.2.1.jar"
        delta_storage_jar = f"{ivy_jars}/io.delta_delta-storage-3.2.1.jar"
        
        # Create Spark session
        spark = (
            SparkSession.builder
            .appName("HealthCheck-Silver-Freshness")
            .master("local[*]")
            .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{delta_spark_jar},{delta_storage_jar}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", HealthCheckConfig.MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", HealthCheckConfig.MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", HealthCheckConfig.MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        
        # Read Silver table
        df = spark.read.format("delta").load(HealthCheckConfig.SILVER_PATH)
        
        # Get max last_updated_at
        max_updated = df.agg({"last_updated_at": "max"}).collect()[0]["max(last_updated_at)"]
        
        if max_updated is None:
            return HealthCheckResult(
                name="silver_freshness",
                status=HealthStatus.WARNING,
                message="Silver table is empty or has no last_updated_at values",
                value=None,
                threshold=float(HealthCheckConfig.SILVER_FRESHNESS_CRITICAL_SECONDS)
            )
        
        # Parse timestamp and calculate lag
        last_updated_dt = datetime.fromisoformat(max_updated)
        lag_seconds = (datetime.utcnow() - last_updated_dt).total_seconds()
        
        # Determine status
        if lag_seconds >= HealthCheckConfig.SILVER_FRESHNESS_CRITICAL_SECONDS:
            status = HealthStatus.CRITICAL
            message = f"Silver freshness is CRITICAL: {lag_seconds:.0f}s since last update (threshold: {HealthCheckConfig.SILVER_FRESHNESS_CRITICAL_SECONDS}s)"
        elif lag_seconds >= HealthCheckConfig.SILVER_FRESHNESS_WARNING_SECONDS:
            status = HealthStatus.WARNING
            message = f"Silver freshness is stale: {lag_seconds:.0f}s since last update (threshold: {HealthCheckConfig.SILVER_FRESHNESS_WARNING_SECONDS}s)"
        else:
            status = HealthStatus.HEALTHY
            message = f"Silver freshness is healthy: {lag_seconds:.0f}s since last update"
        
        return HealthCheckResult(
            name="silver_freshness",
            status=status,
            message=message,
            value=lag_seconds,
            threshold=float(HealthCheckConfig.SILVER_FRESHNESS_CRITICAL_SECONDS),
            details={
                "last_updated_at": last_updated_dt.isoformat()
            }
        )
    
    except Exception as e:
        logger.error(f"Error checking Silver freshness: {e}", exc_info=True)
        return HealthCheckResult(
            name="silver_freshness",
            status=HealthStatus.UNKNOWN,
            message=f"Error checking Silver freshness: {str(e)}",
            value=None,
            threshold=float(HealthCheckConfig.SILVER_FRESHNESS_CRITICAL_SECONDS)
        )
    
    finally:
        if spark:
            spark.stop()


# ========================================
# Data Consistency Check
# ========================================

def check_data_consistency(session: Session) -> HealthCheckResult:
    """
    Check SQL vs Silver row count consistency.
    
    Large mismatches indicate sync issues.
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        HealthCheckResult
    """
    spark = None
    try:
        # Get SQL row count
        sql_query = text("SELECT COUNT(*) as count FROM operational.cows")
        sql_result = session.execute(sql_query).fetchone()
        sql_count = sql_result[0] if sql_result else 0
        
        # Get Delta JAR paths
        import os
        ivy_jars = os.path.expanduser("~/.ivy2/jars")
        hadoop_aws_jar = f"{ivy_jars}/org.apache.hadoop_hadoop-aws-3.3.1.jar"
        aws_sdk_jar = f"{ivy_jars}/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar"
        delta_spark_jar = f"{ivy_jars}/io.delta_delta-spark_2.12-3.2.1.jar"
        delta_storage_jar = f"{ivy_jars}/io.delta_delta-storage-3.2.1.jar"
        
        # Get Silver row count
        spark = (
            SparkSession.builder
            .appName("HealthCheck-Data-Consistency")
            .master("local[*]")
            .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{delta_spark_jar},{delta_storage_jar}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", HealthCheckConfig.MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", HealthCheckConfig.MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", HealthCheckConfig.MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        
        df = spark.read.format("delta").load(HealthCheckConfig.SILVER_PATH)
        silver_count = df.count()
        
        # Calculate mismatch percentage
        if silver_count == 0:
            mismatch_pct = 0 if sql_count == 0 else 100
        else:
            mismatch_pct = abs(sql_count - silver_count) / silver_count * 100
        
        # Determine status
        if mismatch_pct >= HealthCheckConfig.ROW_COUNT_MISMATCH_CRITICAL:
            status = HealthStatus.CRITICAL
            message = f"Row count mismatch is CRITICAL: {mismatch_pct:.1f}% difference (SQL: {sql_count}, Silver: {silver_count})"
        elif mismatch_pct >= HealthCheckConfig.ROW_COUNT_MISMATCH_WARNING:
            status = HealthStatus.WARNING
            message = f"Row count mismatch: {mismatch_pct:.1f}% difference (SQL: {sql_count}, Silver: {silver_count})"
        else:
            status = HealthStatus.HEALTHY
            message = f"Row counts are consistent: SQL={sql_count}, Silver={silver_count} ({mismatch_pct:.1f}% difference)"
        
        return HealthCheckResult(
            name="data_consistency",
            status=status,
            message=message,
            value=mismatch_pct,
            threshold=HealthCheckConfig.ROW_COUNT_MISMATCH_CRITICAL,
            details={
                "sql_row_count": sql_count,
                "silver_row_count": silver_count,
                "mismatch_percentage": mismatch_pct
            }
        )
    
    except Exception as e:
        logger.error(f"Error checking data consistency: {e}", exc_info=True)
        return HealthCheckResult(
            name="data_consistency",
            status=HealthStatus.UNKNOWN,
            message=f"Error checking data consistency: {str(e)}",
            value=None,
            threshold=HealthCheckConfig.ROW_COUNT_MISMATCH_CRITICAL
        )
    
    finally:
        if spark:
            spark.stop()


# ========================================
# Recent Sync Failures Check
# ========================================

def check_recent_failures(session: Session) -> HealthCheckResult:
    """
    Check for recent sync failures.
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        HealthCheckResult
    """
    try:
        # Get last 10 sync runs
        stmt = (
            select(SyncLog)
            .where(SyncLog.table_name == "cows")
            .order_by(SyncLog.started_at.desc())
            .limit(10)
        )
        recent_syncs = session.execute(stmt).scalars().all()
        
        if not recent_syncs:
            return HealthCheckResult(
                name="recent_failures",
                status=HealthStatus.UNKNOWN,
                message="No recent sync runs found",
                value=None,
                threshold=None
            )
        
        # Count failures
        failures = [s for s in recent_syncs if s.status == "failed"]
        failure_count = len(failures)
        failure_rate = failure_count / len(recent_syncs) * 100
        
        # Determine status
        if failure_rate >= 50:
            status = HealthStatus.CRITICAL
            message = f"High failure rate: {failure_rate:.0f}% ({failure_count}/{len(recent_syncs)} failed)"
        elif failure_rate >= 20:
            status = HealthStatus.WARNING
            message = f"Elevated failure rate: {failure_rate:.0f}% ({failure_count}/{len(recent_syncs)} failed)"
        else:
            status = HealthStatus.HEALTHY
            message = f"Failure rate is healthy: {failure_rate:.0f}% ({failure_count}/{len(recent_syncs)} failed)"
        
        # Get last failure details
        last_failure_details = {}
        if failures:
            last_failure = failures[0]
            last_failure_details = {
                "last_failure_at": last_failure.started_at.isoformat(),
                "error_message": last_failure.error_message
            }
        
        return HealthCheckResult(
            name="recent_failures",
            status=status,
            message=message,
            value=failure_rate,
            threshold=50.0,
            details={
                "failure_count": failure_count,
                "total_runs": len(recent_syncs),
                "failure_rate": failure_rate,
                **last_failure_details
            }
        )
    
    except Exception as e:
        logger.error(f"Error checking recent failures: {e}", exc_info=True)
        return HealthCheckResult(
            name="recent_failures",
            status=HealthStatus.UNKNOWN,
            message=f"Error checking recent failures: {str(e)}",
            value=None,
            threshold=None
        )


# ========================================
# Main Health Check Function
# ========================================

def run_health_checks() -> SystemHealthReport:
    """
    Run all health checks and return comprehensive report.
    
    Returns:
        SystemHealthReport
    """
    logger.info("Running health checks...")
    
    report = SystemHealthReport()
    
    # Get database session
    db_manager = get_db_manager()
    session = db_manager.get_session()
    
    try:
        # Run SQL-based checks
        report.add_check(check_sync_lag(session))
        report.add_check(check_event_backlog(session))
        report.add_check(check_recent_failures(session))
        
        # Run Spark-based checks (these create their own sessions)
        report.add_check(check_silver_freshness())
        report.add_check(check_data_consistency(session))
        
        logger.info(f"Health checks completed: {report.overall_status.value}")
        return report
    
    finally:
        session.close()


# ========================================
# Alert Function
# ========================================

def send_alerts(report: SystemHealthReport) -> None:
    """
    Send alerts based on health check results.
    
    In production, this would integrate with:
    - Email (SMTP)
    - Slack
    - PagerDuty
    - CloudWatch Alarms
    
    For now, just logs alerts.
    
    Args:
        report: SystemHealthReport
    """
    critical_checks = [c for c in report.checks if c.status == HealthStatus.CRITICAL]
    warning_checks = [c for c in report.checks if c.status == HealthStatus.WARNING]
    
    if critical_checks:
        logger.error(f"⚠️ CRITICAL ALERTS ({len(critical_checks)}):")
        for check in critical_checks:
            logger.error(f"  - {check.name}: {check.message}")
    
    if warning_checks:
        logger.warning(f"⚠️ WARNING ALERTS ({len(warning_checks)}):")
        for check in warning_checks:
            logger.warning(f"  - {check.name}: {check.message}")
    
    if not critical_checks and not warning_checks:
        logger.info("✅ All health checks passed")


# ========================================
# CLI Entry Point
# ========================================

if __name__ == "__main__":
    """Run health checks from command line."""
    report = run_health_checks()
    send_alerts(report)
    
    # Print report
    import json
    print(json.dumps(report.to_dict(), indent=2))
    
    # Exit with error code if not healthy
    import sys
    sys.exit(0 if report.is_healthy else 1)
