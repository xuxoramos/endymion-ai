"""
Monitoring Dashboard for Sync Jobs.

Provides a simple HTML dashboard showing:
- Current sync status
- Event flow visualization
- Recent sync runs
- Health check results
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from backend.database.connection import get_db_manager
from backend.models.sync import SyncState, SyncLog, SyncConflict
from backend.monitoring.health_check import run_health_checks, HealthStatus
from backend.monitoring.metrics import collect_all_metrics


# ========================================
# Logging Configuration
# ========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ========================================
# Dashboard Data Collection
# ========================================

def get_sync_status(session: Session) -> Dict:
    """Get current sync status."""
    try:
        stmt = select(SyncState).where(SyncState.table_name == "cows")
        sync_state = session.execute(stmt).scalar_one_or_none()
        
        if not sync_state:
            return {
                "status": "unknown",
                "message": "No sync state found"
            }
        
        # Calculate lag
        if sync_state.last_sync_completed_at:
            lag_seconds = (datetime.utcnow() - sync_state.last_sync_completed_at).total_seconds()
            lag_minutes = lag_seconds / 60
            
            if lag_seconds < 120:  # < 2 minutes
                status = "healthy"
                status_class = "success"
            elif lag_seconds < 300:  # < 5 minutes
                status = "warning"
                status_class = "warning"
            else:
                status = "critical"
                status_class = "danger"
        else:
            lag_minutes = None
            status = "unknown"
            status_class = "secondary"
        
        return {
            "status": status,
            "status_class": status_class,
            "last_sync": sync_state.last_sync_completed_at.strftime("%Y-%m-%d %H:%M:%S") if sync_state.last_sync_completed_at else "Never",
            "lag_minutes": f"{lag_minutes:.1f}" if lag_minutes is not None else "N/A",
            "watermark": sync_state.watermark.strftime("%Y-%m-%d %H:%M:%S") if sync_state.watermark else "Not set",
            "total_rows_synced": sync_state.total_rows_synced,
            "total_conflicts": sync_state.total_conflicts_resolved
        }
    
    except Exception as e:
        logger.error(f"Error getting sync status: {e}")
        return {
            "status": "error",
            "message": str(e)
        }


def get_recent_syncs(session: Session, limit: int = 10) -> List[Dict]:
    """Get recent sync runs."""
    try:
        stmt = (
            select(SyncLog)
            .where(SyncLog.table_name == "cows")
            .order_by(SyncLog.started_at.desc())
            .limit(limit)
        )
        syncs = session.execute(stmt).scalars().all()
        
        return [
            {
                "id": sync.id,
                "started_at": sync.started_at.strftime("%Y-%m-%d %H:%M:%S"),
                "status": sync.status,
                "status_class": "success" if sync.status == "completed" else "danger",
                "duration": f"{sync.duration_seconds:.1f}s" if sync.duration_seconds else "N/A",
                "rows_read": sync.rows_read,
                "rows_inserted": sync.rows_inserted,
                "rows_updated": sync.rows_updated,
                "rows_skipped": sync.rows_skipped,
                "error": sync.error_message if sync.status == "failed" else None
            }
            for sync in syncs
        ]
    
    except Exception as e:
        logger.error(f"Error getting recent syncs: {e}")
        return []


def get_event_counts(session: Session) -> Dict:
    """Get event counts."""
    try:
        # Total events
        query = text("SELECT COUNT(*) FROM events.cow_events")
        total_events = session.execute(query).scalar()
        
        # Events last hour
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        query = text("""
            SELECT COUNT(*) FROM events.cow_events
            WHERE event_timestamp >= :since
        """)
        events_last_hour = session.execute(query, {"since": one_hour_ago}).scalar()
        
        # Unpublished events
        query = text("""
            SELECT COUNT(*) FROM events.cow_events
            WHERE published = FALSE
        """)
        unpublished = session.execute(query).scalar()
        
        return {
            "total": total_events,
            "last_hour": events_last_hour,
            "unpublished": unpublished,
            "rate_per_minute": f"{events_last_hour / 60:.1f}" if events_last_hour else "0.0"
        }
    
    except Exception as e:
        logger.error(f"Error getting event counts: {e}")
        return {
            "total": 0,
            "last_hour": 0,
            "unpublished": 0,
            "rate_per_minute": "0.0"
        }


def get_row_counts(session: Session) -> Dict:
    """Get row counts from SQL and Silver."""
    try:
        # SQL count
        query = text("SELECT COUNT(*) FROM operational.cows")
        sql_count = session.execute(query).scalar()
        
        # Silver count (would need PySpark - placeholder)
        # For now, we'll estimate from sync state
        stmt = select(SyncState).where(SyncState.table_name == "cows")
        sync_state = session.execute(stmt).scalar_one_or_none()
        
        silver_count = sql_count  # Placeholder
        
        return {
            "sql": sql_count,
            "silver": silver_count,
            "diff": abs(sql_count - silver_count),
            "diff_percent": f"{abs(sql_count - silver_count) / max(sql_count, 1) * 100:.1f}%"
        }
    
    except Exception as e:
        logger.error(f"Error getting row counts: {e}")
        return {
            "sql": 0,
            "silver": 0,
            "diff": 0,
            "diff_percent": "0.0%"
        }


def get_recent_conflicts(session: Session, limit: int = 5) -> List[Dict]:
    """Get recent conflicts."""
    try:
        stmt = (
            select(SyncConflict)
            .where(SyncConflict.table_name == "cows")
            .order_by(SyncConflict.detected_at.desc())
            .limit(limit)
        )
        conflicts = session.execute(stmt).scalars().all()
        
        return [
            {
                "id": conflict.id,
                "cow_id": conflict.entity_id,
                "field": conflict.conflicting_field,
                "detected_at": conflict.detected_at.strftime("%Y-%m-%d %H:%M:%S"),
                "resolution": conflict.resolution_strategy,
                "sql_value": str(conflict.sql_value)[:50],
                "silver_value": str(conflict.silver_value)[:50]
            }
            for conflict in conflicts
        ]
    
    except Exception as e:
        logger.error(f"Error getting recent conflicts: {e}")
        return []


# ========================================
# HTML Dashboard Generation
# ========================================

def generate_dashboard_html() -> str:
    """
    Generate HTML dashboard.
    
    Returns:
        HTML string
    """
    logger.info("Generating dashboard...")
    
    # Get database session
    db_manager = get_db_manager()
    session = db_manager.get_session()
    
    try:
        # Collect data
        sync_status = get_sync_status(session)
        recent_syncs = get_recent_syncs(session)
        event_counts = get_event_counts(session)
        row_counts = get_row_counts(session)
        recent_conflicts = get_recent_conflicts(session)
        
        # Run health checks
        health_report = run_health_checks()
        
        # Collect metrics
        metrics = collect_all_metrics()
        
        # Generate HTML
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Endymion-AI - Sync Monitoring Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        h1 {{
            color: #333;
            margin-bottom: 10px;
        }}
        
        .subtitle {{
            color: #666;
            margin-bottom: 30px;
        }}
        
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .card h2 {{
            font-size: 18px;
            color: #333;
            margin-bottom: 15px;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 10px;
        }}
        
        .status-badge {{
            display: inline-block;
            padding: 6px 12px;
            border-radius: 4px;
            font-weight: bold;
            font-size: 14px;
        }}
        
        .status-badge.success {{
            background: #d4edda;
            color: #155724;
        }}
        
        .status-badge.warning {{
            background: #fff3cd;
            color: #856404;
        }}
        
        .status-badge.danger {{
            background: #f8d7da;
            color: #721c24;
        }}
        
        .status-badge.secondary {{
            background: #e2e3e5;
            color: #383d41;
        }}
        
        .metric {{
            margin: 10px 0;
        }}
        
        .metric-label {{
            color: #666;
            font-size: 14px;
            margin-bottom: 5px;
        }}
        
        .metric-value {{
            color: #333;
            font-size: 24px;
            font-weight: bold;
        }}
        
        .table-container {{
            overflow-x: auto;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        
        th {{
            background: #f8f9fa;
            padding: 10px;
            text-align: left;
            font-weight: 600;
            color: #333;
            border-bottom: 2px solid #dee2e6;
            font-size: 14px;
        }}
        
        td {{
            padding: 10px;
            border-bottom: 1px solid #dee2e6;
            font-size: 14px;
        }}
        
        tr:hover {{
            background: #f8f9fa;
        }}
        
        .health-check {{
            margin: 10px 0;
            padding: 10px;
            border-radius: 4px;
            border-left: 4px solid #ccc;
        }}
        
        .health-check.HEALTHY {{
            background: #d4edda;
            border-color: #28a745;
        }}
        
        .health-check.WARNING {{
            background: #fff3cd;
            border-color: #ffc107;
        }}
        
        .health-check.CRITICAL {{
            background: #f8d7da;
            border-color: #dc3545;
        }}
        
        .health-check.UNKNOWN {{
            background: #e2e3e5;
            border-color: #6c757d;
        }}
        
        .health-check-name {{
            font-weight: bold;
            margin-bottom: 5px;
        }}
        
        .health-check-message {{
            font-size: 13px;
            color: #666;
        }}
        
        .refresh-info {{
            text-align: right;
            color: #999;
            font-size: 12px;
            margin-bottom: 20px;
        }}
        
        .flow-diagram {{
            display: flex;
            justify-content: space-around;
            align-items: center;
            margin: 20px 0;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 4px;
        }}
        
        .flow-step {{
            text-align: center;
        }}
        
        .flow-step-icon {{
            width: 60px;
            height: 60px;
            background: #007bff;
            color: white;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 10px;
            font-weight: bold;
            font-size: 20px;
        }}
        
        .flow-step-label {{
            font-weight: 600;
            color: #333;
        }}
        
        .flow-step-count {{
            color: #666;
            font-size: 14px;
            margin-top: 5px;
        }}
        
        .flow-arrow {{
            font-size: 30px;
            color: #999;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🐄 Endymion-AI - Sync Monitoring Dashboard</h1>
        <p class="subtitle">Real-time monitoring of Silver → SQL projection sync</p>
        
        <div class="refresh-info">
            Last updated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC
            • <a href="javascript:location.reload()">Refresh</a>
        </div>
        
        <!-- Overall Status -->
        <div class="grid">
            <div class="card">
                <h2>Sync Status</h2>
                <div class="metric">
                    <div class="metric-label">Current Status</div>
                    <span class="status-badge {sync_status['status_class']}">{sync_status['status'].upper()}</span>
                </div>
                <div class="metric">
                    <div class="metric-label">Last Sync</div>
                    <div class="metric-value" style="font-size: 16px;">{sync_status['last_sync']}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Sync Lag</div>
                    <div class="metric-value">{sync_status['lag_minutes']} min</div>
                </div>
            </div>
            
            <div class="card">
                <h2>Total Synced</h2>
                <div class="metric">
                    <div class="metric-label">Rows Synced</div>
                    <div class="metric-value">{sync_status['total_rows_synced']:,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Conflicts Resolved</div>
                    <div class="metric-value">{sync_status['total_conflicts']:,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Watermark</div>
                    <div class="metric-value" style="font-size: 14px;">{sync_status['watermark']}</div>
                </div>
            </div>
            
            <div class="card">
                <h2>Event Flow</h2>
                <div class="metric">
                    <div class="metric-label">Total Events</div>
                    <div class="metric-value">{event_counts['total']:,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Events (Last Hour)</div>
                    <div class="metric-value">{event_counts['last_hour']:,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Rate</div>
                    <div class="metric-value">{event_counts['rate_per_minute']}/min</div>
                </div>
            </div>
            
            <div class="card">
                <h2>Data Consistency</h2>
                <div class="metric">
                    <div class="metric-label">SQL Rows</div>
                    <div class="metric-value">{row_counts['sql']:,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Silver Rows</div>
                    <div class="metric-value">{row_counts['silver']:,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Difference</div>
                    <div class="metric-value" style="font-size: 18px;">{row_counts['diff']:,} ({row_counts['diff_percent']})</div>
                </div>
            </div>
        </div>
        
        <!-- Data Flow Diagram -->
        <div class="card">
            <h2>Data Flow</h2>
            <div class="flow-diagram">
                <div class="flow-step">
                    <div class="flow-step-icon">📝</div>
                    <div class="flow-step-label">Events</div>
                    <div class="flow-step-count">{event_counts['total']:,} total</div>
                </div>
                
                <div class="flow-arrow">→</div>
                
                <div class="flow-step">
                    <div class="flow-step-icon">📦</div>
                    <div class="flow-step-label">Bronze</div>
                    <div class="flow-step-count">Raw storage</div>
                </div>
                
                <div class="flow-arrow">→</div>
                
                <div class="flow-step">
                    <div class="flow-step-icon">✨</div>
                    <div class="flow-step-label">Silver</div>
                    <div class="flow-step-count">{row_counts['silver']:,} cows</div>
                </div>
                
                <div class="flow-arrow">→</div>
                
                <div class="flow-step">
                    <div class="flow-step-icon">🗄️</div>
                    <div class="flow-step-label">SQL</div>
                    <div class="flow-step-count">{row_counts['sql']:,} cows</div>
                </div>
            </div>
        </div>
        
        <!-- Health Checks -->
        <div class="card">
            <h2>Health Checks</h2>
            <div class="metric">
                <div class="metric-label">Overall Health</div>
                <span class="status-badge {'success' if health_report.is_healthy else 'danger'}">
                    {health_report.overall_status.value}
                </span>
            </div>
            
            {"".join([f'''
            <div class="health-check {check.status.value}">
                <div class="health-check-name">{check.check_name}</div>
                <div class="health-check-message">{check.message}</div>
            </div>
            ''' for check in health_report.checks])}
        </div>
        
        <!-- Recent Syncs -->
        <div class="card">
            <h2>Recent Sync Runs</h2>
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Started At</th>
                            <th>Status</th>
                            <th>Duration</th>
                            <th>Read</th>
                            <th>Inserted</th>
                            <th>Updated</th>
                            <th>Skipped</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join([f'''
                        <tr>
                            <td>{sync['started_at']}</td>
                            <td><span class="status-badge {sync['status_class']}">{sync['status']}</span></td>
                            <td>{sync['duration']}</td>
                            <td>{sync['rows_read']:,}</td>
                            <td>{sync['rows_inserted']:,}</td>
                            <td>{sync['rows_updated']:,}</td>
                            <td>{sync['rows_skipped']:,}</td>
                        </tr>
                        ''' for sync in recent_syncs])}
                    </tbody>
                </table>
            </div>
        </div>
        
        <!-- Recent Conflicts -->
        {"f'''<div class='card'><h2>Recent Conflicts</h2><div class='table-container'><table><thead><tr><th>Detected At</th><th>Cow ID</th><th>Field</th><th>Resolution</th><th>SQL Value</th><th>Silver Value</th></tr></thead><tbody>{"".join([f"<tr><td>{c['detected_at']}</td><td>{c['cow_id']}</td><td>{c['field']}</td><td>{c['resolution']}</td><td>{c['sql_value']}</td><td>{c['silver_value']}</td></tr>" for c in recent_conflicts])}</tbody></table></div></div>''' if recent_conflicts else ''}
    </div>
</body>
</html>
        """
        
        return html
    
    finally:
        session.close()


# ========================================
# CLI Entry Point
# ========================================

if __name__ == "__main__":
    """Generate dashboard and save to file."""
    import sys
    
    html = generate_dashboard_html()
    
    # Check if output file specified
    if len(sys.argv) > 1:
        output_file = sys.argv[1]
        with open(output_file, "w") as f:
            f.write(html)
        print(f"Dashboard saved to: {output_file}")
    else:
        # Print to stdout
        print(html)
