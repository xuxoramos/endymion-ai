# Monitoring System Documentation

## Overview

The monitoring system provides comprehensive observability for the Silver → SQL sync jobs that maintain the projection table in sync with the Silver layer.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Monitoring Components                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Health     │  │   Metrics    │  │  Dashboard   │      │
│  │   Checks     │  │  Collection  │  │  Generation  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         │                  │                  │              │
│         └──────────────────┴──────────────────┘              │
│                            │                                 │
│                            ▼                                 │
│                  ┌──────────────────┐                        │
│                  │   FastAPI        │                        │
│                  │   Endpoints      │                        │
│                  └──────────────────┘                        │
│                     /health                                  │
│                     /metrics                                 │
│                     /dashboard                               │
└─────────────────────────────────────────────────────────────┘
```

## Components

### 1. Health Checks (`health_check.py`)

Monitors the health and status of sync jobs.

#### Health Checks Performed

1. **Sync Lag Check**
   - Measures: Time since last successful sync
   - WARNING: > 2 minutes
   - CRITICAL: > 5 minutes
   - Why: Detects when sync scheduler stops or hangs

2. **Event Backlog Check**
   - Measures: Number of unpublished events in `cow_events` table
   - WARNING: > 500 events
   - CRITICAL: > 1000 events
   - Why: Detects when events pile up without being processed

3. **Silver Freshness Check**
   - Measures: Time since Silver layer was last updated
   - WARNING: > 5 minutes
   - CRITICAL: > 10 minutes
   - Why: Detects issues with Bronze → Silver pipeline

4. **Data Consistency Check**
   - Measures: Row count difference between SQL and Silver
   - WARNING: > 5% difference
   - CRITICAL: > 10% difference
   - Why: Detects data drift between source and projection

5. **Recent Failures Check**
   - Measures: Failure rate in last 10 sync runs
   - WARNING: > 30% failure rate
   - CRITICAL: > 50% failure rate
   - Why: Detects persistent sync issues

#### Usage

**Standalone:**
```bash
# Run health checks (exits with 0 if healthy, 1 if unhealthy)
python -m backend.monitoring.health_check

# Output:
# 🏥 Endymion-AI Sync Health Check
# ================================
# 
# Overall Status: HEALTHY ✅
# 
# Individual Checks:
# ------------------
# 
# 1. Sync Lag Check
#    Status: HEALTHY ✅
#    Sync lag: 1.2 minutes
# 
# 2. Event Backlog Check
#    Status: HEALTHY ✅
#    Unpublished events: 42
# ...
```

**Programmatic:**
```python
from backend.monitoring.health_check import run_health_checks

report = run_health_checks()

print(f"Overall: {report.overall_status}")  # HEALTHY, WARNING, CRITICAL
print(f"Is Healthy: {report.is_healthy}")   # True/False

for check in report.checks:
    print(f"{check.check_name}: {check.status}")
    print(f"  {check.message}")
```

#### Configuration

Edit thresholds in `HealthCheckConfig` class:

```python
class HealthCheckConfig:
    # Sync lag thresholds (seconds)
    SYNC_LAG_WARNING_SECONDS = 120     # 2 minutes
    SYNC_LAG_CRITICAL_SECONDS = 300    # 5 minutes
    
    # Event backlog thresholds
    EVENT_BACKLOG_WARNING = 500
    EVENT_BACKLOG_CRITICAL = 1000
    
    # Silver freshness thresholds (seconds)
    SILVER_FRESHNESS_WARNING_SECONDS = 300   # 5 minutes
    SILVER_FRESHNESS_CRITICAL_SECONDS = 600  # 10 minutes
    
    # Row count mismatch thresholds (percentage)
    ROW_COUNT_MISMATCH_WARNING = 5.0    # 5%
    ROW_COUNT_MISMATCH_CRITICAL = 10.0  # 10%
    
    # Failure rate thresholds (percentage)
    FAILURE_RATE_WARNING = 30.0   # 30%
    FAILURE_RATE_CRITICAL = 50.0  # 50%
```

### 2. Metrics Collection (`metrics.py`)

Collects Prometheus-style metrics for monitoring and alerting.

#### Metrics Exported

1. **`endymion_ai_sync_lag_seconds`** (gauge)
   - Time since last successful sync
   - Labels: `table="cows"`

2. **`endymion_ai_events_per_minute`** (gauge)
   - Number of events created per minute
   - Labels: `event_type="cow_events"`

3. **`endymion_ai_sync_duration_seconds`** (gauge)
   - Average sync duration (last 10 runs)
   - Labels: `table="cows"`, `stat="avg"`

4. **`endymion_ai_sync_duration_seconds_last`** (gauge)
   - Last sync duration
   - Labels: `table="cows"`

5. **`endymion_ai_sync_rows_total`** (counter)
   - Total rows synced since tracking began
   - Labels: `table="cows"`

6. **`endymion_ai_sync_conflicts_total`** (counter)
   - Total conflicts resolved
   - Labels: `table="cows"`

7. **`endymion_ai_sync_rows_read_last`** (gauge)
   - Rows read in last sync
   - Labels: `table="cows"`

8. **`endymion_ai_sync_rows_inserted_last`** (gauge)
   - Rows inserted in last sync
   - Labels: `table="cows"`

9. **`endymion_ai_sync_rows_updated_last`** (gauge)
   - Rows updated in last sync
   - Labels: `table="cows"`

10. **`endymion_ai_sync_rows_skipped_last`** (gauge)
    - Rows skipped in last sync
    - Labels: `table="cows"`

11. **`endymion_ai_sql_row_count`** (gauge)
    - Number of rows in SQL projection table
    - Labels: `table="cows"`, `schema="operational"`

12. **`endymion_ai_sync_failure_rate`** (gauge)
    - Sync failure rate (last 20 runs, percentage)
    - Labels: `table="cows"`

13. **`endymion_ai_sync_failures_total`** (gauge)
    - Number of failures in last 20 runs
    - Labels: `table="cows"`

#### Usage

**JSON Format:**
```bash
python -m backend.monitoring.metrics

# Output:
# {
#   "timestamp": "2024-01-15T10:30:00.000Z",
#   "metrics": {
#     "endymion_ai_sync_lag_seconds": {
#       "value": 72.5,
#       "help": "Time since last successful sync",
#       "type": "gauge",
#       "labels": {"table": "cows"}
#     },
#     ...
#   }
# }
```

**Prometheus Format:**
```bash
python -m backend.monitoring.metrics --prometheus

# Output:
# # HELP endymion_ai_sync_lag_seconds Time since last successful sync
# # TYPE endymion_ai_sync_lag_seconds gauge
# endymion_ai_sync_lag_seconds{table="cows"} 72.5
# 
# # HELP endymion_ai_events_per_minute Number of events created per minute
# # TYPE endymion_ai_events_per_minute gauge
# endymion_ai_events_per_minute{event_type="cow_events"} 12.0
# ...
```

**Programmatic:**
```python
from backend.monitoring.metrics import collect_all_metrics

collector = collect_all_metrics()

# Get Prometheus format
prometheus_text = collector.to_prometheus()

# Get JSON format
json_data = collector.to_json()
```

### 3. Dashboard (`dashboard.py`)

Generates a comprehensive HTML dashboard for visual monitoring.

#### Dashboard Features

1. **Overall Status**
   - Current sync status (HEALTHY/WARNING/CRITICAL)
   - Last sync timestamp
   - Sync lag in minutes
   - Total rows synced
   - Total conflicts resolved

2. **Event Flow Metrics**
   - Total events in system
   - Events created in last hour
   - Events per minute rate
   - Unpublished events count

3. **Data Consistency**
   - SQL row count
   - Silver row count
   - Row count difference
   - Percentage difference

4. **Data Flow Visualization**
   - Visual diagram: Events → Bronze → Silver → SQL
   - Shows counts at each stage

5. **Health Checks**
   - All 5 health checks with status
   - Overall system health
   - Detailed messages for each check

6. **Recent Sync Runs**
   - Last 10 sync executions
   - Duration, status, row counts
   - Error messages for failures

7. **Recent Conflicts**
   - Last 5 conflicts detected
   - Conflicting fields
   - SQL vs Silver values
   - Resolution strategy

#### Usage

**Generate Standalone HTML:**
```bash
python -m backend.monitoring.dashboard /tmp/dashboard.html

# Opens in browser:
xdg-open /tmp/dashboard.html
```

**Programmatic:**
```python
from backend.monitoring.dashboard import generate_dashboard_html

html = generate_dashboard_html()

# Save to file
with open("dashboard.html", "w") as f:
    f.write(html)
```

## FastAPI Endpoints

### GET `/health`

Comprehensive health check endpoint.

**Returns:**
- `200 OK` if system is healthy (sync lag < 5 minutes)
- `503 Service Unavailable` if system is unhealthy

**Response:**
```json
{
  "status": "healthy",
  "is_healthy": true,
  "timestamp": "2024-01-15T10:30:00.000Z",
  "database": "connected",
  "version": "1.0.0",
  "checks": [
    {
      "name": "Sync Lag Check",
      "status": "HEALTHY",
      "message": "Sync lag: 1.2 minutes"
    },
    {
      "name": "Event Backlog Check",
      "status": "HEALTHY",
      "message": "Unpublished events: 42"
    },
    ...
  ]
}
```

**Use Cases:**
- Load balancer health checks
- Kubernetes liveness/readiness probes
- External monitoring systems

### GET `/metrics`

Prometheus metrics endpoint.

**Returns:**
- Metrics in Prometheus text format

**Response:**
```text
# HELP endymion_ai_sync_lag_seconds Time since last successful sync
# TYPE endymion_ai_sync_lag_seconds gauge
endymion_ai_sync_lag_seconds{table="cows"} 72.5

# HELP endymion_ai_events_per_minute Number of events created per minute
# TYPE endymion_ai_events_per_minute gauge
endymion_ai_events_per_minute{event_type="cow_events"} 12.0
...
```

**Use Cases:**
- Prometheus scraping
- Grafana dashboards
- Custom alerting rules

### GET `/dashboard`

Web dashboard endpoint.

**Returns:**
- HTML dashboard

**Use Cases:**
- Quick visual monitoring
- Operational visibility
- Debugging sync issues

## Alerting Setup

### 1. Cron-based Alerting

Run health checks periodically and alert on failures:

```bash
# Add to crontab (every 5 minutes)
*/5 * * * * /home/xuxoramos/endymion-ai/backend/monitoring/health_check.py || echo "ALERT: Sync health check failed" | mail -s "Endymion-AI Alert" ops@example.com
```

### 2. Systemd Service with Alerting

Create a systemd service that monitors health:

```ini
[Unit]
Description=Endymion-AI Health Monitor
After=network.target

[Service]
Type=simple
User=xuxoramos
WorkingDirectory=/home/xuxoramos/endymion-ai
ExecStart=/usr/bin/python3 -m backend.monitoring.health_check
Restart=always
RestartSec=300

[Install]
WantedBy=multi-user.target
```

### 3. Prometheus Alerting Rules

Configure Prometheus to alert on metrics:

```yaml
groups:
  - name: endymion_ai_sync_alerts
    rules:
      - alert: SyncLagHigh
        expr: endymion_ai_sync_lag_seconds > 300
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Sync lag is high ({{ $value }}s)"
          description: "Sync lag exceeds 5 minutes"
      
      - alert: EventBacklogHigh
        expr: endymion_ai_events_per_minute > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Event backlog is high ({{ $value }} events)"
          description: "More than 1000 unpublished events"
      
      - alert: SyncFailureRateHigh
        expr: endymion_ai_sync_failure_rate > 50
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "Sync failure rate is high ({{ $value }}%)"
          description: "More than 50% of syncs are failing"
```

### 4. PagerDuty Integration

Use the health check endpoint with PagerDuty:

```python
import requests
from pypd import EventV2

def check_and_alert():
    response = requests.get("http://localhost:8000/health")
    
    if response.status_code != 200:
        # Trigger PagerDuty incident
        EventV2.create(
            routing_key='YOUR_ROUTING_KEY',
            event_action='trigger',
            payload={
                'summary': 'Endymion-AI Sync Unhealthy',
                'severity': 'critical',
                'source': 'endymion-ai-monitoring',
                'custom_details': response.json()
            }
        )

# Run every 5 minutes
```

## Testing

### Full Test Suite

Run the comprehensive test script:

```bash
cd /home/xuxoramos/endymion-ai
./backend/monitoring/test_monitoring.sh
```

This will:
1. ✅ Test health checks with sync running
2. ✅ Test metrics collection (JSON and Prometheus formats)
3. ✅ Test dashboard generation
4. ✅ Stop sync and verify detection (waits 6 minutes)
5. ✅ Restart sync and verify recovery
6. ✅ Test FastAPI endpoints (/health, /metrics, /dashboard)

### Manual Testing

**1. Test health checks:**
```bash
python -m backend.monitoring.health_check
echo "Exit code: $?"  # 0 = healthy, 1 = unhealthy
```

**2. Test metrics:**
```bash
# JSON format
python -m backend.monitoring.metrics > metrics.json

# Prometheus format
python -m backend.monitoring.metrics --prometheus > metrics.prom
```

**3. Test dashboard:**
```bash
python -m backend.monitoring.dashboard dashboard.html
xdg-open dashboard.html
```

**4. Test API endpoints:**
```bash
# Health check
curl http://localhost:8000/health | jq

# Metrics
curl http://localhost:8000/metrics

# Dashboard
curl http://localhost:8000/dashboard > dashboard.html
```

## Troubleshooting

### Health Check Reports CRITICAL

**Sync Lag > 5 minutes:**
1. Check if sync scheduler is running: `pgrep -f sync_scheduler`
2. Check scheduler logs: `tail -f /tmp/sync_scheduler.log`
3. Restart scheduler: `python -m backend.jobs.sync_scheduler &`

**Event Backlog > 1000:**
1. Check Bronze → Silver pipeline
2. Verify PySpark job is running
3. Check for errors in Bronze processing

**Data Consistency Mismatch > 10%:**
1. Run manual sync: `python -m backend.jobs.sync_silver_to_sql`
2. Check for conflicts: `SELECT TOP 10 * FROM sync.sync_conflicts ORDER BY detected_at DESC;`
3. Verify Silver table integrity

**High Failure Rate:**
1. Check recent failures: `SELECT TOP 10 * FROM sync.sync_logs WHERE status = 'failed' ORDER BY started_at DESC;`
2. Analyze error messages
3. Fix underlying issues and restart scheduler

### Metrics Not Updating

1. Verify database connectivity: `docker exec -it cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai`
2. Check if sync_state table has data: `SELECT * FROM sync.sync_state;`
3. Verify sync_logs table: `SELECT COUNT(*) FROM sync.sync_logs;`

### Dashboard Not Loading

1. Check if monitoring modules are importable:
   ```bash
   python -c "from backend.monitoring import health_check, metrics, dashboard; print('OK')"
   ```

2. Check for missing dependencies:
   ```bash
   pip install sqlalchemy pyodbc pyspark
   ```

3. Verify database connection string in environment variables

## Production Deployment

### Recommended Setup

1. **Health Monitoring:**
   - Configure load balancer to use `/health` endpoint
   - Set up Kubernetes liveness/readiness probes
   - Use 503 responses to remove unhealthy instances

2. **Metrics Collection:**
   - Set up Prometheus to scrape `/metrics` endpoint every 30s
   - Configure retention for at least 30 days
   - Set up Grafana dashboards for visualization

3. **Alerting:**
   - Configure Prometheus alerting rules (see examples above)
   - Set up PagerDuty or Opsgenie for critical alerts
   - Email notifications for warning-level issues

4. **Dashboard:**
   - Host `/dashboard` endpoint for operations team
   - Set up auto-refresh every 30 seconds
   - Restrict access with authentication middleware

### Sample Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'endymion-ai-sync'
    scrape_interval: 30s
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
```

### Sample Grafana Dashboard

Import the following panels:
1. **Sync Lag** - Gauge showing current lag
2. **Events per Minute** - Line graph
3. **Sync Duration** - Line graph with avg and last
4. **Row Counts** - Gauge for SQL and Silver
5. **Failure Rate** - Gauge with red/yellow/green zones

## Files Overview

```
backend/monitoring/
├── __init__.py                # Package initialization
├── health_check.py            # Health checks (650+ lines)
├── metrics.py                 # Metrics collection (500+ lines)
├── dashboard.py               # HTML dashboard (700+ lines)
├── test_monitoring.sh         # Test script (executable)
└── MONITORING.md             # This documentation
```

## Summary

The monitoring system provides:

✅ **Comprehensive health checks** - 5 different checks covering all aspects
✅ **Prometheus metrics** - 13 metrics for detailed monitoring
✅ **Visual dashboard** - HTML dashboard with real-time status
✅ **FastAPI integration** - /health, /metrics, /dashboard endpoints
✅ **Alerting support** - Exit codes, HTTP status codes, and metrics
✅ **Testing tools** - Full test suite with automated scenarios

This ensures production-ready monitoring for the Silver → SQL sync system! 🚀
