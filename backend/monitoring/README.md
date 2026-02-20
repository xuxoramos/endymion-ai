# Monitoring & Alerting System

## 🎯 Overview

Complete monitoring and alerting infrastructure for the Silver → SQL sync jobs. This system ensures the projection table stays in sync with the Silver layer and provides visibility into the data pipeline health.

## 📦 What Was Built

### 1. Health Checks (`health_check.py`)
- **650+ lines** of comprehensive health monitoring
- **5 health checks**:
  1. Sync Lag (< 5 min critical)
  2. Event Backlog (< 1000 critical)
  3. Silver Freshness (< 10 min critical)
  4. Data Consistency (< 10% mismatch critical)
  5. Recent Failures (< 50% failure rate critical)
- Exit codes for automation (0 = healthy, 1 = unhealthy)
- Detailed status messages
- Can run standalone or programmatically

### 2. Metrics Collection (`metrics.py`)
- **500+ lines** of metrics collection
- **13 Prometheus-style metrics**:
  - `endymion_ai_sync_lag_seconds` - Time since last sync
  - `endymion_ai_events_per_minute` - Event creation rate
  - `endymion_ai_sync_duration_seconds` - Sync duration
  - `endymion_ai_sync_rows_total` - Total rows synced
  - `endymion_ai_sync_conflicts_total` - Conflicts resolved
  - And 8 more detailed metrics
- Supports JSON and Prometheus formats
- Ready for Prometheus scraping

### 3. Web Dashboard (`dashboard.py`)
- **700+ lines** of dashboard generation
- **Beautiful HTML interface** with:
  - Real-time sync status
  - Event flow visualization
  - Data consistency metrics
  - Health check results
  - Recent sync runs table
  - Recent conflicts table
- Auto-refresh capability
- Mobile-responsive design

### 4. FastAPI Integration
- **3 new endpoints** added to main.py:
  - `GET /health` - Returns 200/503 based on system health
  - `GET /metrics` - Prometheus metrics export
  - `GET /dashboard` - HTML dashboard
- Kubernetes-ready health checks
- Load balancer compatible

### 5. Testing Infrastructure
- **Comprehensive test script** (`test_monitoring.sh`)
- Tests all monitoring components
- Simulates failures and recovery
- Validates endpoints

### 6. Documentation
- **Full documentation** (`MONITORING.md`) - 600+ lines
- **Quick reference** (`QUICKREF.md`)
- Examples, troubleshooting, production deployment guide

## 📁 File Structure

```
backend/monitoring/
├── __init__.py              # Package initialization
├── health_check.py          # Health checks (650+ lines)
├── metrics.py               # Metrics collection (500+ lines)
├── dashboard.py             # HTML dashboard (700+ lines)
├── test_monitoring.sh       # Test script (executable)
├── MONITORING.md            # Full documentation
├── QUICKREF.md              # Quick reference
└── README.md                # This file
```

## 🚀 Quick Start

### Prerequisites

```bash
# Install dependencies
pip install sqlalchemy pyodbc pyspark

# Make sure sync scheduler is running
python -m backend.jobs.sync_scheduler &
```

### Run Health Checks

```bash
cd /home/xuxoramos/endymion-ai
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
```

### Collect Metrics

```bash
# JSON format
python -m backend.monitoring.metrics

# Prometheus format
python -m backend.monitoring.metrics --prometheus
```

### Generate Dashboard

```bash
# Generate and save
python -m backend.monitoring.dashboard dashboard.html

# Open in browser
xdg-open dashboard.html
```

### Use API Endpoints

```bash
# Start FastAPI (if not running)
uvicorn backend.api.main:app --reload

# Health check
curl http://localhost:8000/health | jq

# Metrics
curl http://localhost:8000/metrics

# Dashboard
curl http://localhost:8000/dashboard > dashboard.html
```

## 🧪 Testing

Run the comprehensive test suite:

```bash
./backend/monitoring/test_monitoring.sh
```

This will:
1. ✅ Test health checks with sync running
2. ✅ Test metrics collection (JSON + Prometheus)
3. ✅ Test dashboard generation
4. ✅ Stop sync and verify detection (waits 6 minutes)
5. ✅ Restart sync and verify recovery
6. ✅ Test all FastAPI endpoints

## 🔔 Alerting Setup

### Option 1: Cron Job

```bash
# Add to crontab (check every 5 minutes)
*/5 * * * * cd /home/xuxoramos/endymion-ai && python -m backend.monitoring.health_check || echo "ALERT: Sync unhealthy" | mail -s "Endymion-AI Alert" ops@company.com
```

### Option 2: Prometheus

Configure Prometheus to scrape metrics:

```yaml
scrape_configs:
  - job_name: 'endymion-ai-sync'
    scrape_interval: 30s
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
```

Set up alerting rules:

```yaml
groups:
  - name: endymion_ai_alerts
    rules:
      - alert: SyncLagHigh
        expr: endymion_ai_sync_lag_seconds > 300
        for: 5m
        labels:
          severity: critical
```

### Option 3: Kubernetes Probes

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 30
  periodSeconds: 30

readinessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 10
```

## 📊 What Each Component Does

### Health Checks
- Monitors sync job health in real-time
- Detects when sync falls behind
- Identifies data consistency issues
- Tracks failure rates
- **Use for**: Alerting, automation, troubleshooting

### Metrics
- Collects quantitative data about sync operations
- Tracks performance over time
- Enables trend analysis
- **Use for**: Grafana dashboards, capacity planning, SLO monitoring

### Dashboard
- Visual interface for operators
- Shows current system state
- Displays recent activity
- **Use for**: Operations team visibility, debugging, demonstrations

## 🎯 Key Thresholds

| Check | Warning | Critical |
|-------|---------|----------|
| **Sync Lag** | 2 minutes | 5 minutes |
| **Event Backlog** | 500 events | 1000 events |
| **Silver Freshness** | 5 minutes | 10 minutes |
| **Data Mismatch** | 5% | 10% |
| **Failure Rate** | 30% | 50% |

## 🛠️ Troubleshooting

### Health Check Reports CRITICAL

**Problem: Sync lag > 5 minutes**
```bash
# Check if sync scheduler is running
pgrep -f sync_scheduler

# View logs
tail -f /tmp/sync_scheduler.log

# Restart
python -m backend.jobs.sync_scheduler &
```

**Problem: High event backlog**
```bash
# Check unpublished events
docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai \
  -Q "SELECT COUNT(*) FROM events.cow_events WHERE published = 0;"

# Trigger manual sync
python -m backend.jobs.sync_silver_to_sql
```

**Problem: Data consistency mismatch**
```bash
# Check row counts
docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai \
  -Q "SELECT COUNT(*) FROM operational.cows;"

# View recent conflicts
docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai \
  -Q "SELECT TOP 10 * FROM sync.sync_conflicts ORDER BY detected_at DESC;"
```

### Metrics Not Updating

1. Check database connection:
   ```bash
   docker exec -it cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
     -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai
   ```

2. Verify sync_state table:
   ```bash
   docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
     -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai \
     -Q "SELECT TOP 50 * FROM sync.sync_state ORDER BY updated_at DESC;"
   ```

3. Check for errors:
   ```bash
   python -m backend.monitoring.metrics 2>&1 | grep -i error
   ```

## 🎓 How It Works

### Data Flow

```
┌─────────────┐
│   Events    │ ← API writes create events
└──────┬──────┘
       ↓
┌─────────────┐
│   Bronze    │ ← Raw event storage
└──────┬──────┘
       ↓
┌─────────────┐
│   Silver    │ ← Cleaned, deduplicated
└──────┬──────┘
       ↓
┌─────────────┐
│     SQL     │ ← Projection for reads
└─────────────┘
       ↑
       │
┌──────┴──────┐
│  Monitoring │ ← Health checks, metrics, dashboard
└─────────────┘
```

### Health Check Flow

1. **Query sync_state table** → Get last sync timestamp
2. **Calculate lag** → Compare with current time
3. **Query cow_events** → Count unpublished events
4. **Query Silver Delta table** → Check last update
5. **Compare row counts** → SQL vs Silver
6. **Analyze sync_logs** → Check failure rate
7. **Aggregate results** → Overall health status
8. **Send alerts** → If unhealthy

## 📈 Production Deployment

### Recommended Setup

1. **Health Monitoring**
   - Configure load balancer to use `/health` endpoint
   - Set up Kubernetes liveness/readiness probes
   - Use 503 responses to remove unhealthy instances

2. **Metrics Collection**
   - Prometheus scrapes `/metrics` every 30s
   - Configure 30-day retention
   - Set up Grafana dashboards

3. **Alerting**
   - Prometheus alerting rules for critical metrics
   - PagerDuty for critical alerts
   - Email for warnings

4. **Dashboard**
   - Host `/dashboard` for operations team
   - Auto-refresh every 30 seconds
   - Authentication middleware

## 🌟 Features

✅ **5 comprehensive health checks** covering all aspects of sync system  
✅ **13 Prometheus metrics** for detailed monitoring  
✅ **Beautiful HTML dashboard** with real-time status  
✅ **FastAPI integration** with /health, /metrics, /dashboard endpoints  
✅ **Alerting support** via exit codes, HTTP status codes, and metrics  
✅ **Test suite** with automated failure/recovery scenarios  
✅ **Production-ready** with Kubernetes probes and Prometheus support  
✅ **Comprehensive documentation** with examples and troubleshooting  

## 📚 Documentation

- **[MONITORING.md](MONITORING.md)** - Full documentation (600+ lines)
- **[QUICKREF.md](QUICKREF.md)** - Quick reference card
- **[README.md](README.md)** - This file

## 🎉 Summary

This monitoring system provides **complete observability** for the Silver → SQL sync jobs:

- **Know when things break** - Health checks detect issues immediately
- **Understand trends** - Metrics show performance over time
- **Visual visibility** - Dashboard provides operational awareness
- **Automated alerting** - Integrate with existing monitoring tools
- **Production-ready** - Tested, documented, and Kubernetes-compatible

The sync job is now **production-ready** with comprehensive monitoring! 🚀

## 📞 Support

For issues or questions:
1. Check [MONITORING.md](MONITORING.md) for detailed documentation
2. Run health checks: `python -m backend.monitoring.health_check`
3. View dashboard: `python -m backend.monitoring.dashboard dashboard.html`
4. Check sync logs: `SELECT * FROM sync.sync_logs ORDER BY started_at DESC LIMIT 10;`
