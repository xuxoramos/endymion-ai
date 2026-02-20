# Monitoring Quick Reference

## Health Checks

```bash
# Run health checks
python -m backend.monitoring.health_check

# Exit codes:
# 0 = healthy
# 1 = unhealthy
```

## Metrics

```bash
# JSON format
python -m backend.monitoring.metrics

# Prometheus format
python -m backend.monitoring.metrics --prometheus
```

## Dashboard

```bash
# Generate dashboard
python -m backend.monitoring.dashboard dashboard.html

# View in browser
xdg-open dashboard.html
```

## API Endpoints

```bash
# Health check (200 = healthy, 503 = unhealthy)
curl http://localhost:8000/health | jq

# Metrics
curl http://localhost:8000/metrics

# Dashboard
curl http://localhost:8000/dashboard > dashboard.html
```

## Thresholds

| Check | Warning | Critical |
|-------|---------|----------|
| Sync Lag | 2 min | 5 min |
| Event Backlog | 500 | 1000 |
| Silver Freshness | 5 min | 10 min |
| Data Mismatch | 5% | 10% |
| Failure Rate | 30% | 50% |

## Troubleshooting

**Sync lag high?**
```bash
# Check if running
pgrep -f sync_scheduler

# Restart
python -m backend.jobs.sync_scheduler &
```

**Health check fails?**
```bash
# View recent syncs
docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai \
  -Q "SELECT TOP 5 * FROM sync.sync_logs ORDER BY started_at DESC;"

# View sync state
docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -C -d endymion_ai \
  -Q "SELECT * FROM sync.sync_state WHERE table_name = 'cows';"
```

## Alerting

**Cron job (every 5 min):**
```bash
*/5 * * * * python -m backend.monitoring.health_check || echo "ALERT" | mail -s "Sync Alert" ops@company.com
```

**Prometheus rules:**
```yaml
- alert: SyncLagHigh
  expr: endymion_ai_sync_lag_seconds > 300
  for: 5m
  labels:
    severity: critical
```

## Test Suite

```bash
# Run full test
./backend/monitoring/test_monitoring.sh
```

## Files

```
backend/monitoring/
├── health_check.py      # 5 health checks
├── metrics.py           # 13 Prometheus metrics
├── dashboard.py         # HTML dashboard
├── test_monitoring.sh   # Test script
├── MONITORING.md        # Full docs
└── QUICKREF.md         # This file
```
