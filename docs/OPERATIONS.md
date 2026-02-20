# Endymion-AI Operations Guide

Complete guide for deploying, monitoring, and operating Endymion-AI in production.

## Table of Contents

1. [Production Deployment](#production-deployment)
2. [Health Checks](#health-checks)
3. [Monitoring](#monitoring)
4. [Backup & Restore](#backup--restore)
5. [Scaling Considerations](#scaling-considerations)
6. [Disaster Recovery](#disaster-recovery)
7. [Security](#security)
8. [Performance Tuning](#performance-tuning)

---

## 1. Production Deployment

### Architecture Overview

```mermaid
graph TB
    subgraph "Frontend Tier"
        LB[Load Balancer]
        FE1[Frontend Pod 1]
        FE2[Frontend Pod 2]
    end
    
    subgraph "Application Tier"
        API1[FastAPI Pod 1]
        API2[FastAPI Pod 2]
        SYNC[Sync Job Pod]
    end
    
-- Refresh statistics
UPDATE STATISTICS events.cow_events;
UPDATE STATISTICS operational.cows;
EXEC sp_updatestats;

    subgraph "Data Tier"
      SQL[(SQL Server<br/>Primary)]
      SQL_REPLICA[(SQL Server<br/>Replica)]
      DELTA[Delta Lake<br/>S3/HDFS]
    end
    
    LB --> FE1
    LB --> FE2
ALTER INDEX ALL ON operational.cows REBUILD WITH (FILLFACTOR = 90, ONLINE = ON);
```
    SYNC --> SQL
    SYNC --> DELTA
    SQL --> SQL_REPLICA
```

### Kubernetes Deployment

#### Namespace Setup

```bash
kubectl create namespace endymion-ai
kubectl config set-context --current --namespace=endymion-ai
```

#### ConfigMap

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: endymion-ai-config
  namespace: endymion-ai
data:
  DATABASE_HOST: "sqlserver-service"
  DATABASE_PORT: "1433"
  DATABASE_NAME: "endymion_ai"
  DATABASE_USER: "sa"
  DELTA_LAKE_PATH: "s3://endymion-ai-delta-lake"
  SYNC_INTERVAL_SECONDS: "30"
  LOG_LEVEL: "INFO"
```

#### Secrets

```bash
# Create database credentials secret
kubectl create secret generic endymion-ai-secrets \
  --from-literal=database-password='<strong-password>' \
  --from-literal=jwt-secret='<jwt-secret>' \
  --namespace=endymion-ai
```

#### SQL Server StatefulSet

```yaml
# k8s/sqlserver.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sqlserver
  namespace: endymion-ai
spec:
  serviceName: sqlserver-service
  replicas: 1
  selector:
    matchLabels:
      app: sqlserver
  template:
    metadata:
      labels:
        app: sqlserver
    spec:
      containers:
      - name: sqlserver
        image: mcr.microsoft.com/mssql/server:2022-latest
        ports:
        - containerPort: 1433
        env:
        - name: ACCEPT_EULA
          value: "Y"
        - name: SA_PASSWORD
          valueFrom:
            secretKeyRef:
              name: endymion-ai-secrets
              key: database-password
        volumeMounts:
        - name: sqlserver-storage
          mountPath: /var/opt/mssql
  volumeClaimTemplates:
  - metadata:
      name: sqlserver-storage
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: "fast-ssd"
      resources:
        requests:
          storage: 100Gi
```

#### FastAPI Deployment

```yaml
# k8s/fastapi.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fastapi
  namespace: endymion-ai
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fastapi
  template:
    metadata:
      labels:
        app: fastapi
    spec:
      containers:
      - name: fastapi
        image: endymion-ai/fastapi:latest
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          value: "mssql+pyodbc://$(DATABASE_USER):$(DATABASE_PASSWORD)@$(DATABASE_HOST):$(DATABASE_PORT)/$(DATABASE_NAME)?driver=ODBC+Driver+18+for+SQL+Server"
        - name: DATABASE_USER
          valueFrom:
            configMapKeyRef:
              name: endymion-ai-config
              key: DATABASE_USER
        - name: DATABASE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: endymion-ai-secrets
              key: database-password
        - name: DATABASE_HOST
          valueFrom:
            configMapKeyRef:
              name: endymion-ai-config
              key: DATABASE_HOST
        - name: DATABASE_PORT
          valueFrom:
            configMapKeyRef:
              name: endymion-ai-config
              key: DATABASE_PORT
        - name: DATABASE_NAME
          valueFrom:
            configMapKeyRef:
              name: endymion-ai-config
              key: DATABASE_NAME
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /api/health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /api/health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: fastapi-service
  namespace: endymion-ai
spec:
  selector:
    app: fastapi
  ports:
  - protocol: TCP
    port: 8000
    targetPort: 8000
  type: ClusterIP
```

#### Sync Job CronJob

```yaml
# k8s/sync-job.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: sync-job
  namespace: endymion-ai
spec:
  schedule: "*/1 * * * *"  # Every minute
              value: "mssql+pyodbc://$(DATABASE_USER):$(DATABASE_PASSWORD)@$(DATABASE_HOST):$(DATABASE_PORT)/$(DATABASE_NAME)?driver=ODBC+Driver+18+for+SQL+Server"
    spec:
      template:
        spec:
          containers:
          - name: sync-job
            image: endymion-ai/sync:latest
            env:
            - name: DATABASE_URL
              value: "mssql+pyodbc://$(DATABASE_USER):$(DATABASE_PASSWORD)@$(DATABASE_HOST):$(DATABASE_PORT)/$(DATABASE_NAME)?driver=ODBC+Driver+18+for+SQL+Server"
            # ... (same env vars as FastAPI)
          restartPolicy: OnFailure
```

#### Frontend Deployment

```yaml
# k8s/frontend.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: frontend
  namespace: endymion-ai
spec:
  replicas: 2
  selector:
    matchLabels:
      app: frontend
  template:
    metadata:
      labels:
        app: frontend
    spec:
      containers:
      - name: frontend
        image: endymion-ai/frontend:latest
        ports:
        - containerPort: 3000
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: frontend-service
  namespace: endymion-ai
spec:
  selector:
    app: frontend
  ports:
  - protocol: TCP
    port: 80
    targetPort: 3000
  type: LoadBalancer
```

#### Ingress

```yaml
# k8s/ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: endymion-ai-ingress
  namespace: endymion-ai
  annotations:
    kubernetes.io/ingress.class: nginx
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  tls:
  - hosts:
    - endymion-ai.example.com
    secretName: endymion-ai-tls
  rules:
  - host: endymion-ai.example.com
    http:
      paths:
      - path: /api
        pathType: Prefix
        backend:
          service:
            name: fastapi-service
            port:
              number: 8000
      - path: /
        pathType: Prefix
        backend:
          service:
            name: frontend-service
            port:
              number: 80
```

### Docker Compose (Simple Deployment)

```yaml
# docker-compose.yml
version: '3.8'

services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      SA_PASSWORD: ${DB_PASSWORD}
    volumes:
      - sqlserver-data:/var/opt/mssql
    ports:
      - "1433:1433"
    healthcheck:
      test:
        ["CMD-SHELL", "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${DB_PASSWORD} -Q 'SELECT 1'"]
      interval: 10s
      timeout: 5s
      retries: 5

  fastapi:
    build: ./backend
    environment:
      DATABASE_URL: mssql+pyodbc://sa:${DB_PASSWORD}@sqlserver:1433/endymion_ai?driver=ODBC+Driver+18+for+SQL+Server
      DELTA_LAKE_PATH: /data/delta_lake
    volumes:
      - delta-lake:/data/delta_lake
    ports:
      - "8000:8000"
    depends_on:
      sqlserver:
        condition: service_healthy

  sync-job:
    build: ./backend
    command: python sync/sync_scheduler.py
    environment:
      DATABASE_URL: mssql+pyodbc://sa:${DB_PASSWORD}@sqlserver:1433/endymion_ai?driver=ODBC+Driver+18+for+SQL+Server
      DELTA_LAKE_PATH: /data/delta_lake
      SYNC_INTERVAL_SECONDS: 30
    volumes:
      - delta-lake:/data/delta_lake
    depends_on:
      - sqlserver
      - fastapi

  frontend:
    build: ./frontend
    ports:
      - "3000:3000"
    depends_on:
      - fastapi

volumes:
  sqlserver-data:
  delta-lake:
```

After the SQL Server container is healthy, run `python init_db.py` once to create schemas and seed reference data.

---

## 2. Health Checks

### Application Health Endpoints

#### API Health Check

```bash
curl http://localhost:8000/api/health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-29T16:20:00Z",
  "checks": {
    "database": {
      "status": "ok",
      "latency_ms": 5
    },
    "event_store": {
      "status": "ok",
      "total_events": 1523,
      "unpublished_events": 3
    },
    "delta_lake": {
      "status": "ok",
      "bronze_accessible": true,
      "silver_accessible": true,
      "gold_accessible": true
    }
  },
  "version": "1.0.0"
}
```

#### Database Health Check

```bash
# SQL Server connectivity
sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d endymion_ai -Q "SELECT 1";

# Check active connections
sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d endymion_ai -Q "
  SELECT COUNT(*) AS active_sessions
  FROM sys.dm_exec_sessions
  WHERE is_user_process = 1;
"

# Check replication / failover sync state (if using availability groups)
sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d master -Q "
  SELECT replica_server_name,
         synchronization_state_desc,
         database_state_desc,
         redo_queue_size
  FROM sys.dm_hadr_database_replica_states;
"
```

#### Sync Job Health Check

```bash
# Check last sync time
curl http://localhost:8000/api/sync/status | jq .

# Check if sync lag is acceptable
SYNC_LAG=$(curl -s http://localhost:8000/api/sync/status | jq -r '.sync_lag_seconds')
if [ "$SYNC_LAG" -gt 60 ]; then
    echo "WARNING: Sync lag is $SYNC_LAG seconds"
fi
```

### Automated Health Monitoring Script

```bash
#!/bin/bash
# health-check.sh

set -e

API_URL="http://localhost:8000/api"
ALERT_EMAIL="ops@example.com"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
SQLCMD=${SQLCMD:-/opt/mssql-tools/bin/sqlcmd}
SA_PASSWORD=${SA_PASSWORD:-"StrongP@ssw0rd"}

echo "🏥 Endymion-AI Health Check"
echo "=========================="

# 1. Check API
echo -n "Checking API... "
if curl -s -f "$API_URL/health" > /dev/null; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "API health check failed" | mail -s "ALERT: Endymion-AI API Down" $ALERT_EMAIL
    exit 1
fi

# 2. Check Database
echo -n "Checking Database... "
if $SQLCMD -S localhost -U sa -P "$SA_PASSWORD" -d endymion_ai -Q "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "Database check failed" | mail -s "ALERT: Endymion-AI DB Down" $ALERT_EMAIL
    exit 1
fi

# 3. Check Sync Lag
echo -n "Checking Sync Lag... "
SYNC_LAG=$(curl -s "$API_URL/sync/status" | jq -r '.sync_lag_seconds')
if [ "$SYNC_LAG" -lt 60 ]; then
    echo -e "${GREEN}${SYNC_LAG}s${NC}"
elif [ "$SYNC_LAG" -lt 300 ]; then
    echo -e "${YELLOW}${SYNC_LAG}s (WARNING)${NC}"
else
    echo -e "${RED}${SYNC_LAG}s (CRITICAL)${NC}"
    echo "Sync lag is $SYNC_LAG seconds" | mail -s "ALERT: Endymion-AI High Sync Lag" $ALERT_EMAIL
fi

# 4. Check Unpublished Events
echo -n "Checking Event Backlog... "
UNPUBLISHED=$(curl -s "$API_URL/events/unpublished" | jq -r '.count')
if [ "$UNPUBLISHED" -lt 100 ]; then
    echo -e "${GREEN}${UNPUBLISHED} events${NC}"
else
    echo -e "${RED}${UNPUBLISHED} events (HIGH)${NC}"
    echo "Unpublished events: $UNPUBLISHED" | mail -s "ALERT: Endymion-AI Event Backlog" $ALERT_EMAIL
fi

# 5. Check Disk Space
echo -n "Checking Disk Space... "
DISK_USAGE=$(df -h /var/opt/mssql | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -lt 80 ]; then
    echo -e "${GREEN}${DISK_USAGE}%${NC}"
elif [ "$DISK_USAGE" -lt 90 ]; then
    echo -e "${YELLOW}${DISK_USAGE}% (WARNING)${NC}"
else
    echo -e "${RED}${DISK_USAGE}% (CRITICAL)${NC}"
    echo "Disk usage at $DISK_USAGE%" | mail -s "ALERT: Endymion-AI Disk Space" $ALERT_EMAIL
fi

echo ""
echo "✅ Health check complete"
```

**Cron Schedule:**
```bash
# Run health check every 5 minutes
*/5 * * * * /opt/endymion-ai/health-check.sh >> /var/log/endymion-ai/health.log 2>&1
```

---

## 3. Monitoring

### Prometheus Metrics

**Metrics Endpoint:** `http://localhost:8000/metrics`

#### Key Metrics

```python
# backend/monitoring/metrics.py

# Event Store Metrics
endymion_ai_events_total{event_type="cow_created"} 1523
endymion_ai_events_unpublished 3
endymion_ai_event_store_latency_seconds{quantile="0.95"} 0.015

# API Metrics
endymion_ai_api_requests_total{method="POST",endpoint="/cows",status="201"} 1523
endymion_ai_api_request_duration_seconds{quantile="0.95"} 0.250

# Sync Metrics
endymion_ai_sync_lag_seconds 12
endymion_ai_sync_runs_total{status="success"} 2880
endymion_ai_sync_rows_synced_total 1523
endymion_ai_sync_conflicts_total 2

# Database Metrics
endymion_ai_db_connections_active 15
endymion_ai_db_query_duration_seconds{quantile="0.95"} 0.025
```

### Grafana Dashboard

**Import Dashboard:**
```bash
# Import JSON from backend/monitoring/grafana-dashboard.json
```

**Key Panels:**

1. **Event Rate** - Events per minute
2. **Sync Lag** - Time lag in seconds (green < 60s, yellow 60-300s, red > 300s)
3. **API Latency** - P95 response time
4. **Error Rate** - Errors per minute
5. **Database Connections** - Active/idle connections
6. **Throughput** - Requests per second
7. **Event Backlog** - Unpublished events count
8. **Sync Duration** - Time to complete sync job

### Alerting Rules

```yaml
# prometheus-alerts.yaml
groups:
- name: endymion-ai
  interval: 30s
  rules:
  - alert: HighSyncLag
    expr: endymion_ai_sync_lag_seconds > 300
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "High sync lag detected"
      description: "Sync lag is {{ $value }} seconds"

  - alert: EventBacklog
    expr: endymion_ai_events_unpublished > 1000
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Large event backlog"
      description: "{{ $value }} unpublished events"

  - alert: APIHighErrorRate
    expr: rate(endymion_ai_api_requests_total{status=~"5.."}[5m]) > 0.05
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "High API error rate"
      description: "Error rate is {{ $value | humanizePercentage }}"

  - alert: DatabaseConnectionsHigh
    expr: endymion_ai_db_connections_active > 80
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High database connections"
      description: "{{ $value }} active connections"

  - alert: SyncJobFailed
    expr: increase(endymion_ai_sync_runs_total{status="failed"}[10m]) > 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Sync job failure"
      description: "Sync job failed in last 10 minutes"
```

### Log Aggregation

**Structured Logging:**
```python
# backend/utils/logging.py
import logging
import json

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)

# Configure logger
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger = logging.getLogger("endymion-ai")
logger.addHandler(handler)
logger.setLevel(logging.INFO)
```

**ELK Stack Integration:**
```yaml
# filebeat.yml
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /var/log/endymion-ai/*.log
  json.keys_under_root: true
  json.add_error_key: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "endymion-ai-%{+yyyy.MM.dd}"

setup.kibana:
  host: "kibana:5601"
```

---

## 4. Backup & Restore

### Database Backup

**Automated Backup Script:**
```bash
#!/bin/bash
# backup-db.sh

BACKUP_DIR="/backups/sqlserver"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/endymion_ai_$DATE.bak"
SA_PASSWORD=${SA_PASSWORD:-"StrongP@ssw0rd"}
SQLCMD=${SQLCMD:-/opt/mssql-tools/bin/sqlcmd}

mkdir -p $BACKUP_DIR

# Full backup with compression
$SQLCMD -S localhost -U sa -P "$SA_PASSWORD" \
  -Q "BACKUP DATABASE [endymion_ai] TO DISK = N'$BACKUP_FILE' WITH INIT, COMPRESSION"

if [ -f "$BACKUP_FILE" ]; then
  SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
  echo "✅ Backup successful: $BACKUP_FILE ($SIZE)"

  # Delete backups older than 30 days
  find $BACKUP_DIR -name "*.bak" -mtime +30 -delete
else
  echo "❌ Backup failed"
  exit 1
fi
```

**Cron Schedule:**
```bash
# Daily backup at 2 AM
0 2 * * * /opt/endymion-ai/backup-db.sh >> /var/log/endymion-ai/backup.log 2>&1
```

### Database Restore

```bash
#!/bin/bash
# restore-db.sh

BACKUP_FILE=$1

if [ -z "$BACKUP_FILE" ]; then
    echo "Usage: $0 <backup_file>"
    exit 1
fi

echo "⚠️  This will replace the current database!"
read -p "Are you sure? (type 'yes'): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

# Stop services
kubectl scale deployment fastapi --replicas=0 -n endymion-ai
kubectl scale deployment sync-job --replicas=0 -n endymion_ai

# Restore (uses single-user mode for safety)
SQLCMD=${SQLCMD:-/opt/mssql-tools/bin/sqlcmd}
SA_PASSWORD=${SA_PASSWORD:-"StrongP@ssw0rd"}

$SQLCMD -S localhost -U sa -P "$SA_PASSWORD" -Q "
ALTER DATABASE [endymion_ai] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
RESTORE DATABASE [endymion_ai]
FROM DISK = N'$BACKUP_FILE'
WITH REPLACE;
ALTER DATABASE [endymion_ai] SET MULTI_USER;
"

# Restart services
kubectl scale deployment fastapi --replicas=3 -n endymion_ai
kubectl scale deployment sync-job --replicas=1 -n endymion-ai

echo "✅ Database restored successfully"
```

### Delta Lake Backup

```bash
#!/bin/bash
# backup-delta-lake.sh

SOURCE_PATH="/data/delta_lake"
BACKUP_PATH="s3://endymion-ai-backups/delta_lake"
DATE=$(date +%Y%m%d)

# Sync to S3 with versioning
aws s3 sync $SOURCE_PATH $BACKUP_PATH/$DATE/ \
    --storage-class STANDARD_IA \
    --exclude "*.crc" \
    --exclude "_temporary/*"

echo "✅ Delta Lake backed up to $BACKUP_PATH/$DATE/"
```

### Backup to S3 (Kubernetes)

```yaml
# k8s/backup-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: backup-job
  namespace: endymion-ai
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: amazon/aws-cli
            command:
            - /bin/sh
            - -c
            - |
              /opt/mssql-tools/bin/sqlcmd \
                -S sqlserver-service -U sa -P $SA_PASSWORD \
                -Q "BACKUP DATABASE [endymion_ai] TO DISK='/tmp/endymion_ai.bak' WITH INIT, COMPRESSION" && \
              aws s3 cp /tmp/endymion_ai.bak s3://endymion-ai-backups/db-$(date +%Y%m%d).bak && \
              rm /tmp/endymion_ai.bak
            env:
            - name: SA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: endymion-ai-secrets
                  key: database-password
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: aws-credentials
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: aws-credentials
                  key: secret-access-key
          restartPolicy: OnFailure
```

---

## 5. Scaling Considerations

### Horizontal Scaling

#### FastAPI (Stateless)

```bash
# Scale API pods
kubectl scale deployment fastapi --replicas=10 -n endymion-ai

# Autoscaling
kubectl autoscale deployment fastapi \
  --cpu-percent=70 \
  --min=3 \
  --max=20 \
  -n endymion-ai
```

#### Sync Job (Single Instance)

```yaml
# Only ONE sync job should run at a time
# Use leader election for HA

apiVersion: apps/v1
kind: Deployment
metadata:
  name: sync-job
spec:
  replicas: 1  # Always 1
  strategy:
    type: Recreate  # Prevent multiple instances
```

### Vertical Scaling

#### Database

```sql
-- Increase connections
ALTER SYSTEM SET max_connections = 200;

-- Tune shared buffers (25% of RAM)
ALTER SYSTEM SET shared_buffers = '4GB';

-- Tune work memory
ALTER SYSTEM SET work_mem = '64MB';

-- Restart required
SELECT pg_reload_conf();
```

#### Application

```yaml
# Increase resources
resources:
  requests:
    memory: "2Gi"
    cpu: "2000m"
  limits:
    memory: "4Gi"
    cpu: "4000m"
```

### Database Sharding (Future)

**Partition by Date:**
```sql
-- Partition events table
CREATE TABLE events.cow_events_2024_01 PARTITION OF events.cow_events
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE events.cow_events_2024_02 PARTITION OF events.cow_events
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
```

### Read Replicas

For Azure SQL, use auto-failover groups to provision read replicas with a ready-only endpoint:

```bash
az sql failover-group create \
  --name fg-endymion-ai \
  --partner-server endymion-ai-dr \
  --partner-database endymion-ai \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --failover-policy Automatic \
  --grace-period 1

# Read-only endpoint
READONLY_ENDPOINT=$(az sql failover-group show \
  --name fg-endymion-ai \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --query readOnlyEndpoint \
  -o tsv)
```

Point reporting workloads to `READONLY_ENDPOINT` to offload traffic from the primary database.

---

## 6. Disaster Recovery

### Recovery Time Objective (RTO)

**Target:** 1 hour

### Recovery Point Objective (RPO)

**Target:** 1 hour (based on backup frequency)

### DR Runbook

**Scenario: Complete Database Loss**

1. **Stop all services:**
   ```bash
   kubectl scale deployment --all --replicas=0 -n endymion-ai
   ```

2. **Restore database from latest backup:**
  ```bash
  ./restore-db.sh /backups/sqlserver/endymion_ai_latest.bak
  ```

3. **Verify data integrity:**
  ```bash
  /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" \
    -Q "SELECT COUNT(*) AS total_events FROM operational.cow_events"
  ```

4. **Restart services:**
   ```bash
   kubectl scale deployment fastapi --replicas=3 -n endymion_ai
   kubectl scale deployment sync-job --replicas=1 -n endymion-ai
   ```

5. **Force full resync:**
   ```bash
   python backend/sync/force_resync.py
   ```

**Scenario: Delta Lake Corruption**

1. **Restore from S3 backup:**
   ```bash
   aws s3 sync s3://endymion-ai-backups/delta_lake/latest/ /data/delta_lake/
   ```

2. **Rebuild Silver from Bronze:**
   ```bash
   python backend/spark/rebuild_silver.py
   ```

3. **Resync SQL projection:**
   ```bash
   TRUNCATE operational.cows;
   python backend/sync/sync_silver_to_sql.py --force-full
   ```

### Multi-Region Setup

```mermaid
graph LR
    subgraph "Region 1 (Primary)"
      API1[FastAPI]
      DB1[(SQL Server)]
        DELTA1[Delta Lake]
    end
    
    subgraph "Region 2 (DR)"
        API2[FastAPI]
      DB2[(SQL Server)]
        DELTA2[Delta Lake]
    end
    
    DB1 -.Replication.-> DB2
    DELTA1 -.Sync.-> DELTA2
```

---

## 7. Security

### Network Security

**Firewall Rules:**
```bash
# Allow only specific IPs to database
iptables -A INPUT -p tcp --dport 1433 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 1433 -j DROP

# Allow API from load balancer only
iptables -A INPUT -p tcp --dport 8000 -s <LB_IP> -j ACCEPT
```

### Database Security

```sql
-- Create read-only login/user for replicas
CREATE LOGIN readonly WITH PASSWORD = '<StrongPassword1>';
CREATE USER readonly FOR LOGIN readonly;
EXEC sp_addrolemember 'db_datareader', 'readonly';
EXEC sp_addrolemember 'db_denydatawriter', 'readonly';

-- Create sync job login/user
CREATE LOGIN sync_user WITH PASSWORD = '<StrongPassword2>';
CREATE USER sync_user FOR LOGIN sync_user;
GRANT SELECT ON SCHEMA::operational TO sync_user;
GRANT SELECT ON SCHEMA::events TO sync_user;
GRANT EXECUTE ON SCHEMA::sync TO sync_user;
```

### Encryption

**At Rest:**
```sql
-- Enable Transparent Data Encryption (TDE)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<StrongKeyPass1>';
CREATE CERTIFICATE EndymionAI_TDE WITH SUBJECT = 'Endymion-AI TDE Cert';
CREATE DATABASE ENCRYPTION KEY
  WITH ALGORITHM = AES_256
  ENCRYPTION BY SERVER CERTIFICATE EndymionAI_TDE;
ALTER DATABASE endymion_ai SET ENCRYPTION ON;
```

**In Transit:**
```yaml
# Use TLS for all connections
DATABASE_URL: mssql+pyodbc://user:pass@host:1433/endymion_ai?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no
```

### Secrets Management

**Using Kubernetes Secrets:**
```bash
kubectl create secret generic db-credentials \
  --from-literal=username=sa \
  --from-literal=password='<strong-password>' \
  -n endymion-ai
```

**Using HashiCorp Vault:**
```bash
# Store secret
vault kv put secret/endymion-ai/db password='<password>'

# Retrieve in pod
VAULT_ADDR=https://vault.example.com vault kv get secret/endymion-ai/db
```

---

## 8. Performance Tuning

### Database Optimization

**Indexes:**
```sql
-- Event store indexes
CREATE INDEX idx_events_aggregate_id ON events.cow_events(aggregate_id);
CREATE INDEX idx_events_timestamp ON events.cow_events(event_timestamp);
CREATE INDEX idx_events_published ON events.cow_events(published);

-- SQL projection indexes
CREATE INDEX idx_cows_breed ON operational.cows(breed);
CREATE INDEX idx_cows_is_active ON operational.cows(is_active);
CREATE INDEX idx_cows_updated_at ON operational.cows(updated_at);
```

**Query Optimization:**
```sql
-- Refresh statistics
UPDATE STATISTICS events.cow_events;
UPDATE STATISTICS operational.cows;
EXEC sp_updatestats;

-- Rebuild indexes when fragmentation > 30%
ALTER INDEX ALL ON operational.cows REBUILD WITH (FILLFACTOR = 90, ONLINE = ON);
```

### Connection Pooling

```python
# backend/db/connection.py
from sqlalchemy.pool import QueuePool

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=40,
    pool_pre_ping=True,
    pool_recycle=3600
)
```

### Caching

**Redis Cache:**
```python
# backend/cache/redis.py
import redis

cache = redis.Redis(host='redis', port=6379, db=0)

def get_cow(cow_id):
    # Check cache
    cached = cache.get(f"cow:{cow_id}")
    if cached:
        return json.loads(cached)
    
    # Query database
    cow = db.query(Cow).filter_by(cow_id=cow_id).first()
    
    # Cache for 60 seconds
    cache.setex(f"cow:{cow_id}", 60, json.dumps(cow))
    return cow
```

---

## Checklists

### Pre-Deployment Checklist

- [ ] Database schema applied
- [ ] Secrets configured
- [ ] Backups scheduled
- [ ] Monitoring configured
- [ ] Alerts set up
- [ ] Load testing completed
- [ ] DR runbook tested
- [ ] Security audit passed
- [ ] Documentation updated

### Post-Deployment Checklist

- [ ] Health checks passing
- [ ] Metrics flowing to Prometheus
- [ ] Logs aggregated
- [ ] Backups running
- [ ] Alerts triggering correctly
- [ ] Performance acceptable
- [ ] No errors in logs

---

## Additional Resources

- [Developer Guide](./DEVELOPER.md)
- [API Documentation](./API.md)
- [Architecture Overview](../ARCHITECTURE.md)
- [Monitoring Dashboard](../backend/monitoring/README.md)

---

**Last Updated:** January 2026  
**Version:** 1.0.0  
**Maintainer:** Endymion-AI DevOps Team
