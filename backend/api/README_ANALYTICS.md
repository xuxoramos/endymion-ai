# Analytics API - Quick Start

## Overview

REST API endpoints for querying pre-computed analytics from the Gold layer Delta tables.

## Features

✅ **Herd Composition** - Breed/status/sex breakdown  
⏳ **Weight Trends** - Cow weight history (planned)  
⏳ **Sales Summary** - Daily sales aggregations (planned)  
🚀 **60-second caching** - Fast response times  
🔒 **Tenant isolation** - Multi-tenant support  
📊 **PySpark + Delta Lake** - Direct Delta table queries

## Quick Start

### 1. Start Backend API

```bash
cd backend
uvicorn api.main:app --reload
```

API will be available at: http://localhost:8000

### 2. Ensure Gold Layer is Populated

```bash
cd databricks/gold
python rebuild_all.py --rebuild-all
```

### 3. Test Endpoints

```bash
cd backend/api
./test_analytics_api.sh 550e8400-e29b-41d4-a716-446655440000
```

## Example Usage

### Get Herd Composition

```bash
# Latest snapshot
curl "http://localhost:8000/api/v1/analytics/herd-composition?tenant_id=550e8400-e29b-41d4-a716-446655440000"

# Specific date
curl "http://localhost:8000/api/v1/analytics/herd-composition?tenant_id=550e8400-e29b-41d4-a716-446655440000&date=2024-01-15"
```

### Response Example

```json
{
  "as_of": "2024-01-15T10:30:00.123456",
  "cached": false,
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": {
    "snapshot_date": "2024-01-15",
    "total_cows": 150,
    "by_breed": [
      {"dimension": "Holstein", "count": 80, "percentage": 53.33},
      {"dimension": "Jersey", "count": 70, "percentage": 46.67}
    ],
    "by_status": [
      {"dimension": "active", "count": 140, "percentage": 93.33},
      {"dimension": "inactive", "count": 10, "percentage": 6.67}
    ],
    "by_sex": [
      {"dimension": "female", "count": 130, "percentage": 86.67},
      {"dimension": "male", "count": 20, "percentage": 13.33}
    ]
  }
}
```

## Architecture

```
┌─────────────┐
│   Client    │
│  (curl/UI)  │
└──────┬──────┘
       │ HTTP GET
       ↓
┌──────────────────────────────────────────┐
│         FastAPI Analytics Router         │
│  ┌────────────────────────────────────┐  │
│  │  In-Memory Cache (60s TTL)         │  │
│  │  - Cache HIT → return immediately  │  │
│  │  - Cache MISS → query Gold layer   │  │
│  └────────────────────────────────────┘  │
└──────┬───────────────────────────────────┘
       │ Cache MISS
       ↓
┌──────────────────┐
│  PySpark Session │
│  - Delta Lake    │
│  - MinIO S3      │
└──────┬───────────┘
       │
       ↓
┌─────────────────────────────────────┐
│  Gold Delta Tables (MinIO)          │
│  - s3a://gold/herd_composition      │
│  - s3a://gold/cow_weight_trends     │
│  - s3a://gold/daily_sales           │
└─────────────────────────────────────┘
```

## Available Endpoints

| Endpoint | Status | Description |
|----------|--------|-------------|
| `GET /herd-composition` | ✅ Working | Herd breakdown by breed/status/sex |
| `GET /weight-trends/{cow_id}` | ⏳ Planned | Cow weight history |
| `GET /sales-summary` | ⏳ Planned | Daily sales aggregations |
| `DELETE /cache` | ✅ Working | Clear analytics cache |

## API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Full Docs**: [ANALYTICS_API.md](./ANALYTICS_API.md)

## Caching

- **TTL**: 60 seconds
- **Type**: In-memory (per-instance)
- **Key Format**: `{endpoint}:{tenant_id}:{params}`
- **Clear Cache**: `DELETE /api/v1/analytics/cache`

### Cache Performance

| Scenario | Response Time |
|----------|--------------|
| Cache HIT | 5-10ms |
| Cache MISS | 50-200ms |

## Testing

Run the test suite to verify all endpoints:

```bash
./test_analytics_api.sh [tenant_id]
```

Tests include:
- ✅ Herd composition (latest)
- ✅ Herd composition (specific date)
- ✅ Cache verification (MISS → HIT)
- ✅ Error handling (404, 422, 501)
- ✅ Cache management

## Prerequisites

1. **Python Dependencies**:
   ```bash
   pip install fastapi uvicorn pyspark==3.5.0 delta-spark==3.0.0
   ```

2. **MinIO Running**:
   - Endpoint: http://localhost:9000
   - Credentials: minioadmin/minioadmin
   - Bucket: `gold`

3. **Gold Layer Tables**:
   - Run `databricks/gold/rebuild_all.py`
   - Verify tables exist in `s3a://gold/`

## Files

```
backend/api/
├── routers/
│   ├── analytics.py          # Analytics router (NEW)
│   ├── cows.py                # Cow CRUD endpoints
│   └── __init__.py            # Router exports
├── main.py                    # FastAPI app (updated)
├── test_analytics_api.sh      # Test script (NEW)
├── ANALYTICS_API.md           # Full documentation (NEW)
└── README_ANALYTICS.md        # This file (NEW)
```

## Next Steps

### Implement Remaining Endpoints

1. **Weight Trends**:
   ```bash
   cd databricks/gold
   # Create gold_cow_weight_trends.py
   # Build weight trends Delta table
   # Update analytics.py to query it
   ```

2. **Sales Summary**:
   ```bash
   cd databricks/gold
   # Create gold_daily_sales.py
   # Build sales Delta table
   # Update analytics.py to query it
   ```

### Production Enhancements

- Migrate to **Redis** for distributed caching
- Switch to **Databricks SQL Connector** (replace PySpark)
- Add **JWT authentication** (replace query param tenant_id)
- Add **rate limiting** per tenant
- Add **pagination** for large result sets
- Add **monitoring** and metrics collection

## Troubleshooting

### No data found for tenant

**Solution**: Populate Gold layer
```bash
cd databricks/gold
python rebuild_all.py --rebuild-all
```

### PySpark connection failed

**Solution**: Check MinIO is running
```bash
curl http://localhost:9000/minio/health/live
```

### Cache not working

**Solution**: Check logs for cache HIT/MISS events
```bash
# Look for:
# INFO - Cache HIT for herd_composition:...
# INFO - Cache MISS for herd_composition:... - querying Gold layer
```

## Support

- Full docs: [ANALYTICS_API.md](./ANALYTICS_API.md)
- Gold layer: `databricks/gold/README.md`
- Delta tables: `databricks/gold/GOLD_REFERENCE.md`
