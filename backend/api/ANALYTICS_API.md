# Analytics API Documentation

## Overview

The Analytics API provides pre-computed analytics from the Gold layer Delta tables. It offers high-performance read access to aggregated cattle management metrics with built-in caching.

## Architecture

```
API Request → FastAPI Router → PySpark → Gold Delta Tables (MinIO S3)
                    ↓
              In-Memory Cache (60s TTL)
```

### Key Features

- **Pre-computed Analytics**: All analytics are pre-computed in Gold layer for fast queries
- **Direct Delta Lake Access**: Uses PySpark to query Delta tables directly
- **Intelligent Caching**: 60-second in-memory cache reduces load on Delta tables
- **Tenant Isolation**: Multi-tenant support with tenant_id filtering
- **Data Freshness**: Each response includes "as_of" timestamp
- **Cache Visibility**: Responses indicate if data was served from cache

## Endpoints

### 1. Herd Composition

**Endpoint**: `GET /api/v1/analytics/herd-composition`

**Description**: Get herd composition breakdown by breed, status, and sex.

**Query Parameters**:
- `tenant_id` (required): UUID of the tenant
- `date` (optional): Snapshot date in YYYY-MM-DD format. Defaults to latest available date.

**Response Model**:
```json
{
  "as_of": "2024-01-15T10:30:00.123456",
  "cached": false,
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": {
    "snapshot_date": "2024-01-15",
    "total_cows": 150,
    "by_breed": [
      {
        "dimension": "Holstein",
        "count": 80,
        "percentage": 53.33
      },
      {
        "dimension": "Jersey",
        "count": 70,
        "percentage": 46.67
      }
    ],
    "by_status": [
      {
        "dimension": "active",
        "count": 140,
        "percentage": 93.33
      },
      {
        "dimension": "inactive",
        "count": 10,
        "percentage": 6.67
      }
    ],
    "by_sex": [
      {
        "dimension": "female",
        "count": 130,
        "percentage": 86.67
      },
      {
        "dimension": "male",
        "count": 20,
        "percentage": 13.33
      }
    ]
  }
}
```

**Examples**:

```bash
# Get latest herd composition
curl "http://localhost:8000/api/v1/analytics/herd-composition?tenant_id=550e8400-e29b-41d4-a716-446655440000"

# Get herd composition for specific date
curl "http://localhost:8000/api/v1/analytics/herd-composition?tenant_id=550e8400-e29b-41d4-a716-446655440000&date=2024-01-15"
```

**Status Codes**:
- `200 OK`: Success
- `404 Not Found`: No data found for tenant/date
- `422 Unprocessable Entity`: Invalid query parameters
- `500 Internal Server Error`: Query execution failed

**Data Source**: `s3a://gold/herd_composition` Delta table

---

### 2. Weight Trends

**Endpoint**: `GET /api/v1/analytics/weight-trends/{cow_id}`

**Description**: Get weight trend analytics for a specific cow.

**Status**: ⚠️ **Not Yet Implemented** (501 Not Implemented)

**Path Parameters**:
- `cow_id` (required): UUID of the cow

**Query Parameters**:
- `tenant_id` (required): UUID of the tenant

**Planned Response Model**:
```json
{
  "as_of": "2024-01-15T10:30:00.123456",
  "cached": false,
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": {
    "cow_id": "00000000-0000-0000-0000-000000000001",
    "cow_name": "Bessie",
    "measurements": [
      {
        "date": "2024-01-01",
        "weight_kg": 450.5,
        "trend": "up"
      },
      {
        "date": "2024-01-08",
        "weight_kg": 455.2,
        "trend": "up"
      }
    ],
    "average_weight_kg": 452.85,
    "weight_change_kg": 4.7
  }
}
```

**Example**:
```bash
curl "http://localhost:8000/api/v1/analytics/weight-trends/00000000-0000-0000-0000-000000000001?tenant_id=550e8400-e29b-41d4-a716-446655440000"
```

**Prerequisites**: Requires `gold_cow_weight_trends` Delta table to be created.

**Planned Data Source**: `s3a://gold/cow_weight_trends` Delta table

---

### 3. Sales Summary

**Endpoint**: `GET /api/v1/analytics/sales-summary`

**Description**: Get sales summary analytics for a date range.

**Status**: ⚠️ **Not Yet Implemented** (501 Not Implemented)

**Query Parameters**:
- `tenant_id` (required): UUID of the tenant
- `start_date` (required): Start date in YYYY-MM-DD format
- `end_date` (required): End date in YYYY-MM-DD format

**Planned Response Model**:
```json
{
  "as_of": "2024-01-15T10:30:00.123456",
  "cached": false,
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": {
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "total_sales": 25,
    "total_revenue": 125000.00,
    "daily_breakdown": [
      {
        "date": "2024-01-05",
        "total_sales": 3,
        "total_revenue": 15000.00,
        "average_price": 5000.00
      },
      {
        "date": "2024-01-12",
        "total_sales": 5,
        "total_revenue": 25000.00,
        "average_price": 5000.00
      }
    ]
  }
}
```

**Example**:
```bash
curl "http://localhost:8000/api/v1/analytics/sales-summary?tenant_id=550e8400-e29b-41d4-a716-446655440000&start_date=2024-01-01&end_date=2024-01-31"
```

**Prerequisites**: Requires `gold_daily_sales` Delta table to be created.

**Planned Data Source**: `s3a://gold/daily_sales` Delta table

---

### 4. Clear Cache

**Endpoint**: `DELETE /api/v1/analytics/cache`

**Description**: Clear the in-memory analytics cache. Forces fresh data retrieval on next request.

**Response Model**:
```json
{
  "status": "success",
  "message": "Analytics cache cleared",
  "timestamp": "2024-01-15T10:30:00.123456"
}
```

**Example**:
```bash
curl -X DELETE "http://localhost:8000/api/v1/analytics/cache"
```

**Use Cases**:
- Force refresh after Gold layer rebuild
- Development/testing
- Clear stale cached data

---

## Caching

### Cache Strategy

The Analytics API uses a **simple in-memory cache** with the following characteristics:

- **TTL**: 60 seconds
- **Scope**: Per-endpoint, per-tenant, per-parameter combination
- **Implementation**: Python dictionary with expiry timestamps
- **Invalidation**: Automatic (TTL expiry) or manual (DELETE /cache)

### Cache Keys

Cache keys are generated based on endpoint and parameters:

```
herd_composition:{tenant_id}:{date}
weight_trends:{tenant_id}:{cow_id}
sales_summary:{tenant_id}:{start_date}:{end_date}
```

### Cache Behavior

1. **First Request** (Cache MISS):
   - Query Gold Delta table via PySpark
   - Cache result with 60s TTL
   - Return `cached: false`

2. **Subsequent Requests** (Cache HIT):
   - Return cached data
   - Skip Delta query
   - Return `cached: true`
   - Faster response time

3. **After TTL Expiry**:
   - Cache entry automatically removed
   - Next request becomes Cache MISS

### Production Considerations

For production deployments, consider upgrading to:

- **Redis**: Distributed cache for multi-instance deployments
- **Databricks SQL**: Direct SQL queries instead of PySpark
- **CDN**: Cache at edge locations for global users

---

## Performance

### Query Performance

| Endpoint | Data Source | Typical Response Time |
|----------|-------------|----------------------|
| Herd Composition | gold_herd_composition | 50-200ms (uncached) |
| Weight Trends | gold_cow_weight_trends | TBD |
| Sales Summary | gold_daily_sales | TBD |

### Cache Performance

| Scenario | Response Time |
|----------|--------------|
| Cache HIT | 5-10ms |
| Cache MISS | 50-200ms |
| Cache Cleared | Next request = MISS |

### Optimization Tips

1. **Use specific dates**: Requesting latest date every time may bypass cache
2. **Batch requests**: If querying multiple tenants, parallelize requests
3. **Monitor cache hit rate**: Use logs to track cache effectiveness
4. **Adjust TTL**: Increase TTL (e.g., 300s) for less frequently changing data

---

## Error Handling

### Common Error Responses

#### 400 Bad Request
```json
{
  "error": "ValidationError",
  "message": "Missing tenant_id query parameter"
}
```

#### 404 Not Found
```json
{
  "detail": "No herd composition data found for tenant 550e8400-e29b-41d4-a716-446655440000"
}
```

#### 422 Unprocessable Entity
```json
{
  "error": "ValidationError",
  "message": "Request validation failed",
  "details": [
    {
      "loc": ["query", "tenant_id"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

#### 500 Internal Server Error
```json
{
  "detail": "Failed to query herd composition: Connection to MinIO failed"
}
```

#### 501 Not Implemented
```json
{
  "detail": "Weight trends endpoint not yet implemented. Requires gold_cow_weight_trends table."
}
```

---

## Testing

### Prerequisites

1. **Backend API Running**:
   ```bash
   cd backend
   uvicorn api.main:app --reload
   ```

2. **Gold Layer Populated**:
   ```bash
   cd databricks/gold
   python rebuild_all.py --rebuild-all
   ```

3. **MinIO Running**:
   ```bash
   # MinIO should be accessible at localhost:9000
   # Bucket: gold
   ```

### Run Test Suite

```bash
cd backend/api
./test_analytics_api.sh [tenant_id]
```

**Example**:
```bash
./test_analytics_api.sh 550e8400-e29b-41d4-a716-446655440000
```

### Test Coverage

The test suite covers:

1. ✅ Herd composition (latest snapshot)
2. ✅ Herd composition (specific date)
3. ✅ Cache verification (MISS → HIT)
4. ✅ Weight trends (501 Not Implemented)
5. ✅ Sales summary (501 Not Implemented)
6. ✅ Clear cache
7. ✅ Verify cache cleared
8. ✅ Error handling (invalid tenant)
9. ✅ Error handling (missing tenant)

---

## Development Guide

### Adding New Analytics Endpoint

1. **Create Gold Table**: Build Delta table in `databricks/gold/`
   ```python
   # Example: gold_new_metric.py
   def compute_new_metric(date):
       # Compute metric from Silver layer
       pass
   ```

2. **Add Response Models**: Define Pydantic models in `analytics.py`
   ```python
   class NewMetricData(BaseModel):
       metric_value: float
       # ... other fields
   
   class NewMetricResponse(AnalyticsResponse):
       data: NewMetricData
   ```

3. **Implement Endpoint**: Add GET handler in `analytics.py`
   ```python
   @router.get("/new-metric", response_model=NewMetricResponse)
   async def get_new_metric(tenant_id: UUID = Depends(get_tenant_id)):
       # Cache check
       # PySpark query
       # Return response
   ```

4. **Test**: Add test case to `test_analytics_api.sh`

### PySpark Session Management

The analytics router maintains a **singleton PySpark session** configured for MinIO:

```python
spark = get_spark_session()  # Reuses existing session
```

**Key Configurations**:
- Delta Lake support enabled
- MinIO S3 endpoint: `http://localhost:9000`
- Path-style S3 access
- Adaptive query execution

**Cleanup**: Call `stop_spark_session()` on API shutdown (if needed)

---

## API Documentation

### Interactive API Docs

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### OpenAPI Spec

- **JSON**: http://localhost:8000/openapi.json

---

## Future Enhancements

### Short Term

1. ✅ Implement weight trends endpoint
2. ✅ Implement sales summary endpoint
3. ✅ Add pagination for large result sets
4. ✅ Add filtering/sorting options

### Medium Term

1. Migrate to Databricks SQL Connector (replace PySpark)
2. Add Redis for distributed caching
3. Add GraphQL support for flexible queries
4. Add streaming/WebSocket for real-time updates

### Long Term

1. Add machine learning predictions (e.g., weight forecasting)
2. Add anomaly detection alerts
3. Add custom analytics dashboards
4. Add export to Excel/PDF

---

## Troubleshooting

### Issue: "No herd composition data found"

**Cause**: Gold layer tables not populated

**Solution**:
```bash
cd databricks/gold
python rebuild_all.py --rebuild-all
```

### Issue: "Connection to MinIO failed"

**Cause**: MinIO not running or incorrect credentials

**Solution**:
```bash
# Check MinIO is running
curl http://localhost:9000/minio/health/live

# Verify credentials in analytics.py match MinIO
```

### Issue: "PySpark session creation failed"

**Cause**: Missing PySpark/Delta Lake dependencies

**Solution**:
```bash
pip install pyspark==3.5.0 delta-spark==3.0.0
```

### Issue: Cache not working

**Cause**: Multiple API instances or cache cleared

**Solution**:
- Use Redis for multi-instance deployments
- Check logs for cache HIT/MISS events
- Verify TTL settings

---

## Monitoring

### Logs

Analytics API logs cache performance:

```
INFO - Cache HIT for herd_composition:550e8400-e29b-41d4-a716-446655440000:latest
INFO - Cache MISS for herd_composition:550e8400-e29b-41d4-a716-446655440000:2024-01-15 - querying Gold layer
INFO - Analytics cache cleared
```

### Metrics to Monitor

- Cache hit rate
- Query response times
- PySpark session health
- Delta table read performance
- API error rates

---

## Security

### Tenant Isolation

All endpoints enforce tenant isolation:
- Tenant ID required in query parameters
- Delta queries filtered by `tenant_id` column
- No cross-tenant data leakage

### Authentication

**Current**: Tenant ID from query parameter (development)

**Production**: Should implement:
- JWT token validation
- Extract tenant ID from authenticated user context
- Rate limiting per tenant
- API key management

---

## Contact

For issues or questions:
- Check logs: `backend/api/logs/`
- Review Gold layer docs: `databricks/gold/README.md`
- Check Delta tables: `databricks/gold/GOLD_REFERENCE.md`
