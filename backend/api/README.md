# FastAPI Backend - Pure Projection Pattern A

Complete event-driven FastAPI backend implementing Pure Projection Pattern A (Event Sourcing + CQRS).

## Architecture Overview

```
Write Path:  API → cow_events → Bronze → Silver → Gold
Read Path:   API ← cows (projection) ← Silver
```

### Key Principles

1. **Write Operations** create immutable events in `cow_events` table
2. **Read Operations** query `cows` projection table
3. **Eventual Consistency** - projection lags behind events by 60-120 seconds
4. **Append-Only Events** - events are never updated or deleted

## Project Structure

```
backend/api/
├── main.py              # FastAPI app with lifecycle management
├── dependencies.py      # Dependency injection (DB, tenant, pagination)
├── routers/
│   ├── __init__.py
│   └── cows.py         # Cow endpoints (event-driven CRUD)
├── test-api.sh         # API testing script
└── verify-pattern-a.sh # SQL verification script
```

## API Endpoints

### Health

- `GET /health` - Health check with database status

### Cows (Event-Driven CRUD)

#### Write Operations (Create Events)

All write operations return `202 Accepted` with event acknowledgment:

```json
{
  "status": "accepted",
  "event_id": "uuid",
  "cow_id": "uuid",
  "event_type": "cow_created",
  "event_time": "2024-06-15T10:30:00Z",
  "message": "Event accepted. Changes will be reflected after sync.",
  "estimated_sync_time_seconds": 60
}
```

**Endpoints:**

- `POST /api/v1/cows` - Create cow (emits `cow_created` event)
- `PUT /api/v1/cows/{cow_id}` - Update cow (emits `cow_updated` event)
- `DELETE /api/v1/cows/{cow_id}` - Deactivate cow (emits `cow_deactivated` event)
- `POST /api/v1/cows/{cow_id}/weight` - Record weight (emits `cow_weight_recorded` event)
- `POST /api/v1/cows/{cow_id}/health` - Record health event (emits `cow_health_event`)

#### Read Operations (Query Projections)

All read operations return `200 OK` with current projection state:

- `GET /api/v1/cows` - List cows (with pagination and filters)
- `GET /api/v1/cows/{cow_id}` - Get specific cow
- `GET /api/v1/cows/{cow_id}/events` - Get event history (audit trail)

## Running the API

### 1. Install Dependencies

```bash
cd /home/xuxoramos/endymion-ai

# Install Python packages
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit with your settings
nano .env
```

Required variables:

```env
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=cattledb
SQLSERVER_USERNAME=sa
SQLSERVER_PASSWORD=YourStrong@Password

# Optional
SQLSERVER_POOL_SIZE=5
SYNC_INTERVAL_SECONDS=60
```

### 3. Start the API

```bash
# From backend/api directory
cd backend/api
python main.py

# Or with uvicorn directly
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 4. Access Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## Testing the API

### Automated Test Suite

```bash
cd backend/api
./test-api.sh
```

This script:
1. ✅ Checks health endpoint
2. ✅ Creates two cows (emits events)
3. ✅ Updates cow (emits event)
4. ✅ Records weight (emits event)
5. ✅ Deactivates cow (emits event)
6. ✅ Verifies GET returns 404 (projection not synced)
7. ✅ Checks event history

### Manual Testing with curl

#### 1. Create a Cow

```bash
curl -X POST http://localhost:8000/api/v1/cows \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "tag_number": "US-001",
    "name": "Bessie",
    "breed": "Holstein",
    "birth_date": "2022-01-15",
    "sex": "female",
    "weight_kg": 450.5,
    "current_location": "Barn A"
  }'
```

**Expected Response (202 Accepted):**

```json
{
  "status": "accepted",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "cow_id": "123e4567-e89b-12d3-a456-426614174000",
  "event_type": "cow_created",
  "event_time": "2024-06-15T10:30:00Z",
  "message": "Cow creation event accepted...",
  "estimated_sync_time_seconds": 60
}
```

#### 2. Try to GET Cow (Should Return 404)

```bash
COW_ID="123e4567-e89b-12d3-a456-426614174000"

curl -X GET http://localhost:8000/api/v1/cows/$COW_ID \
  -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000"
```

**Expected Response (404 Not Found):**

```json
{
  "detail": "Cow 123e4567-e89b-12d3-a456-426614174000 not found. If recently created, it may not be synced yet."
}
```

This proves Pure Projection Pattern A is working - the event was created but the projection hasn't been synced yet!

#### 3. Update Cow

```bash
curl -X PUT http://localhost:8000/api/v1/cows/$COW_ID \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "weight_kg": 475.0,
    "current_location": "Barn B"
  }'
```

#### 4. Record Weight

```bash
curl -X POST http://localhost:8000/api/v1/cows/$COW_ID/weight \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "weight_kg": 480.5,
    "measured_by": "John Smith",
    "measurement_method": "Scale"
  }'
```

#### 5. Get Event History

```bash
curl -X GET http://localhost:8000/api/v1/cows/$COW_ID/events \
  -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000"
```

**Expected Response:**

```json
[
  {
    "event_id": "...",
    "event_type": "cow_weight_recorded",
    "payload": {"weight_kg": 480.5, ...},
    "event_time": "2024-06-15T10:35:00Z"
  },
  {
    "event_id": "...",
    "event_type": "cow_updated",
    "payload": {"weight_kg": 475.0, ...},
    "event_time": "2024-06-15T10:32:00Z"
  },
  {
    "event_id": "...",
    "event_type": "cow_created",
    "payload": {"tag_number": "US-001", ...},
    "event_time": "2024-06-15T10:30:00Z"
  }
]
```

#### 6. List Cows (With Filters)

```bash
# List all active cows
curl "http://localhost:8000/api/v1/cows?status=active&page=1&page_size=20" \
  -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000"

# Filter by breed
curl "http://localhost:8000/api/v1/cows?breed=Holstein" \
  -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000"

# Filter by age range
curl "http://localhost:8000/api/v1/cows?min_age_days=365&max_age_days=1095" \
  -H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000"
```

## SQL Verification

### Verify Events in Database

```bash
cd backend/api
./verify-pattern-a.sh
```

This script checks:
1. ✅ Events exist in `cow_events` table
2. ✅ Events are grouped by type
3. ✅ Projection table (`cows`) is empty (before sync)
4. ✅ Tenant isolation is working

### Manual SQL Queries

#### Check Events

```sql
-- Count events
SELECT COUNT(*) as total_events
FROM operational.cow_events;

-- Events by type
SELECT event_type, COUNT(*) as count
FROM operational.cow_events
GROUP BY event_type;

-- Recent events
SELECT TOP 10
    event_id,
    cow_id,
    event_type,
    event_time,
    published_to_bronze
FROM operational.cow_events
ORDER BY event_time DESC;
```

#### Check Projection (Should Be Empty)

```sql
-- Count cows in projection
SELECT COUNT(*) as total_cows
FROM operational.cows;

-- If populated, check sync status
SELECT 
    id,
    tag_number,
    breed,
    status,
    last_synced_at,
    sync_version
FROM operational.cows
ORDER BY last_synced_at DESC;
```

## Important Constraints

### ❌ What NOT to Expect

```bash
# After POST /cows, GET will return 404
POST /api/v1/cows → 202 Accepted (event created)
GET /api/v1/cows/{id} → 404 Not Found (projection not synced yet)

# Wait 60-120 seconds for sync...
GET /api/v1/cows/{id} → 200 OK (projection synced)
```

### ✅ Expected Behavior

1. **Write Operations**:
   - Return `202 Accepted` immediately
   - Create event in `cow_events` table
   - DO NOT update `cows` projection
   - Return `event_id` for tracking

2. **Read Operations**:
   - Query `cows` projection table
   - Return `404` if not synced yet
   - Include `is_stale` flag for freshness
   - Show `last_synced_at` timestamp

3. **Event History**:
   - Always available via `/cows/{id}/events`
   - Shows complete audit trail
   - Events are append-only (immutable)

## Response Models

### EventAcceptedResponse (Write Operations)

```json
{
  "status": "accepted",
  "event_id": "uuid",
  "cow_id": "uuid",
  "event_type": "cow_created",
  "event_time": "2024-06-15T10:30:00Z",
  "message": "Event accepted...",
  "estimated_sync_time_seconds": 60
}
```

### CowResponse (Read Operations)

```json
{
  "id": "uuid",
  "tenant_id": "uuid",
  "tag_number": "US-001",
  "name": "Bessie",
  "breed": "Holstein",
  "birth_date": "2022-01-15",
  "sex": "female",
  "status": "active",
  "weight_kg": 475.5,
  "age_days": 880,
  "age_display": "2y 5m",
  "created_at": "2022-01-15T08:00:00Z",
  "updated_at": "2024-06-10T10:00:00Z",
  "last_synced_at": "2024-06-15T10:25:00Z",
  "sync_version": 42,
  "is_stale": false
}
```

## Error Handling

### Validation Errors (422)

```json
{
  "error": "ValidationError",
  "message": "Request validation failed",
  "details": [
    {
      "loc": ["body", "birth_date"],
      "msg": "Birth date cannot be in the future",
      "type": "value_error"
    }
  ],
  "timestamp": "2024-06-15T10:30:00Z"
}
```

### Not Found (404)

```json
{
  "detail": "Cow 123e4567-e89b-12d3-a456-426614174000 not found. If recently created, it may not be synced yet."
}
```

### Conflict (409)

```json
{
  "detail": "Cow with tag_number 'US-001' already exists"
}
```

## Multi-Tenancy

All endpoints require `X-Tenant-ID` header:

```bash
-H "X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000"
```

Missing tenant ID returns `401 Unauthorized`:

```json
{
  "detail": "Missing tenant ID. Provide X-Tenant-ID header."
}
```

## Pagination

List endpoints support pagination:

```bash
# Page 1, 20 items per page
GET /api/v1/cows?page=1&page_size=20

# Page 2
GET /api/v1/cows?page=2&page_size=20
```

**Response:**

```json
{
  "items": [...],
  "total": 42,
  "page": 1,
  "page_size": 20,
  "total_pages": 3
}
```

## Filtering

List cows with filters:

```bash
# By status
GET /api/v1/cows?status=active

# By breed
GET /api/v1/cows?breed=Holstein

# By age range (in days)
GET /api/v1/cows?min_age_days=365&max_age_days=1095

# Combined
GET /api/v1/cows?status=active&breed=Holstein&page=1
```

## Development

### Hot Reload

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Debug Logging

Set environment variable:

```bash
export DEBUG=true
python main.py
```

### Database Connection

Test connection:

```python
from backend.database.connection import get_db_manager

db = get_db_manager()
if db.test_connection():
    print("✅ Connected")
else:
    print("❌ Connection failed")
```

## Next Steps

1. **Run API**: `python backend/api/main.py`
2. **Test Endpoints**: `./backend/api/test-api.sh`
3. **Verify Pattern**: `./backend/api/verify-pattern-a.sh`
4. **Implement Sync**: Create Databricks job to sync events to projection
5. **Add Authentication**: Implement JWT token validation
6. **Add Monitoring**: Set up logging and metrics

## Related Documentation

- [Models README](../models/README.md) - SQLAlchemy ORM models
- [Database Schema](../database/schema.sql) - SQL table definitions
- [Architecture Document](../../docs/) - System architecture
- [API Documentation](http://localhost:8000/docs) - Interactive Swagger UI
