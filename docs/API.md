# Endymion-AI API Documentation

Complete API reference for the Endymion-AI REST API.

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Endpoints](#endpoints)
4. [Event Payload Schemas](#event-payload-schemas)
5. [Response Formats](#response-formats)
6. [Error Codes](#error-codes)

---

## Overview

**Base URL:** `http://localhost:8000/api`  
**Protocol:** HTTP/REST  
**Format:** JSON  
**OpenAPI Docs:** http://localhost:8000/docs

### API Characteristics

- **Event-Driven:** Write operations emit events to event store
- **Eventually Consistent:** Reads may lag writes by ~30 seconds (sync interval)
- **Idempotent:** Safe to retry PUT/DELETE operations
- **Versioned:** API version in URL (currently v1 implied)

---

## Authentication

**Current:** No authentication (development mode)

**Production:** Will use JWT tokens

```bash
# Future authentication header
Authorization: Bearer <jwt_token>
```

---

## Endpoints

### Cows API

#### List All Cows

**Endpoint:** `GET /api/cows`

**Description:** Get all cows from SQL projection (operational read model).

**Query Parameters:**
- `is_active` (optional, boolean): Filter by active status
- `breed` (optional, string): Filter by breed
- `limit` (optional, int): Max results (default: 100)
- `offset` (optional, int): Pagination offset (default: 0)

**Example Request:**
```bash
curl http://localhost:8000/api/cows?is_active=true&breed=Holstein
```

**Example Response:**
```json
[
  {
    "cow_id": "550e8400-e29b-41d4-a716-446655440000",
    "breed": "Holstein",
    "birth_date": "2024-01-15",
    "sex": "Female",
    "is_active": true,
    "created_at": "2024-01-20T10:30:00Z",
    "updated_at": "2024-01-20T10:30:00Z"
  }
]
```

**Status Codes:**
- `200 OK`: Success
- `500 Internal Server Error`: Database error

---

#### Get Single Cow

**Endpoint:** `GET /api/cows/{cow_id}`

**Description:** Get detailed information for a specific cow.

**Path Parameters:**
- `cow_id` (required, UUID): Cow identifier

**Example Request:**
```bash
curl http://localhost:8000/api/cows/550e8400-e29b-41d4-a716-446655440000
```

**Example Response:**
```json
{
  "cow_id": "550e8400-e29b-41d4-a716-446655440000",
  "breed": "Holstein",
  "birth_date": "2024-01-15",
  "sex": "Female",
  "is_active": true,
  "created_at": "2024-01-20T10:30:00Z",
  "updated_at": "2024-01-20T10:30:00Z"
}
```

**Status Codes:**
- `200 OK`: Success
- `404 Not Found`: Cow does not exist
- `422 Unprocessable Entity`: Invalid UUID format

---

#### Create Cow

**Endpoint:** `POST /api/cows`

**Description:** Create a new cow by emitting a `cow_created` event.

**Request Body:**
```json
{
  "breed": "string (required, max 100 chars)",
  "birth_date": "date (required, ISO 8601 format)",
  "sex": "string (required, 'Male' or 'Female')"
}
```

**Example Request:**
```bash
curl -X POST http://localhost:8000/api/cows \
  -H "Content-Type: application/json" \
  -d '{
    "breed": "Holstein",
    "birth_date": "2024-01-15",
    "sex": "Female"
  }'
```

**Example Response:**
```json
{
  "cow_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_id": "660e8400-e29b-41d4-a716-446655440001",
  "message": "Cow created successfully. Will be available after sync (~30s)."
}
```

**Status Codes:**
- `201 Created`: Event emitted successfully
- `400 Bad Request`: Invalid request body
- `422 Unprocessable Entity`: Validation error

**Notes:**
- Cow appears in SQL projection after sync job runs (~30 seconds)
- Use `event_id` to track event processing
- Idempotent if using client-generated UUIDs

---

#### Update Cow Breed

**Endpoint:** `PUT /api/cows/{cow_id}/breed`

**Description:** Update cow breed by emitting a `cow_updated` event.

**Path Parameters:**
- `cow_id` (required, UUID): Cow identifier

**Request Body:**
```json
{
  "breed": "string (required, max 100 chars)"
}
```

**Example Request:**
```bash
curl -X PUT http://localhost:8000/api/cows/550e8400-e29b-41d4-a716-446655440000/breed \
  -H "Content-Type: application/json" \
  -d '{"breed": "Angus"}'
```

**Example Response:**
```json
{
  "event_id": "770e8400-e29b-41d4-a716-446655440002",
  "message": "Breed update event emitted. Changes will reflect after sync."
}
```

**Status Codes:**
- `200 OK`: Event emitted successfully
- `404 Not Found`: Cow does not exist
- `400 Bad Request`: Invalid breed value

**Notes:**
- Update appears after sync job (~30 seconds)
- Creates new version in Silver layer (SCD Type 2)

---

#### Deactivate Cow

**Endpoint:** `DELETE /api/cows/{cow_id}`

**Description:** Mark cow as inactive by emitting a `cow_deactivated` event.

**Path Parameters:**
- `cow_id` (required, UUID): Cow identifier

**Query Parameters:**
- `reason` (optional, string): Deactivation reason (e.g., "sold", "deceased")

**Example Request:**
```bash
curl -X DELETE "http://localhost:8000/api/cows/550e8400-e29b-41d4-a716-446655440000?reason=sold"
```

**Example Response:**
```json
{
  "event_id": "880e8400-e29b-41d4-a716-446655440003",
  "message": "Cow deactivation event emitted. Status will update after sync."
}
```

**Status Codes:**
- `200 OK`: Event emitted successfully
- `404 Not Found`: Cow does not exist

**Notes:**
- Cow remains in database with `is_active = FALSE`
- Can be reactivated by emitting new event
- Soft delete preserves history

---

### Analytics API

**Architecture:** FastAPI queries Gold Delta Lake directly via DuckDB  
**Performance:** 10-50ms (direct Delta queries), 3-10ms (cached)  
**Update Frequency:** Real-time (queries canonical Gold Delta, zero sync lag)  
**Source of Truth:** Gold Delta Lake (single source of truth)

#### Get Herd Composition

**Endpoint:** `GET /api/v1/analytics/herd-composition`

**Description:** Get herd composition breakdown by breed, status, and sex. DuckDB queries Gold Delta tables directly.

**Query Parameters:**
- `tenant_id` (required, UUID): Tenant identifier
- `date` (optional, YYYY-MM-DD): Snapshot date (defaults to latest)

**Example Request:**
```bash
curl "http://localhost:8000/api/v1/analytics/herd-composition?tenant_id=550e8400-e29b-41d4-a716-446655440000"
```

**Example Response:**
```json
{
  "as_of": "2026-01-28T06:09:05.669752",
  "cached": false,
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": {
    "snapshot_date": "2026-01-28",
    "total_cows": 7,
    "by_breed": [
      {"dimension": "Simmental", "count": 2, "percentage": 28.0},
      {"dimension": "Jersey", "count": 1, "percentage": 14.0},
      {"dimension": "Black Angus", "count": 2, "percentage": 28.0},
      {"dimension": "Holstein", "count": 2, "percentage": 28.0}
    ],
    "by_status": [
      {"dimension": "inactive", "count": 2, "percentage": 28.0},
      {"dimension": "active", "count": 5, "percentage": 71.0}
    ],
    "by_sex": [
      {"dimension": "male", "count": 2, "percentage": 28.0},
      {"dimension": "female", "count": 5, "percentage": 71.0}
    ]
  }
}
```

**Status Codes:**
- `200 OK`: Success
- `404 Not Found`: No analytics data available for tenant or date
- `400 Bad Request`: Invalid date format

**Performance:**
- Direct DuckDB query: 10-50ms
- Cached query (5s TTL): 3-10ms
- Memory usage: ~100MB (FastAPI + DuckDB)

---

#### Get Cow Lifecycle (Legacy Endpoint)

**Endpoint:** `GET /api/v1/analytics/cow/{cow_id}`

**Description:** Get lifecycle tracking for a specific cow from analytics schema.

**Path Parameters:**
- `cow_id` (required, UUID): Cow identifier

**Query Parameters:**
- `tenant_id` (required, UUID): Tenant identifier

**Example Request:**
```bash
curl "http://localhost:8000/api/v1/analytics/cow/550e8400-e29b-41d4-a716-446655440000?tenant_id=550e8400-e29b-41d4-a716-446655440000"
```

**Example Response:**
```json
{
  "as_of": "2026-01-28T06:10:00.123456",
  "cached": false,
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": {
    "cow_id": "550e8400-e29b-41d4-a716-446655440000",
    "cow_name": null,
    "measurements": [
      {"date": "2026-01-15", "weight_kg": 0.0, "trend": "stable"},
      {"date": "2026-01-28", "weight_kg": 0.0, "trend": "stable"}
    ],
    "average_weight_kg": null,
    "weight_change_kg": null
  }
}
```

**Status Codes:**
- `200 OK`: Success
- `404 Not Found`: No lifecycle data available for cow

**Description:** Get aggregated herd statistics from Gold layer.

**Example Request:**
```bash
curl http://localhost:8000/api/analytics/herd
```

**Example Response:**
```json
{
  "total_cows": 150,
  "active_cows": 145,
  "inactive_cows": 5,
  "breed_distribution": {
    "Holstein": 60,
    "Angus": 50,
    "Hereford": 35,
    "Jersey": 5
  },
  "avg_age_months": 24,
  "total_weight_kg": 67500,
  "avg_weight_kg": 450,
  "last_updated": "2024-01-29T12:00:00Z"
}
```

**Status Codes:**
- `200 OK`: Success
- `500 Internal Server Error`: Gold layer unavailable

---

### Events API

#### Get Cow Events

**Endpoint:** `GET /api/events/cow/{cow_id}`

**Description:** Get event history for a specific cow from event store.

**Path Parameters:**
- `cow_id` (required, UUID): Cow identifier (aggregate_id)

**Example Request:**
```bash
curl http://localhost:8000/api/events/cow/550e8400-e29b-41d4-a716-446655440000
```

**Example Response:**
```json
[
  {
    "event_id": "660e8400-e29b-41d4-a716-446655440001",
    "aggregate_id": "550e8400-e29b-41d4-a716-446655440000",
    "event_type": "cow_created",
    "event_timestamp": "2024-01-20T10:30:00Z",
    "data": {
      "breed": "Holstein",
      "birth_date": "2024-01-15",
      "sex": "Female"
    },
    "published": true
  },
  {
    "event_id": "770e8400-e29b-41d4-a716-446655440002",
    "aggregate_id": "550e8400-e29b-41d4-a716-446655440000",
    "event_type": "cow_updated",
    "event_timestamp": "2024-01-21T15:45:00Z",
    "data": {
      "breed": "Angus",
      "previous_breed": "Holstein"
    },
    "published": true
  }
]
```

**Status Codes:**
- `200 OK`: Success (may return empty array)
- `422 Unprocessable Entity`: Invalid UUID

---

#### Get Recent Events

**Endpoint:** `GET /api/events/recent`

**Description:** Get most recent events across all cows.

**Query Parameters:**
- `limit` (optional, int): Max results (default: 10, max: 100)

**Example Request:**
```bash
curl "http://localhost:8000/api/events/recent?limit=20"
```

**Example Response:**
```json
[
  {
    "event_id": "990e8400-e29b-41d4-a716-446655440004",
    "aggregate_id": "550e8400-e29b-41d4-a716-446655440000",
    "event_type": "cow_updated",
    "event_timestamp": "2024-01-29T16:20:00Z",
    "data": {"breed": "Simmental"},
    "published": true
  }
]
```

---

#### Get Unpublished Events

**Endpoint:** `GET /api/events/unpublished`

**Description:** Count of events not yet published to Bronze layer.

**Example Request:**
```bash
curl http://localhost:8000/api/events/unpublished
```

**Example Response:**
```json
{
  "count": 3,
  "oldest_event_age_seconds": 15
}
```

**Status Codes:**
- `200 OK`: Success

---

### Sync API

#### Get Sync Status

**Endpoint:** `GET /api/sync/status`

**Description:** Get current synchronization status and lag.

**Example Request:**
```bash
curl http://localhost:8000/api/sync/status
```

**Example Response:**
```json
{
  "table_name": "cows",
  "last_watermark": "2024-01-29T16:00:00Z",
  "last_sync_started_at": "2024-01-29T16:15:30Z",
  "last_sync_completed_at": "2024-01-29T16:15:45Z",
  "sync_lag_seconds": 12,
  "total_rows_synced": 1523,
  "total_conflicts_resolved": 2,
  "status": "healthy"
}
```

**Status Codes:**
- `200 OK`: Success

**Status Values:**
- `healthy`: Lag < 60 seconds
- `warning`: Lag 60-300 seconds
- `critical`: Lag > 300 seconds
- `unknown`: No sync completed yet

---

#### Get Sync History

**Endpoint:** `GET /api/sync/history`

**Description:** Get recent sync job execution history.

**Query Parameters:**
- `limit` (optional, int): Max results (default: 20)

**Example Request:**
```bash
curl "http://localhost:8000/api/sync/history?limit=10"
```

**Example Response:**
```json
[
  {
    "run_id": "aa0e8400-e29b-41d4-a716-446655440005",
    "started_at": "2024-01-29T16:15:30Z",
    "completed_at": "2024-01-29T16:15:45Z",
    "rows_synced": 15,
    "conflicts_resolved": 0,
    "duration_seconds": 15,
    "status": "success"
  }
]
```

---

### Health API

#### Health Check

**Endpoint:** `GET /api/health`

**Description:** Check API health status.

**Example Request:**
```bash
curl http://localhost:8000/api/health
```

**Example Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-29T16:20:00Z",
  "checks": {
    "database": "ok",
    "event_store": "ok",
    "delta_lake": "ok"
  }
}
```

**Status Codes:**
- `200 OK`: All checks passed
- `503 Service Unavailable`: One or more checks failed

---

## Event Payload Schemas

### cow_created

```json
{
  "event_id": "uuid",
  "aggregate_id": "uuid (cow_id)",
  "event_type": "cow_created",
  "event_timestamp": "ISO 8601 datetime",
  "data": {
    "breed": "string",
    "birth_date": "ISO 8601 date",
    "sex": "Male|Female"
  },
  "published": false
}
```

### cow_updated

```json
{
  "event_id": "uuid",
  "aggregate_id": "uuid (cow_id)",
  "event_type": "cow_updated",
  "event_timestamp": "ISO 8601 datetime",
  "data": {
    "breed": "string",
    "previous_breed": "string (optional)"
  },
  "published": false
}
```

### cow_deactivated

```json
{
  "event_id": "uuid",
  "aggregate_id": "uuid (cow_id)",
  "event_type": "cow_deactivated",
  "event_timestamp": "ISO 8601 datetime",
  "data": {
    "reason": "string (optional)",
    "deactivated_at": "ISO 8601 datetime"
  },
  "published": false
}
```

---

## Response Formats

### Success Response

```json
{
  "data": {},
  "message": "Optional success message",
  "metadata": {
    "timestamp": "ISO 8601 datetime",
    "request_id": "uuid"
  }
}
```

### Error Response

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "details": {}
  },
  "metadata": {
    "timestamp": "ISO 8601 datetime",
    "request_id": "uuid"
  }
}
```

---

## Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `COW_NOT_FOUND` | 404 | Cow with specified ID not found |
| `INVALID_UUID` | 422 | Invalid UUID format |
| `VALIDATION_ERROR` | 400 | Request validation failed |
| `DATABASE_ERROR` | 500 | Database operation failed |
| `SYNC_ERROR` | 500 | Sync job error |
| `UNAUTHORIZED` | 401 | Authentication required (future) |
| `FORBIDDEN` | 403 | Insufficient permissions (future) |

### Example Error Response

```json
{
  "detail": "Cow not found with ID: 550e8400-e29b-41d4-a716-446655440000",
  "error_code": "COW_NOT_FOUND",
  "timestamp": "2024-01-29T16:20:00Z"
}
```

---

## Rate Limiting

**Current:** No rate limiting

**Production:** 
- 1000 requests/hour per IP
- 100 write operations/minute per IP

```http
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 995
X-RateLimit-Reset: 1706545200
```

---

## Webhooks (Future)

**Planned:** Subscribe to events via webhooks

```json
{
  "url": "https://your-app.com/webhooks/cattle",
  "events": ["cow_created", "cow_updated", "cow_deactivated"],
  "secret": "webhook_secret_key"
}
```

---

## Client Libraries

### Python

```python
# endymion_ai_client.py
import requests

class Endymion-AIClient:
    def __init__(self, base_url="http://localhost:8000/api"):
        self.base_url = base_url
    
    def get_cows(self):
        response = requests.get(f"{self.base_url}/cows")
        response.raise_for_status()
        return response.json()
    
    def create_cow(self, breed, birth_date, sex):
        data = {
            "breed": breed,
            "birth_date": birth_date,
            "sex": sex
        }
        response = requests.post(f"{self.base_url}/cows", json=data)
        response.raise_for_status()
        return response.json()

# Usage
client = Endymion-AIClient()
cows = client.get_cows()
new_cow = client.create_cow("Holstein", "2024-01-15", "Female")
```

### JavaScript

```javascript
// endymion-ai-client.js
class Endymion-AIClient {
  constructor(baseUrl = 'http://localhost:8000/api') {
    this.baseUrl = baseUrl;
  }

  async getCows() {
    const response = await fetch(`${this.baseUrl}/cows`);
    if (!response.ok) throw new Error('Failed to fetch cows');
    return response.json();
  }

  async createCow(breed, birthDate, sex) {
    const response = await fetch(`${this.baseUrl}/cows`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ breed, birth_date: birthDate, sex })
    });
    if (!response.ok) throw new Error('Failed to create cow');
    return response.json();
  }
}

// Usage
const client = new Endymion-AIClient();
const cows = await client.getCows();
const newCow = await client.createCow('Holstein', '2024-01-15', 'Female');
```

---

## OpenAPI Specification

**Interactive Docs:** http://localhost:8000/docs  
**ReDoc:** http://localhost:8000/redoc  
**OpenAPI JSON:** http://localhost:8000/openapi.json

### Download OpenAPI Spec

```bash
curl http://localhost:8000/openapi.json > openapi.json
```

### Generate Client

```bash
# Using openapi-generator
openapi-generator generate \
  -i http://localhost:8000/openapi.json \
  -g python \
  -o ./client-python

openapi-generator generate \
  -i http://localhost:8000/openapi.json \
  -g typescript-fetch \
  -o ./client-typescript
```

---

## Testing Examples

### Using curl

```bash
# Create cow
COW_RESPONSE=$(curl -s -X POST http://localhost:8000/api/cows \
  -H "Content-Type: application/json" \
  -d '{"breed": "Holstein", "birth_date": "2024-01-15", "sex": "Female"}')

COW_ID=$(echo $COW_RESPONSE | jq -r '.cow_id')
echo "Created cow: $COW_ID"

# Wait for sync
sleep 35

# Get cow
curl http://localhost:8000/api/cows/$COW_ID | jq .

# Update breed
curl -X PUT http://localhost:8000/api/cows/$COW_ID/breed \
  -H "Content-Type: application/json" \
  -d '{"breed": "Angus"}' | jq .

# Get events
curl http://localhost:8000/api/events/cow/$COW_ID | jq .
```

### Using Postman

Import OpenAPI spec:
1. File → Import → Link
2. Enter: http://localhost:8000/openapi.json
3. Import as Postman Collection

### Using HTTPie

```bash
# Create cow
http POST localhost:8000/api/cows \
  breed="Holstein" \
  birth_date="2024-01-15" \
  sex="Female"

# Get cow
http GET localhost:8000/api/cows/$COW_ID

# Update breed
http PUT localhost:8000/api/cows/$COW_ID/breed \
  breed="Angus"
```

---

## Versioning

**Current Version:** v1 (implied, no prefix)

**Future Versions:** Will use URL prefix
- v1: `/api/v1/cows`
- v2: `/api/v2/cows`

**Deprecation Policy:**
- 6 months notice before deprecation
- 12 months support for deprecated versions

---

## Additional Resources

- [Developer Guide](./DEVELOPER.md)
- [Architecture Documentation](../ARCHITECTURE.md)
- [Frontend README](../frontend/README.md)
- [Operations Guide](./OPERATIONS.md)

---

**Last Updated:** January 2026  
**API Version:** 1.0.0  
**Maintainer:** Endymion-AI Team
