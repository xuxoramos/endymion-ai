# Database Schema Documentation

## Overview

This database implements the **Pure Projection Pattern A** from the CattleSaaS architecture, which separates write and read concerns:

- **Write Path**: API → `cow_events` → Bronze → Silver → Gold
- **Read Path**: Silver → `cows` (projection) → API

## ⚠️ Important Architectural Pattern

### Source of Truth
- **`cow_events`** table: SOURCE OF TRUTH for all writes
- **`cows`** table: READ-ONLY projection synchronized from Databricks Silver layer

### Never Do This! ❌
```sql
-- WRONG! Never insert/update cows table directly
UPDATE operational.cows SET breed = 'Jersey' WHERE cow_id = '...';
```

### Always Do This! ✅
```sql
-- CORRECT! Write to events table
INSERT INTO operational.cow_events (tenant_id, cow_id, event_type, payload, event_time)
VALUES (...);
```

## Schema Files

| File | Purpose | When to Run |
|------|---------|-------------|
| `schema.sql` | Creates all tables, indexes, views | Once during setup |
| `seed.sql` | Inserts sample test data | Development/testing only |
| `test-schema.sh` | Validates schema and runs tests | After any schema changes |

## Tables

### 1. `operational.cow_events` (Event Sourcing / Outbox)

**Purpose**: Append-only event log for all cow operations

**Columns**:
- `event_id` (PK): Unique event identifier
- `tenant_id`: Multi-tenant isolation
- `cow_id`: Reference to cow entity
- `event_type`: Type of event (cow_created, cow_updated, etc.)
- `payload`: JSON data containing event details
- `event_time`: When the event occurred
- `published_to_bronze`: Sync status flag
- `created_at`: Record creation timestamp

**Event Types**:
- `cow_created` - New cow registered
- `cow_updated` - Cow details modified
- `cow_deactivated` - Cow removed from active herd
- `cow_weight_recorded` - Weight measurement
- `cow_health_event` - Health/medical event

**Indexes**:
- `IX_cow_events_tenant_id` - Tenant queries
- `IX_cow_events_cow_id` - Cow history
- `IX_cow_events_published` - Unpublished events (filtered)
- `IX_cow_events_event_type` - Event type filtering

**Example**:
```sql
-- Create a new cow event
INSERT INTO operational.cow_events (
    tenant_id,
    cow_id,
    event_type,
    payload,
    event_time,
    published_to_bronze
)
VALUES (
    '12345678-1234-1234-1234-123456789012',
    NEWID(),
    'cow_created',
    JSON_QUERY('{
        "tag_number": "US-123456",
        "breed": "Holstein",
        "birth_date": "2023-05-15",
        "sex": "F"
    }'),
    GETUTCDATE(),
    0
);
```

### 2. `operational.cows` (Projection Table - READ ONLY)

**Purpose**: Materialized view for fast API reads, synchronized from Silver layer

**⚠️ Critical**: This is NOT the source of truth. Never write to this table directly!

**Columns**:
- `cow_id` (PK): Unique cow identifier
- `tenant_id`: Multi-tenant isolation
- `tag_number`: Visual ID tag (unique per tenant)
- `breed`, `birth_date`, `sex`: Core attributes
- `status`: active, inactive, sold, deceased
- `weight_kg`: Latest weight
- `dam_id`, `sire_id`: Genealogy references
- `silver_last_updated_at`: When Silver layer updated this record
- `last_synced_at`: When we synced from Silver
- `sync_version`: Optimistic concurrency control

**Indexes**:
- `IX_cows_tenant_id` - Tenant queries
- `IX_cows_tag_number` - Tag lookups
- `IX_cows_status` - Status filtering
- `IX_cows_breed` - Breed filtering
- `IX_cows_sync` - Sync management

**Sync Process**:
```sql
-- This is done by automated sync job, NOT by API!
UPDATE operational.cows
SET 
    breed = @breed,
    weight_kg = @weight,
    silver_last_updated_at = @silver_timestamp,
    last_synced_at = GETUTCDATE(),
    sync_version = sync_version + 1
WHERE cow_id = @cow_id;
```

### 3. `operational.categories` (Reference Data)

**Purpose**: Lookup/reference data for breeds, locations, health conditions

**Columns**:
- `category_id` (PK): Unique identifier
- `tenant_id`: NULL for global, UUID for tenant-specific
- `category_type`: Type of category (breed, location, health_condition)
- `name`: Display name
- `description`: Optional description
- `parent_category_id`: For hierarchical categories
- `is_active`: Soft delete flag

**Examples**:
```sql
-- Global breed category
INSERT INTO operational.categories (category_type, name, is_active)
VALUES ('breed', 'Holstein', 1);

-- Tenant-specific location
INSERT INTO operational.categories (tenant_id, category_type, name, is_active)
VALUES ('tenant-uuid', 'location', 'North Pasture', 1);
```

### 4. `tenant.tenants` (Tenant Management)

**Purpose**: Multi-tenant configuration and management

**Columns**:
- `tenant_id` (PK): Unique tenant identifier
- `tenant_name`: Display name
- `tenant_code`: Short URL-friendly code
- `subscription_tier`: basic, premium, enterprise
- `subscription_status`: active, suspended, cancelled
- `max_cows`, `max_users`: Limits
- `settings`: JSON configuration

## Views

### `operational.vw_active_cows`

Active cows across all tenants with basic information.

```sql
SELECT * FROM operational.vw_active_cows
WHERE tenant_id = @tenant_id;
```

### `operational.vw_recent_events`

Events from the last 30 days.

```sql
SELECT * FROM operational.vw_recent_events
WHERE tenant_id = @tenant_id
ORDER BY event_time DESC;
```

## Setup Instructions

### 1. Start SQL Server Container

```bash
docker compose -f docker/docker-compose.yml up -d sqlserver
```

### 2. Run Schema Creation

```bash
docker exec -i cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' \
  < backend/database/schema.sql
```

### 3. Load Sample Data (Optional)

```bash
docker exec -i cattlesaas-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' \
  < backend/database/seed.sql
```

### 4. Run Tests

```bash
./backend/database/test-schema.sh
```

## Usage Patterns

### Writing Data (API Operations)

**Always** write to `cow_events` table:

```sql
-- Create cow
INSERT INTO operational.cow_events (tenant_id, cow_id, event_type, payload, event_time)
VALUES (@tenant_id, NEWID(), 'cow_created', @json_payload, GETUTCDATE());

-- Update cow
INSERT INTO operational.cow_events (tenant_id, cow_id, event_type, payload, event_time)
VALUES (@tenant_id, @cow_id, 'cow_updated', @json_payload, GETUTCDATE());

-- Record weight
INSERT INTO operational.cow_events (tenant_id, cow_id, event_type, payload, event_time)
VALUES (@tenant_id, @cow_id, 'cow_weight_recorded', @json_payload, GETUTCDATE());
```

### Reading Data (API Queries)

Read from `cows` projection table:

```sql
-- Get active cows for tenant
SELECT * FROM operational.cows
WHERE tenant_id = @tenant_id
  AND status = 'active'
ORDER BY tag_number;

-- Get cow by tag
SELECT * FROM operational.cows
WHERE tenant_id = @tenant_id
  AND tag_number = @tag_number;

-- Get cows by breed
SELECT * FROM operational.cows
WHERE tenant_id = @tenant_id
  AND breed = @breed
  AND status = 'active';
```

### Sync Operations

Check unpublished events:

```sql
-- Events waiting to be published to Bronze
SELECT * FROM operational.cow_events
WHERE published_to_bronze = 0
ORDER BY event_time ASC;

-- Mark events as published
UPDATE operational.cow_events
SET published_to_bronze = 1,
    published_at = GETUTCDATE()
WHERE event_id IN (SELECT TOP 100 event_id FROM operational.cow_events WHERE published_to_bronze = 0);
```

## Common Queries

### Tenant Statistics

```sql
SELECT 
    t.tenant_name,
    COUNT(DISTINCT c.cow_id) as total_cows,
    SUM(CASE WHEN c.status = 'active' THEN 1 ELSE 0 END) as active_cows,
    COUNT(DISTINCT e.event_id) as total_events
FROM tenant.tenants t
LEFT JOIN operational.cows c ON t.tenant_id = c.tenant_id
LEFT JOIN operational.cow_events e ON t.tenant_id = e.tenant_id
WHERE t.is_active = 1
GROUP BY t.tenant_name;
```

### Cow History

```sql
-- Get full event history for a cow
SELECT 
    event_id,
    event_type,
    payload,
    event_time,
    published_to_bronze
FROM operational.cow_events
WHERE cow_id = @cow_id
ORDER BY event_time DESC;
```

### Sync Status

```sql
-- Check sync lag
SELECT 
    tenant_id,
    COUNT(*) as cow_count,
    MAX(last_synced_at) as last_sync,
    DATEDIFF(MINUTE, MAX(last_synced_at), GETUTCDATE()) as minutes_since_sync
FROM operational.cows
GROUP BY tenant_id;
```

## Data Flow

```
┌─────────────┐
│   FastAPI   │
│     API     │
└──────┬──────┘
       │ writes
       ▼
┌─────────────────┐
│  cow_events     │ ◄── SOURCE OF TRUTH
│  (Event Log)    │
└────────┬────────┘
         │ CDC/Batch
         ▼
┌─────────────────┐
│ Bronze Layer    │
│  (Databricks)   │
└────────┬────────┘
         │ DLT Pipeline
         ▼
┌─────────────────┐
│ Silver Layer    │
│  (Databricks)   │
└────────┬────────┘
         │ Sync Job
         ▼
┌─────────────────┐
│  cows           │ ◄── READ PROJECTION
│  (Projection)   │
└────────┬────────┘
         │ reads
         ▼
┌─────────────────┐
│   FastAPI       │
│     API         │
└─────────────────┘
```

## Troubleshooting

### Problem: Projection table is out of sync

**Check sync status:**
```sql
SELECT cow_id, last_synced_at, silver_last_updated_at
FROM operational.cows
WHERE last_synced_at < DATEADD(HOUR, -1, GETUTCDATE());
```

### Problem: Events not being published

**Check unpublished events:**
```sql
SELECT COUNT(*), MIN(event_time), MAX(event_time)
FROM operational.cow_events
WHERE published_to_bronze = 0;
```

### Problem: Duplicate tag numbers

**Check for duplicates:**
```sql
SELECT tenant_id, tag_number, COUNT(*)
FROM operational.cows
GROUP BY tenant_id, tag_number
HAVING COUNT(*) > 1;
```

## Best Practices

1. ✅ **Always write to cow_events table**
2. ✅ **Always read from cows table** (for current state)
3. ✅ **Use cow_events for history/audit**
4. ✅ **Include tenant_id in all queries**
5. ✅ **Use JSON for flexible event payloads**
6. ❌ **Never write directly to cows table**
7. ❌ **Never delete from cow_events** (it's append-only)

## Migration Strategy

When adding new fields:

1. Update `cow_events` payload structure (it's JSON, so flexible)
2. Add column to `cows` table
3. Update sync job to populate new field
4. Backfill existing records if needed

## Performance Considerations

- `cow_events` table grows indefinitely (consider partitioning)
- Indexes on `tenant_id` are crucial for multi-tenancy
- Filtered index on `published_to_bronze = 0` optimizes sync queries
- Use `INCLUDE` columns in indexes for covering queries

## Security

- Row-level security should filter by `tenant_id`
- API should always include tenant context
- Never expose raw event payloads to users
- Consider encrypting sensitive fields in JSON payloads
