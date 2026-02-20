# SQLAlchemy ORM Models

Complete SQLAlchemy ORM models matching the SQL schema defined in [`schema.sql`](../database/schema.sql).

## Architecture

This models package implements the **Pure Projection Pattern A**:

- **Write Path**: API → `cow_events` (event sourcing) → Bronze → Silver → Gold
- **Read Path**: API ← `cows` (projection table) ← Silver layer

### Event Sourcing (cow_events)

All write operations create immutable events in the `cow_events` table:

```python
from backend.models import CowEvent, CowEventType

# Create event (source of truth)
event = CowEvent.create_event(
    tenant_id=tenant_id,
    cow_id=cow_id,
    event_type=CowEventType.COW_CREATED,
    payload='{"breed": "Holstein", "tag_number": "US-001"}',
    created_by="user@example.com"
)
session.add(event)
session.commit()

# Events are IMMUTABLE - this will raise an error:
# event.event_type = "something_else"  # ❌ AttributeError

# Only publishing status can be updated:
event.mark_as_published()  # ✅ Called by sync job
```

### Projection Tables (cows)

The `cows` table is a **read-only projection** maintained by the data pipeline:

```python
from backend.models import Cow

# Read from projection (always use for queries)
active_cows = Cow.get_active_cows(session, tenant_id).all()

# Check data freshness
for cow in active_cows:
    if cow.is_stale(max_age_minutes=15):
        print(f"Cow {cow.tag_number} projection is stale!")

# ❌ NEVER write directly to projection tables from API!
# Writes must go through cow_events
```

## Models

### Base Classes

Located in [`base.py`](base.py):

- **`Base`**: SQLAlchemy declarative base
- **`TenantMixin`**: Adds `tenant_id` and tenant filtering methods
- **`TimestampMixin`**: Adds `created_at`, `updated_at` timestamps
- **`UUIDPrimaryKeyMixin`**: UUID primary key with auto-generation
- **`ProjectionMixin`**: Adds sync metadata for projection tables
- **`SoftDeleteMixin`**: Soft delete functionality
- **`TenantBaseModel`**: Combines tenant + timestamp + UUID + soft delete
- **`ProjectionBaseModel`**: Combines tenant + timestamp + UUID + projection

### Event Models

#### CowEvent ([events.py](events.py))

Event sourcing table for cow lifecycle events (append-only).

**Key Features:**
- ✅ Immutable after creation (append-only enforced)
- ✅ Event type validation via `CowEventType` enum
- ✅ JSON payload for flexibility
- ✅ Publishing status tracking
- ✅ Full event history per cow
- ❌ No updates allowed (only publishing status)
- ❌ No deletes allowed

**Event Types:**
- `cow_created` - New cow registration
- `cow_updated` - Cow attribute changes
- `cow_deactivated` - Cow status change to inactive
- `cow_weight_recorded` - Weight measurement
- `cow_health_event` - Health-related events

**Usage:**
```python
# Create event
event = CowEvent.create_event(
    tenant_id=tenant_id,
    cow_id=cow_id,
    event_type=CowEventType.COW_WEIGHT_RECORDED,
    payload='{"weight_kg": 475.5, "measured_by": "John"}',
    event_time=datetime.utcnow(),
    created_by="api-user",
    correlation_id=request_id
)
session.add(event)
session.commit()

# Query unpublished events (for sync job)
unpublished = CowEvent.get_unpublished_events(session, limit=100).all()

# Get cow history
history = CowEvent.get_cow_history(session, cow_id, tenant_id).all()
```

### Projection Models

#### Cow ([cows.py](cows.py))

Projection table representing current state of cows (read-only).

**Key Features:**
- ✅ `is_projection()` returns `True`
- ✅ Sync metadata (last_synced_at, sync_version)
- ✅ Staleness detection
- ✅ Age calculation helpers
- ✅ Genealogy tracking (dam_id, sire_id)
- ❌ Read-only from API perspective

**Attributes:**
- Identity: `tag_number`, `name`
- Biology: `breed`, `birth_date`, `sex`
- Status: `status` (active/sold/deceased/transferred)
- Physical: `weight_kg`, `last_weight_date`
- Genealogy: `dam_id`, `sire_id`
- Location: `current_location`

**Usage:**
```python
# Query active cows
active_cows = Cow.get_active_cows(session, tenant_id).all()

# Find by tag
cow = Cow.get_by_tag(session, "US-001", tenant_id)

# Calculate age
print(f"Age: {cow.get_age_display()}")  # "2y 3m"
print(f"Age in days: {cow.age_days}")

# Check data freshness
if cow.is_stale(max_age_minutes=15):
    print("Warning: Data may be outdated")

# Get offspring
children = Cow.get_offspring(session, parent_id, tenant_id).all()

# Serialize
cow_dict = cow.to_dict(include_sync_info=True)
```

### Reference Models

#### Category ([categories.py](categories.py))

Flexible taxonomy system for categorization (read-write).

**Key Features:**
- ✅ Global categories (tenant_id=NULL)
- ✅ Tenant-specific categories
- ✅ Hierarchical structure (parent_category_id)
- ✅ Display properties (color, icon, order)
- ✅ Active/inactive status

**Category Types:**
- `cow_type` - Breed classifications
- `health` - Health event categories
- `location` - Location/facility categories
- Custom types per tenant

**Usage:**
```python
# Create global category
global_cat = Category.create_category(
    name="Dairy Breeds",
    category_type="cow_type",
    is_global=True,
    description="Global dairy breed category"
)
session.add(global_cat)
session.commit()

# Create tenant category
tenant_cat = Category.create_category(
    name="Custom Breed Group",
    category_type="cow_type",
    tenant_id=tenant_id,
    is_global=False,
    parent_category_id=global_cat.id
)
session.add(tenant_cat)
session.commit()

# Query categories for tenant (includes global)
categories = Category.get_tenant_categories(
    session,
    tenant_id=tenant_id,
    category_type="cow_type",
    include_global=True
).all()

# Check visibility
if category.is_visible_to_tenant(tenant_id):
    print(f"Visible: {category.full_path}")
```

## Database Connection

Located in [`../database/connection.py`](../database/connection.py).

### Configuration

Database connection is configured via environment variables:

```bash
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=cattledb
SQLSERVER_USERNAME=sa
SQLSERVER_PASSWORD=YourStrong@Password
SQLSERVER_DRIVER=ODBC Driver 18 for SQL Server

# Connection pool settings
SQLSERVER_POOL_SIZE=5
SQLSERVER_MAX_OVERFLOW=10
SQLSERVER_POOL_TIMEOUT=30
SQLSERVER_POOL_RECYCLE=3600
```

### Usage

#### Basic Session Management

```python
from backend.database.connection import get_db_session

# Context manager (recommended)
with get_db_session() as session:
    cows = session.query(Cow).all()
# Session automatically closed
```

#### DatabaseManager

```python
from backend.database.connection import get_db_manager

# Get singleton instance
db = get_db_manager()

# Test connection
if db.test_connection():
    print("Connected successfully!")

# Get connection info
info = db.get_connection_info()
print(f"Connected to {info['host']}:{info['port']}")
```

#### FastAPI Dependency

```python
from fastapi import Depends
from sqlalchemy.orm import Session
from backend.database.connection import get_session

@app.get("/cows")
def list_cows(
    session: Session = Depends(get_session),
    tenant_id: UUID = Depends(get_current_tenant)
):
    return Cow.get_active_cows(session, tenant_id).all()
```

#### Tenant-Scoped Sessions

```python
from backend.database.connection import get_db_manager

db = get_db_manager()

with db.get_tenant_session(tenant_id) as session:
    # All queries automatically filtered by tenant
    cows = session.query(Cow).all()
```

## Multi-Tenancy

All models with `TenantMixin` include tenant isolation:

```python
from backend.models import TenantMixin

# Get tenant filter
filter_clause = Cow.get_tenant_filter(tenant_id)

# Query with tenant isolation
cows = session.query(Cow).filter(filter_clause).all()

# Check ownership
if cow.is_owned_by_tenant(tenant_id):
    print("Access granted")
```

## Testing

Run the model tests:

```bash
# Run all model tests
pytest backend/tests/test_models.py -v

# Run specific test class
pytest backend/tests/test_models.py::TestCowEvent -v

# Run specific test
pytest backend/tests/test_models.py::TestCowEvent::test_cow_event_immutability -v

# Show print statements
pytest backend/tests/test_models.py -v -s
```

### Test Coverage

Tests verify:
- ✅ Model creation and validation
- ✅ Append-only constraint on CowEvent
- ✅ Event immutability (cannot modify after creation)
- ✅ Tenant isolation across all models
- ✅ Projection table markers (`is_projection()`)
- ✅ Staleness detection for projections
- ✅ Category hierarchy and visibility
- ✅ Query methods and filters
- ✅ Serialization (to_dict methods)

## Important Constraints

### ❌ Never Do This

```python
# DON'T: Modify events after creation
event.event_type = "something_else"  # ❌ Raises AttributeError

# DON'T: Write to projection tables from API
cow = Cow(...)
session.add(cow)  # ❌ Violates Pure Projection Pattern

# DON'T: Delete events
session.delete(event)  # ❌ Events are append-only

# DON'T: Query across tenants without filter
cows = session.query(Cow).all()  # ❌ Missing tenant filter
```

### ✅ Always Do This

```python
# DO: Create events for write operations
event = CowEvent.create_event(...)
session.add(event)
session.commit()

# DO: Read from projection tables
cows = Cow.get_active_cows(session, tenant_id).all()

# DO: Use tenant filters
cows = session.query(Cow).filter(
    Cow.get_tenant_filter(tenant_id)
).all()

# DO: Check projection freshness
if cow.is_stale(max_age_minutes=15):
    print("Warning: stale data")

# DO: Only mark events as published (sync job only)
event.mark_as_published()  # ✅ Only allowed modification
```

## Data Flow Summary

```
┌─────────────┐
│   API       │
└──────┬──────┘
       │
       │ writes
       ▼
┌─────────────────┐
│  cow_events     │  ◄── Source of Truth (append-only)
│  (this module)  │
└────────┬────────┘
         │
         │ CDC/batch
         ▼
┌─────────────────┐
│  Bronze Layer   │
└────────┬────────┘
         │
         │ transform
         ▼
┌─────────────────┐
│  Silver Layer   │
└────────┬────────┘
         │
         │ sync back
         ▼
┌─────────────────┐
│  cows           │  ◄── Read by API
│  (projection)   │
└─────────────────┘
```

## Related Documentation

- [Database Schema](../database/schema.sql) - SQL table definitions
- [Database README](../database/README.md) - Database usage guide
- [Architecture Document](../../docs/architecture/) - System architecture
- [API Endpoints](../api/) - REST API implementation

## Migration Notes

For production, use **Alembic** for database migrations:

```bash
# Initialize Alembic
alembic init alembic

# Create migration
alembic revision --autogenerate -m "Initial models"

# Apply migration
alembic upgrade head
```

Never use `Base.metadata.create_all()` in production!
