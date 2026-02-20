"""
Unit tests for SQLAlchemy ORM models.

Tests cover:
- Model creation and validation
- Append-only constraint on CowEvent
- Tenant isolation
- Projection table markers
- Relationships and queries
"""

import json
import pytest
from datetime import datetime, date, timedelta
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError, InvalidRequestError

from backend.models import (
    Base,
    CowEvent,
    CowEventType,
    Cow,
    CowStatus,
    CowSex,
    Category,
)
from backend.database.connection import DatabaseManager


@pytest.fixture(scope="function")
def db_engine():
    """Create SQL Server test database engine."""
    db_manager = DatabaseManager()
    engine = db_manager.init_engine()
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine):
    """Create database session for testing."""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    
    # Clean up test data before test
    session.execute(text("DELETE FROM operational.cows"))
    session.execute(text("DELETE FROM operational.cow_events"))
    session.execute(text("DELETE FROM operational.categories"))
    session.commit()
    
    yield session
    
    # Clean up after test
    session.rollback()
    session.execute(text("DELETE FROM operational.cows"))
    session.execute(text("DELETE FROM operational.cow_events"))
    session.execute(text("DELETE FROM operational.categories"))
    session.commit()
    session.close()


@pytest.fixture
def tenant_id():
    """Generate test tenant ID."""
    return uuid4()


@pytest.fixture
def cow_id():
    """Generate test cow ID."""
    return uuid4()


class TestCowEvent:
    """Tests for CowEvent model (append-only event sourcing)."""
    
    def test_create_cow_event(self, db_session: Session, tenant_id, cow_id):
        """Test creating a cow event."""
        event = CowEvent.create_event(
            tenant_id=tenant_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"breed": "Holstein", "tag_number": "US-001"}',
            created_by="test@example.com"
        )
        
        db_session.add(event)
        db_session.commit()
        
        assert event.event_id is not None
        assert event.tenant_id == tenant_id
        assert event.cow_id == cow_id
        assert event.event_type == CowEventType.COW_CREATED.value
        assert event.published_to_bronze is False
        assert event.created_by == "test@example.com"
    
    def test_cow_event_immutability(self, db_session: Session, tenant_id, cow_id):
        """Test that cow events cannot be modified after creation."""
        event = CowEvent.create_event(
            tenant_id=tenant_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"breed": "Holstein"}',
        )
        
        db_session.add(event)
        db_session.commit()
        
        # Try to modify immutable field - should raise error
        with pytest.raises(AttributeError, match="immutable"):
            event.event_type = CowEventType.COW_UPDATED.value
    
    def test_cow_event_mark_as_published(self, db_session: Session, tenant_id, cow_id):
        """Test marking event as published (only allowed modification)."""
        event = CowEvent.create_event(
            tenant_id=tenant_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"breed": "Holstein"}',
        )
        
        db_session.add(event)
        db_session.commit()
        
        # Refresh to ensure all attributes are loaded
        db_session.refresh(event)
        
        assert event.is_pending is True
        assert event.is_published is False
        
        # Mark as published (this should work)
        event.mark_as_published()
        db_session.commit()
        
        assert event.is_published is True
        assert event.published_at is not None
    
    def test_get_unpublished_events(self, db_session: Session, tenant_id, cow_id):
        """Test querying unpublished events."""
        # Create published and unpublished events
        event1 = CowEvent.create_event(
            tenant_id=tenant_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"test": 1}',
        )
        event2 = CowEvent.create_event(
            tenant_id=tenant_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_UPDATED,
            payload='{"test": 2}',
        )
        
        db_session.add_all([event1, event2])
        db_session.commit()
        
        # Mark one as published
        event1.mark_as_published()
        db_session.commit()
        
        # Query unpublished
        unpublished = CowEvent.get_unpublished_events(db_session).all()
        
        assert len(unpublished) == 1
        assert unpublished[0].event_id == event2.event_id
    
    def test_get_cow_history(self, db_session: Session, tenant_id, cow_id):
        """Test getting event history for a cow."""
        # Create multiple events for same cow
        events = [
            CowEvent.create_event(
                tenant_id=tenant_id,
                cow_id=cow_id,
                event_type=CowEventType.COW_CREATED,
                payload='{"step": 1}',
                event_time=datetime.utcnow() - timedelta(hours=3)
            ),
            CowEvent.create_event(
                tenant_id=tenant_id,
                cow_id=cow_id,
                event_type=CowEventType.COW_UPDATED,
                payload='{"step": 2}',
                event_time=datetime.utcnow() - timedelta(hours=2)
            ),
            CowEvent.create_event(
                tenant_id=tenant_id,
                cow_id=cow_id,
                event_type=CowEventType.COW_WEIGHT_RECORDED,
                payload='{"step": 3}',
                event_time=datetime.utcnow() - timedelta(hours=1)
            ),
        ]
        
        db_session.add_all(events)
        db_session.commit()
        
        # Get history (should be ordered newest first)
        history = CowEvent.get_cow_history(db_session, cow_id, tenant_id).all()
        
        assert len(history) == 3
        assert history[0].event_type == CowEventType.COW_WEIGHT_RECORDED.value
        assert history[1].event_type == CowEventType.COW_UPDATED.value
        assert history[2].event_type == CowEventType.COW_CREATED.value
    
    def test_cow_event_to_dict(self, db_session: Session, tenant_id, cow_id):
        """Test serializing event to dictionary."""
        event = CowEvent.create_event(
            tenant_id=tenant_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"breed": "Holstein"}',
        )
        
        db_session.add(event)
        db_session.commit()
        
        event_dict = event.to_dict()
        
        assert event_dict["event_id"] == str(event.event_id)
        assert event_dict["tenant_id"] == str(tenant_id)
        assert event_dict["cow_id"] == str(cow_id)
        assert event_dict["event_type"] == CowEventType.COW_CREATED.value
        assert event_dict["payload"] == '{"breed": "Holstein"}'
    
    def test_tenant_isolation_events(self, db_session: Session):
        """Test that events are isolated by tenant."""
        tenant1_id = uuid4()
        tenant2_id = uuid4()
        cow_id = uuid4()
        
        event1 = CowEvent.create_event(
            tenant_id=tenant1_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"tenant": 1}',
        )
        event2 = CowEvent.create_event(
            tenant_id=tenant2_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"tenant": 2}',
        )
        
        db_session.add_all([event1, event2])
        db_session.commit()
        
        # Query for tenant1
        tenant1_events = (
            db_session.query(CowEvent)
            .filter(CowEvent.get_tenant_filter(tenant1_id))
            .all()
        )
        
        assert len(tenant1_events) == 1
        assert tenant1_events[0].event_id == event1.event_id


class TestCow:
    """Tests for Cow model (projection table)."""
    
    def test_create_cow_projection(self, db_session: Session, tenant_id):
        """Test creating a cow projection."""
        cow = Cow(
            tenant_id=tenant_id,
            tag_number="US-001",
            name="Bessie",
            breed="Holstein",
            birth_date=date(2022, 1, 15),
            sex=CowSex.FEMALE,
            status=CowStatus.ACTIVE,
            weight_kg=450.5,
            last_synced_at=datetime.utcnow(),
            sync_version=1,
        )
        
        db_session.add(cow)
        db_session.commit()
        
        assert cow.cow_id is not None
        assert cow.tag_number == "US-001"
        assert cow.breed == "Holstein"
        assert cow.is_projection() is True
    
    def test_cow_age_calculation(self, db_session: Session, tenant_id):
        """Test cow age calculation."""
        cow = Cow(
            tenant_id=tenant_id,
            tag_number="US-002",
            breed="Jersey",
            birth_date=date.today() - timedelta(days=730),  # 2 years old
            sex=CowSex.FEMALE,
            status=CowStatus.ACTIVE,
            last_synced_at=datetime.utcnow(),
            sync_version=1,
        )
        
        db_session.add(cow)
        db_session.commit()
        
        assert cow.age_days == 730
        assert 1.9 < cow.age_years < 2.1  # ~2 years
        assert "2 years" in cow.get_age_display() or "2y" in cow.get_age_display()
    
    def test_cow_properties(self, db_session: Session, tenant_id):
        """Test cow property methods."""
        cow = Cow(
            tenant_id=tenant_id,
            tag_number="US-003",
            breed="Angus",
            birth_date=date(2021, 5, 10),
            sex=CowSex.MALE,
            status=CowStatus.SOLD,
            weight_kg=600.0,
            dam_id=uuid4(),
            sire_id=uuid4(),
            last_synced_at=datetime.utcnow(),
            sync_version=1,
        )
        
        db_session.add(cow)
        db_session.commit()
        
        assert cow.is_male is True
        assert cow.is_female is False
        assert cow.is_active is False  # status is SOLD
        assert cow.has_genealogy is True
        assert cow.has_weight_data is True
    
    def test_get_active_cows(self, db_session: Session, tenant_id):
        """Test querying active cows."""
        cows = [
            Cow(
                tenant_id=tenant_id,
                tag_number=f"US-{i:03d}",
                breed="Holstein",
                birth_date=date(2021, 1, 1),
                sex=CowSex.FEMALE,
                status=CowStatus.ACTIVE if i < 3 else CowStatus.SOLD,
                last_synced_at=datetime.utcnow(),
                sync_version=1,
            )
            for i in range(5)
        ]
        
        db_session.add_all(cows)
        db_session.commit()
        
        active_cows = Cow.get_active_cows(db_session, tenant_id).all()
        
        assert len(active_cows) == 3
        assert all(c.status == CowStatus.ACTIVE for c in active_cows)
    
    def test_get_cow_by_tag(self, db_session: Session, tenant_id):
        """Test finding cow by tag number."""
        cow = Cow(
            tenant_id=tenant_id,
            tag_number="US-999",
            breed="Jersey",
            birth_date=date(2020, 3, 15),
            sex=CowSex.FEMALE,
            status=CowStatus.ACTIVE,
            last_synced_at=datetime.utcnow(),
            sync_version=1,
        )
        
        db_session.add(cow)
        db_session.commit()
        
        found_cow = Cow.get_by_tag(db_session, "US-999", tenant_id)
        
        assert found_cow is not None
        assert found_cow.cow_id == cow.cow_id
        assert found_cow.tag_number == "US-999"
    
    def test_cow_projection_staleness(self, db_session: Session, tenant_id):
        """Test checking if projection is stale."""
        # Recent sync
        fresh_cow = Cow(
            tenant_id=tenant_id,
            tag_number="US-FRESH",
            breed="Holstein",
            birth_date=date(2020, 1, 1),
            sex=CowSex.FEMALE,
            status=CowStatus.ACTIVE,
            last_synced_at=datetime.utcnow(),
            sync_version=1,
        )
        
        # Old sync
        stale_cow = Cow(
            tenant_id=tenant_id,
            tag_number="US-STALE",
            breed="Holstein",
            birth_date=date(2020, 1, 1),
            sex=CowSex.FEMALE,
            status=CowStatus.ACTIVE,
            last_synced_at=datetime.utcnow() - timedelta(hours=1),
            sync_version=1,
        )
        
        db_session.add_all([fresh_cow, stale_cow])
        db_session.commit()
        
        # Check staleness (15 minute threshold)
        assert fresh_cow.is_stale(max_age_minutes=15) is False
        assert stale_cow.is_stale(max_age_minutes=15) is True
    
    def test_cow_to_dict(self, db_session: Session, tenant_id):
        """Test serializing cow to dictionary."""
        cow = Cow(
            tenant_id=tenant_id,
            tag_number="US-DICT",
            name="Test Cow",
            breed="Holstein",
            birth_date=date(2020, 6, 15),
            sex=CowSex.FEMALE,
            status=CowStatus.ACTIVE,
            weight_kg=500.0,
            last_synced_at=datetime.utcnow(),
            sync_version=5,
        )
        
        db_session.add(cow)
        db_session.commit()
        
        cow_dict = cow.to_dict(include_sync_info=True)
        
        assert cow_dict["tag_number"] == "US-DICT"
        assert cow_dict["name"] == "Test Cow"
        assert cow_dict["breed"] == "Holstein"
        assert cow_dict["weight_kg"] == 500.0
        assert "cow_id" in cow_dict
        assert "sync_info" in cow_dict
        assert cow_dict["sync_info"]["sync_version"] == 5


class TestCategory:
    """Tests for Category model."""
    
    def test_create_global_category(self, db_session: Session):
        """Test creating a global category."""
        category = Category.create_category(
            name="Dairy",
            category_type="cow_type",
            is_global=True,
            description="Dairy cattle category"
        )
        
        db_session.add(category)
        db_session.commit()
        
        assert category.id is not None
        assert category.is_global is True
        assert category.tenant_id is None
        assert category.name == "Dairy"
    
    def test_create_tenant_category(self, db_session: Session, tenant_id):
        """Test creating a tenant-specific category."""
        category = Category.create_category(
            name="Custom Breed",
            category_type="cow_type",
            tenant_id=tenant_id,
            is_global=False,
            description="Custom tenant category"
        )
        
        db_session.add(category)
        db_session.commit()
        
        assert category.id is not None
        assert category.is_global is False
        assert category.tenant_id == tenant_id
    
    def test_category_hierarchy(self, db_session: Session, tenant_id):
        """Test hierarchical categories."""
        parent = Category.create_category(
            name="Livestock",
            category_type="animal_type",
            tenant_id=tenant_id,
            is_global=False
        )
        
        db_session.add(parent)
        db_session.commit()
        
        child = Category.create_category(
            name="Cattle",
            category_type="animal_type",
            tenant_id=tenant_id,
            is_global=False,
            parent_category_id=parent.id
        )
        
        db_session.add(child)
        db_session.commit()
        
        assert child.parent_category_id == parent.id
        assert child.depth == 1
        assert parent.depth == 0
        assert child.is_descendant_of(parent.id) is True
    
    def test_category_visibility(self, db_session: Session):
        """Test category visibility to tenants."""
        tenant1_id = uuid4()
        tenant2_id = uuid4()
        
        # Global category
        global_cat = Category.create_category(
            name="Global",
            category_type="test",
            is_global=True
        )
        
        # Tenant1 category
        tenant1_cat = Category.create_category(
            name="Tenant1",
            category_type="test",
            tenant_id=tenant1_id,
            is_global=False
        )
        
        db_session.add_all([global_cat, tenant1_cat])
        db_session.commit()
        
        # Check visibility
        assert global_cat.is_visible_to_tenant(tenant1_id) is True
        assert global_cat.is_visible_to_tenant(tenant2_id) is True
        assert tenant1_cat.is_visible_to_tenant(tenant1_id) is True
        assert tenant1_cat.is_visible_to_tenant(tenant2_id) is False
    
    def test_get_tenant_categories(self, db_session: Session):
        """Test querying categories for a tenant."""
        tenant_id = uuid4()
        
        categories = [
            Category.create_category(
                name="Global1",
                category_type="test",
                is_global=True
            ),
            Category.create_category(
                name="Tenant1",
                category_type="test",
                tenant_id=tenant_id,
                is_global=False
            ),
            Category.create_category(
                name="OtherTenant",
                category_type="test",
                tenant_id=uuid4(),
                is_global=False
            ),
        ]
        
        db_session.add_all(categories)
        db_session.commit()
        
        # Get categories visible to tenant (global + owned)
        visible = Category.get_tenant_categories(
            db_session,
            tenant_id=tenant_id,
            include_global=True
        ).all()
        
        assert len(visible) == 2  # Global1 + Tenant1
        assert any(c.name == "Global1" for c in visible)
        assert any(c.name == "Tenant1" for c in visible)
        assert not any(c.name == "OtherTenant" for c in visible)


class TestTenantIsolation:
    """Tests for tenant isolation across models."""
    
    def test_tenant_mixin_filter(self, db_session: Session):
        """Test tenant filter method."""
        tenant1_id = uuid4()
        tenant2_id = uuid4()
        
        # Create cows for different tenants
        cows = [
            Cow(
                tenant_id=tenant1_id,
                tag_number="T1-001",
                breed="Holstein",
                birth_date=date(2020, 1, 1),
                sex=CowSex.FEMALE,
                status=CowStatus.ACTIVE,
                last_synced_at=datetime.utcnow(),
                sync_version=1,
            ),
            Cow(
                tenant_id=tenant2_id,
                tag_number="T2-001",
                breed="Jersey",
                birth_date=date(2020, 1, 1),
                sex=CowSex.FEMALE,
                status=CowStatus.ACTIVE,
                last_synced_at=datetime.utcnow(),
                sync_version=1,
            ),
        ]
        
        db_session.add_all(cows)
        db_session.commit()
        
        # Query with tenant filter
        tenant1_cows = (
            db_session.query(Cow)
            .filter(Cow.get_tenant_filter(tenant1_id))
            .all()
        )
        
        assert len(tenant1_cows) == 1
        assert tenant1_cows[0].tag_number == "T1-001"
    
    def test_cross_tenant_isolation(self, db_session: Session):
        """Test that tenants cannot see each other's data."""
        tenant1_id = uuid4()
        tenant2_id = uuid4()
        cow_id = uuid4()
        
        # Create event for tenant1
        event = CowEvent.create_event(
            tenant_id=tenant1_id,
            cow_id=cow_id,
            event_type=CowEventType.COW_CREATED,
            payload='{"tenant": 1}',
        )
        
        db_session.add(event)
        db_session.commit()
        
        # Try to query with tenant2's filter
        tenant2_events = (
            db_session.query(CowEvent)
            .filter(CowEvent.tenant_id == tenant2_id)
            .all()
        )
        
        assert len(tenant2_events) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
