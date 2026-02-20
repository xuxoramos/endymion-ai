-- =====================================================================================
-- CattleSaaS Database - Seed Data
-- Sample data for testing and development
-- =====================================================================================
-- 
-- This script creates:
-- - 2 test tenants
-- - 10 categories (breeds, locations, health conditions)
-- - 10 cows (5 per tenant)
-- - 15 events (lifecycle events for the cows)
--
-- =====================================================================================

USE cattlesaas;
GO

PRINT '';
PRINT '=====================================================================================';
PRINT 'Starting seed data insertion...';
PRINT '=====================================================================================';
PRINT '';

-- =====================================================================================
-- TENANTS
-- =====================================================================================

PRINT 'Inserting tenants...';

DECLARE @tenant1_id UNIQUEIDENTIFIER = NEWID();
DECLARE @tenant2_id UNIQUEIDENTIFIER = NEWID();

INSERT INTO tenant.tenants (
    tenant_id, tenant_name, tenant_code, subscription_tier, subscription_status,
    contact_email, contact_phone, max_cows, max_users, is_active
)
VALUES
    (
        @tenant1_id,
        'Green Valley Ranch',
        'greenvalley',
        'premium',
        'active',
        'contact@greenvalleyranch.com',
        '+1-555-0100',
        500,
        10,
        1
    ),
    (
        @tenant2_id,
        'Mountain View Dairy',
        'mountainview',
        'basic',
        'active',
        'info@mountainviewdairy.com',
        '+1-555-0200',
        200,
        5,
        1
    );

PRINT '  ✓ Inserted 2 tenants';
PRINT '';

-- =====================================================================================
-- CATEGORIES (Reference Data)
-- =====================================================================================

PRINT 'Inserting categories...';

-- Breed categories (global)
INSERT INTO operational.categories (category_type, name, description, is_active)
VALUES
    ('breed', 'Holstein', 'Black and white dairy breed, high milk production', 1),
    ('breed', 'Jersey', 'Brown dairy breed, high butterfat content', 1),
    ('breed', 'Angus', 'Black beef breed, excellent meat quality', 1),
    ('breed', 'Hereford', 'Red and white beef breed, hardy and adaptable', 1),
    ('breed', 'Simmental', 'Red and white dual-purpose breed', 1);

-- Location categories (tenant-specific)
INSERT INTO operational.categories (tenant_id, category_type, name, description, is_active)
VALUES
    (@tenant1_id, 'location', 'North Pasture', 'Main grazing area, 50 acres', 1),
    (@tenant1_id, 'location', 'Barn A', 'Primary dairy barn', 1),
    (@tenant2_id, 'location', 'East Field', 'Grazing field near creek', 1),
    (@tenant2_id, 'location', 'Barn 1', 'Main barn facility', 1);

-- Health condition categories (global)
INSERT INTO operational.categories (category_type, name, description, is_active)
VALUES
    ('health_condition', 'Mastitis', 'Udder infection', 1),
    ('health_condition', 'Lameness', 'Hoof or leg issues', 1),
    ('health_condition', 'Respiratory', 'Breathing problems', 1);

PRINT '  ✓ Inserted 12 categories (5 breeds, 4 locations, 3 health conditions)';
PRINT '';

-- =====================================================================================
-- COWS (Projection Table - representing synced data from Silver)
-- =====================================================================================

PRINT 'Inserting cows (projection data)...';

-- Tenant 1: Green Valley Ranch - Holstein dairy cows
DECLARE @cow1_id UNIQUEIDENTIFIER = NEWID();
DECLARE @cow2_id UNIQUEIDENTIFIER = NEWID();
DECLARE @cow3_id UNIQUEIDENTIFIER = NEWID();
DECLARE @cow4_id UNIQUEIDENTIFIER = NEWID();
DECLARE @cow5_id UNIQUEIDENTIFIER = NEWID();

INSERT INTO operational.cows (
    cow_id, tenant_id, tag_number, breed, birth_date, sex, status,
    color, weight_kg, current_location, acquisition_date, acquisition_source,
    silver_last_updated_at, last_synced_at
)
VALUES
    (
        @cow1_id,
        @tenant1_id,
        'GV-H-001',
        'Holstein',
        '2021-03-15',
        'F',
        'active',
        'Black and White',
        650.5,
        'Barn A',
        '2021-03-15',
        'Born on farm',
        DATEADD(MINUTE, -30, GETUTCDATE()), -- Synced 30 min ago
        DATEADD(MINUTE, -30, GETUTCDATE())
    ),
    (
        @cow2_id,
        @tenant1_id,
        'GV-H-002',
        'Holstein',
        '2020-11-22',
        'F',
        'active',
        'Black and White',
        720.0,
        'North Pasture',
        '2020-11-22',
        'Born on farm',
        DATEADD(HOUR, -2, GETUTCDATE()),
        DATEADD(HOUR, -2, GETUTCDATE())
    ),
    (
        @cow3_id,
        @tenant1_id,
        'GV-H-003',
        'Holstein',
        '2022-05-10',
        'F',
        'active',
        'Black and White',
        580.0,
        'Barn A',
        '2022-05-10',
        'Born on farm',
        DATEADD(MINUTE, -15, GETUTCDATE()),
        DATEADD(MINUTE, -15, GETUTCDATE())
    ),
    (
        @cow4_id,
        @tenant1_id,
        'GV-J-001',
        'Jersey',
        '2021-08-30',
        'F',
        'active',
        'Brown',
        450.5,
        'North Pasture',
        '2022-01-15',
        'Purchased',
        DATEADD(HOUR, -1, GETUTCDATE()),
        DATEADD(HOUR, -1, GETUTCDATE())
    ),
    (
        @cow5_id,
        @tenant1_id,
        'GV-H-004',
        'Holstein',
        '2019-12-05',
        'F',
        'active',
        'Black and White',
        785.0,
        'Barn A',
        '2019-12-05',
        'Born on farm',
        DATEADD(HOUR, -3, GETUTCDATE()),
        DATEADD(HOUR, -3, GETUTCDATE())
    );

-- Tenant 2: Mountain View Dairy - Mixed breeds
DECLARE @cow6_id UNIQUEIDENTIFIER = NEWID();
DECLARE @cow7_id UNIQUEIDENTIFIER = NEWID();
DECLARE @cow8_id UNIQUEIDENTIFIER = NEWID();
DECLARE @cow9_id UNIQUEIDENTIFIER = NEWID();
DECLARE @cow10_id UNIQUEIDENTIFIER = NEWID();

INSERT INTO operational.cows (
    cow_id, tenant_id, tag_number, breed, birth_date, sex, status,
    color, weight_kg, current_location, acquisition_date, acquisition_source,
    silver_last_updated_at, last_synced_at
)
VALUES
    (
        @cow6_id,
        @tenant2_id,
        'MV-A-001',
        'Angus',
        '2020-04-20',
        'M',
        'active',
        'Black',
        900.0,
        'East Field',
        '2020-04-20',
        'Born on farm',
        DATEADD(MINUTE, -45, GETUTCDATE()),
        DATEADD(MINUTE, -45, GETUTCDATE())
    ),
    (
        @cow7_id,
        @tenant2_id,
        'MV-A-002',
        'Angus',
        '2021-06-15',
        'F',
        'active',
        'Black',
        650.5,
        'East Field',
        '2021-06-15',
        'Born on farm',
        DATEADD(HOUR, -1, GETUTCDATE()),
        DATEADD(HOUR, -1, GETUTCDATE())
    ),
    (
        @cow8_id,
        @tenant2_id,
        'MV-H-001',
        'Hereford',
        '2020-09-10',
        'M',
        'active',
        'Red and White',
        850.0,
        'Barn 1',
        '2021-03-01',
        'Purchased',
        DATEADD(MINUTE, -20, GETUTCDATE()),
        DATEADD(MINUTE, -20, GETUTCDATE())
    ),
    (
        @cow9_id,
        @tenant2_id,
        'MV-A-003',
        'Angus',
        '2022-03-25',
        'F',
        'active',
        'Black',
        550.0,
        'East Field',
        '2022-03-25',
        'Born on farm',
        DATEADD(HOUR, -2, GETUTCDATE()),
        DATEADD(HOUR, -2, GETUTCDATE())
    ),
    (
        @cow10_id,
        @tenant2_id,
        'MV-S-001',
        'Simmental',
        '2021-01-18',
        'F',
        'active',
        'Red and White',
        700.5,
        'Barn 1',
        '2021-09-10',
        'Purchased',
        DATEADD(HOUR, -4, GETUTCDATE()),
        DATEADD(HOUR, -4, GETUTCDATE())
    );

PRINT '  ✓ Inserted 10 cows (5 per tenant)';
PRINT '';

-- =====================================================================================
-- COW EVENTS (Event Sourcing Table)
-- =====================================================================================

PRINT 'Inserting cow events...';

-- Events for Tenant 1 cows
INSERT INTO operational.cow_events (
    tenant_id, cow_id, event_type, payload, event_time, published_to_bronze
)
VALUES
    -- Cow 1 events
    (
        @tenant1_id,
        @cow1_id,
        'cow_created',
        JSON_QUERY('{
            "tag_number": "GV-H-001",
            "breed": "Holstein",
            "birth_date": "2021-03-15",
            "sex": "F",
            "color": "Black and White",
            "acquisition_source": "Born on farm",
            "created_by": "admin@greenvalleyranch.com"
        }'),
        '2021-03-15 10:30:00',
        1
    ),
    (
        @tenant1_id,
        @cow1_id,
        'cow_weight_recorded',
        JSON_QUERY('{
            "weight_kg": 650.5,
            "measured_at": "2026-01-20T08:15:00Z",
            "measured_by": "john.smith@greenvalleyranch.com",
            "notes": "Regular monthly weigh-in"
        }'),
        '2026-01-20 08:15:00',
        0
    ),
    -- Cow 2 events
    (
        @tenant1_id,
        @cow2_id,
        'cow_created',
        JSON_QUERY('{
            "tag_number": "GV-H-002",
            "breed": "Holstein",
            "birth_date": "2020-11-22",
            "sex": "F",
            "color": "Black and White",
            "acquisition_source": "Born on farm"
        }'),
        '2020-11-22 14:20:00',
        1
    ),
    (
        @tenant1_id,
        @cow2_id,
        'cow_updated',
        JSON_QUERY('{
            "changed_fields": ["current_location"],
            "current_location": "North Pasture",
            "previous_location": "Barn A",
            "reason": "Moved to pasture for grazing season"
        }'),
        '2026-01-18 09:00:00',
        0
    ),
    -- Cow 4 events (Jersey - purchased)
    (
        @tenant1_id,
        @cow4_id,
        'cow_created',
        JSON_QUERY('{
            "tag_number": "GV-J-001",
            "breed": "Jersey",
            "birth_date": "2021-08-30",
            "sex": "F",
            "color": "Brown",
            "acquisition_source": "Purchased",
            "acquisition_date": "2022-01-15",
            "seller": "Jersey Breeders Co."
        }'),
        '2022-01-15 11:00:00',
        1
    );

-- Events for Tenant 2 cows
INSERT INTO operational.cow_events (
    tenant_id, cow_id, event_type, payload, event_time, published_to_bronze
)
VALUES
    -- Cow 6 events (Angus bull)
    (
        @tenant2_id,
        @cow6_id,
        'cow_created',
        JSON_QUERY('{
            "tag_number": "MV-A-001",
            "breed": "Angus",
            "birth_date": "2020-04-20",
            "sex": "M",
            "color": "Black",
            "acquisition_source": "Born on farm",
            "notes": "Breeding bull"
        }'),
        '2020-04-20 16:45:00',
        1
    ),
    (
        @tenant2_id,
        @cow6_id,
        'cow_weight_recorded',
        JSON_QUERY('{
            "weight_kg": 900.0,
            "measured_at": "2026-01-22T10:30:00Z",
            "measured_by": "sarah.jones@mountainviewdairy.com"
        }'),
        '2026-01-22 10:30:00',
        0
    ),
    -- Cow 7 events
    (
        @tenant2_id,
        @cow7_id,
        'cow_created',
        JSON_QUERY('{
            "tag_number": "MV-A-002",
            "breed": "Angus",
            "birth_date": "2021-06-15",
            "sex": "F",
            "dam_id": null,
            "sire_id": null
        }'),
        '2021-06-15 08:15:00',
        1
    ),
    -- Cow 8 events (Hereford - purchased)
    (
        @tenant2_id,
        @cow8_id,
        'cow_created',
        JSON_QUERY('{
            "tag_number": "MV-H-001",
            "breed": "Hereford",
            "birth_date": "2020-09-10",
            "sex": "M",
            "acquisition_source": "Purchased",
            "acquisition_date": "2021-03-01"
        }'),
        '2021-03-01 13:20:00',
        1
    ),
    (
        @tenant2_id,
        @cow8_id,
        'cow_health_event',
        JSON_QUERY('{
            "condition": "Lameness",
            "severity": "mild",
            "treatment": "Hoof trimming and antibiotic",
            "veterinarian": "Dr. Martinez",
            "event_date": "2026-01-10",
            "follow_up_required": true
        }'),
        '2026-01-10 14:30:00',
        1
    ),
    -- Recent unpublished event
    (
        @tenant2_id,
        @cow9_id,
        'cow_weight_recorded',
        JSON_QUERY('{
            "weight_kg": 550.0,
            "measured_at": "2026-01-23T07:00:00Z",
            "measured_by": "sarah.jones@mountainviewdairy.com",
            "notes": "6-month checkup"
        }'),
        '2026-01-23 07:00:00',
        0
    );

PRINT '  ✓ Inserted 12 events (cow lifecycle and health events)';
PRINT '';

-- =====================================================================================
-- VERIFICATION QUERIES
-- =====================================================================================

PRINT '';
PRINT '=====================================================================================';
PRINT 'Seed data insertion completed!';
PRINT '=====================================================================================';
PRINT '';
PRINT 'Summary:';

-- Count tenants
DECLARE @tenant_count INT;
SELECT @tenant_count = COUNT(*) FROM tenant.tenants;
PRINT '  • Tenants: ' + CAST(@tenant_count AS VARCHAR);

-- Count categories
DECLARE @category_count INT;
SELECT @category_count = COUNT(*) FROM operational.categories;
PRINT '  • Categories: ' + CAST(@category_count AS VARCHAR);

-- Count cows
DECLARE @cow_count INT;
SELECT @cow_count = COUNT(*) FROM operational.cows;
PRINT '  • Cows (projection): ' + CAST(@cow_count AS VARCHAR);

-- Count events
DECLARE @event_count INT;
SELECT @event_count = COUNT(*) FROM operational.cow_events;
PRINT '  • Events: ' + CAST(@event_count AS VARCHAR);

-- Unpublished events
DECLARE @unpublished_count INT;
SELECT @unpublished_count = COUNT(*) FROM operational.cow_events WHERE published_to_bronze = 0;
PRINT '  • Unpublished events: ' + CAST(@unpublished_count AS VARCHAR);

PRINT '';
PRINT 'Tenants created:';
SELECT 
    tenant_name,
    tenant_code,
    subscription_tier,
    contact_email
FROM tenant.tenants
ORDER BY tenant_name;

PRINT '';
PRINT 'Cows per tenant:';
SELECT 
    t.tenant_name,
    COUNT(*) as cow_count,
    STRING_AGG(c.tag_number, ', ') WITHIN GROUP (ORDER BY c.tag_number) as tag_numbers
FROM operational.cows c
INNER JOIN tenant.tenants t ON c.tenant_id = t.tenant_id
GROUP BY t.tenant_name
ORDER BY t.tenant_name;

PRINT '';
PRINT 'Recent events (last 7 days):';
SELECT 
    t.tenant_name,
    e.event_type,
    COUNT(*) as event_count
FROM operational.cow_events e
INNER JOIN tenant.tenants t ON e.tenant_id = t.tenant_id
WHERE e.event_time >= DATEADD(DAY, -7, GETUTCDATE())
GROUP BY t.tenant_name, e.event_type
ORDER BY t.tenant_name, e.event_type;

PRINT '';
PRINT '=====================================================================================';
PRINT 'Test queries you can run:';
PRINT '=====================================================================================';
PRINT '';
PRINT '-- View all active cows';
PRINT 'SELECT * FROM operational.vw_active_cows;';
PRINT '';
PRINT '-- View recent events';
PRINT 'SELECT * FROM operational.vw_recent_events;';
PRINT '';
PRINT '-- Unpublished events (need to sync to Bronze)';
PRINT 'SELECT * FROM operational.cow_events WHERE published_to_bronze = 0;';
PRINT '';
PRINT '-- Cows by breed for a tenant';
PRINT 'SELECT breed, COUNT(*) as count FROM operational.cows';
PRINT 'WHERE tenant_id = (SELECT tenant_id FROM tenant.tenants WHERE tenant_code = ''greenvalley'')';
PRINT 'GROUP BY breed;';
PRINT '';
PRINT '=====================================================================================';

GO
