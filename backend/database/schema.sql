-- =====================================================================================
-- CattleSaaS Database Schema
-- Pure Projection Pattern A Implementation
-- =====================================================================================
-- 
-- ARCHITECTURE NOTES:
-- This schema implements the "Pure Projection Pattern A" where:
-- 1. cow_events: Outbox table (append-only) - Source of truth for operational writes
-- 2. cows: Projection table - Read-only copy synced FROM Databricks Silver layer
-- 3. categories: Reference data for lookups
--
-- DATA FLOW:
-- Write Path:  API → cow_events → Bronze → Silver → Gold
-- Read Path:   Silver → cows (projection) → API
-- 
-- The 'cows' table is NOT the source of truth. It's a materialized view/projection
-- synchronized from the Silver layer for fast API reads.
-- =====================================================================================

USE cattlesaas;
GO

-- =====================================================================================
-- SCHEMA: operational
-- Contains operational tables for the transactional database
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- TABLE: cow_events (OUTBOX/EVENT SOURCING TABLE)
-- -------------------------------------------------------------------------------------
-- Purpose: Append-only event log for all cow-related operations
-- Pattern: Event Sourcing / Outbox Pattern
-- Source of Truth: YES - This is where writes happen
-- Sync Target: Databricks Bronze layer
-- 
-- This table captures all cow lifecycle events before they're published to the lakehouse.
-- Events are published to Bronze layer via CDC or batch sync.
-- -------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cow_events' AND schema_id = SCHEMA_ID('operational'))
BEGIN
    CREATE TABLE operational.cow_events (
        -- Primary Key
        event_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        
        -- Multi-tenancy
        tenant_id UNIQUEIDENTIFIER NOT NULL,
        
        -- Entity Reference
        cow_id UNIQUEIDENTIFIER NOT NULL,
        
        -- Event Information
        event_type VARCHAR(50) NOT NULL,
        -- Valid event_type values:
        --   'cow_created'      - New cow registered
        --   'cow_updated'      - Cow details modified
        --   'cow_deactivated'  - Cow removed from active herd
        --   'cow_weight_recorded' - Weight measurement
        --   'cow_health_event' - Health/medical event
        
        -- Event Payload (JSON)
        payload NVARCHAR(MAX) NOT NULL,
        -- JSON structure example:
        -- {
        --   "breed": "Holstein",
        --   "birth_date": "2023-05-15",
        --   "sex": "F",
        --   "tag_number": "US123456",
        --   "changed_fields": ["breed", "status"],
        --   "changed_by": "user_id",
        --   ...
        -- }
        
        -- Timestamps
        event_time DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
        created_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
        
        -- Publishing Status
        published_to_bronze BIT NOT NULL DEFAULT 0,
        published_at DATETIME2(3) NULL,
        
        -- Metadata
        created_by NVARCHAR(255) NULL,
        source_system NVARCHAR(100) NULL DEFAULT 'api',
        correlation_id UNIQUEIDENTIFIER NULL, -- For tracing related events
        
        -- Constraints
        CONSTRAINT PK_cow_events PRIMARY KEY CLUSTERED (event_id),
        CONSTRAINT CK_cow_events_event_type CHECK (event_type IN (
            'cow_created', 
            'cow_updated', 
            'cow_deactivated',
            'cow_weight_recorded',
            'cow_health_event'
        )),
        CONSTRAINT CK_cow_events_payload_json CHECK (ISJSON(payload) = 1)
    );
    
    PRINT 'Table operational.cow_events created successfully';
END
ELSE
BEGIN
    PRINT 'Table operational.cow_events already exists';
END
GO

-- Indexes for cow_events
CREATE NONCLUSTERED INDEX IX_cow_events_tenant_id 
    ON operational.cow_events(tenant_id, event_time DESC);
GO

CREATE NONCLUSTERED INDEX IX_cow_events_cow_id 
    ON operational.cow_events(cow_id, event_time DESC);
GO

CREATE NONCLUSTERED INDEX IX_cow_events_published 
    ON operational.cow_events(published_to_bronze, event_time ASC)
    WHERE published_to_bronze = 0; -- Filtered index for unpublished events
GO

CREATE NONCLUSTERED INDEX IX_cow_events_event_type 
    ON operational.cow_events(event_type, tenant_id, event_time DESC);
GO

-- Add table description
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Event sourcing table for cow lifecycle events. This is the SOURCE OF TRUTH for writes. Events are published to Bronze layer.',
    @level0type = N'SCHEMA', @level0name = 'operational',
    @level1type = N'TABLE',  @level1name = 'cow_events';
GO

-- -------------------------------------------------------------------------------------
-- TABLE: cows (PROJECTION TABLE - READ-ONLY)
-- -------------------------------------------------------------------------------------
-- Purpose: Materialized view/projection of cow data for fast API reads
-- Pattern: CQRS - Command Query Responsibility Segregation
-- Source of Truth: NO - This is synchronized FROM Databricks Silver layer
-- Sync Source: Databricks Silver layer
-- 
-- ⚠️  IMPORTANT: DO NOT write directly to this table from the API!
-- ⚠️  This table is periodically synchronized from the Silver layer.
-- ⚠️  All writes must go through cow_events → Bronze → Silver → cows
-- 
-- This projection provides:
-- - Fast read access for API queries
-- - Current state view (vs event log)
-- - Tenant-isolated data access
-- -------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cows' AND schema_id = SCHEMA_ID('operational'))
BEGIN
    CREATE TABLE operational.cows (
        -- Primary Key
        cow_id UNIQUEIDENTIFIER NOT NULL,
        
        -- Multi-tenancy
        tenant_id UNIQUEIDENTIFIER NOT NULL,
        
        -- Core Attributes
        tag_number NVARCHAR(50) NOT NULL, -- Visual ID tag
        breed NVARCHAR(100) NULL,
        birth_date DATE NULL,
        sex CHAR(1) NULL, -- 'M', 'F', 'U' (Unknown)
        
        -- Status
        status NVARCHAR(20) NOT NULL DEFAULT 'active',
        -- Valid status values: 'active', 'inactive', 'sold', 'deceased'
        
        -- Physical Attributes
        color NVARCHAR(50) NULL,
        weight_kg DECIMAL(8,2) NULL, -- Latest weight
        
        -- Genealogy
        dam_id UNIQUEIDENTIFIER NULL, -- Mother
        sire_id UNIQUEIDENTIFIER NULL, -- Father
        
        -- Business Data
        acquisition_date DATE NULL,
        acquisition_source NVARCHAR(100) NULL,
        current_location NVARCHAR(100) NULL,
        
        -- Synchronization Metadata
        silver_last_updated_at DATETIME2(3) NULL, -- When record was updated in Silver
        last_synced_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(), -- When we last synced
        sync_version INT NOT NULL DEFAULT 1, -- Optimistic concurrency
        
        -- Audit Trail
        created_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
        updated_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
        
        -- Constraints
        CONSTRAINT PK_cows PRIMARY KEY CLUSTERED (cow_id),
        CONSTRAINT CK_cows_sex CHECK (sex IN ('M', 'F', 'U')),
        CONSTRAINT CK_cows_status CHECK (status IN ('active', 'inactive', 'sold', 'deceased')),
        CONSTRAINT CK_cows_weight CHECK (weight_kg > 0 AND weight_kg < 2000), -- Sanity check
        CONSTRAINT UQ_cows_tenant_tag UNIQUE (tenant_id, tag_number) -- Unique tag per tenant
    );
    
    PRINT 'Table operational.cows created successfully';
END
ELSE
BEGIN
    PRINT 'Table operational.cows already exists';
END
GO

-- Indexes for cows
CREATE NONCLUSTERED INDEX IX_cows_tenant_id 
    ON operational.cows(tenant_id, status, created_at DESC);
GO

CREATE NONCLUSTERED INDEX IX_cows_tag_number 
    ON operational.cows(tag_number) INCLUDE (tenant_id, breed, status);
GO

CREATE NONCLUSTERED INDEX IX_cows_status 
    ON operational.cows(status, tenant_id);
GO

CREATE NONCLUSTERED INDEX IX_cows_breed 
    ON operational.cows(breed, tenant_id)
    WHERE breed IS NOT NULL;
GO

CREATE NONCLUSTERED INDEX IX_cows_birth_date 
    ON operational.cows(birth_date DESC)
    WHERE birth_date IS NOT NULL;
GO

CREATE NONCLUSTERED INDEX IX_cows_sync 
    ON operational.cows(last_synced_at, tenant_id);
GO

-- Add table description
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'⚠️ PROJECTION TABLE - READ ONLY! Synchronized from Databricks Silver layer. DO NOT write directly to this table. This is NOT the source of truth.',
    @level0type = N'SCHEMA', @level0name = 'operational',
    @level1type = N'TABLE',  @level1name = 'cows';
GO

-- Add column descriptions
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Timestamp when this record was last updated in the Silver layer (source system)',
    @level0type = N'SCHEMA', @level0name = 'operational',
    @level1type = N'TABLE',  @level1name = 'cows',
    @level2type = N'COLUMN', @level2name = 'silver_last_updated_at';
GO

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Timestamp when this record was last synchronized from Silver layer to this projection',
    @level0type = N'SCHEMA', @level0name = 'operational',
    @level1type = N'TABLE',  @level1name = 'cows',
    @level2type = N'COLUMN', @level2name = 'last_synced_at';
GO

-- -------------------------------------------------------------------------------------
-- TABLE: categories (REFERENCE DATA)
-- -------------------------------------------------------------------------------------
-- Purpose: Reference/lookup data for categorization
-- Examples: Breed types, Health conditions, Location names, etc.
-- This is standard reference data that can be written to directly
-- -------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'categories' AND schema_id = SCHEMA_ID('operational'))
BEGIN
    CREATE TABLE operational.categories (
        -- Primary Key
        category_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        
        -- Multi-tenancy (NULL = global/system category)
        tenant_id UNIQUEIDENTIFIER NULL,
        
        -- Category Information
        category_type NVARCHAR(50) NOT NULL,
        -- Examples: 'breed', 'health_condition', 'location', 'feed_type'
        
        name NVARCHAR(100) NOT NULL,
        description NVARCHAR(500) NULL,
        
        -- Hierarchy Support
        parent_category_id UNIQUEIDENTIFIER NULL,
        display_order INT NULL,
        
        -- Status
        is_active BIT NOT NULL DEFAULT 1,
        
        -- Metadata
        metadata NVARCHAR(MAX) NULL, -- JSON for extensibility
        
        -- Audit
        created_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
        updated_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
        created_by NVARCHAR(255) NULL,
        
        -- Constraints
        CONSTRAINT PK_categories PRIMARY KEY CLUSTERED (category_id),
        CONSTRAINT FK_categories_parent FOREIGN KEY (parent_category_id) 
            REFERENCES operational.categories(category_id),
        CONSTRAINT UQ_categories_tenant_type_name UNIQUE (tenant_id, category_type, name),
        CONSTRAINT CK_categories_metadata_json CHECK (metadata IS NULL OR ISJSON(metadata) = 1)
    );
    
    PRINT 'Table operational.categories created successfully';
END
ELSE
BEGIN
    PRINT 'Table operational.categories already exists';
END
GO

-- Indexes for categories
CREATE NONCLUSTERED INDEX IX_categories_tenant_type 
    ON operational.categories(tenant_id, category_type, is_active);
GO

CREATE NONCLUSTERED INDEX IX_categories_type_name 
    ON operational.categories(category_type, name)
    WHERE is_active = 1;
GO

CREATE NONCLUSTERED INDEX IX_categories_parent 
    ON operational.categories(parent_category_id)
    WHERE parent_category_id IS NOT NULL;
GO

-- Add table description
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Reference/lookup data for categorization across the system. Can be tenant-specific or global (tenant_id = NULL).',
    @level0type = N'SCHEMA', @level0name = 'operational',
    @level1type = N'TABLE',  @level1name = 'categories';
GO

-- =====================================================================================
-- SCHEMA: tenant
-- Contains tenant management tables
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- TABLE: tenants (TENANT MANAGEMENT)
-- -------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'tenants' AND schema_id = SCHEMA_ID('tenant'))
BEGIN
    CREATE TABLE tenant.tenants (
        tenant_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        tenant_name NVARCHAR(200) NOT NULL,
        tenant_code NVARCHAR(50) NOT NULL, -- Short code for URLs, etc.
        
        -- Subscription
        subscription_tier NVARCHAR(50) NOT NULL DEFAULT 'basic',
        subscription_status NVARCHAR(20) NOT NULL DEFAULT 'active',
        
        -- Contact
        contact_email NVARCHAR(255) NOT NULL,
        contact_phone NVARCHAR(50) NULL,
        
        -- Settings
        settings NVARCHAR(MAX) NULL, -- JSON configuration
        
        -- Limits
        max_cows INT NULL,
        max_users INT NULL,
        
        -- Status
        is_active BIT NOT NULL DEFAULT 1,
        
        -- Audit
        created_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
        updated_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
        deactivated_at DATETIME2(3) NULL,
        
        CONSTRAINT PK_tenants PRIMARY KEY CLUSTERED (tenant_id),
        CONSTRAINT UQ_tenants_code UNIQUE (tenant_code),
        CONSTRAINT CK_tenants_settings_json CHECK (settings IS NULL OR ISJSON(settings) = 1),
        CONSTRAINT CK_tenants_subscription_status CHECK (subscription_status IN ('active', 'suspended', 'cancelled'))
    );
    
    PRINT 'Table tenant.tenants created successfully';
END
ELSE
BEGIN
    PRINT 'Table tenant.tenants already exists';
END
GO

CREATE NONCLUSTERED INDEX IX_tenants_code 
    ON tenant.tenants(tenant_code, is_active);
GO

CREATE NONCLUSTERED INDEX IX_tenants_status 
    ON tenant.tenants(subscription_status, is_active);
GO

-- =====================================================================================
-- FOREIGN KEYS (Adding after all tables are created)
-- =====================================================================================

-- cow_events → tenants
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_cow_events_tenant')
BEGIN
    ALTER TABLE operational.cow_events
        ADD CONSTRAINT FK_cow_events_tenant FOREIGN KEY (tenant_id)
        REFERENCES tenant.tenants(tenant_id);
    PRINT 'Foreign key FK_cow_events_tenant created';
END
GO

-- cows → tenants
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_cows_tenant')
BEGIN
    ALTER TABLE operational.cows
        ADD CONSTRAINT FK_cows_tenant FOREIGN KEY (tenant_id)
        REFERENCES tenant.tenants(tenant_id);
    PRINT 'Foreign key FK_cows_tenant created';
END
GO

-- cows → cows (dam)
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_cows_dam')
BEGIN
    ALTER TABLE operational.cows
        ADD CONSTRAINT FK_cows_dam FOREIGN KEY (dam_id)
        REFERENCES operational.cows(cow_id);
    PRINT 'Foreign key FK_cows_dam created';
END
GO

-- cows → cows (sire)
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_cows_sire')
BEGIN
    ALTER TABLE operational.cows
        ADD CONSTRAINT FK_cows_sire FOREIGN KEY (sire_id)
        REFERENCES operational.cows(cow_id);
    PRINT 'Foreign key FK_cows_sire created';
END
GO

-- =====================================================================================
-- VIEWS FOR COMMON QUERIES
-- =====================================================================================

-- View: Active cows per tenant
IF OBJECT_ID('operational.vw_active_cows', 'V') IS NOT NULL
    DROP VIEW operational.vw_active_cows;
GO

CREATE VIEW operational.vw_active_cows
AS
    SELECT 
        c.cow_id,
        c.tenant_id,
        t.tenant_name,
        c.tag_number,
        c.breed,
        c.birth_date,
        DATEDIFF(MONTH, c.birth_date, GETUTCDATE()) / 12 AS age_years,
        c.sex,
        c.weight_kg,
        c.current_location,
        c.last_synced_at,
        c.created_at
    FROM operational.cows c
    INNER JOIN tenant.tenants t ON c.tenant_id = t.tenant_id
    WHERE c.status = 'active'
        AND t.is_active = 1;
GO

-- View: Recent events per tenant
IF OBJECT_ID('operational.vw_recent_events', 'V') IS NOT NULL
    DROP VIEW operational.vw_recent_events;
GO

CREATE VIEW operational.vw_recent_events
AS
    SELECT 
        e.event_id,
        e.tenant_id,
        t.tenant_name,
        e.cow_id,
        e.event_type,
        e.event_time,
        e.published_to_bronze,
        e.created_at
    FROM operational.cow_events e
    INNER JOIN tenant.tenants t ON e.tenant_id = t.tenant_id
    WHERE e.event_time >= DATEADD(DAY, -30, GETUTCDATE());
GO

-- =====================================================================================
-- SUMMARY
-- =====================================================================================

PRINT '';
PRINT '=====================================================================================';
PRINT 'Schema creation completed successfully!';
PRINT '=====================================================================================';
PRINT '';
PRINT 'Tables created:';
PRINT '  • operational.cow_events    - Event sourcing table (SOURCE OF TRUTH for writes)';
PRINT '  • operational.cows          - Projection table (synced FROM Silver) [READ ONLY]';
PRINT '  • operational.categories    - Reference data';
PRINT '  • tenant.tenants           - Tenant management';
PRINT '';
PRINT 'Views created:';
PRINT '  • operational.vw_active_cows';
PRINT '  • operational.vw_recent_events';
PRINT '';
PRINT 'Data Flow Pattern:';
PRINT '  WRITE: API → cow_events → Bronze → Silver → Gold';
PRINT '  READ:  Silver → cows (sync) → API';
PRINT '';
PRINT '⚠️  REMEMBER: Never write directly to operational.cows!';
PRINT '=====================================================================================';
GO
