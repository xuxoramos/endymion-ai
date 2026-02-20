-- Analytics Schema for SQL Server Projection
-- Purpose: Disposable projections from Gold Delta Lake tables
-- Principle: SQL tables can be rebuilt from Gold at any time

-- Create analytics schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'analytics')
BEGIN
    EXEC('CREATE SCHEMA analytics');
END
GO

-- Herd Composition table (from gold.herd_composition)
IF OBJECT_ID('analytics.herd_composition', 'U') IS NOT NULL
    DROP TABLE analytics.herd_composition;
GO

CREATE TABLE analytics.herd_composition (
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    snapshot_date DATE NOT NULL,
    dimension_type VARCHAR(50) NOT NULL,
    dimension_value VARCHAR(100) NOT NULL,
    count INT NOT NULL,
    percentage DECIMAL(5,2),
    total_cows INT NOT NULL,
    PRIMARY KEY (tenant_id, snapshot_date, dimension_type, dimension_value)
);
GO

CREATE INDEX idx_herd_comp_date ON analytics.herd_composition(snapshot_date);
CREATE INDEX idx_herd_comp_tenant ON analytics.herd_composition(tenant_id);
GO

-- Cow Lifecycle table (from gold.cow_lifecycle)
IF OBJECT_ID('analytics.cow_lifecycle', 'U') IS NOT NULL
    DROP TABLE analytics.cow_lifecycle;
GO

CREATE TABLE analytics.cow_lifecycle (
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    cow_id VARCHAR(50) NOT NULL,
    current_breed VARCHAR(50),
    current_status VARCHAR(20),
    birth_date DATE,
    total_events INT,
    first_event_time DATETIME2,
    last_event_time DATETIME2,
    days_in_system INT,
    last_known_weight_kg DECIMAL(10,2),
    PRIMARY KEY (tenant_id, cow_id)
);
GO

CREATE INDEX idx_lifecycle_status ON analytics.cow_lifecycle(current_status);
CREATE INDEX idx_lifecycle_breed ON analytics.cow_lifecycle(current_breed);
GO

-- Daily Snapshots table (from gold.daily_snapshots)
IF OBJECT_ID('analytics.daily_snapshots', 'U') IS NOT NULL
    DROP TABLE analytics.daily_snapshots;
GO

CREATE TABLE analytics.daily_snapshots (
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    snapshot_date DATE NOT NULL,
    total_cows INT,
    active_cows INT,
    inactive_cows INT,
    PRIMARY KEY (tenant_id, snapshot_date)
);
GO

CREATE INDEX idx_snapshots_date ON analytics.daily_snapshots(snapshot_date);
GO
