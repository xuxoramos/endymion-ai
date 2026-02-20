-- Initialize CattleSaaS Database
-- This script runs when SQL Server container starts for the first time

USE master;
GO

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'cattlesaas')
BEGIN
    CREATE DATABASE cattlesaas;
    PRINT 'Database cattlesaas created successfully';
END
ELSE
BEGIN
    PRINT 'Database cattlesaas already exists';
END
GO

USE cattlesaas;
GO

-- Create schemas for organization
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'auth')
BEGIN
    EXEC('CREATE SCHEMA auth');
    PRINT 'Schema auth created';
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tenant')
BEGIN
    EXEC('CREATE SCHEMA tenant');
    PRINT 'Schema tenant created';
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'operational')
BEGIN
    EXEC('CREATE SCHEMA operational');
    PRINT 'Schema operational created';
END
GO

PRINT 'CattleSaaS database initialized successfully';
GO
