-- =============================================================================
-- Databricks Observability Platform - Environment Setup
-- =============================================================================
-- Purpose: Create Unity Catalog structure and schemas for observability platform
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. CATALOG CREATION
-- =============================================================================

-- Create main observability catalog
CREATE CATALOG IF NOT EXISTS obs
COMMENT 'Databricks Observability Platform - Main catalog for all observability data';

-- Set catalog properties for governance
ALTER CATALOG obs SET PROPERTIES (
    'owner' = 'data-platform-team@company.com',
    'description' = 'Comprehensive observability platform for Databricks workloads, costs, and performance'
);

-- =============================================================================
-- 2. SCHEMA CREATION
-- =============================================================================

-- Bronze Layer - Raw system table data
CREATE SCHEMA IF NOT EXISTS obs.bronze
COMMENT 'Bronze layer - Raw system table data with minimal transformation'
LOCATION 's3://company-databricks-obs/bronze/';

-- Silver Layer - Curated and unified data
CREATE SCHEMA IF NOT EXISTS obs.silver
COMMENT 'Silver layer - Curated data with SCD2 history tracking and harmonization'
LOCATION 's3://company-databricks-obs/silver/';

-- Silver Staging - Temporary staging for harmonization (moved to silver schema)
-- CREATE SCHEMA IF NOT EXISTS obs.silver_stg
-- COMMENT 'Silver staging - Temporary tables for data harmonization and SCD2 processing'
-- LOCATION 's3://company-databricks-obs/silver_stg/';

-- Gold Layer - Analytics-ready data
CREATE SCHEMA IF NOT EXISTS obs.gold
COMMENT 'Gold layer - Analytics-ready dimensions and facts for business intelligence'
LOCATION 's3://company-databricks-obs/gold/';

-- Meta Schema - Metadata and configuration management
CREATE SCHEMA IF NOT EXISTS obs.meta
COMMENT 'Meta schema - Metadata management, watermarks, lineage, and configuration'
LOCATION 's3://company-databricks-obs/meta/';

-- Ops Schema - Operational monitoring and metrics
CREATE SCHEMA IF NOT EXISTS obs.ops
COMMENT 'Ops schema - Operational monitoring, data quality metrics, and alerting'
LOCATION 's3://company-databricks-obs/ops/';

-- =============================================================================
-- 3. SCHEMA PROPERTIES AND GOVERNANCE
-- =============================================================================

-- Set schema properties for data governance
ALTER SCHEMA obs.bronze SET PROPERTIES (
    'owner' = 'data-platform-team@company.com',
    'retention_days' = '365',
    'data_classification' = 'internal'
);

ALTER SCHEMA obs.silver SET PROPERTIES (
    'owner' = 'data-platform-team@company.com',
    'retention_days' = '730',
    'data_classification' = 'internal'
);

-- ALTER SCHEMA obs.silver_stg SET PROPERTIES (
--     'owner' = 'data-platform-team@company.com',
--     'retention_days' = '30',
--     'data_classification' = 'internal'
-- );

ALTER SCHEMA obs.gold SET PROPERTIES (
    'owner' = 'data-platform-team@company.com',
    'retention_days' = '1095',
    'data_classification' = 'internal'
);

ALTER SCHEMA obs.meta SET PROPERTIES (
    'owner' = 'data-platform-team@company.com',
    'retention_days' = '365',
    'data_classification' = 'internal'
);

ALTER SCHEMA obs.ops SET PROPERTIES (
    'owner' = 'data-platform-team@company.com',
    'retention_days' = '365',
    'data_classification' = 'internal'
);

-- =============================================================================
-- 4. VERIFICATION
-- =============================================================================

-- Verify catalog and schema creation
SHOW CATALOGS LIKE 'obs';
SHOW SCHEMAS IN obs;

-- Display schema information
SELECT 
    catalog_name,
    schema_name,
    comment,
    location,
    properties
FROM information_schema.schemata 
WHERE catalog_name = 'obs'
ORDER BY schema_name;

-- =============================================================================
-- 5. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 02_permissions_setup.sql - Set up access controls
-- 2. 03_watermark_table.sql - Create watermark management system
-- 3. Bronze table creation scripts
-- 4. Silver table creation scripts
-- 5. Gold table creation scripts

PRINT 'Environment setup completed successfully!';
PRINT 'Catalog: obs';
PRINT 'Schemas created: bronze, silver, gold, meta, ops';
PRINT 'Ready for permissions setup and table creation.';
