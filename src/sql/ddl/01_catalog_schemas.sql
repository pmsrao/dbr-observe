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

-- Set catalog owner
ALTER CATALOG obs OWNER TO `data-platform-owner`;

-- =============================================================================
-- 2. SCHEMA CREATION
-- =============================================================================

-- Bronze Layer - Raw system table data
CREATE SCHEMA IF NOT EXISTS obs.bronze
;

-- Silver Layer - Curated and unified data
CREATE SCHEMA IF NOT EXISTS obs.silver
;


-- Gold Layer - Analytics-ready data
CREATE SCHEMA IF NOT EXISTS obs.gold
;

-- Meta Schema - Metadata and configuration management
CREATE SCHEMA IF NOT EXISTS obs.meta
;

-- Ops Schema - Operational monitoring and metrics
CREATE SCHEMA IF NOT EXISTS obs.ops
;

-- =============================================================================
-- 3. SCHEMA PROPERTIES AND GOVERNANCE
-- =============================================================================

-- Set schema owners (owner is reserved and cannot be set via SET PROPERTIES)
ALTER SCHEMA obs.bronze OWNER TO `data-platform-owner`;
ALTER SCHEMA obs.silver OWNER TO `data-platform-owner`;
ALTER SCHEMA obs.gold OWNER TO `data-platform-owner`;
ALTER SCHEMA obs.meta OWNER TO `data-platform-owner`;
ALTER SCHEMA obs.ops OWNER TO `data-platform-owner`;

-- Set schema properties for data governance (excluding owner)
ALTER SCHEMA obs.bronze SET PROPERTIES (
    'retention_days' = '365',
    'data_classification' = 'internal'
);

ALTER SCHEMA obs.silver SET PROPERTIES (
    'retention_days' = '730',
    'data_classification' = 'internal'
);

ALTER SCHEMA obs.gold SET PROPERTIES (
    'retention_days' = '1095',
    'data_classification' = 'internal'
);

ALTER SCHEMA obs.meta SET PROPERTIES (
    'retention_days' = '365',
    'data_classification' = 'internal'
);

ALTER SCHEMA obs.ops SET PROPERTIES (
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
    created,
    created_by,
    last_altered
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

-- Environment setup completed successfully!
SELECT 'Environment setup completed successfully!' as status;
SELECT 'Catalog: obs' as catalog_info;
SELECT 'Schemas created: bronze, silver, gold, meta, ops' as schemas_info;
SELECT 'Ready for permissions setup and table creation.' as next_steps;
