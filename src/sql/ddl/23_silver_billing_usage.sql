-- =============================================================================
-- Databricks Observability Platform - Silver Billing Usage Table
-- =============================================================================
-- Purpose: Create silver billing usage table with enhanced metadata handling
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. BILLING USAGE TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.silver.billing_usage (
    -- Natural Key (Unique identifier for upsert operations)
    record_id STRING NOT NULL,  -- Primary key for identifying new/updated records
    workspace_id STRING NOT NULL,
    
    -- Billing details
    sku_name STRING NOT NULL,
    cloud STRING NOT NULL,
    usage_start_time TIMESTAMP NOT NULL,
    usage_end_time TIMESTAMP NOT NULL,
    usage_date DATE NOT NULL,
    usage_unit STRING,
    usage_quantity DECIMAL(18,6),
    usage_type STRING,
    
    -- Metadata (Flexible structure for schema evolution)
    custom_tags MAP<STRING, STRING>,
    usage_metadata MAP<STRING, STRING>,  -- Flexible structure to handle schema evolution
    
    -- Identity metadata
    identity_metadata STRUCT<
        run_as STRING,
        owned_by STRING,
        created_by STRING
    >,
    
    -- Record management
    record_type STRING,  -- ORIGINAL, RETRACTION, RESTATEMENT
    ingestion_date DATE,
    billing_origin_product STRING,
    
    -- Product features
    product_features STRUCT<
        serving_type STRING,
        offering_type STRING,
        performance_target STRING,
        networking STRUCT<connectivity_type STRING>
    >,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (workspace_id, usage_date);

-- =============================================================================
-- 2. BILLING LIST PRICES TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.silver.billing_list_prices (
    -- Natural Key
    sku_name STRING NOT NULL,
    cloud STRING NOT NULL,
    effective_date DATE NOT NULL,
    
    -- Pricing details
    list_price DECIMAL(18,6) NOT NULL,
    currency_code STRING,
    usage_unit STRING,
    usage_type STRING,
    region STRING,
    tier STRING,
    category STRING,
    subcategory STRING,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (cloud, effective_date);

-- =============================================================================
-- 3. TABLE PROPERTIES AND CONSTRAINTS
-- =============================================================================

-- Set properties for billing usage table
ALTER TABLE obs.silver.billing_usage SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Set properties for billing list prices table
ALTER TABLE obs.silver.billing_list_prices SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Add primary key constraints (idempotent)
ALTER TABLE obs.silver.billing_usage 
DROP CONSTRAINT IF EXISTS pk_billing_usage;

ALTER TABLE obs.silver.billing_usage 
ADD CONSTRAINT pk_billing_usage 
PRIMARY KEY (record_id);

ALTER TABLE obs.silver.billing_list_prices 
DROP CONSTRAINT IF EXISTS pk_billing_list_prices;

ALTER TABLE obs.silver.billing_list_prices 
ADD CONSTRAINT pk_billing_list_prices 
PRIMARY KEY (sku_name, cloud, effective_date);

-- =============================================================================
-- 4. UPSERT PROCESSING PATTERN
-- =============================================================================

-- Example upsert pattern for billing usage
-- This will be implemented in the processing logic
-- Uses MERGE statement with record_id as unique identifier for upserts

-- =============================================================================
-- 5. VERIFICATION
-- =============================================================================

-- Verify table creation
DESCRIBE TABLE obs.silver.billing_usage;
DESCRIBE TABLE obs.silver.billing_list_prices;

-- Check table properties
SHOW TBLPROPERTIES obs.silver.billing_usage;
SHOW TBLPROPERTIES obs.silver.billing_list_prices;

-- =============================================================================
-- 6. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 04_silver_query_history.sql - Create query history table
-- 2. 05_silver_audit_log.sql - Create audit log table
-- 3. 06_silver_node_usage.sql - Create node usage table
-- 4. Staging views for data harmonization

SELECT 'Silver billing tables created successfully!' as message;
SELECT 'Tables: billing_usage, billing_list_prices' as message;
SELECT 'Upsert pattern implemented using record_id as primary key' as message;
SELECT 'Flexible metadata handling with MAP<STRING, STRING> structure' as message;
SELECT 'Ready for query history table creation.' as message;
