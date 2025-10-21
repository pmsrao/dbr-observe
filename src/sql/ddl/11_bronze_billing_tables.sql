-- =============================================================================
-- Databricks Observability Platform - Bronze Billing Tables
-- =============================================================================
-- Purpose: Create bronze tables for billing system data
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. BILLING USAGE TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.bronze.system_billing_usage (
    -- Raw data preservation
    raw_data STRUCT<
        record_id STRING,
        workspace_id STRING,
        sku_name STRING,
        cloud STRING,
        usage_start_time TIMESTAMP,
        usage_end_time TIMESTAMP,
        usage_date DATE,
        usage_unit STRING,
        usage_quantity DECIMAL(18,6),
        usage_type STRING,
        custom_tags MAP<STRING, STRING>,
        usage_metadata STRUCT<
            cluster_id STRING,
            job_id STRING,
            warehouse_id STRING,
            instance_pool_id STRING,
            node_type STRING,
            job_run_id STRING,
            notebook_id STRING,
            dlt_pipeline_id STRING,
            endpoint_name STRING,
            endpoint_id STRING,
            dlt_update_id STRING,
            dlt_maintenance_id STRING,
            metastore_id STRING,
            job_name STRING,
            notebook_path STRING,
            central_clean_room_id STRING,
            source_region STRING,
            destination_region STRING,
            app_id STRING,
            app_name STRING,
            budget_policy_id STRING,
            base_environment_id STRING
        >,
        identity_metadata STRUCT<
            run_as STRING,
            owned_by STRING,
            created_by STRING
        >,
        record_type STRING,
        ingestion_date DATE,
        billing_origin_product STRING,
        product_features STRUCT<
            serving_type STRING,
            offering_type STRING,
            performance_target STRING,
            networking STRUCT<connectivity_type STRING>
        >
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    usage_date DATE,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, usage_date);

-- =============================================================================
-- 2. BILLING LIST PRICES TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.bronze.system_billing_list_prices (
    -- Raw data preservation
    raw_data STRUCT<
        sku_name STRING,
        cloud STRING,
        effective_date DATE,
        list_price DECIMAL(18,6),
        currency_code STRING,
        usage_unit STRING,
        usage_type STRING,
        region STRING,
        tier STRING,
        category STRING,
        subcategory STRING
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    effective_date DATE,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (effective_date);

-- =============================================================================
-- 3. TABLE PROPERTIES AND OPTIMIZATION
-- =============================================================================

-- Set properties for billing usage table
ALTER TABLE obs.bronze.system_billing_usage SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Set properties for billing list prices table
ALTER TABLE obs.bronze.system_billing_list_prices SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- =============================================================================
-- 4. VERIFICATION
-- =============================================================================

-- Verify table creation
DESCRIBE TABLE obs.bronze.system_billing_usage;
DESCRIBE TABLE obs.bronze.system_billing_list_prices;

-- Check table properties
SHOW TBLPROPERTIES obs.bronze.system_billing_usage;
SHOW TBLPROPERTIES obs.bronze.system_billing_list_prices;

-- =============================================================================
-- 5. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 02_bronze_compute_tables.sql - Create compute system tables
-- 2. 03_bronze_lakeflow_tables.sql - Create lakeflow system tables
-- 3. 04_bronze_query_tables.sql - Create query system tables
-- 4. 05_bronze_storage_tables.sql - Create storage system tables
-- 5. 06_bronze_access_tables.sql - Create access system tables

SELECT 'Bronze billing tables created successfully!' as message;
SELECT 'Tables: system_billing_usage, system_billing_list_prices' as message;
SELECT 'Ready for compute table creation.' as message;
