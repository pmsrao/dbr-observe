-- =============================================================================
-- Databricks Observability Platform - Watermark Management System
-- =============================================================================
-- Purpose: Create watermark management system for incremental processing
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. WATERMARK TABLE CREATION
-- =============================================================================

-- Create watermark management table in meta schema
CREATE TABLE IF NOT EXISTS obs.meta.watermarks (
    -- Source and target table identification
    source_table_name STRING NOT NULL,
    target_table_name STRING NOT NULL,
    watermark_column STRING NOT NULL,
    
    -- Watermark values
    watermark_value TIMESTAMP,
    last_updated TIMESTAMP,
    
    -- Processing status and metrics
    processing_status STRING,  -- 'SUCCESS', 'FAILED', 'IN_PROGRESS'
    error_message STRING,
    records_processed BIGINT,
    processing_duration_ms BIGINT,
    
    -- Audit columns
    created_by STRING,
    created_at TIMESTAMP,
    updated_by STRING,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Watermark management table for incremental processing tracking';

-- =============================================================================
-- 2. TABLE PROPERTIES AND OPTIMIZATION
-- =============================================================================

-- Set table properties for governance
ALTER TABLE obs.meta.watermarks SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Create primary key constraint (drop if exists first)
ALTER TABLE obs.meta.watermarks DROP CONSTRAINT IF EXISTS pk_watermarks;
ALTER TABLE obs.meta.watermarks 
ADD CONSTRAINT pk_watermarks 
PRIMARY KEY (source_table_name, target_table_name, watermark_column);

-- =============================================================================
-- 3. INITIAL WATERMARK RECORDS
-- =============================================================================

-- Clear existing watermark records to avoid duplicates
DELETE FROM obs.meta.watermarks;

-- Insert initial watermark records for all system tables
INSERT INTO obs.meta.watermarks (
    source_table_name,
    target_table_name,
    watermark_column,
    watermark_value,
    last_updated,
    processing_status,
    error_message,
    records_processed,
    processing_duration_ms,
    created_by,
    created_at,
    updated_by,
    updated_at
)
VALUES
    -- Billing system tables
    ('system.billing.usage', 'obs.silver.billing_usage', 'usage_start_time', 
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    ('system.billing.list_prices', 'obs.silver.billing_list_prices', 'effective_date',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    -- Compute system tables
    ('system.compute.clusters', 'obs.silver.compute_entities', 'change_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    ('system.compute.warehouses', 'obs.silver.compute_entities', 'change_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    ('system.compute.node_timeline', 'obs.silver.node_usage_minutely', 'start_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    ('system.compute.warehouse_events', 'obs.silver.warehouse_events', 'event_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    -- Lakeflow system tables
    ('system.lakeflow.jobs', 'obs.silver.workflow_entities', 'change_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    ('system.lakeflow.pipelines', 'obs.silver.workflow_entities', 'change_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    ('system.lakeflow.job_run_timeline', 'obs.silver.workflow_runs', 'start_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    ('system.lakeflow.job_task_run_timeline', 'obs.silver.job_task_runs', 'start_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    -- Query system tables
    ('system.query.history', 'obs.silver.query_history', 'start_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    -- Storage system tables
    ('system.storage.ops', 'obs.silver.storage_ops', 'start_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP()),
    
    -- Access system tables
    ('system.access.audit', 'obs.silver.audit_log', 'event_time',
     TIMESTAMP('1900-01-01'), CURRENT_TIMESTAMP(), 'SUCCESS', NULL, 0, 0,
     'system', CURRENT_TIMESTAMP(), 'system', CURRENT_TIMESTAMP());

-- =============================================================================
-- 5. VERIFICATION
-- =============================================================================

-- Verify watermark table creation
DESCRIBE TABLE obs.meta.watermarks;

-- Verify initial watermark records
SELECT 
    source_table_name,
    target_table_name,
    watermark_column,
    watermark_value,
    processing_status,
    records_processed
FROM obs.meta.watermarks
ORDER BY source_table_name, target_table_name;

-- Test watermark functions (commented out - UDFs removed)
-- SELECT obs.meta.get_watermark('system.billing.usage', 'obs.silver.billing_usage', 'usage_start_time') as test_watermark;

-- =============================================================================
-- 6. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. Bronze table creation scripts
-- 2. Silver table creation scripts
-- 3. Gold table creation scripts
-- 4. Data processing logic that uses these watermark functions

SELECT 'Watermark management system created successfully!' as message;
SELECT 'Table: obs.meta.watermarks' as message;
SELECT 'Note: UDFs removed - watermark management will use regular SQL queries' as message;
SELECT 'Initial watermark records inserted for all system tables' as message;
SELECT 'Ready for bronze table creation.' as message;
