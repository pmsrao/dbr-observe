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
COMMENT 'Watermark management table for incremental processing tracking'
LOCATION 's3://company-databricks-obs/meta/watermarks/';

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

-- Create primary key constraint
ALTER TABLE obs.meta.watermarks 
ADD CONSTRAINT pk_watermarks 
PRIMARY KEY (source_table_name, target_table_name, watermark_column);

-- =============================================================================
-- 3. INITIAL WATERMARK RECORDS
-- =============================================================================

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
-- 4. WATERMARK MANAGEMENT FUNCTIONS
-- =============================================================================

-- Function to get watermark value for a source-target pair
CREATE OR REPLACE FUNCTION obs.meta.get_watermark(
    source_table_name STRING,
    target_table_name STRING,
    watermark_column STRING
)
RETURNS TIMESTAMP
LANGUAGE SQL
AS $$
    SELECT COALESCE(
        (SELECT watermark_value 
         FROM obs.meta.watermarks 
         WHERE obs.meta.watermarks.source_table_name = get_watermark.source_table_name
           AND obs.meta.watermarks.target_table_name = get_watermark.target_table_name
           AND obs.meta.watermarks.watermark_column = get_watermark.watermark_column
           AND processing_status = 'SUCCESS'
         ORDER BY last_updated DESC 
         LIMIT 1),
        TIMESTAMP('1900-01-01')
    )
$$;

-- Function to update watermark after successful processing
CREATE OR REPLACE FUNCTION obs.meta.update_watermark(
    source_table_name STRING,
    target_table_name STRING,
    watermark_column STRING,
    new_watermark_value TIMESTAMP,
    records_processed BIGINT,
    processing_duration_ms BIGINT,
    updated_by STRING
)
RETURNS VOID
LANGUAGE SQL
AS $$
    MERGE INTO obs.meta.watermarks AS target
    USING (
        SELECT 
            update_watermark.source_table_name,
            update_watermark.target_table_name,
            update_watermark.watermark_column,
            update_watermark.new_watermark_value,
            CURRENT_TIMESTAMP() as last_updated,
            'SUCCESS' as processing_status,
            NULL as error_message,
            update_watermark.records_processed,
            update_watermark.processing_duration_ms,
            update_watermark.updated_by,
            CURRENT_TIMESTAMP() as updated_at
    ) AS source
    ON target.source_table_name = source.source_table_name 
      AND target.target_table_name = source.target_table_name
      AND target.watermark_column = source.watermark_column
    WHEN MATCHED THEN UPDATE SET 
        watermark_value = source.new_watermark_value,
        last_updated = source.last_updated,
        processing_status = source.processing_status,
        error_message = source.error_message,
        records_processed = source.records_processed,
        processing_duration_ms = source.processing_duration_ms,
        updated_by = source.updated_by,
        updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT VALUES (
        source.source_table_name, source.target_table_name, source.watermark_column,
        source.new_watermark_value, source.last_updated, source.processing_status,
        source.error_message, source.records_processed, source.processing_duration_ms,
        'system', CURRENT_TIMESTAMP(), source.updated_by, source.updated_at
    )
$$;

-- Function to update watermark with error status
CREATE OR REPLACE FUNCTION obs.meta.update_watermark_error(
    source_table_name STRING,
    target_table_name STRING,
    watermark_column STRING,
    error_message STRING,
    updated_by STRING
)
RETURNS VOID
LANGUAGE SQL
AS $$
    MERGE INTO obs.meta.watermarks AS target
    USING (
        SELECT 
            update_watermark_error.source_table_name,
            update_watermark_error.target_table_name,
            update_watermark_error.watermark_column,
            CURRENT_TIMESTAMP() as last_updated,
            'FAILED' as processing_status,
            update_watermark_error.error_message,
            NULL as records_processed,
            NULL as processing_duration_ms,
            update_watermark_error.updated_by,
            CURRENT_TIMESTAMP() as updated_at
    ) AS source
    ON target.source_table_name = source.source_table_name 
      AND target.target_table_name = source.target_table_name
      AND target.watermark_column = source.watermark_column
    WHEN MATCHED THEN UPDATE SET 
        last_updated = source.last_updated,
        processing_status = source.processing_status,
        error_message = source.error_message,
        updated_by = source.updated_by,
        updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT VALUES (
        source.source_table_name, source.target_table_name, source.watermark_column,
        TIMESTAMP('1900-01-01'), source.last_updated, source.processing_status,
        source.error_message, source.records_processed, source.processing_duration_ms,
        'system', CURRENT_TIMESTAMP(), source.updated_by, source.updated_at
    )
$$;

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

-- Test watermark functions
SELECT obs.meta.get_watermark('system.billing.usage', 'obs.silver.billing_usage', 'usage_start_time') as test_watermark;

-- =============================================================================
-- 6. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. Bronze table creation scripts
-- 2. Silver table creation scripts
-- 3. Gold table creation scripts
-- 4. Data processing logic that uses these watermark functions

PRINT 'Watermark management system created successfully!';
PRINT 'Table: obs.meta.watermarks';
PRINT 'Functions created: get_watermark, update_watermark, update_watermark_error';
PRINT 'Initial watermark records inserted for all system tables';
PRINT 'Ready for bronze table creation.';
