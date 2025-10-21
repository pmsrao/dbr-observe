-- =============================================================================
-- Databricks Observability Platform - Bronze Storage Tables
-- =============================================================================
-- Purpose: Create bronze tables for storage system data
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. STORAGE OPERATIONS TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_storage_ops;

CREATE TABLE IF NOT EXISTS obs.bronze.system_storage_ops (
    raw_data STRUCT<
        workspace_id STRING,
        operation_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        operation_type STRING,
        object_type STRING,
        object_id STRING,
        object_name STRING,
        rows_affected BIGINT,
        bytes_optimized BIGINT,
        status STRING,
        details MAP<STRING, STRING>
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    start_time TIMESTAMP,
    start_date DATE,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, start_date);

-- =============================================================================
-- 2. TABLE PROPERTIES
-- =============================================================================

ALTER TABLE obs.bronze.system_storage_ops SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

SELECT 'Bronze storage tables created successfully!' as message;
