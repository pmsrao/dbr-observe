-- =============================================================================
-- Databricks Observability Platform - Bronze Access Tables
-- =============================================================================
-- Purpose: Create bronze tables for access system data
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. ACCESS AUDIT TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.bronze.system_access_audit (
    raw_data STRUCT<
        workspace_id STRING,
        event_id STRING,
        event_time TIMESTAMP,
        event_type STRING,
        action_name STRING,
        resource_type STRING,
        resource_id STRING,
        user_id STRING,
        user_name STRING,
        service_principal_name STRING,
        request_id STRING,
        session_id STRING,
        ip_address STRING,
        user_agent STRING,
        result STRING,
        error_message STRING,
        request_params MAP<STRING, STRING>,
        response STRING
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    event_time TIMESTAMP,
    event_date DATE,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, event_date);

-- =============================================================================
-- 2. TABLE PROPERTIES
-- =============================================================================

ALTER TABLE obs.bronze.system_access_audit SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

SELECT 'Bronze access tables created successfully!' as message;
