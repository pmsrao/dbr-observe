-- =============================================================================
-- Databricks Observability Platform - Silver Query History Table
-- =============================================================================
-- Purpose: Create silver query history table with compute linkage
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.silver.query_history (
    -- Basic identifiers
    workspace_id STRING NOT NULL,
    statement_id STRING NOT NULL,
    session_id STRING,
    
    -- Execution details
    execution_status STRING,
    statement_text STRING,
    statement_type STRING,
    error_message STRING,
    
    -- Compute linkage
    compute_type STRING,  -- 'CLUSTER' | 'WAREHOUSE'
    compute_id STRING,
    
    -- User context
    executed_by_user_id STRING,
    executed_by STRING,
    executed_as STRING,
    executed_as_user_id STRING,
    
    -- Timing
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    total_duration_ms BIGINT,
    waiting_for_compute_duration_ms BIGINT,
    waiting_at_capacity_duration_ms BIGINT,
    execution_duration_ms BIGINT,
    compilation_duration_ms BIGINT,
    total_task_duration_ms BIGINT,
    result_fetch_duration_ms BIGINT,
    
    -- Performance metrics
    read_partitions BIGINT,
    pruned_files BIGINT,
    read_files BIGINT,
    read_rows BIGINT,
    produced_rows BIGINT,
    read_bytes BIGINT,
    read_io_cache_percent INT,
    spilled_local_bytes BIGINT,
    written_bytes BIGINT,
    written_rows BIGINT,
    written_files BIGINT,
    shuffle_read_bytes BIGINT,
    
    -- Caching
    from_result_cache BOOLEAN,
    cache_origin_statement_id STRING,
    
    -- Source context
    client_application STRING,
    client_driver STRING,
    query_source STRUCT<
        alert_id STRING,
        sql_query_id STRING,
        dashboard_id STRING,
        notebook_id STRING,
        job_info STRUCT<
            job_id STRING,
            job_run_id STRING,
            job_task_run_id STRING
        >,
        legacy_dashboard_id STRING,
        genie_space_id STRING
    >,
    query_parameters STRUCT<
        named_parameters MAP<STRING, STRING>,
        pos_parameters ARRAY<STRING>,
        is_truncated BOOLEAN
    >,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Silver table for query history with compute linkage and performance metrics'
LOCATION 's3://company-databricks-obs/silver/query_history/'
PARTITIONED BY (workspace_id, date(start_time));

-- Set properties
ALTER TABLE obs.silver.query_history SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- Add primary key constraint
ALTER TABLE obs.silver.query_history 
ADD CONSTRAINT pk_query_history 
PRIMARY KEY (workspace_id, statement_id);

PRINT 'Silver query history table created successfully!';
