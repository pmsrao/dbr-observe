-- =============================================================================
-- Databricks Observability Platform - Bronze Query Tables
-- =============================================================================
-- Purpose: Create bronze tables for query system data
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. QUERY HISTORY TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_query_history;

CREATE TABLE IF NOT EXISTS obs.bronze.system_query_history (
    raw_data STRUCT<
        workspace_id STRING,
        statement_id STRING,
        session_id STRING,
        execution_status STRING,
        statement_text STRING,
        statement_type STRING,
        error_message STRING,
        executed_by_user_id STRING,
        executed_by STRING,
        executed_as STRING,
        executed_as_user_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        total_duration_ms BIGINT,
        waiting_for_compute_duration_ms BIGINT,
        waiting_at_capacity_duration_ms BIGINT,
        execution_duration_ms BIGINT,
        compilation_duration_ms BIGINT,
        total_task_duration_ms BIGINT,
        result_fetch_duration_ms BIGINT,
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
        from_result_cache BOOLEAN,
        cache_origin_statement_id STRING,
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
        >
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

ALTER TABLE obs.bronze.system_query_history SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

SELECT 'Bronze query tables created successfully!' as message;
