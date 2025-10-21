-- =============================================================================
-- Databricks Observability Platform - Bronze Lakeflow Tables
-- =============================================================================
-- Purpose: Create bronze tables for lakeflow system data
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. LAKEFLOW JOBS TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_lakeflow_jobs;

CREATE TABLE IF NOT EXISTS obs.bronze.system_lakeflow_jobs (
    raw_data STRUCT<
        job_id STRING,
        workspace_id STRING,
        name STRING,
        description STRING,
        creator_id STRING,
        run_as STRING,
        job_parameters MAP<STRING, STRING>,
        tags MAP<STRING, STRING>,
        create_time TIMESTAMP,
        delete_time TIMESTAMP,
        change_time TIMESTAMP
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    change_time TIMESTAMP,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, change_time);

-- =============================================================================
-- 2. LAKEFLOW JOB TASKS TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_lakeflow_job_tasks;

CREATE TABLE IF NOT EXISTS obs.bronze.system_lakeflow_job_tasks (
    raw_data STRUCT<
        job_id STRING,
        task_key STRING,
        workspace_id STRING,
        task_type STRING,
        depends_on ARRAY<STRING>,
        task_parameters MAP<STRING, STRING>,
        create_time TIMESTAMP,
        change_time TIMESTAMP
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    change_time TIMESTAMP,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, change_time);

-- =============================================================================
-- 3. LAKEFLOW JOB RUN TIMELINE TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_lakeflow_job_run_timeline;

CREATE TABLE IF NOT EXISTS obs.bronze.system_lakeflow_job_run_timeline (
    raw_data STRUCT<
        workspace_id STRING,
        job_id STRING,
        run_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        result_state STRING,
        termination_code STRING,
        job_parameters MAP<STRING, STRING>,
        parent_run_id STRING
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
-- 4. LAKEFLOW JOB TASK RUN TIMELINE TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_lakeflow_job_task_run_timeline;

CREATE TABLE IF NOT EXISTS obs.bronze.system_lakeflow_job_task_run_timeline (
    raw_data STRUCT<
        workspace_id STRING,
        job_id STRING,
        task_key STRING,
        job_task_run_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        result_state STRING,
        termination_code STRING,
        depends_on_task_keys ARRAY<STRING>
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
-- 5. LAKEFLOW PIPELINES TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_lakeflow_pipelines;

CREATE TABLE IF NOT EXISTS obs.bronze.system_lakeflow_pipelines (
    raw_data STRUCT<
        pipeline_id STRING,
        workspace_id STRING,
        name STRING,
        description STRING,
        creator_id STRING,
        run_as STRING,
        pipeline_type STRING,
        settings MAP<STRING, STRING>,
        tags MAP<STRING, STRING>,
        create_time TIMESTAMP,
        delete_time TIMESTAMP,
        change_time TIMESTAMP
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    change_time TIMESTAMP,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, change_time);

-- =============================================================================
-- 6. LAKEFLOW PIPELINE UPDATE TIMELINE TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_lakeflow_pipeline_update_timeline;

CREATE TABLE IF NOT EXISTS obs.bronze.system_lakeflow_pipeline_update_timeline (
    raw_data STRUCT<
        workspace_id STRING,
        pipeline_id STRING,
        update_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        result_state STRING,
        trigger_type STRING,
        update_type STRING,
        performance_target STRING
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
-- 7. TABLE PROPERTIES
-- =============================================================================

-- Set properties for all lakeflow tables
ALTER TABLE obs.bronze.system_lakeflow_jobs SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.bronze.system_lakeflow_job_tasks SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.bronze.system_lakeflow_job_run_timeline SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.bronze.system_lakeflow_job_task_run_timeline SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.bronze.system_lakeflow_pipelines SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.bronze.system_lakeflow_pipeline_update_timeline SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

SELECT 'Bronze lakeflow tables created successfully!' as message;
