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
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN DEFAULT false
)
USING DELTA
COMMENT 'Bronze table for system.lakeflow.jobs - Raw job data'
LOCATION 's3://company-databricks-obs/bronze/system_lakeflow_jobs/'
PARTITIONED BY (workspace_id, date(change_time));

-- =============================================================================
-- 2. LAKEFLOW JOB TASKS TABLE
-- =============================================================================

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
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN DEFAULT false
)
USING DELTA
COMMENT 'Bronze table for system.lakeflow.job_tasks - Raw job task data'
LOCATION 's3://company-databricks-obs/bronze/system_lakeflow_job_tasks/'
PARTITIONED BY (workspace_id, date(change_time));

-- =============================================================================
-- 3. LAKEFLOW JOB RUN TIMELINE TABLE
-- =============================================================================

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
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN DEFAULT false
)
USING DELTA
COMMENT 'Bronze table for system.lakeflow.job_run_timeline - Raw job run data'
LOCATION 's3://company-databricks-obs/bronze/system_lakeflow_job_run_timeline/'
PARTITIONED BY (workspace_id, date(start_time));

-- =============================================================================
-- 4. LAKEFLOW JOB TASK RUN TIMELINE TABLE
-- =============================================================================

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
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN DEFAULT false
)
USING DELTA
COMMENT 'Bronze table for system.lakeflow.job_task_run_timeline - Raw job task run data'
LOCATION 's3://company-databricks-obs/bronze/system_lakeflow_job_task_run_timeline/'
PARTITIONED BY (workspace_id, date(start_time));

-- =============================================================================
-- 5. LAKEFLOW PIPELINES TABLE
-- =============================================================================

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
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN DEFAULT false
)
USING DELTA
COMMENT 'Bronze table for system.lakeflow.pipelines - Raw pipeline data'
LOCATION 's3://company-databricks-obs/bronze/system_lakeflow_pipelines/'
PARTITIONED BY (workspace_id, date(change_time));

-- =============================================================================
-- 6. LAKEFLOW PIPELINE UPDATE TIMELINE TABLE
-- =============================================================================

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
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN DEFAULT false
)
USING DELTA
COMMENT 'Bronze table for system.lakeflow.pipeline_update_timeline - Raw pipeline update data'
LOCATION 's3://company-databricks-obs/bronze/system_lakeflow_pipeline_update_timeline/'
PARTITIONED BY (workspace_id, date(start_time));

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

PRINT 'Bronze lakeflow tables created successfully!';
