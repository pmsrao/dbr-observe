-- =============================================================================
-- Databricks Observability Platform - Silver Workflow Runs Table
-- =============================================================================
-- Purpose: Create silver workflow runs table with enhanced runtime metrics
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. WORKFLOW RUNS TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.silver.workflow_runs;

CREATE TABLE IF NOT EXISTS obs.silver.workflow_runs (
    -- Basic identifiers
    workspace_id STRING NOT NULL,
    workflow_type STRING NOT NULL,  -- 'JOB' | 'PIPELINE'
    workflow_id STRING NOT NULL,
    run_id STRING NOT NULL,
    
    -- Unified Naming
    workflow_name STRING,  -- Unified job_name or pipeline_name
    
    -- Timing
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_ms BIGINT,
    start_date DATE,
    
    -- Status
    result_state STRING,
    termination_code STRING,
    
    -- Job-specific (nullable for pipelines)
    job_parameters MAP<STRING, STRING>,
    parent_run_id STRING,
    
    -- Pipeline-specific (nullable for jobs)
    trigger_type STRING,
    update_type STRING,
    performance_target STRING,
    
    -- Enhanced Runtime Attributes
    
    -- Resource Utilization Metrics
    cpu_utilization_percent DOUBLE,
    memory_utilization_percent DOUBLE,
    disk_utilization_percent DOUBLE,
    network_utilization_percent DOUBLE,
    gpu_utilization_percent DOUBLE,
    
    -- Performance Metrics
    records_processed BIGINT,
    data_volume_bytes BIGINT,
    data_volume_mb DOUBLE,
    throughput_records_per_second DOUBLE,
    throughput_mb_per_second DOUBLE,
    execution_efficiency DOUBLE,
    resource_efficiency DOUBLE,
    
    -- Error and Retry Metrics
    retry_count INT,
    max_retry_count INT,
    error_message STRING,
    error_category STRING,
    error_severity STRING,
    stack_trace STRING,
    last_error_time TIMESTAMP,
    
    -- Resource Allocation
    cluster_size INT,
    min_cluster_size INT,
    max_cluster_size INT,
    node_type STRING,
    driver_node_type STRING,
    worker_node_type STRING,
    auto_scaling_enabled BOOLEAN,
    spot_instances_used BOOLEAN,
    
    -- Cost and Billing Metrics
    dbu_consumed DECIMAL(18,6),
    dbu_per_hour DECIMAL(18,6),
    cost_usd DECIMAL(18,6),
    cost_per_hour_usd DECIMAL(18,6),
    cost_per_record_usd DECIMAL(18,6),
    cost_efficiency_score DOUBLE,
    
    -- Data Processing Metrics
    input_files_count BIGINT,
    output_files_count BIGINT,
    input_partitions BIGINT,
    output_partitions BIGINT,
    shuffle_read_bytes BIGINT,
    shuffle_write_bytes BIGINT,
    spilled_bytes BIGINT,
    cache_hit_ratio DOUBLE,
    
    -- Dependencies and Orchestration
    dependency_wait_time_ms BIGINT,
    queue_wait_time_ms BIGINT,
    resource_wait_time_ms BIGINT,
    parent_workflow_id STRING,
    child_workflow_count INT,
    parallel_execution_count INT,
    
    -- Quality and Reliability Metrics
    success_rate DOUBLE,
    failure_rate DOUBLE,
    avg_duration_ms DOUBLE,
    duration_variance DOUBLE,
    reliability_score DOUBLE,
    sla_compliance BOOLEAN,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (workspace_id, start_date);

-- =============================================================================
-- 2. JOB TASK RUNS TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.silver.job_task_runs;

CREATE TABLE IF NOT EXISTS obs.silver.job_task_runs (
    -- Basic identifiers
    workspace_id STRING NOT NULL,
    job_id STRING NOT NULL,
    task_key STRING NOT NULL,
    job_task_run_id STRING NOT NULL,
    
    -- Timing
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_ms BIGINT,
    start_date DATE,
    
    -- Status
    result_state STRING,
    termination_code STRING,
    
    -- Enhanced Runtime Attributes
    
    -- Resource Utilization Metrics
    cpu_utilization_percent DOUBLE,
    memory_utilization_percent DOUBLE,
    disk_utilization_percent DOUBLE,
    network_utilization_percent DOUBLE,
    
    -- Performance Metrics
    records_processed BIGINT,
    data_volume_bytes BIGINT,
    data_volume_mb DOUBLE,
    throughput_records_per_second DOUBLE,
    throughput_mb_per_second DOUBLE,
    execution_efficiency DOUBLE,
    
    -- Error and Retry Metrics
    retry_count INT,
    max_retry_count INT,
    error_message STRING,
    error_category STRING,
    error_severity STRING,
    stack_trace STRING,
    last_error_time TIMESTAMP,
    
    -- Resource Allocation
    cluster_size INT,
    node_type STRING,
    driver_node_type STRING,
    worker_node_type STRING,
    
    -- Cost and Billing Metrics
    dbu_consumed DECIMAL(18,6),
    cost_usd DECIMAL(18,6),
    cost_per_record_usd DECIMAL(18,6),
    
    -- Data Processing Metrics
    input_files_count BIGINT,
    output_files_count BIGINT,
    input_partitions BIGINT,
    output_partitions BIGINT,
    shuffle_read_bytes BIGINT,
    shuffle_write_bytes BIGINT,
    spilled_bytes BIGINT,
    cache_hit_ratio DOUBLE,
    
    -- Dependencies and Orchestration
    dependency_wait_time_ms BIGINT,
    queue_wait_time_ms BIGINT,
    resource_wait_time_ms BIGINT,
    depends_on_task_keys ARRAY<STRING>,
    upstream_task_count INT,
    downstream_task_count INT,
    
    -- Quality and Reliability Metrics
    success_rate DOUBLE,
    failure_rate DOUBLE,
    avg_duration_ms DOUBLE,
    duration_variance DOUBLE,
    reliability_score DOUBLE,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (workspace_id, start_date);

-- =============================================================================
-- 3. TABLE PROPERTIES AND CONSTRAINTS
-- =============================================================================

-- Set properties for workflow runs table
ALTER TABLE obs.silver.workflow_runs SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Set properties for job task runs table
ALTER TABLE obs.silver.job_task_runs SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Add primary key constraints (idempotent)
ALTER TABLE obs.silver.workflow_runs 
DROP CONSTRAINT IF EXISTS pk_workflow_runs;

ALTER TABLE obs.silver.workflow_runs 
ADD CONSTRAINT pk_workflow_runs 
PRIMARY KEY (workspace_id, workflow_type, workflow_id, run_id);

ALTER TABLE obs.silver.job_task_runs 
DROP CONSTRAINT IF EXISTS pk_job_task_runs;

ALTER TABLE obs.silver.job_task_runs 
ADD CONSTRAINT pk_job_task_runs 
PRIMARY KEY (workspace_id, job_id, task_key, job_task_run_id);

-- =============================================================================
-- 4. VERIFICATION
-- =============================================================================

-- Verify table creation
DESCRIBE TABLE obs.silver.workflow_runs;
DESCRIBE TABLE obs.silver.job_task_runs;

-- Check table properties
SHOW TBLPROPERTIES obs.silver.workflow_runs;
SHOW TBLPROPERTIES obs.silver.job_task_runs;

-- =============================================================================
-- 5. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 03_silver_billing_usage.sql - Create billing usage table
-- 2. 04_silver_query_history.sql - Create query history table
-- 3. 05_silver_audit_log.sql - Create audit log table
-- 4. Staging views for data harmonization

SELECT 'Silver workflow runs tables created successfully!' as message;
SELECT 'Tables: workflow_runs, job_task_runs' as message;
SELECT 'Enhanced runtime metrics and performance tracking implemented' as message;
SELECT 'Ready for billing usage table creation.' as message;
