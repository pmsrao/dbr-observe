-- =============================================================================
-- Databricks Observability Platform - Staging Views
-- =============================================================================
-- Purpose: Create staging views for data harmonization and SCD2 processing
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. COMPUTE ENTITIES STAGING VIEW
-- =============================================================================

CREATE OR REPLACE VIEW obs.silver.compute_entities_staging AS
WITH cluster_data AS (
    SELECT 
        raw_data.workspace_id,
        'CLUSTER' as compute_type,
        raw_data.cluster_id as compute_id,
        raw_data.name,
        raw_data.owner,
        raw_data.driver_node_type,
        raw_data.worker_node_type,
        raw_data.worker_count,
        raw_data.min_autoscale_workers,
        raw_data.max_autoscale_workers,
        raw_data.auto_termination_minutes,
        raw_data.enable_elastic_disk,
        raw_data.data_security_mode,
        raw_data.policy_id,
        raw_data.dbr_version,
        raw_data.cluster_source,
        NULL as warehouse_type,
        NULL as warehouse_size,
        NULL as warehouse_channel,
        NULL as min_clusters,
        NULL as max_clusters,
        NULL as auto_stop_minutes,
        raw_data.tags,
        raw_data.create_time,
        raw_data.delete_time,
        raw_data.change_time,
        ingestion_timestamp as processing_timestamp
    FROM obs.bronze.system_compute_clusters
    WHERE NOT is_deleted
),
warehouse_data AS (
    SELECT 
        raw_data.workspace_id,
        'WAREHOUSE' as compute_type,
        raw_data.warehouse_id as compute_id,
        raw_data.name,
        raw_data.owner,
        NULL as driver_node_type,
        NULL as worker_node_type,
        NULL as worker_count,
        NULL as min_autoscale_workers,
        NULL as max_autoscale_workers,
        NULL as auto_termination_minutes,
        NULL as enable_elastic_disk,
        NULL as data_security_mode,
        NULL as policy_id,
        NULL as dbr_version,
        NULL as cluster_source,
        raw_data.warehouse_type,
        raw_data.warehouse_size,
        raw_data.warehouse_channel,
        raw_data.min_clusters,
        raw_data.max_clusters,
        raw_data.auto_stop_minutes,
        raw_data.tags,
        raw_data.create_time,
        raw_data.delete_time,
        raw_data.change_time,
        ingestion_timestamp as processing_timestamp
    FROM obs.bronze.system_compute_warehouses
    WHERE NOT is_deleted
)
SELECT 
    workspace_id,
    compute_type,
    compute_id,
    name,
    owner,
    driver_node_type,
    worker_node_type,
    worker_count,
    min_autoscale_workers,
    max_autoscale_workers,
    auto_termination_minutes,
    enable_elastic_disk,
    data_security_mode,
    policy_id,
    dbr_version,
    cluster_source,
    warehouse_type,
    warehouse_size,
    warehouse_channel,
    min_clusters,
    max_clusters,
    auto_stop_minutes,
    tags,
    create_time,
    delete_time,
    change_time,
    processing_timestamp
FROM cluster_data
UNION ALL
SELECT 
    workspace_id,
    compute_type,
    compute_id,
    name,
    owner,
    driver_node_type,
    worker_node_type,
    worker_count,
    min_autoscale_workers,
    max_autoscale_workers,
    auto_termination_minutes,
    enable_elastic_disk,
    data_security_mode,
    policy_id,
    dbr_version,
    cluster_source,
    warehouse_type,
    warehouse_size,
    warehouse_channel,
    min_clusters,
    max_clusters,
    auto_stop_minutes,
    tags,
    create_time,
    delete_time,
    change_time,
    processing_timestamp
FROM warehouse_data;

-- =============================================================================
-- 2. WORKFLOW ENTITIES STAGING VIEW
-- =============================================================================

CREATE OR REPLACE VIEW obs.silver.workflow_entities_staging AS
WITH job_data AS (
    SELECT 
        raw_data.workspace_id,
        'JOB' as workflow_type,
        raw_data.job_id as workflow_id,
        raw_data.name,
        raw_data.description,
        raw_data.creator_id,
        raw_data.run_as,
        NULL as pipeline_type,
        raw_data.job_parameters as settings,
        raw_data.tags,
        raw_data.create_time,
        raw_data.delete_time,
        raw_data.change_time,
        ingestion_timestamp as processing_timestamp
    FROM obs.bronze.system_lakeflow_jobs
    WHERE NOT is_deleted
),
pipeline_data AS (
    SELECT 
        raw_data.workspace_id,
        'PIPELINE' as workflow_type,
        raw_data.pipeline_id as workflow_id,
        raw_data.name,
        raw_data.description,
        raw_data.creator_id,
        raw_data.run_as,
        raw_data.pipeline_type,
        raw_data.settings,
        raw_data.tags,
        raw_data.create_time,
        raw_data.delete_time,
        raw_data.change_time,
        ingestion_timestamp as processing_timestamp
    FROM obs.bronze.system_lakeflow_pipelines
    WHERE NOT is_deleted
)
SELECT 
    workspace_id,
    workflow_type,
    workflow_id,
    name,
    description,
    creator_id,
    run_as,
    pipeline_type,
    settings,
    tags,
    create_time,
    delete_time,
    change_time,
    processing_timestamp
FROM job_data
UNION ALL
SELECT 
    workspace_id,
    workflow_type,
    workflow_id,
    name,
    description,
    creator_id,
    run_as,
    pipeline_type,
    settings,
    tags,
    create_time,
    delete_time,
    change_time,
    processing_timestamp
FROM pipeline_data;

-- =============================================================================
-- 3. WORKFLOW RUNS STAGING VIEW
-- =============================================================================

CREATE OR REPLACE VIEW obs.silver.workflow_runs_staging AS
WITH job_runs AS (
    SELECT 
        raw_data.workspace_id,
        'JOB' as workflow_type,
        raw_data.job_id as workflow_id,
        raw_data.run_id,
        raw_data.name as workflow_name,
        raw_data.start_time,
        raw_data.end_time,
        raw_data.result_state,
        raw_data.termination_code,
        raw_data.job_parameters,
        raw_data.parent_run_id,
        NULL as trigger_type,
        NULL as update_type,
        NULL as performance_target,
        ingestion_timestamp as processing_timestamp
    FROM obs.bronze.system_lakeflow_job_run_timeline
    WHERE NOT is_deleted
),
pipeline_runs AS (
    SELECT 
        raw_data.workspace_id,
        'PIPELINE' as workflow_type,
        raw_data.pipeline_id as workflow_id,
        raw_data.update_id as run_id,
        raw_data.name as workflow_name,
        raw_data.start_time,
        raw_data.end_time,
        raw_data.result_state,
        NULL as termination_code,
        NULL as job_parameters,
        NULL as parent_run_id,
        raw_data.trigger_type,
        raw_data.update_type,
        raw_data.performance_target,
        ingestion_timestamp as processing_timestamp
    FROM obs.bronze.system_lakeflow_pipeline_update_timeline
    WHERE NOT is_deleted
)
SELECT 
    workspace_id,
    workflow_type,
    workflow_id,
    run_id,
    workflow_name,
    start_time,
    end_time,
    result_state,
    termination_code,
    job_parameters,
    parent_run_id,
    trigger_type,
    update_type,
    performance_target,
    processing_timestamp
FROM job_runs
UNION ALL
SELECT 
    workspace_id,
    workflow_type,
    workflow_id,
    run_id,
    workflow_name,
    start_time,
    end_time,
    result_state,
    termination_code,
    job_parameters,
    parent_run_id,
    trigger_type,
    update_type,
    performance_target,
    processing_timestamp
FROM pipeline_runs;

-- =============================================================================
-- 4. BILLING USAGE STAGING VIEW
-- =============================================================================

CREATE OR REPLACE VIEW obs.silver.billing_usage_staging AS
SELECT 
    raw_data.record_id,
    raw_data.workspace_id,
    raw_data.sku_name,
    raw_data.cloud,
    raw_data.usage_start_time,
    raw_data.usage_end_time,
    raw_data.usage_date,
    raw_data.usage_unit,
    raw_data.usage_quantity,
    raw_data.usage_type,
    raw_data.custom_tags,
    raw_data.usage_metadata,
    raw_data.identity_metadata,
    raw_data.record_type,
    raw_data.ingestion_date,
    raw_data.billing_origin_product,
    raw_data.product_features,
    ingestion_timestamp as processing_timestamp
FROM obs.bronze.system_billing_usage
WHERE NOT is_deleted;

-- =============================================================================
-- 5. QUERY HISTORY STAGING VIEW
-- =============================================================================

CREATE OR REPLACE VIEW obs.silver.query_history_staging AS
SELECT 
    raw_data.workspace_id,
    raw_data.statement_id,
    raw_data.session_id,
    raw_data.execution_status,
    raw_data.statement_text,
    raw_data.statement_type,
    raw_data.error_message,
    raw_data.executed_by_user_id,
    raw_data.executed_by,
    raw_data.executed_as,
    raw_data.executed_as_user_id,
    raw_data.start_time,
    raw_data.end_time,
    raw_data.total_duration_ms,
    raw_data.waiting_for_compute_duration_ms,
    raw_data.waiting_at_capacity_duration_ms,
    raw_data.execution_duration_ms,
    raw_data.compilation_duration_ms,
    raw_data.total_task_duration_ms,
    raw_data.result_fetch_duration_ms,
    raw_data.read_partitions,
    raw_data.pruned_files,
    raw_data.read_files,
    raw_data.read_rows,
    raw_data.produced_rows,
    raw_data.read_bytes,
    raw_data.read_io_cache_percent,
    raw_data.spilled_local_bytes,
    raw_data.written_bytes,
    raw_data.written_rows,
    raw_data.written_files,
    raw_data.shuffle_read_bytes,
    raw_data.from_result_cache,
    raw_data.cache_origin_statement_id,
    raw_data.client_application,
    raw_data.client_driver,
    raw_data.query_source,
    raw_data.query_parameters,
    ingestion_timestamp as processing_timestamp
FROM obs.bronze.system_query_history
WHERE NOT is_deleted;

-- =============================================================================
-- 6. VERIFICATION
-- =============================================================================

-- Verify staging views creation
SHOW VIEWS IN obs.silver;

-- Test staging views
SELECT COUNT(*) as compute_entities_count FROM obs.silver.compute_entities_staging;
SELECT COUNT(*) as workflow_entities_count FROM obs.silver.workflow_entities_staging;
SELECT COUNT(*) as workflow_runs_count FROM obs.silver.workflow_runs_staging;
SELECT COUNT(*) as billing_usage_count FROM obs.silver.billing_usage_staging;
SELECT COUNT(*) as query_history_count FROM obs.silver.query_history_staging;

-- =============================================================================
-- 7. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 02_scd2_functions.sql - Create SCD2 merge functions
-- 2. 03_processing_logic.sql - Create data processing logic
-- 3. 04_tag_extraction_functions.sql - Create tag extraction functions

PRINT 'Staging views created successfully!';
PRINT 'Views: compute_entities_staging, workflow_entities_staging, workflow_runs_staging, billing_usage_staging, query_history_staging (all in silver schema)';
PRINT 'Data harmonization implemented for unified entity processing';
PRINT 'Ready for SCD2 functions.';
