-- =============================================================================
-- Databricks Observability Platform - Silver to Gold Processing
-- =============================================================================
-- Purpose: Process data from silver to gold layer for analytics
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- Set catalog context
USE CATALOG obs;

-- =============================================================================
-- 1. DIMENSION TABLES PROCESSING
-- =============================================================================

-- Process dim_compute from silver.compute_entities
INSERT INTO obs.gold.dim_compute
SELECT 
    SHA2(CONCAT(workspace_id, '|', compute_type, '|', compute_id), 256) as compute_sk,
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
    effective_start_ts,
    effective_end_ts,
    is_current,
    dw_created_ts,
    dw_updated_ts
FROM obs.silver.compute_entities
WHERE is_current = true;

-- Process dim_workflow from silver.workflow_entities
INSERT INTO obs.gold.dim_workflow
SELECT 
    SHA2(CONCAT(workspace_id, '|', workflow_type, '|', workflow_id), 256) as workflow_sk,
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
    effective_start_ts,
    effective_end_ts,
    is_current,
    dw_created_ts,
    dw_updated_ts
FROM obs.silver.workflow_entities
WHERE is_current = true;

-- Process dim_user from various sources
INSERT INTO obs.gold.dim_user
SELECT DISTINCT
    SHA2(COALESCE(user_id, user_name, service_principal_name), 256) as user_sk,
    user_id,
    user_name,
    service_principal_name,
    'USER' as identity_type,
    CURRENT_TIMESTAMP() as dw_created_ts,
    CURRENT_TIMESTAMP() as dw_updated_ts
FROM (
    SELECT executed_by_user_id as user_id, executed_by as user_name, NULL as service_principal_name
    FROM obs.silver.query_history
    WHERE executed_by_user_id IS NOT NULL
    UNION ALL
    SELECT user_id, user_name, service_principal_name
    FROM obs.silver.audit_log
    WHERE user_id IS NOT NULL
) users;

-- Process dim_sku from silver.billing_usage
INSERT INTO obs.gold.dim_sku
SELECT DISTINCT
    SHA2(CONCAT(sku_name, '|', cloud), 256) as sku_sk,
    sku_name,
    cloud,
    usage_unit,
    usage_type,
    CURRENT_TIMESTAMP() as dw_created_ts,
    CURRENT_TIMESTAMP() as dw_updated_ts
FROM obs.silver.billing_usage;

-- Process dim_node_type from bronze system tables
INSERT INTO obs.gold.dim_node_type
SELECT DISTINCT
    SHA2(raw_data.node_type, 256) as node_type_sk,
    raw_data.node_type,
    raw_data.core_count,
    raw_data.memory_mb,
    raw_data.gpu_count,
    CURRENT_TIMESTAMP() as dw_created_ts,
    CURRENT_TIMESTAMP() as dw_updated_ts
FROM obs.bronze.system_compute_node_types
WHERE raw_data IS NOT NULL AND NOT is_deleted;

-- =============================================================================
-- 2. FACT TABLES PROCESSING
-- =============================================================================

-- Process fct_billing_usage_hourly
INSERT INTO obs.gold.fct_billing_usage_hourly
SELECT 
    SHA2(CONCAT(record_id, '|', workspace_id, '|', usage_start_time), 256) as billing_sk,
    SHA2(CONCAT(workspace_id, '|', 'CLUSTER', '|', COALESCE(usage_metadata['cluster_id'], 'unknown')), 256) as compute_sk,
    SHA2(CONCAT(workspace_id, '|', 'JOB', '|', COALESCE(usage_metadata['job_id'], 'unknown')), 256) as workflow_sk,
    SHA2(COALESCE(identity_metadata.run_as, identity_metadata.owned_by, identity_metadata.created_by), 256) as user_sk,
    SHA2(CONCAT(sku_name, '|', cloud), 256) as sku_sk,
    usage_date,
    HOUR(usage_start_time) as usage_hour,
    usage_start_time,
    usage_end_time,
    workspace_id,
    sku_name,
    cloud,
    usage_quantity,
    usage_unit,
    usage_type,
    NULL as list_price,  -- To be populated from dim_sku
    NULL as total_cost,  -- To be calculated
    custom_tags['cost_center'] as cost_center,
    custom_tags['business_unit'] as business_unit,
    custom_tags['department'] as department,
    custom_tags['data_product'] as data_product,
    custom_tags['environment'] as environment,
    custom_tags['team'] as team,
    custom_tags['project'] as project,
    record_type,
    billing_origin_product,
    processing_timestamp
FROM obs.silver.billing_usage;

-- Process fct_workflow_runs
INSERT INTO obs.gold.fct_workflow_runs
SELECT 
    SHA2(CONCAT(workspace_id, '|', workflow_type, '|', workflow_id, '|', run_id), 256) as workflow_run_sk,
    SHA2(CONCAT(workspace_id, '|', workflow_type, '|', workflow_id), 256) as workflow_sk,
    SHA2(CONCAT(workspace_id, '|', 'CLUSTER', '|', 'unknown'), 256) as compute_sk,  -- To be linked properly
    SHA2('unknown', 256) as user_sk,  -- To be linked properly
    DATE(start_time) as run_date,
    start_time,
    end_time,
    duration_ms,
    workspace_id,
    workflow_type,
    workflow_id,
    run_id,
    result_state,
    termination_code,
    CASE 
        WHEN result_state = 'SUCCEEDED' THEN 1.0
        WHEN result_state = 'FAILED' THEN 0.0
        ELSE NULL
    END as success_rate,
    NULL as cost_center,  -- To be linked from workflow entities
    NULL as business_unit,
    NULL as department,
    NULL as data_product,
    NULL as environment,
    NULL as team,
    NULL as project,
    processing_timestamp
FROM obs.silver.workflow_runs;

-- Process fct_query_performance
INSERT INTO obs.gold.fct_query_performance
SELECT 
    SHA2(CONCAT(workspace_id, '|', statement_id), 256) as query_sk,
    SHA2(CONCAT(workspace_id, '|', compute_type, '|', compute_id), 256) as compute_sk,
    SHA2(COALESCE(executed_by_user_id, executed_by), 256) as user_sk,
    SHA2(CONCAT(workspace_id, '|', 'JOB', '|', COALESCE(query_source.job_info.job_id, 'unknown')), 256) as workflow_sk,
    DATE(start_time) as query_date,
    start_time,
    end_time,
    total_duration_ms,
    workspace_id,
    statement_id,
    statement_type,
    execution_status,
    read_partitions,
    read_files,
    read_rows,
    read_bytes,
    written_bytes,
    spilled_local_bytes,
    shuffle_read_bytes,
    CASE 
        WHEN total_duration_ms > 0 THEN read_bytes / (total_duration_ms / 1000.0)
        ELSE NULL
    END as bytes_per_second,
    CASE 
        WHEN total_duration_ms > 0 THEN read_rows / (total_duration_ms / 1000.0)
        ELSE NULL
    END as rows_per_second,
    CASE 
        WHEN read_io_cache_percent IS NOT NULL THEN read_io_cache_percent / 100.0
        ELSE NULL
    END as cache_hit_ratio,
    'Unknown' as cost_center,  -- To be extracted from compute tags
    'Unknown' as business_unit,
    'Unknown' as department,
    'Unknown' as data_product,
    'Unknown' as environment,
    'Unknown' as team,
    'Unknown' as project,
    processing_timestamp
FROM obs.silver.query_history;

-- =============================================================================
-- 3. VERIFICATION
-- =============================================================================

-- Verify dimension table processing
SELECT 'dim_compute' as table_name, COUNT(*) as record_count FROM obs.gold.dim_compute
UNION ALL
SELECT 'dim_workflow' as table_name, COUNT(*) as record_count FROM obs.gold.dim_workflow
UNION ALL
SELECT 'dim_user' as table_name, COUNT(*) as record_count FROM obs.gold.dim_user
UNION ALL
SELECT 'dim_sku' as table_name, COUNT(*) as record_count FROM obs.gold.dim_sku
UNION ALL
SELECT 'dim_node_type' as table_name, COUNT(*) as record_count FROM obs.gold.dim_node_type;

-- Verify fact table processing
SELECT 'fct_billing_usage_hourly' as table_name, COUNT(*) as record_count FROM obs.gold.fct_billing_usage_hourly
UNION ALL
SELECT 'fct_workflow_runs' as table_name, COUNT(*) as record_count FROM obs.gold.fct_workflow_runs
UNION ALL
SELECT 'fct_query_performance' as table_name, COUNT(*) as record_count FROM obs.gold.fct_query_performance;

-- =============================================================================
-- 4. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 03_metrics_calculation.sql - Calculate enhanced metrics
-- 2. 04_data_quality_checks.sql - Perform data quality validation
-- 3. 05_cost_calculation.sql - Calculate costs and showback metrics

SELECT 'Silver to Gold processing completed successfully!' as message;
SELECT 'Dimension tables populated with current records' as message;
SELECT 'Fact tables populated with business keys and surrogate keys' as message;
SELECT 'Tag extraction applied for cost allocation' as message;
SELECT 'Ready for metrics calculation.' as message;
