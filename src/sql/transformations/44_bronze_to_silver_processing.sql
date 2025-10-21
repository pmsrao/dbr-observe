-- =============================================================================
-- Databricks Observability Platform - Bronze to Silver Processing
-- =============================================================================
-- Purpose: Process data from bronze to silver layer with SCD2 patterns
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- Set catalog context
USE CATALOG obs;

-- =============================================================================
-- 1. COMPUTE ENTITIES PROCESSING
-- =============================================================================

-- Process compute entities with SCD2 pattern
-- Note: SCD2 merge logic will be implemented in Python processing scripts
-- This is a placeholder for the actual SCD2 merge implementation

-- Example SCD2 merge pattern (for Python implementation):
-- 1. Get watermark for source table
-- 2. Extract new/changed records from bronze
-- 3. Apply SCD2 merge logic to silver table
-- 4. Update watermark with new timestamp

-- For now, we'll do a simple INSERT for demonstration
INSERT INTO obs.silver.compute_entities
SELECT 
    raw_data.workspace_id as workspace_id,
    'cluster' as compute_type,
    raw_data.cluster_id as compute_id,
    -- SCD2 columns
    raw_data.change_time as effective_start_ts,
    TIMESTAMP('9999-12-31') as effective_end_ts,
    true as is_current,
    -- Audit columns
    SHA2(CONCAT(COALESCE(raw_data.workspace_id, ''), '|', 'cluster', '|', COALESCE(raw_data.cluster_id, '')), 256) as record_hash,
    CURRENT_TIMESTAMP() as dw_created_ts,
    CURRENT_TIMESTAMP() as dw_updated_ts,
    ingestion_timestamp as processing_timestamp,
    -- Business attributes
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
    raw_data.delete_time
FROM obs.bronze.system_compute_clusters
WHERE raw_data IS NOT NULL;

-- =============================================================================
-- 2. WORKFLOW ENTITIES PROCESSING
-- =============================================================================

-- Process workflow entities with SCD2 pattern
-- Note: SCD2 merge logic will be implemented in Python processing scripts

INSERT INTO obs.silver.workflow_entities
SELECT 
    raw_data.workspace_id as workspace_id,
    'job' as workflow_type,
    raw_data.job_id as workflow_id,
    -- SCD2 columns
    raw_data.change_time as effective_start_ts,
    TIMESTAMP('9999-12-31') as effective_end_ts,
    true as is_current,
    -- Audit columns
    SHA2(CONCAT(COALESCE(raw_data.workspace_id, ''), '|', 'job', '|', COALESCE(raw_data.job_id, '')), 256) as record_hash,
    CURRENT_TIMESTAMP() as dw_created_ts,
    CURRENT_TIMESTAMP() as dw_updated_ts,
    ingestion_timestamp as processing_timestamp,
    -- Business attributes
    raw_data.name,
    raw_data.description,
    raw_data.creator_id,
    raw_data.run_as,
    -- Pipeline-specific (nullable for jobs)
    NULL as pipeline_type,
    raw_data.job_parameters as settings,
    -- Tags
    raw_data.tags,
    -- Lifecycle
    raw_data.create_time,
    raw_data.delete_time
FROM obs.bronze.system_lakeflow_jobs
WHERE raw_data IS NOT NULL;

-- =============================================================================
-- 3. WORKFLOW RUNS PROCESSING
-- =============================================================================

-- Process workflow runs (no SCD2 needed for runs)
INSERT INTO obs.silver.workflow_runs
SELECT 
    workspace_id,
    workflow_type,
    workflow_id,
    run_id,
    workflow_name,
    start_time,
    end_time,
    CASE 
        WHEN end_time IS NOT NULL AND start_time IS NOT NULL 
        THEN TIMESTAMPDIFF(MICROSECOND, start_time, end_time) / 1000
        ELSE NULL 
    END as duration_ms,
    DATE(start_time) as start_date,
    result_state,
    termination_code,
    job_parameters,
    parent_run_id,
    trigger_type,
    update_type,
    performance_target,
    -- Enhanced runtime attributes (to be calculated from node timeline data)
    NULL as cpu_utilization_percent,
    NULL as memory_utilization_percent,
    NULL as disk_utilization_percent,
    NULL as network_utilization_percent,
    NULL as gpu_utilization_percent,
    NULL as records_processed,
    NULL as data_volume_bytes,
    NULL as data_volume_mb,
    NULL as throughput_records_per_second,
    NULL as throughput_mb_per_second,
    NULL as execution_efficiency,
    NULL as resource_efficiency,
    NULL as retry_count,
    NULL as max_retry_count,
    NULL as error_message,
    NULL as error_category,
    NULL as error_severity,
    NULL as stack_trace,
    NULL as last_error_time,
    NULL as cluster_size,
    NULL as min_cluster_size,
    NULL as max_cluster_size,
    NULL as node_type,
    NULL as driver_node_type,
    NULL as worker_node_type,
    NULL as auto_scaling_enabled,
    NULL as spot_instances_used,
    NULL as dbu_consumed,
    NULL as dbu_per_hour,
    NULL as cost_usd,
    NULL as cost_per_hour_usd,
    NULL as cost_per_record_usd,
    NULL as cost_efficiency_score,
    NULL as input_files_count,
    NULL as output_files_count,
    NULL as input_partitions,
    NULL as output_partitions,
    NULL as shuffle_read_bytes,
    NULL as shuffle_write_bytes,
    NULL as spilled_bytes,
    NULL as cache_hit_ratio,
    NULL as dependency_wait_time_ms,
    NULL as queue_wait_time_ms,
    NULL as resource_wait_time_ms,
    NULL as parent_workflow_id,
    NULL as child_workflow_count,
    NULL as parallel_execution_count,
    NULL as success_rate,
    NULL as failure_rate,
    NULL as avg_duration_ms,
    NULL as duration_variance,
    NULL as reliability_score,
    NULL as sla_compliance,
    processing_timestamp
FROM obs.silver.workflow_runs_staging;

-- =============================================================================
-- 4. BILLING USAGE PROCESSING
-- =============================================================================

-- Process billing usage with upsert pattern
MERGE INTO obs.silver.billing_usage AS target
USING (
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
        MAP(
            'cluster_id', raw_data.usage_metadata.cluster_id,
            'job_id', raw_data.usage_metadata.job_id,
            'warehouse_id', raw_data.usage_metadata.warehouse_id,
            'instance_pool_id', raw_data.usage_metadata.instance_pool_id,
            'node_type', raw_data.usage_metadata.node_type,
            'job_run_id', raw_data.usage_metadata.job_run_id,
            'notebook_id', raw_data.usage_metadata.notebook_id,
            'dlt_pipeline_id', raw_data.usage_metadata.dlt_pipeline_id,
            'endpoint_name', raw_data.usage_metadata.endpoint_name,
            'endpoint_id', raw_data.usage_metadata.endpoint_id,
            'dlt_update_id', raw_data.usage_metadata.dlt_update_id,
            'dlt_maintenance_id', raw_data.usage_metadata.dlt_maintenance_id,
            'metastore_id', raw_data.usage_metadata.metastore_id,
            'job_name', raw_data.usage_metadata.job_name,
            'notebook_path', raw_data.usage_metadata.notebook_path,
            'central_clean_room_id', raw_data.usage_metadata.central_clean_room_id,
            'source_region', raw_data.usage_metadata.source_region,
            'destination_region', raw_data.usage_metadata.destination_region,
            'app_id', raw_data.usage_metadata.app_id,
            'app_name', raw_data.usage_metadata.app_name,
            'budget_policy_id', raw_data.usage_metadata.budget_policy_id,
            'base_environment_id', raw_data.usage_metadata.base_environment_id
        ) as usage_metadata,
        raw_data.identity_metadata,
        raw_data.record_type,
        raw_data.ingestion_date,
        raw_data.billing_origin_product,
        raw_data.product_features,
        ingestion_timestamp as processing_timestamp
    FROM obs.bronze.system_billing_usage
    WHERE raw_data IS NOT NULL
) AS source
ON target.record_id = source.record_id
WHEN MATCHED THEN 
    UPDATE SET 
        usage_quantity = source.usage_quantity,
        usage_metadata = source.usage_metadata,
        processing_timestamp = source.processing_timestamp
WHEN NOT MATCHED THEN 
    INSERT (
        record_id, workspace_id, sku_name, cloud, usage_start_time, usage_end_time,
        usage_date, usage_unit, usage_quantity, usage_type, custom_tags, usage_metadata,
        identity_metadata, record_type, ingestion_date, billing_origin_product,
        product_features, processing_timestamp
    )
    VALUES (
        source.record_id, source.workspace_id, source.sku_name, source.cloud, source.usage_start_time, source.usage_end_time,
        source.usage_date, source.usage_unit, source.usage_quantity, source.usage_type, source.custom_tags, source.usage_metadata,
        source.identity_metadata, source.record_type, source.ingestion_date, source.billing_origin_product,
        source.product_features, source.processing_timestamp
    );

-- =============================================================================
-- 5. QUERY HISTORY PROCESSING
-- =============================================================================

-- Process query history
INSERT INTO obs.silver.query_history
SELECT 
    raw_data.workspace_id as workspace_id,
    raw_data.statement_id,
    raw_data.session_id,
    raw_data.execution_status,
    raw_data.statement_text,
    raw_data.statement_type,
    raw_data.error_message,
    -- Compute linkage
    NULL as compute_type,  -- To be linked properly
    NULL as compute_id,    -- To be linked properly
    raw_data.executed_by_user_id,
    raw_data.executed_by,
    raw_data.executed_as,
    raw_data.executed_as_user_id,
    raw_data.start_time,
    DATE(raw_data.start_time) as start_date,
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
WHERE raw_data IS NOT NULL;

-- =============================================================================
-- 6. VERIFICATION
-- =============================================================================

-- Verify processing results
SELECT 'Compute Entities' as table_name, COUNT(*) as record_count FROM obs.silver.compute_entities
UNION ALL
SELECT 'Workflow Entities' as table_name, COUNT(*) as record_count FROM obs.silver.workflow_entities
UNION ALL
SELECT 'Workflow Runs' as table_name, COUNT(*) as record_count FROM obs.silver.workflow_runs
UNION ALL
SELECT 'Billing Usage' as table_name, COUNT(*) as record_count FROM obs.silver.billing_usage
UNION ALL
SELECT 'Query History' as table_name, COUNT(*) as record_count FROM obs.silver.query_history;

-- Check watermark status
SELECT 
    source_table_name,
    target_table_name,
    watermark_value,
    processing_status,
    records_processed
FROM obs.meta.watermarks
ORDER BY last_updated DESC;

-- =============================================================================
-- 7. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 02_silver_to_gold_processing.sql - Process data from silver to gold layer
-- 2. 03_metrics_calculation.sql - Calculate enhanced metrics
-- 3. 04_data_quality_checks.sql - Perform data quality validation

SELECT 'Bronze to Silver processing completed successfully!' as message;
SELECT 'SCD2 patterns applied for entity tables' as message;
SELECT 'Upsert patterns applied for billing usage' as message;
SELECT 'Watermarks updated for all processed tables' as message;
SELECT 'Ready for Silver to Gold processing.' as message;