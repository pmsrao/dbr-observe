-- =============================================================================
-- Databricks Observability Platform - Bronze to Silver Processing
-- =============================================================================
-- Purpose: Process data from bronze to silver layer with SCD2 patterns
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. COMPUTE ENTITIES PROCESSING
-- =============================================================================

-- Process compute entities with SCD2 pattern
CALL obs.meta.merge_compute_entities_scd2();

-- Update watermark after successful processing
SELECT obs.meta.update_watermark(
    'system.compute.clusters',
    'obs.silver.compute_entities',
    'change_time',
    CURRENT_TIMESTAMP(),
    (SELECT COUNT(*) FROM obs.silver.compute_entities_staging),
    0,  -- processing_duration_ms (to be calculated)
    'system'
);

-- =============================================================================
-- 2. WORKFLOW ENTITIES PROCESSING
-- =============================================================================

-- Process workflow entities with SCD2 pattern
CALL obs.meta.merge_workflow_entities_scd2();

-- Update watermark after successful processing
SELECT obs.meta.update_watermark(
    'system.lakeflow.jobs',
    'obs.silver.workflow_entities',
    'change_time',
    CURRENT_TIMESTAMP(),
    (SELECT COUNT(*) FROM obs.silver.workflow_entities_staging),
    0,  -- processing_duration_ms (to be calculated)
    'system'
);

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

-- Update watermark after successful processing
SELECT obs.meta.update_watermark(
    'system.lakeflow.job_run_timeline',
    'obs.silver.workflow_runs',
    'start_time',
    CURRENT_TIMESTAMP(),
    (SELECT COUNT(*) FROM obs.silver.workflow_runs_staging),
    0,  -- processing_duration_ms (to be calculated)
    'system'
);

-- =============================================================================
-- 4. BILLING USAGE PROCESSING
-- =============================================================================

-- Process billing usage with upsert pattern
MERGE INTO obs.silver.billing_usage AS target
USING (
    SELECT 
        record_id,
        workspace_id,
        sku_name,
        cloud,
        usage_start_time,
        usage_end_time,
        usage_date,
        usage_unit,
        usage_quantity,
        usage_type,
        custom_tags,
        usage_metadata,
        identity_metadata,
        record_type,
        ingestion_date,
        billing_origin_product,
        product_features,
        processing_timestamp
    FROM obs.silver.billing_usage_staging
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

-- Update watermark after successful processing
SELECT obs.meta.update_watermark(
    'system.billing.usage',
    'obs.silver.billing_usage',
    'usage_start_time',
    CURRENT_TIMESTAMP(),
    (SELECT COUNT(*) FROM obs.silver.billing_usage_staging),
    0,  -- processing_duration_ms (to be calculated)
    'system'
);

-- =============================================================================
-- 5. QUERY HISTORY PROCESSING
-- =============================================================================

-- Process query history
INSERT INTO obs.silver.query_history
SELECT 
    workspace_id,
    statement_id,
    session_id,
    execution_status,
    statement_text,
    statement_type,
    error_message,
    executed_by_user_id,
    executed_by,
    executed_as,
    executed_as_user_id,
    start_time,
    end_time,
    total_duration_ms,
    waiting_for_compute_duration_ms,
    waiting_at_capacity_duration_ms,
    execution_duration_ms,
    compilation_duration_ms,
    total_task_duration_ms,
    result_fetch_duration_ms,
    read_partitions,
    pruned_files,
    read_files,
    read_rows,
    produced_rows,
    read_bytes,
    read_io_cache_percent,
    spilled_local_bytes,
    written_bytes,
    written_rows,
    written_files,
    shuffle_read_bytes,
    from_result_cache,
    cache_origin_statement_id,
    client_application,
    client_driver,
    query_source,
    query_parameters,
    processing_timestamp
FROM obs.silver.query_history_staging;

-- Update watermark after successful processing
SELECT obs.meta.update_watermark(
    'system.query.history',
    'obs.silver.query_history',
    'start_time',
    CURRENT_TIMESTAMP(),
    (SELECT COUNT(*) FROM obs.silver.query_history_staging),
    0,  -- processing_duration_ms (to be calculated)
    'system'
);

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

PRINT 'Bronze to Silver processing completed successfully!';
PRINT 'SCD2 patterns applied for entity tables';
PRINT 'Upsert patterns applied for billing usage';
PRINT 'Watermarks updated for all processed tables';
PRINT 'Ready for Silver to Gold processing.';
