-- =============================================================================
-- Databricks Observability Platform - Metrics Calculation
-- =============================================================================
-- Purpose: Calculate enhanced metrics for runtime, performance, and cost analysis
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. WORKFLOW RUNS METRICS CALCULATION
-- =============================================================================

-- Update workflow runs with calculated metrics
UPDATE obs.silver.workflow_runs
SET 
    -- Resource Utilization Metrics
    cpu_utilization_percent = (
        SELECT AVG(cpu_user_percent + cpu_system_percent)
        FROM obs.silver.node_usage_minutely n
        WHERE n.cluster_id = obs.silver.workflow_runs.workflow_id
          AND n.start_time >= obs.silver.workflow_runs.start_time
          AND n.start_time <= obs.silver.workflow_runs.end_time
    ),
    memory_utilization_percent = (
        SELECT AVG(mem_used_percent)
        FROM obs.silver.node_usage_minutely n
        WHERE n.cluster_id = obs.silver.workflow_runs.workflow_id
          AND n.start_time >= obs.silver.workflow_runs.start_time
          AND n.start_time <= obs.silver.workflow_runs.end_time
    ),
    disk_utilization_percent = (
        SELECT (1 - AVG(disk_free_percent)) * 100
        FROM obs.silver.node_usage_minutely n
        WHERE n.cluster_id = obs.silver.workflow_runs.workflow_id
          AND n.start_time >= obs.silver.workflow_runs.start_time
          AND n.start_time <= obs.silver.workflow_runs.end_time
    ),
    network_utilization_percent = (
        SELECT (SUM(network_sent_bytes) + SUM(network_received_bytes)) / (duration_ms / 1000.0)
        FROM obs.silver.node_usage_minutely n
        WHERE n.cluster_id = obs.silver.workflow_runs.workflow_id
          AND n.start_time >= obs.silver.workflow_runs.start_time
          AND n.start_time <= obs.silver.workflow_runs.end_time
    ),
    
    -- Performance Metrics
    records_processed = (
        SELECT SUM(read_rows)
        FROM obs.silver.query_history q
        WHERE q.workspace_id = obs.silver.workflow_runs.workspace_id
          AND q.start_time >= obs.silver.workflow_runs.start_time
          AND q.start_time <= obs.silver.workflow_runs.end_time
    ),
    data_volume_bytes = (
        SELECT SUM(read_bytes)
        FROM obs.silver.query_history q
        WHERE q.workspace_id = obs.silver.workflow_runs.workspace_id
          AND q.start_time >= obs.silver.workflow_runs.start_time
          AND q.start_time <= obs.silver.workflow_runs.end_time
    ),
    data_volume_mb = (
        SELECT SUM(read_bytes) / (1024.0 * 1024.0)
        FROM obs.silver.query_history q
        WHERE q.workspace_id = obs.silver.workflow_runs.workspace_id
          AND q.start_time >= obs.silver.workflow_runs.start_time
          AND q.start_time <= obs.silver.workflow_runs.end_time
    ),
    throughput_records_per_second = (
        CASE 
            WHEN duration_ms > 0 THEN records_processed / (duration_ms / 1000.0)
            ELSE NULL
        END
    ),
    throughput_mb_per_second = (
        CASE 
            WHEN duration_ms > 0 THEN data_volume_mb / (duration_ms / 1000.0)
            ELSE NULL
        END
    ),
    execution_efficiency = (
        CASE 
            WHEN duration_ms > 0 THEN (execution_duration_ms / duration_ms) * 100
            ELSE NULL
        END
    ),
    resource_efficiency = (
        (cpu_utilization_percent + memory_utilization_percent) / 2
    ),
    
    -- Error and Retry Metrics
    retry_count = (
        SELECT COUNT(*) - 1
        FROM obs.silver.workflow_runs wr2
        WHERE wr2.workflow_id = obs.silver.workflow_runs.workflow_id
          AND wr2.start_time < obs.silver.workflow_runs.start_time
          AND wr2.result_state = 'FAILED'
    ),
    max_retry_count = 3,  -- Default value
    error_message = (
        CASE 
            WHEN result_state = 'FAILED' THEN 'Workflow execution failed'
            ELSE NULL
        END
    ),
    error_category = (
        CASE 
            WHEN result_state = 'FAILED' AND duration_ms > 3600000 THEN 'TIMEOUT'
            WHEN result_state = 'FAILED' AND error_message LIKE '%memory%' THEN 'MEMORY'
            WHEN result_state = 'FAILED' AND error_message LIKE '%permission%' THEN 'PERMISSION'
            WHEN result_state = 'FAILED' AND error_message LIKE '%connection%' THEN 'NETWORK'
            WHEN result_state = 'FAILED' THEN 'UNKNOWN'
            ELSE NULL
        END
    ),
    error_severity = (
        CASE
            WHEN retry_count > 3 THEN 'HIGH'
            WHEN retry_count > 1 THEN 'MEDIUM'
            WHEN retry_count = 1 THEN 'LOW'
            ELSE 'NONE'
        END
    ),
    
    -- Cost and Billing Metrics
    dbu_consumed = (
        SELECT SUM(usage_quantity)
        FROM obs.silver.billing_usage b
        WHERE b.workspace_id = obs.silver.workflow_runs.workspace_id
          AND b.usage_start_time >= obs.silver.workflow_runs.start_time
          AND b.usage_start_time <= obs.silver.workflow_runs.end_time
          AND b.sku_name LIKE '%DBU%'
    ),
    dbu_per_hour = (
        CASE 
            WHEN duration_ms > 0 THEN dbu_consumed / (duration_ms / 3600000.0)
            ELSE NULL
        END
    ),
    cost_usd = (
        SELECT SUM(usage_quantity * COALESCE(lp.list_price, 0))
        FROM obs.silver.billing_usage b
        LEFT JOIN obs.silver.billing_list_prices lp ON b.sku_name = lp.sku_name AND b.cloud = lp.cloud
        WHERE b.workspace_id = obs.silver.workflow_runs.workspace_id
          AND b.usage_start_time >= obs.silver.workflow_runs.start_time
          AND b.usage_start_time <= obs.silver.workflow_runs.end_time
    ),
    cost_per_hour_usd = (
        CASE 
            WHEN duration_ms > 0 THEN cost_usd / (duration_ms / 3600000.0)
            ELSE NULL
        END
    ),
    cost_per_record_usd = (
        CASE 
            WHEN records_processed > 0 THEN cost_usd / records_processed
            ELSE NULL
        END
    ),
    cost_efficiency_score = (
        CASE 
            WHEN cost_usd > 0 THEN records_processed / cost_usd
            ELSE NULL
        END
    ),
    
    -- Quality and Reliability Metrics
    success_rate = (
        CASE 
            WHEN result_state = 'SUCCEEDED' THEN 1.0
            WHEN result_state = 'FAILED' THEN 0.0
            ELSE NULL
        END
    ),
    failure_rate = (
        CASE 
            WHEN result_state = 'FAILED' THEN 1.0
            WHEN result_state = 'SUCCEEDED' THEN 0.0
            ELSE NULL
        END
    ),
    reliability_score = (
        success_rate - (failure_rate * 2)
    ),
    sla_compliance = (
        CASE 
            WHEN duration_ms <= 3600000 THEN true  -- 1 hour SLA
            ELSE false
        END
    )
WHERE processing_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 day';

-- =============================================================================
-- 2. JOB TASK RUNS METRICS CALCULATION
-- =============================================================================

-- Update job task runs with calculated metrics
UPDATE obs.silver.job_task_runs
SET 
    -- Resource Utilization Metrics
    cpu_utilization_percent = (
        SELECT AVG(cpu_user_percent + cpu_system_percent)
        FROM obs.silver.node_usage_minutely n
        WHERE n.cluster_id = obs.silver.job_task_runs.job_id
          AND n.start_time >= obs.silver.job_task_runs.start_time
          AND n.start_time <= obs.silver.job_task_runs.end_time
    ),
    memory_utilization_percent = (
        SELECT AVG(mem_used_percent)
        FROM obs.silver.node_usage_minutely n
        WHERE n.cluster_id = obs.silver.job_task_runs.job_id
          AND n.start_time >= obs.silver.job_task_runs.start_time
          AND n.start_time <= obs.silver.job_task_runs.end_time
    ),
    
    -- Performance Metrics
    records_processed = (
        SELECT SUM(read_rows)
        FROM obs.silver.query_history q
        WHERE q.workspace_id = obs.silver.job_task_runs.workspace_id
          AND q.start_time >= obs.silver.job_task_runs.start_time
          AND q.start_time <= obs.silver.job_task_runs.end_time
    ),
    data_volume_bytes = (
        SELECT SUM(read_bytes)
        FROM obs.silver.query_history q
        WHERE q.workspace_id = obs.silver.job_task_runs.workspace_id
          AND q.start_time >= obs.silver.job_task_runs.start_time
          AND q.start_time <= obs.silver.job_task_runs.end_time
    ),
    data_volume_mb = (
        SELECT SUM(read_bytes) / (1024.0 * 1024.0)
        FROM obs.silver.query_history q
        WHERE q.workspace_id = obs.silver.job_task_runs.workspace_id
          AND q.start_time >= obs.silver.job_task_runs.start_time
          AND q.start_time <= obs.silver.job_task_runs.end_time
    ),
    throughput_records_per_second = (
        CASE 
            WHEN duration_ms > 0 THEN records_processed / (duration_ms / 1000.0)
            ELSE NULL
        END
    ),
    throughput_mb_per_second = (
        CASE 
            WHEN duration_ms > 0 THEN data_volume_mb / (duration_ms / 1000.0)
            ELSE NULL
        END
    ),
    execution_efficiency = (
        CASE 
            WHEN duration_ms > 0 THEN (execution_duration_ms / duration_ms) * 100
            ELSE NULL
        END
    ),
    
    -- Cost and Billing Metrics
    dbu_consumed = (
        SELECT SUM(usage_quantity)
        FROM obs.silver.billing_usage b
        WHERE b.workspace_id = obs.silver.job_task_runs.workspace_id
          AND b.usage_start_time >= obs.silver.job_task_runs.start_time
          AND b.usage_start_time <= obs.silver.job_task_runs.end_time
          AND b.sku_name LIKE '%DBU%'
    ),
    cost_usd = (
        SELECT SUM(usage_quantity * COALESCE(lp.list_price, 0))
        FROM obs.silver.billing_usage b
        LEFT JOIN obs.silver.billing_list_prices lp ON b.sku_name = lp.sku_name AND b.cloud = lp.cloud
        WHERE b.workspace_id = obs.silver.job_task_runs.workspace_id
          AND b.usage_start_time >= obs.silver.job_task_runs.start_time
          AND b.usage_start_time <= obs.silver.job_task_runs.end_time
    ),
    cost_per_record_usd = (
        CASE 
            WHEN records_processed > 0 THEN cost_usd / records_processed
            ELSE NULL
        END
    ),
    
    -- Quality and Reliability Metrics
    success_rate = (
        CASE 
            WHEN result_state = 'SUCCEEDED' THEN 1.0
            WHEN result_state = 'FAILED' THEN 0.0
            ELSE NULL
        END
    ),
    failure_rate = (
        CASE 
            WHEN result_state = 'FAILED' THEN 1.0
            WHEN result_state = 'SUCCEEDED' THEN 0.0
            ELSE NULL
        END
    ),
    reliability_score = (
        success_rate - (failure_rate * 2)
    )
WHERE processing_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 day';

-- =============================================================================
-- 3. GOLD FACT TABLES METRICS UPDATE
-- =============================================================================

-- Update fct_billing_usage_hourly with calculated costs
UPDATE obs.gold.fct_billing_usage_hourly
SET 
    list_price = (
        SELECT lp.list_price
        FROM obs.gold.dim_sku ds
        LEFT JOIN obs.silver.billing_list_prices lp ON ds.sku_name = lp.sku_name AND ds.cloud = lp.cloud
        WHERE ds.sku_sk = obs.gold.fct_billing_usage_hourly.sku_sk
    ),
    total_cost = (
        usage_quantity * list_price
    )
WHERE list_price IS NULL;

-- Update fct_workflow_runs with calculated success rates
UPDATE obs.gold.fct_workflow_runs
SET 
    success_rate = (
        SELECT AVG(
            CASE 
                WHEN result_state = 'SUCCEEDED' THEN 1.0
                WHEN result_state = 'FAILED' THEN 0.0
                ELSE NULL
            END
        )
        FROM obs.silver.workflow_runs wr
        WHERE wr.workspace_id = obs.gold.fct_workflow_runs.workspace_id
          AND wr.workflow_id = obs.gold.fct_workflow_runs.workflow_id
    )
WHERE success_rate IS NULL;

-- =============================================================================
-- 4. VERIFICATION
-- =============================================================================

-- Verify metrics calculation
SELECT 
    'Workflow Runs with Metrics' as metric_name,
    COUNT(*) as record_count
FROM obs.silver.workflow_runs
WHERE cpu_utilization_percent IS NOT NULL
  AND memory_utilization_percent IS NOT NULL
  AND cost_usd IS NOT NULL;

SELECT 
    'Job Task Runs with Metrics' as metric_name,
    COUNT(*) as record_count
FROM obs.silver.job_task_runs
WHERE cpu_utilization_percent IS NOT NULL
  AND memory_utilization_percent IS NOT NULL
  AND cost_usd IS NOT NULL;

SELECT 
    'Billing Usage with Costs' as metric_name,
    COUNT(*) as record_count
FROM obs.gold.fct_billing_usage_hourly
WHERE total_cost IS NOT NULL;

-- =============================================================================
-- 5. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 04_data_quality_checks.sql - Perform data quality validation
-- 2. 05_cost_calculation.sql - Calculate costs and showback metrics
-- 3. 06_operational_metrics.sql - Calculate operational metrics

PRINT 'Metrics calculation completed successfully!';
PRINT 'Enhanced runtime metrics calculated for workflow runs';
PRINT 'Performance metrics calculated for job task runs';
PRINT 'Cost metrics calculated for billing usage';
PRINT 'Ready for data quality checks.';
