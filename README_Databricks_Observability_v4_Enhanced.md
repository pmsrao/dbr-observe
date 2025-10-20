# Databricks Observability Platform — Enhanced Design (v4)

**Author**: Data Platform Architect  
**Version**: 4.0  
**Date**: December 2024

> **What's new in v4**
> - Enhanced SCD2 implementation with proper audit columns and late-arriving data handling
> - Complete schema definitions with all missing fields from system tables
> - **Comprehensive runtime metrics** for workflow runs and job task runs
> - **Resource utilization tracking** (CPU, memory, disk, network, GPU)
> - **Performance metrics** (throughput, efficiency, reliability scores)
> - **Cost attribution** with detailed billing and efficiency metrics
> - **Error analysis** with categorization and severity classification
> - Tag extraction strategy without dimensional tables
> - Query history integration for comprehensive observability
> - Audit log processing for governance and security
> - Operational metrics and data quality tracking
> - Watermark-based incremental processing patterns
> - Showback-focused cost allocation methodology

---

## 1) Architecture Overview

### 1.1 Design Principles
- **Unified Silver Layer**: Conform dimensions early to ensure consistent keys across all facts
- **SCD Type 2**: Full historical tracking for compute and workflow entities
- **Incremental Processing**: Watermark-based delta loads with 2-day lookback
- **Showback Focus**: Cost visibility and resource optimization insights
- **Governance Ready**: Audit trails and data quality metrics

### 1.2 Data Flow
```
System Tables → Bronze → Silver (SCD2) → Gold (Analytics) → Consumption
```

---

## 2) Enhanced Schema Definitions

### 2.1 Bronze Layer (Raw System Tables)
**Purpose**: Direct ingestion from system tables with minimal transformation

**Tables**:
- `bronze.system_billing_usage`
- `bronze.system_billing_list_prices`
- `bronze.system_compute_clusters`
- `bronze.system_compute_warehouses`
- `bronze.system_compute_node_types`
- `bronze.system_compute_node_timeline`
- `bronze.system_compute_warehouse_events`
- `bronze.system_lakeflow_jobs`
- `bronze.system_lakeflow_job_tasks`
- `bronze.system_lakeflow_job_run_timeline`
- `bronze.system_lakeflow_job_task_run_timeline`
- `bronze.system_lakeflow_pipelines`
- `bronze.system_lakeflow_pipeline_update_timeline`
- `bronze.system_query_history`
- `bronze.system_storage_ops`
- `bronze.system_access_audit`

**Common Bronze Columns**:
```sql
-- All bronze tables include
ingestion_timestamp TIMESTAMP,
source_file STRING,
record_hash STRING,  -- SHA2 hash for deduplication
is_deleted BOOLEAN DEFAULT false,
raw_data STRUCT<*>  -- Complete original record
```

### 2.2 Silver Layer (Curated & Unified)

#### 2.2.1 Enhanced SCD2 Pattern
**Standard SCD2 Columns for Entity Tables**:
```sql
-- Natural Key Components
workspace_id STRING,
entity_type STRING,  -- 'CLUSTER', 'WAREHOUSE', 'JOB', 'PIPELINE'
entity_id STRING,

-- SCD2 Columns
effective_start_ts TIMESTAMP,
effective_end_ts TIMESTAMP,
is_current BOOLEAN,

-- Audit Columns
record_hash STRING,  -- For change detection
dw_created_ts TIMESTAMP,
dw_updated_ts TIMESTAMP,
processing_timestamp TIMESTAMP,

-- Business Attributes (varies by entity)
-- ... entity-specific columns ...

-- Raw Data Preservation
_raw STRUCT<source STRING, raw_data STRUCT<*>>
```

#### 2.2.2 Unified Compute Entities (SCD2)
```sql
CREATE TABLE silver.compute_entities (
  -- Natural Key
  workspace_id STRING,
  compute_type STRING,  -- 'CLUSTER' | 'WAREHOUSE'
  compute_id STRING,
  
  -- SCD2 Columns
  effective_start_ts TIMESTAMP,
  effective_end_ts TIMESTAMP,
  is_current BOOLEAN,
  
  -- Audit
  record_hash STRING,
  dw_created_ts TIMESTAMP,
  dw_updated_ts TIMESTAMP,
  processing_timestamp TIMESTAMP,
  
  -- Business Attributes
  name STRING,
  owner STRING,
  driver_node_type STRING,
  worker_node_type STRING,
  worker_count BIGINT,
  min_autoscale_workers BIGINT,
  max_autoscale_workers BIGINT,
  auto_termination_minutes BIGINT,
  enable_elastic_disk BOOLEAN,
  data_security_mode STRING,
  policy_id STRING,
  dbr_version STRING,
  cluster_source STRING,
  
  -- Warehouse-specific (nullable for clusters)
  warehouse_type STRING,
  warehouse_size STRING,
  warehouse_channel STRING,
  min_clusters INT,
  max_clusters INT,
  auto_stop_minutes INT,
  
  -- Tags (extracted as key-value pairs)
  tags MAP<STRING, STRING>,
  
  -- Lifecycle
  create_time TIMESTAMP,
  delete_time TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, compute_type)
CLUSTERED BY (compute_id) INTO 8 BUCKETS;
```

#### 2.2.3 Unified Workflow Entities (SCD2)
```sql
CREATE TABLE silver.workflow_entities (
  -- Natural Key
  workspace_id STRING,
  workflow_type STRING,  -- 'JOB' | 'PIPELINE'
  workflow_id STRING,
  
  -- SCD2 Columns
  effective_start_ts TIMESTAMP,
  effective_end_ts TIMESTAMP,
  is_current BOOLEAN,
  
  -- Audit
  record_hash STRING,
  dw_created_ts TIMESTAMP,
  dw_updated_ts TIMESTAMP,
  processing_timestamp TIMESTAMP,
  
  -- Business Attributes
  name STRING,
  description STRING,
  creator_id STRING,
  run_as STRING,
  
  -- Pipeline-specific (nullable for jobs)
  pipeline_type STRING,
  settings MAP<STRING, STRING>,
  
  -- Tags
  tags MAP<STRING, STRING>,
  
  -- Lifecycle
  create_time TIMESTAMP,
  delete_time TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, workflow_type)
CLUSTERED BY (workflow_id) INTO 8 BUCKETS;
```

#### 2.2.4 Workflow Runs (Unified)
```sql
CREATE TABLE silver.workflow_runs (
  workspace_id STRING,
  workflow_type STRING,  -- 'JOB' | 'PIPELINE'
  workflow_id STRING,
  run_id STRING,
  
  -- Unified Naming
  workflow_name STRING,  -- Unified job_name or pipeline_name
  
  -- Timing
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_ms BIGINT,
  
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
  processing_timestamp TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, workflow_type, date(start_time))
CLUSTERED BY (workflow_id) INTO 8 BUCKETS;
```

#### 2.2.5 Job Task Runs
```sql
CREATE TABLE silver.job_task_runs (
  workspace_id STRING,
  job_id STRING,
  task_key STRING,
  job_task_run_id STRING,
  
  -- Timing
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_ms BIGINT,
  
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
  processing_timestamp TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, date(start_time))
CLUSTERED BY (job_id) INTO 8 BUCKETS;
```

#### 2.2.6 Node Usage (Minutely)
```sql
CREATE TABLE silver.node_usage_minutely (
  workspace_id STRING,
  cluster_id STRING,
  instance_id STRING,
  
  -- Timing
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  
  -- Node info
  driver BOOLEAN,
  
  -- Metrics
  cpu_user_percent DOUBLE,
  cpu_system_percent DOUBLE,
  cpu_wait_percent DOUBLE,
  mem_used_percent DOUBLE,
  mem_swap_percent DOUBLE,
  network_sent_bytes BIGINT,
  network_received_bytes BIGINT,
  disk_free_bytes_per_mount_point MAP<STRING, BIGINT>,
  
  -- Audit
  processing_timestamp TIMESTAMP
)
PARTITIONED BY (workspace_id, date(start_time))
CLUSTERED BY (cluster_id) INTO 8 BUCKETS;
```

#### 2.2.7 Warehouse Events
```sql
CREATE TABLE silver.warehouse_events (
  workspace_id STRING,
  warehouse_id STRING,
  
  -- Event details
  event_type STRING,
  event_time TIMESTAMP,
  cluster_count INT,
  
  -- Audit
  processing_timestamp TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, date(event_time))
CLUSTERED BY (warehouse_id) INTO 8 BUCKETS;
```

#### 2.2.8 Billing Usage (Enhanced)
```sql
CREATE TABLE silver.billing_usage (
  -- Natural Key
  record_id STRING,
  workspace_id STRING,
  
  -- Billing details
  sku_name STRING,
  cloud STRING,
  usage_start_time TIMESTAMP,
  usage_end_time TIMESTAMP,
  usage_date DATE,
  usage_unit STRING,
  usage_quantity DECIMAL(18,6),
  usage_type STRING,
  
  -- Metadata
  custom_tags MAP<STRING, STRING>,
  usage_metadata STRUCT<
    cluster_id STRING,
    job_id STRING,
    warehouse_id STRING,
    instance_pool_id STRING,
    node_type STRING,
    job_run_id STRING,
    notebook_id STRING,
    dlt_pipeline_id STRING,
    endpoint_name STRING,
    endpoint_id STRING,
    dlt_update_id STRING,
    dlt_maintenance_id STRING,
    metastore_id STRING,
    job_name STRING,
    notebook_path STRING,
    central_clean_room_id STRING,
    source_region STRING,
    destination_region STRING,
    app_id STRING,
    app_name STRING,
    budget_policy_id STRING,
    base_environment_id STRING
  >,
  identity_metadata STRUCT<
    run_as STRING,
    owned_by STRING,
    created_by STRING
  >,
  
  -- Record management
  record_type STRING,  -- ORIGINAL, RETRACTION, RESTATEMENT
  ingestion_date DATE,
  billing_origin_product STRING,
  product_features STRUCT<
    serving_type STRING,
    offering_type STRING,
    performance_target STRING,
    networking STRUCT<connectivity_type STRING>
  >,
  
  -- Audit
  processing_timestamp TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, usage_date)
CLUSTERED BY (sku_name) INTO 8 BUCKETS;
```

#### 2.2.9 Query History (Enhanced)
```sql
CREATE TABLE silver.query_history (
  workspace_id STRING,
  statement_id STRING,
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
  start_time TIMESTAMP,
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
  processing_timestamp TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, date(start_time))
CLUSTERED BY (compute_id) INTO 8 BUCKETS;
```

#### 2.2.10 Audit Log
```sql
CREATE TABLE silver.audit_log (
  workspace_id STRING,
  event_id STRING,
  
  -- Event details
  event_time TIMESTAMP,
  event_type STRING,
  action_name STRING,
  resource_type STRING,
  resource_id STRING,
  
  -- User context
  user_id STRING,
  user_name STRING,
  service_principal_name STRING,
  
  -- Request context
  request_id STRING,
  session_id STRING,
  ip_address STRING,
  user_agent STRING,
  
  -- Result
  result STRING,
  error_message STRING,
  
  -- Additional context
  request_params MAP<STRING, STRING>,
  response STRING,
  
  -- Audit
  processing_timestamp TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, date(event_time))
CLUSTERED BY (event_type) INTO 8 BUCKETS;
```

#### 2.2.11 Storage Operations
```sql
CREATE TABLE silver.storage_ops (
  workspace_id STRING,
  operation_id STRING,
  
  -- Operation details
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  operation_type STRING,  -- ANALYZE, OPTIMIZE, VACUUM
  object_type STRING,
  object_id STRING,
  object_name STRING,
  
  -- Impact metrics
  rows_affected BIGINT,
  bytes_optimized BIGINT,
  status STRING,
  
  -- Audit
  processing_timestamp TIMESTAMP,
  
  -- Raw preservation
  _raw STRUCT<source STRING, raw_data STRUCT<*>>
)
PARTITIONED BY (workspace_id, date(start_time))
CLUSTERED BY (object_id) INTO 8 BUCKETS;
```

### 2.3 Enhanced Metrics Calculation

**Runtime Metrics Derivation**:
```sql
-- Resource Utilization Calculation
cpu_utilization_percent = AVG(cpu_user_percent + cpu_system_percent) FROM node_timeline
memory_utilization_percent = AVG(mem_used_percent) FROM node_timeline
disk_utilization_percent = (1 - AVG(disk_free_percent)) * 100
network_utilization_percent = (network_sent_bytes + network_received_bytes) / duration_seconds

-- Performance Metrics Calculation
throughput_records_per_second = records_processed / (duration_ms / 1000.0)
throughput_mb_per_second = data_volume_mb / (duration_ms / 1000.0)
execution_efficiency = (execution_duration_ms / total_duration_ms) * 100
resource_efficiency = (cpu_utilization_percent + memory_utilization_percent) / 2

-- Cost Metrics Calculation
dbu_per_hour = dbu_consumed / (duration_ms / 3600000.0)
cost_per_hour_usd = cost_usd / (duration_ms / 3600000.0)
cost_per_record_usd = cost_usd / records_processed
cost_efficiency_score = records_processed / cost_usd

-- Quality Metrics Calculation
success_rate = COUNT(CASE WHEN result_state = 'SUCCEEDED' THEN 1 END) / COUNT(*) * 100
failure_rate = COUNT(CASE WHEN result_state = 'FAILED' THEN 1 END) / COUNT(*) * 100
reliability_score = success_rate - (failure_rate * 2)
sla_compliance = CASE WHEN duration_ms <= sla_threshold_ms THEN true ELSE false END

-- Error Classification
error_category = CASE 
  WHEN error_message LIKE '%timeout%' THEN 'TIMEOUT'
  WHEN error_message LIKE '%memory%' THEN 'MEMORY'
  WHEN error_message LIKE '%permission%' THEN 'PERMISSION'
  WHEN error_message LIKE '%connection%' THEN 'NETWORK'
  ELSE 'UNKNOWN'
END

error_severity = CASE
  WHEN retry_count > 3 THEN 'HIGH'
  WHEN retry_count > 1 THEN 'MEDIUM'
  WHEN retry_count = 1 THEN 'LOW'
  ELSE 'NONE'
END
```

### 2.4 Tag Extraction Strategy

**Tag Categories for Showback**:
```sql
-- Extract from tags and custom_tags
cost_center STRING,      -- From 'cost_center' or 'CostCenter' tag
business_unit STRING,    -- From 'business_unit' or 'BusinessUnit' tag  
department STRING,       -- From 'department' or 'Department' tag
data_product STRING,     -- From 'data_product' or 'DataProduct' tag
environment STRING,      -- From 'env' or 'environment' tag
team STRING,            -- From 'team' or 'Team' tag
project STRING,         -- From 'project' or 'Project' tag
```

**Tag Extraction Logic**:
```sql
-- Standardized tag extraction function
CREATE FUNCTION extract_tag(tags MAP<STRING, STRING>, tag_name STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
  COALESCE(
    tags[tag_name],
    tags[UPPER(tag_name)],
    tags[LOWER(tag_name)],
    tags[INITCAP(tag_name)],
    'Unknown'
  )
$$;
```

### 2.4 Gold Layer (Analytics)

#### 2.4.1 Dimensions
```sql
-- Thin views over Silver entities
CREATE VIEW gold.dim_compute AS
SELECT 
  SHA2(CONCAT(compute_type, '|', workspace_id, '|', compute_id), 256) AS compute_sk,
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
  is_current
FROM silver.compute_entities
WHERE is_current = true;

CREATE VIEW gold.dim_workflow AS
SELECT 
  SHA2(CONCAT(workflow_type, '|', workspace_id, '|', workflow_id), 256) AS workflow_sk,
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
  is_current
FROM silver.workflow_entities
WHERE is_current = true;

CREATE VIEW gold.dim_user AS
SELECT DISTINCT
  SHA2(COALESCE(user_id, user_name), 256) AS user_sk,
  user_id,
  user_name,
  service_principal_name,
  'USER' AS identity_type
FROM (
  SELECT executed_by_user_id AS user_id, executed_by AS user_name, NULL AS service_principal_name FROM silver.query_history
  UNION ALL
  SELECT NULL AS user_id, run_as AS user_name, run_as AS service_principal_name FROM silver.workflow_entities
  UNION ALL
  SELECT NULL AS user_id, owned_by AS user_name, NULL AS service_principal_name FROM silver.compute_entities
  UNION ALL
  SELECT user_id, user_name, service_principal_name FROM silver.audit_log
) users
WHERE user_id IS NOT NULL OR user_name IS NOT NULL;

CREATE VIEW gold.dim_sku AS
SELECT DISTINCT
  SHA2(CONCAT(sku_name, '|', cloud), 256) AS sku_sk,
  sku_name,
  cloud,
  usage_unit,
  usage_type
FROM silver.billing_usage;

CREATE VIEW gold.dim_node_type AS
SELECT DISTINCT
  SHA2(node_type, 256) AS node_type_sk,
  node_type,
  core_count,
  memory_mb,
  gpu_count
FROM bronze.system_compute_node_types;
```

#### 2.4.2 Fact Tables

**Billing Usage Hourly**:
```sql
CREATE TABLE gold.fct_billing_usage_hourly (
  -- Keys
  billing_sk STRING,  -- SHA2 hash of natural key
  compute_sk STRING,
  workflow_sk STRING,
  user_sk STRING,
  sku_sk STRING,
  
  -- Time
  usage_date DATE,
  usage_hour INT,
  usage_start_time TIMESTAMP,
  usage_end_time TIMESTAMP,
  
  -- Billing
  workspace_id STRING,
  sku_name STRING,
  cloud STRING,
  usage_quantity DECIMAL(18,6),
  usage_unit STRING,
  usage_type STRING,
  
  -- Cost (from list prices)
  list_price DECIMAL(18,6),
  total_cost DECIMAL(18,6),
  
  -- Tags (extracted)
  cost_center STRING,
  business_unit STRING,
  department STRING,
  data_product STRING,
  environment STRING,
  team STRING,
  project STRING,
  
  -- Metadata
  record_type STRING,
  billing_origin_product STRING,
  
  -- Audit
  processing_timestamp TIMESTAMP
)
PARTITIONED BY (workspace_id, usage_date)
CLUSTERED BY (sku_name) INTO 8 BUCKETS;
```

**Workflow Runs**:
```sql
CREATE TABLE gold.fct_workflow_runs (
  -- Keys
  workflow_run_sk STRING,
  workflow_sk STRING,
  compute_sk STRING,
  user_sk STRING,
  
  -- Time
  run_date DATE,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_ms BIGINT,
  
  -- Workflow
  workspace_id STRING,
  workflow_type STRING,
  workflow_id STRING,
  run_id STRING,
  
  -- Status
  result_state STRING,
  termination_code STRING,
  
  -- Performance
  success_rate DOUBLE,
  
  -- Tags
  cost_center STRING,
  business_unit STRING,
  department STRING,
  data_product STRING,
  environment STRING,
  team STRING,
  project STRING,
  
  -- Audit
  processing_timestamp TIMESTAMP
)
PARTITIONED BY (workspace_id, run_date)
CLUSTERED BY (workflow_id) INTO 8 BUCKETS;
```

**Query Performance**:
```sql
CREATE TABLE gold.fct_query_performance (
  -- Keys
  query_sk STRING,
  compute_sk STRING,
  user_sk STRING,
  workflow_sk STRING,
  
  -- Time
  query_date DATE,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  total_duration_ms BIGINT,
  
  -- Query details
  workspace_id STRING,
  statement_id STRING,
  statement_type STRING,
  execution_status STRING,
  
  -- Performance metrics
  read_partitions BIGINT,
  read_files BIGINT,
  read_rows BIGINT,
  read_bytes BIGINT,
  written_bytes BIGINT,
  spilled_local_bytes BIGINT,
  shuffle_read_bytes BIGINT,
  
  -- Efficiency metrics
  bytes_per_second DOUBLE,
  rows_per_second DOUBLE,
  cache_hit_ratio DOUBLE,
  
  -- Tags
  cost_center STRING,
  business_unit STRING,
  department STRING,
  data_product STRING,
  environment STRING,
  team STRING,
  project STRING,
  
  -- Audit
  processing_timestamp TIMESTAMP
)
PARTITIONED BY (workspace_id, query_date)
CLUSTERED BY (statement_type) INTO 8 BUCKETS;
```

**Node Usage Hourly**:
```sql
CREATE TABLE gold.fct_node_usage_hourly (
  -- Keys
  node_usage_sk STRING,
  compute_sk STRING,
  
  -- Time
  usage_date DATE,
  usage_hour INT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  
  -- Node details
  workspace_id STRING,
  cluster_id STRING,
  instance_id STRING,
  driver BOOLEAN,
  
  -- Aggregated metrics
  avg_cpu_user_percent DOUBLE,
  avg_cpu_system_percent DOUBLE,
  avg_cpu_wait_percent DOUBLE,
  avg_mem_used_percent DOUBLE,
  avg_mem_swap_percent DOUBLE,
  total_network_sent_bytes BIGINT,
  total_network_received_bytes BIGINT,
  min_disk_free_bytes BIGINT,
  
  -- Tags
  cost_center STRING,
  business_unit STRING,
  department STRING,
  data_product STRING,
  environment STRING,
  team STRING,
  project STRING,
  
  -- Audit
  processing_timestamp TIMESTAMP
)
PARTITIONED BY (workspace_id, usage_date)
CLUSTERED BY (cluster_id) INTO 8 BUCKETS;
```

### 2.5 Operational Metrics

**Data Quality Metrics**:
```sql
CREATE TABLE gold.data_quality_metrics (
  metric_date DATE,
  table_name STRING,
  metric_type STRING,  -- 'completeness', 'timeliness', 'validity', 'consistency'
  metric_value DOUBLE,
  threshold_value DOUBLE,
  status STRING,  -- 'PASS', 'WARN', 'FAIL'
  details STRING,
  processing_timestamp TIMESTAMP
)
PARTITIONED BY (metric_date)
CLUSTERED BY (table_name) INTO 4 BUCKETS;
```

**Pipeline Performance**:
```sql
CREATE TABLE gold.pipeline_performance (
  pipeline_date DATE,
  pipeline_name STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds BIGINT,
  records_processed BIGINT,
  records_failed BIGINT,
  success_rate DOUBLE,
  processing_timestamp TIMESTAMP
)
PARTITIONED BY (pipeline_date)
CLUSTERED BY (pipeline_name) INTO 4 BUCKETS;
```

---

## 3) Processing Patterns

### 3.1 Watermark-Based Incremental Processing

**Enhanced Watermark Management**:
```sql
CREATE TABLE silver.watermarks (
  source_table_name STRING,      -- e.g., 'system.billing.usage'
  target_table_name STRING,      -- e.g., 'silver.billing_usage'
  watermark_column STRING,       -- e.g., 'usage_start_time'
  watermark_value TIMESTAMP,     -- Last processed timestamp
  last_updated TIMESTAMP,        -- When watermark was last updated
  processing_status STRING,      -- 'SUCCESS', 'FAILED', 'IN_PROGRESS'
  error_message STRING,          -- Error details if failed
  records_processed BIGINT,      -- Number of records processed
  processing_duration_ms BIGINT  -- Processing time in milliseconds
);
```

**Enhanced Incremental Load Pattern**:
```sql
-- Example for billing usage
WITH watermark AS (
  SELECT COALESCE(MAX(watermark_value), '1900-01-01'::TIMESTAMP) as last_watermark
  FROM silver.watermarks 
  WHERE source_table_name = 'system.billing.usage' 
    AND target_table_name = 'silver.billing_usage'
    AND watermark_column = 'usage_start_time'
    AND processing_status = 'SUCCESS'
),
new_data AS (
  SELECT *
  FROM bronze.system_billing_usage
  WHERE usage_start_time > (SELECT last_watermark FROM watermark)
    AND usage_start_time >= CURRENT_TIMESTAMP - INTERVAL '2 days'
)
-- Process new_data...

-- Update watermark after successful processing
MERGE INTO silver.watermarks AS target
USING (
  SELECT 
    'system.billing.usage' as source_table_name,
    'silver.billing_usage' as target_table_name,
    'usage_start_time' as watermark_column,
    MAX(usage_start_time) as watermark_value,
    CURRENT_TIMESTAMP as last_updated,
    'SUCCESS' as processing_status,
    NULL as error_message,
    COUNT(*) as records_processed,
    {processing_duration} as processing_duration_ms
  FROM new_data
) AS source
ON target.source_table_name = source.source_table_name 
  AND target.target_table_name = source.target_table_name
  AND target.watermark_column = source.watermark_column
WHEN MATCHED THEN UPDATE SET 
  watermark_value = source.watermark_value,
  last_updated = source.last_updated,
  processing_status = source.processing_status,
  error_message = source.error_message,
  records_processed = source.records_processed,
  processing_duration_ms = source.processing_duration_ms
WHEN NOT MATCHED THEN INSERT VALUES (
  source.source_table_name, source.target_table_name, source.watermark_column,
  source.watermark_value, source.last_updated, source.processing_status,
  source.error_message, source.records_processed, source.processing_duration_ms
);
```

### 3.2 SCD2 Merge Pattern

**Generic SCD2 Merge**:
```sql
MERGE INTO silver.{entity_table} AS target
USING (
  SELECT 
    *,
    SHA2(CONCAT(workspace_id, '|', entity_type, '|', entity_id, '|', 
                COALESCE(name, ''), '|', COALESCE(owner, ''), '|', 
                COALESCE(TO_JSON_STRING(tags), '')), 256) AS record_hash
  FROM silver_stg.{entity_table}_staging
) AS source
ON target.workspace_id = source.workspace_id
  AND target.entity_type = source.entity_type  
  AND target.entity_id = source.entity_id
  AND target.is_current = true
WHEN MATCHED AND target.record_hash != source.record_hash THEN
  UPDATE SET 
    effective_end_ts = source.change_time,
    is_current = false,
    dw_updated_ts = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
  INSERT (workspace_id, entity_type, entity_id, effective_start_ts, effective_end_ts, 
          is_current, record_hash, dw_created_ts, dw_updated_ts, processing_timestamp,
          name, owner, tags, create_time, delete_time, _raw)
  VALUES (source.workspace_id, source.entity_type, source.entity_id, 
          source.change_time, TIMESTAMP('9999-12-31'), true, source.record_hash,
          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
          source.name, source.owner, source.tags, source.create_time, 
          source.delete_time, source._raw);
```

### 3.3 Late-Arriving Data Handling

**Late-Arriving Dimension Changes**:
```sql
-- Check for late-arriving changes in the last 7 days
WITH late_changes AS (
  SELECT *
  FROM bronze.system_compute_clusters
  WHERE change_time < CURRENT_TIMESTAMP - INTERVAL '1 day'
    AND change_time >= CURRENT_TIMESTAMP - INTERVAL '7 days'
)
-- Process late changes with proper SCD2 handling
```

---

## 4) Recommended KPIs and Metrics

### 4.1 Cost & Resource Metrics
- **Total DBU Consumption** by workspace, team, project
- **Cost per Job/Workflow** execution
- **Resource Utilization** (CPU, Memory, Storage)
- **Waste Analysis** (idle clusters, oversized resources)
- **Trend Analysis** (month-over-month cost changes)

### 4.2 Performance Metrics
- **Query Performance** (execution time, throughput)
- **Job Success Rates** by team/project
- **Resource Efficiency** (DBU per output)
- **Cache Hit Ratios** for warehouses
- **Data Processing Velocity**

### 4.3 Governance Metrics
- **User Activity** patterns
- **Security Events** (failed logins, permission changes)
- **Data Access** patterns
- **Compliance** metrics (data lineage, audit trails)

### 4.4 Operational Metrics
- **Pipeline Health** (success rates, processing times)
- **Data Quality** scores
- **System Availability** metrics
- **Alert Response** times

---

## 5) Tag Taxonomy Recommendations

### 5.1 Standard Tag Categories
```yaml
# Cost Allocation Tags
cost_center: "finance", "engineering", "marketing", "sales"
business_unit: "data-platform", "analytics", "ml-ops", "data-engineering"
department: "data-engineering", "business-intelligence", "machine-learning"

# Operational Tags  
environment: "dev", "staging", "prod", "test"
team: "data-eng", "analytics", "ml-team", "platform"
project: "customer-analytics", "recommendation-engine", "data-lake"

# Data Product Tags
data_product: "customer-360", "recommendation-engine", "fraud-detection"
data_domain: "customer", "product", "financial", "operational"
```

### 5.2 Tag Naming Conventions
- **Consistent casing**: Use lowercase with hyphens (`cost-center`)
- **Standardized values**: Define allowed values for each tag
- **Hierarchical structure**: Use dot notation for sub-categories (`team.data-eng`)
- **Validation rules**: Implement tag validation in the pipeline

---

## 6) Data Retention Strategy

### 6.1 Retention Periods
- **Bronze**: 1 year (raw system table data)
- **Silver**: 2 years (curated data with full history)
- **Gold**: 3 years (analytics-ready data)
- **Audit Logs**: 7 years (compliance requirement)

### 6.2 Archival Strategy
```sql
-- Archive old data to cheaper storage
CREATE TABLE silver_archive.compute_entities_2023
AS SELECT * FROM silver.compute_entities 
WHERE effective_start_ts < '2024-01-01';

-- Drop archived data from main tables
DELETE FROM silver.compute_entities 
WHERE effective_start_ts < '2024-01-01';
```

---

## 7) Security and Access Control

### 7.1 Access Patterns
- **System Tables**: Service Principal with minimal required permissions
- **Observability Layer**: Platform team with read/write access
- **Gold Layer**: Business users with read-only access
- **Sensitive Data**: PII masking for user emails, query text

### 7.2 Data Classification
```sql
-- PII masking for sensitive fields
CREATE VIEW gold.query_history_masked AS
SELECT 
  *,
  CASE 
    WHEN executed_by LIKE '%@%' THEN REGEXP_REPLACE(executed_by, '^[^@]+', '***')
    ELSE executed_by 
  END AS executed_by_masked,
  CASE 
    WHEN LENGTH(statement_text) > 100 THEN CONCAT(LEFT(statement_text, 100), '...')
    ELSE statement_text 
  END AS statement_text_masked
FROM silver.query_history;
```

---

## 8) Monitoring and Alerting

### 8.1 Data Quality Alerts
- **Missing data** for critical tables
- **Late-arriving data** beyond SLA
- **Data volume anomalies** (unexpected spikes/drops)
- **Schema evolution** detection

### 8.2 Performance Alerts  
- **Pipeline failures** or delays
- **Resource utilization** thresholds
- **Cost anomalies** (unexpected spending spikes)
- **Query performance** degradation

### 8.3 Business Alerts
- **Job failure rates** by team/project
- **Cost threshold** breaches
- **Security events** (unusual access patterns)
- **Compliance violations** (missing audit trails)

---

This enhanced design provides a comprehensive, production-ready observability platform for Databricks with proper SCD2 handling, incremental processing, and showback-focused cost allocation.
