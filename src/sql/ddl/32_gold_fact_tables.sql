-- =============================================================================
-- Databricks Observability Platform - Gold Fact Tables
-- =============================================================================
-- Purpose: Create gold fact tables for analytics and reporting
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. FACT BILLING USAGE HOURLY TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.fct_billing_usage_hourly (
    -- Surrogate keys
    billing_sk STRING NOT NULL,
    compute_sk STRING,
    workflow_sk STRING,
    user_sk STRING,
    sku_sk STRING,
    
    -- Time dimensions
    usage_date DATE NOT NULL,
    usage_hour INT NOT NULL,
    usage_start_time TIMESTAMP NOT NULL,
    usage_end_time TIMESTAMP NOT NULL,
    
    -- Billing details
    workspace_id STRING NOT NULL,
    sku_name STRING NOT NULL,
    cloud STRING NOT NULL,
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
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold fact table for billing usage with hourly granularity and cost attribution'
LOCATION 's3://company-databricks-obs/gold/fct_billing_usage_hourly/'
PARTITIONED BY (workspace_id, usage_date);

-- =============================================================================
-- 2. FACT WORKFLOW RUNS TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.fct_workflow_runs (
    -- Surrogate keys
    workflow_run_sk STRING NOT NULL,
    workflow_sk STRING,
    compute_sk STRING,
    user_sk STRING,
    
    -- Time dimensions
    run_date DATE NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_ms BIGINT,
    
    -- Workflow details
    workspace_id STRING NOT NULL,
    workflow_type STRING NOT NULL,
    workflow_id STRING NOT NULL,
    run_id STRING NOT NULL,
    
    -- Status
    result_state STRING,
    termination_code STRING,
    
    -- Performance
    success_rate DOUBLE,
    
    -- Tags (extracted)
    cost_center STRING,
    business_unit STRING,
    department STRING,
    data_product STRING,
    environment STRING,
    team STRING,
    project STRING,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold fact table for workflow runs with performance metrics'
LOCATION 's3://company-databricks-obs/gold/fct_workflow_runs/'
PARTITIONED BY (workspace_id, run_date);

-- =============================================================================
-- 3. FACT QUERY PERFORMANCE TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.fct_query_performance (
    -- Surrogate keys
    query_sk STRING NOT NULL,
    compute_sk STRING,
    user_sk STRING,
    workflow_sk STRING,
    
    -- Time dimensions
    query_date DATE NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    total_duration_ms BIGINT,
    
    -- Query details
    workspace_id STRING NOT NULL,
    statement_id STRING NOT NULL,
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
    
    -- Tags (extracted)
    cost_center STRING,
    business_unit STRING,
    department STRING,
    data_product STRING,
    environment STRING,
    team STRING,
    project STRING,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold fact table for query performance with efficiency metrics'
LOCATION 's3://company-databricks-obs/gold/fct_query_performance/'
PARTITIONED BY (workspace_id, query_date);

-- =============================================================================
-- 4. FACT NODE USAGE HOURLY TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.fct_node_usage_hourly (
    -- Surrogate keys
    node_usage_sk STRING NOT NULL,
    compute_sk STRING,
    
    -- Time dimensions
    usage_date DATE NOT NULL,
    usage_hour INT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    
    -- Node details
    workspace_id STRING NOT NULL,
    cluster_id STRING NOT NULL,
    instance_id STRING NOT NULL,
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
    
    -- Tags (extracted)
    cost_center STRING,
    business_unit STRING,
    department STRING,
    data_product STRING,
    environment STRING,
    team STRING,
    project STRING,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold fact table for node usage with hourly aggregation'
LOCATION 's3://company-databricks-obs/gold/fct_node_usage_hourly/'
PARTITIONED BY (workspace_id, usage_date);

-- =============================================================================
-- 5. TABLE PROPERTIES AND CONSTRAINTS
-- =============================================================================

-- Set properties for all fact tables
ALTER TABLE obs.gold.fct_billing_usage_hourly SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.gold.fct_workflow_runs SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.gold.fct_query_performance SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.gold.fct_node_usage_hourly SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- Add primary key constraints
ALTER TABLE obs.gold.fct_billing_usage_hourly 
ADD CONSTRAINT pk_fct_billing_usage_hourly 
PRIMARY KEY (billing_sk);

ALTER TABLE obs.gold.fct_workflow_runs 
ADD CONSTRAINT pk_fct_workflow_runs 
PRIMARY KEY (workflow_run_sk);

ALTER TABLE obs.gold.fct_query_performance 
ADD CONSTRAINT pk_fct_query_performance 
PRIMARY KEY (query_sk);

ALTER TABLE obs.gold.fct_node_usage_hourly 
ADD CONSTRAINT pk_fct_node_usage_hourly 
PRIMARY KEY (node_usage_sk);

-- =============================================================================
-- 6. VERIFICATION
-- =============================================================================

-- Verify table creation
DESCRIBE TABLE obs.gold.fct_billing_usage_hourly;
DESCRIBE TABLE obs.gold.fct_workflow_runs;
DESCRIBE TABLE obs.gold.fct_query_performance;
DESCRIBE TABLE obs.gold.fct_node_usage_hourly;

-- =============================================================================
-- 7. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. Staging views for data harmonization
-- 2. SCD2 merge logic and functions
-- 3. Data processing logic
-- 4. Tag extraction functions

PRINT 'Gold fact tables created successfully!';
PRINT 'Tables: fct_billing_usage_hourly, fct_workflow_runs, fct_query_performance, fct_node_usage_hourly';
PRINT 'Star schema implemented with proper surrogate keys';
PRINT 'Ready for staging views and processing logic.';
