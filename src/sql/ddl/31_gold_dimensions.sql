-- =============================================================================
-- Databricks Observability Platform - Gold Dimension Tables
-- =============================================================================
-- Purpose: Create gold dimension tables for analytics
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. DIM COMPUTE TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.dim_compute (
    -- Surrogate key
    compute_sk STRING NOT NULL,
    
    -- Natural keys
    workspace_id STRING NOT NULL,
    compute_type STRING NOT NULL,
    compute_id STRING NOT NULL,
    
    -- Business attributes
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
    
    -- Warehouse-specific
    warehouse_type STRING,
    warehouse_size STRING,
    warehouse_channel STRING,
    min_clusters INT,
    max_clusters INT,
    auto_stop_minutes INT,
    
    -- Tags
    tags MAP<STRING, STRING>,
    
    -- Lifecycle
    create_time TIMESTAMP,
    delete_time TIMESTAMP,
    
    -- SCD2 columns
    effective_start_ts TIMESTAMP NOT NULL,
    effective_end_ts TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL,
    
    -- Audit columns
    dw_created_ts TIMESTAMP NOT NULL,
    dw_updated_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (workspace_id);

-- =============================================================================
-- 2. DIM WORKFLOW TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.dim_workflow (
    -- Surrogate key
    workflow_sk STRING NOT NULL,
    
    -- Natural keys
    workspace_id STRING NOT NULL,
    workflow_type STRING NOT NULL,
    workflow_id STRING NOT NULL,
    
    -- Business attributes
    name STRING,
    description STRING,
    creator_id STRING,
    run_as STRING,
    
    -- Pipeline-specific
    pipeline_type STRING,
    settings MAP<STRING, STRING>,
    
    -- Tags
    tags MAP<STRING, STRING>,
    
    -- Lifecycle
    create_time TIMESTAMP,
    delete_time TIMESTAMP,
    
    -- SCD2 columns
    effective_start_ts TIMESTAMP NOT NULL,
    effective_end_ts TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL,
    
    -- Audit columns
    dw_created_ts TIMESTAMP NOT NULL,
    dw_updated_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (workspace_id);

-- =============================================================================
-- 3. DIM USER TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.dim_user (
    -- Surrogate key
    user_sk STRING NOT NULL,
    
    -- Natural keys
    user_id STRING NOT NULL,
    user_name STRING,
    service_principal_name STRING,
    identity_type STRING,
    
    -- Audit columns
    dw_created_ts TIMESTAMP NOT NULL,
    dw_updated_ts TIMESTAMP NOT NULL
)
USING DELTA;

-- =============================================================================
-- 4. DIM SKU TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.dim_sku (
    -- Surrogate key
    sku_sk STRING NOT NULL,
    
    -- Natural keys
    sku_name STRING NOT NULL,
    cloud STRING NOT NULL,
    usage_unit STRING,
    usage_type STRING,
    
    -- Audit columns
    dw_created_ts TIMESTAMP NOT NULL,
    dw_updated_ts TIMESTAMP NOT NULL
)
USING DELTA;

-- =============================================================================
-- 5. DIM NODE TYPE TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.gold.dim_node_type (
    -- Surrogate key
    node_type_sk STRING NOT NULL,
    
    -- Natural keys
    node_type STRING NOT NULL,
    core_count DOUBLE,
    memory_mb BIGINT,
    gpu_count BIGINT,
    
    -- Audit columns
    dw_created_ts TIMESTAMP NOT NULL,
    dw_updated_ts TIMESTAMP NOT NULL
)
USING DELTA;

-- =============================================================================
-- 6. TABLE PROPERTIES AND CONSTRAINTS
-- =============================================================================

-- Set properties for all dimension tables
ALTER TABLE obs.gold.dim_compute SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.gold.dim_workflow SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.gold.dim_user SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.gold.dim_sku SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE obs.gold.dim_node_type SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- Add primary key constraints (idempotent)
ALTER TABLE obs.gold.dim_compute 
DROP CONSTRAINT IF EXISTS pk_dim_compute;

ALTER TABLE obs.gold.dim_compute 
ADD CONSTRAINT pk_dim_compute 
PRIMARY KEY (compute_sk);

ALTER TABLE obs.gold.dim_workflow 
DROP CONSTRAINT IF EXISTS pk_dim_workflow;

ALTER TABLE obs.gold.dim_workflow 
ADD CONSTRAINT pk_dim_workflow 
PRIMARY KEY (workflow_sk);

ALTER TABLE obs.gold.dim_user 
DROP CONSTRAINT IF EXISTS pk_dim_user;

ALTER TABLE obs.gold.dim_user 
ADD CONSTRAINT pk_dim_user 
PRIMARY KEY (user_sk);

ALTER TABLE obs.gold.dim_sku 
DROP CONSTRAINT IF EXISTS pk_dim_sku;

ALTER TABLE obs.gold.dim_sku 
ADD CONSTRAINT pk_dim_sku 
PRIMARY KEY (sku_sk);

ALTER TABLE obs.gold.dim_node_type 
DROP CONSTRAINT IF EXISTS pk_dim_node_type;

ALTER TABLE obs.gold.dim_node_type 
ADD CONSTRAINT pk_dim_node_type 
PRIMARY KEY (node_type_sk);

-- =============================================================================
-- 7. VERIFICATION
-- =============================================================================

-- Verify table creation
DESCRIBE TABLE obs.gold.dim_compute;
DESCRIBE TABLE obs.gold.dim_workflow;
DESCRIBE TABLE obs.gold.dim_user;
DESCRIBE TABLE obs.gold.dim_sku;
DESCRIBE TABLE obs.gold.dim_node_type;

-- =============================================================================
-- 8. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 02_gold_fact_tables.sql - Create fact tables
-- 2. Staging views for data harmonization
-- 3. SCD2 merge logic and functions
-- 4. Data processing logic

SELECT 'Gold dimension tables created successfully!' as message;
SELECT 'Tables: dim_compute, dim_workflow, dim_user, dim_sku, dim_node_type' as message;
SELECT 'SCD2 patterns implemented for compute and workflow dimensions' as message;
SELECT 'Ready for fact table creation.' as message;
