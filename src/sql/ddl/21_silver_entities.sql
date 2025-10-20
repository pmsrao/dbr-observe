-- =============================================================================
-- Databricks Observability Platform - Silver Entity Tables
-- =============================================================================
-- Purpose: Create silver entity tables with SCD2 patterns
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. COMPUTE ENTITIES TABLE (SCD2)
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.silver.compute_entities (
    -- Natural Key
    workspace_id STRING NOT NULL,
    compute_type STRING NOT NULL,  -- 'CLUSTER' | 'WAREHOUSE'
    compute_id STRING NOT NULL,
    
    -- SCD2 Columns
    effective_start_ts TIMESTAMP NOT NULL,
    effective_end_ts TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL,
    
    -- Audit Columns
    record_hash STRING NOT NULL,
    dw_created_ts TIMESTAMP NOT NULL,
    dw_updated_ts TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP NOT NULL,
    
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
    delete_time TIMESTAMP
)
USING DELTA
COMMENT 'Silver table for unified compute entities (clusters and warehouses) with SCD2 history tracking'
LOCATION 's3://company-databricks-obs/silver/compute_entities/'
PARTITIONED BY (workspace_id, compute_type);

-- =============================================================================
-- 2. WORKFLOW ENTITIES TABLE (SCD2)
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.silver.workflow_entities (
    -- Natural Key
    workspace_id STRING NOT NULL,
    workflow_type STRING NOT NULL,  -- 'JOB' | 'PIPELINE'
    workflow_id STRING NOT NULL,
    
    -- SCD2 Columns
    effective_start_ts TIMESTAMP NOT NULL,
    effective_end_ts TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL,
    
    -- Audit Columns
    record_hash STRING NOT NULL,
    dw_created_ts TIMESTAMP NOT NULL,
    dw_updated_ts TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP NOT NULL,
    
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
    delete_time TIMESTAMP
)
USING DELTA
COMMENT 'Silver table for unified workflow entities (jobs and pipelines) with SCD2 history tracking'
LOCATION 's3://company-databricks-obs/silver/workflow_entities/'
PARTITIONED BY (workspace_id, workflow_type);

-- =============================================================================
-- 3. TABLE PROPERTIES AND CONSTRAINTS
-- =============================================================================

-- Set properties for compute entities table
ALTER TABLE obs.silver.compute_entities SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Set properties for workflow entities table
ALTER TABLE obs.silver.workflow_entities SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Add constraints for data integrity
ALTER TABLE obs.silver.compute_entities 
ADD CONSTRAINT pk_compute_entities 
PRIMARY KEY (workspace_id, compute_type, compute_id, effective_start_ts);

ALTER TABLE obs.silver.workflow_entities 
ADD CONSTRAINT pk_workflow_entities 
PRIMARY KEY (workspace_id, workflow_type, workflow_id, effective_start_ts);

-- =============================================================================
-- 4. INDEXES FOR PERFORMANCE
-- =============================================================================

-- Create indexes for common query patterns
-- Note: These will be created after initial data load

-- =============================================================================
-- 5. VERIFICATION
-- =============================================================================

-- Verify table creation
DESCRIBE TABLE obs.silver.compute_entities;
DESCRIBE TABLE obs.silver.workflow_entities;

-- Check table properties
SHOW TBLPROPERTIES obs.silver.compute_entities;
SHOW TBLPROPERTIES obs.silver.workflow_entities;

-- =============================================================================
-- 6. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 02_silver_workflow_runs.sql - Create workflow runs table
-- 2. 03_silver_billing_usage.sql - Create billing usage table
-- 3. 04_silver_query_history.sql - Create query history table
-- 4. 05_silver_audit_log.sql - Create audit log table
-- 5. Staging views for data harmonization

PRINT 'Silver entity tables created successfully!';
PRINT 'Tables: compute_entities, workflow_entities';
PRINT 'SCD2 patterns implemented with proper audit columns';
PRINT 'Ready for workflow runs table creation.';
