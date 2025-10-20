-- =============================================================================
-- Databricks Observability Platform - Silver Node Usage Table
-- =============================================================================
-- Purpose: Create silver node usage table for resource monitoring
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.silver.node_usage_minutely (
    -- Basic identifiers
    workspace_id STRING NOT NULL,
    cluster_id STRING NOT NULL,
    instance_id STRING NOT NULL,
    
    -- Timing
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    
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
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Silver table for node usage metrics with minutely granularity'
LOCATION 's3://company-databricks-obs/silver/node_usage_minutely/'
PARTITIONED BY (workspace_id, date(start_time));

-- Set properties
ALTER TABLE obs.silver.node_usage_minutely SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- Add primary key constraint
ALTER TABLE obs.silver.node_usage_minutely 
ADD CONSTRAINT pk_node_usage_minutely 
PRIMARY KEY (workspace_id, cluster_id, instance_id, start_time);

PRINT 'Silver node usage table created successfully!';
