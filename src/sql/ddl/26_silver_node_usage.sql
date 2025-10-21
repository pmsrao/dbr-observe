-- =============================================================================
-- Databricks Observability Platform - Silver Node Usage Table
-- =============================================================================
-- Purpose: Create silver node usage table for resource monitoring
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.silver.node_usage_minutely;

CREATE TABLE IF NOT EXISTS obs.silver.node_usage_minutely (
    -- Basic identifiers
    workspace_id STRING NOT NULL,
    cluster_id STRING NOT NULL,
    instance_id STRING NOT NULL,
    
    -- Timing
    start_time TIMESTAMP NOT NULL,
    start_date DATE,
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
PARTITIONED BY (workspace_id, start_date);

-- Set properties
ALTER TABLE obs.silver.node_usage_minutely SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- Add primary key constraint (idempotent)
ALTER TABLE obs.silver.node_usage_minutely 
DROP CONSTRAINT IF EXISTS pk_node_usage_minutely;

ALTER TABLE obs.silver.node_usage_minutely 
ADD CONSTRAINT pk_node_usage_minutely 
PRIMARY KEY (workspace_id, cluster_id, instance_id, start_time);

SELECT 'Silver node usage table created successfully!' as message;
