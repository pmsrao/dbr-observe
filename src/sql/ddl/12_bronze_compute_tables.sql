-- =============================================================================
-- Databricks Observability Platform - Bronze Compute Tables
-- =============================================================================
-- Purpose: Create bronze tables for compute system data
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. COMPUTE CLUSTERS TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_compute_clusters;

CREATE TABLE IF NOT EXISTS obs.bronze.system_compute_clusters (
    -- Raw data preservation
    raw_data STRUCT<
        cluster_id STRING,
        workspace_id STRING,
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
        tags MAP<STRING, STRING>,
        create_time TIMESTAMP,
        delete_time TIMESTAMP,
        change_time TIMESTAMP
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    change_time TIMESTAMP,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, change_time);

-- =============================================================================
-- 2. COMPUTE WAREHOUSES TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_compute_warehouses;

CREATE TABLE IF NOT EXISTS obs.bronze.system_compute_warehouses (
    -- Raw data preservation
    raw_data STRUCT<
        warehouse_id STRING,
        workspace_id STRING,
        name STRING,
        owner STRING,
        warehouse_type STRING,
        warehouse_size STRING,
        warehouse_channel STRING,
        min_clusters INT,
        max_clusters INT,
        auto_stop_minutes INT,
        tags MAP<STRING, STRING>,
        create_time TIMESTAMP,
        delete_time TIMESTAMP,
        change_time TIMESTAMP
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    change_time TIMESTAMP,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, change_time);

-- =============================================================================
-- 3. COMPUTE NODE TYPES TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_compute_node_types;

CREATE TABLE IF NOT EXISTS obs.bronze.system_compute_node_types (
    -- Raw data preservation
    raw_data STRUCT<
        node_type STRING,
        core_count DOUBLE,
        memory_mb BIGINT,
        gpu_count BIGINT,
        cloud STRING,
        region STRING,
        availability_zones ARRAY<STRING>
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    cloud STRING,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (cloud);

-- =============================================================================
-- 4. COMPUTE NODE TIMELINE TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_compute_node_timeline;

CREATE TABLE IF NOT EXISTS obs.bronze.system_compute_node_timeline (
    -- Raw data preservation
    raw_data STRUCT<
        workspace_id STRING,
        cluster_id STRING,
        instance_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        driver BOOLEAN,
        cpu_user_percent DOUBLE,
        cpu_system_percent DOUBLE,
        cpu_wait_percent DOUBLE,
        mem_used_percent DOUBLE,
        mem_swap_percent DOUBLE,
        network_sent_bytes BIGINT,
        network_received_bytes BIGINT,
        disk_free_bytes_per_mount_point MAP<STRING, BIGINT>
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    start_time TIMESTAMP,
    start_date DATE,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, start_date);

-- =============================================================================
-- 5. COMPUTE WAREHOUSE EVENTS TABLE
-- =============================================================================

-- Drop table if exists (for schema changes)
DROP TABLE IF EXISTS obs.bronze.system_compute_warehouse_events;

CREATE TABLE IF NOT EXISTS obs.bronze.system_compute_warehouse_events (
    -- Raw data preservation
    raw_data STRUCT<
        workspace_id STRING,
        warehouse_id STRING,
        event_type STRING,
        event_time TIMESTAMP,
        cluster_count INT,
        details MAP<STRING, STRING>
    >,
    
    -- Partitioning columns (extracted from raw_data for performance)
    workspace_id STRING,
    event_time TIMESTAMP,
    event_date DATE,
    
    -- Common bronze columns
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_hash STRING,
    is_deleted BOOLEAN
)
USING DELTA
PARTITIONED BY (workspace_id, event_date);

-- =============================================================================
-- 6. TABLE PROPERTIES AND OPTIMIZATION
-- =============================================================================

-- Set properties for all compute tables
ALTER TABLE obs.bronze.system_compute_clusters SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

ALTER TABLE obs.bronze.system_compute_warehouses SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

ALTER TABLE obs.bronze.system_compute_node_types SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

ALTER TABLE obs.bronze.system_compute_node_timeline SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

ALTER TABLE obs.bronze.system_compute_warehouse_events SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- =============================================================================
-- 7. VERIFICATION
-- =============================================================================

-- Verify table creation
DESCRIBE TABLE obs.bronze.system_compute_clusters;
DESCRIBE TABLE obs.bronze.system_compute_warehouses;
DESCRIBE TABLE obs.bronze.system_compute_node_types;
DESCRIBE TABLE obs.bronze.system_compute_node_timeline;
DESCRIBE TABLE obs.bronze.system_compute_warehouse_events;

-- Check table properties
SHOW TBLPROPERTIES obs.bronze.system_compute_clusters;

-- =============================================================================
-- 8. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 03_bronze_lakeflow_tables.sql - Create lakeflow system tables
-- 2. 04_bronze_query_tables.sql - Create query system tables
-- 3. 05_bronze_storage_tables.sql - Create storage system tables
-- 4. 06_bronze_access_tables.sql - Create access system tables

SELECT 'Bronze compute tables created successfully!' as message;
SELECT 'Tables: system_compute_clusters, system_compute_warehouses, system_compute_node_types, system_compute_node_timeline, system_compute_warehouse_events' as message;
SELECT 'Ready for lakeflow table creation.' as message;
