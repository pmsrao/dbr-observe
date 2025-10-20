# Databricks Observability Platform — Naming Standards & Best Practices

## 1) Table Naming Standards

### 1.1 Layer Prefixes
```sql
-- Bronze Layer (Raw System Tables)
bronze.system_billing_usage
bronze.system_compute_clusters
bronze.system_lakeflow_jobs

-- Silver Layer (Curated)
silver.billing_usage
silver.compute_entities
silver.workflow_entities
silver.workflow_runs
silver.query_history
silver.audit_log

-- Silver Staging
silver_stg.compute_entities_harmonized
silver_stg.workflow_entities_harmonized

-- Gold Layer (Analytics)
gold.dim_compute
gold.dim_workflow
gold.dim_user
gold.dim_sku
gold.fct_billing_usage_hourly
gold.fct_workflow_runs
gold.fct_query_performance

-- Meta Schema (Metadata and Configuration)
meta.watermarks
meta.table_lineage
meta.schema_versions
meta.processing_config

-- Ops Schema (Operational and Monitoring)
ops.data_quality_metrics
ops.pipeline_performance
ops.operational_alerts
ops.monitoring_metrics
ops.audit_trail
ops.error_logs
```

### 1.2 Table Type Suffixes
```sql
-- Dimensions
gold.dim_compute
gold.dim_workflow
gold.dim_user
gold.dim_sku
gold.dim_node_type

-- Facts
gold.fct_billing_usage_hourly
gold.fct_workflow_runs
gold.fct_query_performance
gold.fct_node_usage_hourly

-- Bridge Tables
gold.bridge_workflow_compute
gold.bridge_resource_tags

-- Operational Tables
gold.data_quality_metrics
gold.pipeline_performance
gold.operational_alerts
```

### 1.3 Special Purpose Tables
```sql
-- Meta Schema (Metadata and Configuration)
meta.watermarks                    -- Watermark management
meta.table_lineage                 -- Data lineage tracking
meta.schema_versions               -- Schema evolution tracking
meta.processing_config             -- Processing configuration

-- Ops Schema (Operational and Monitoring)
ops.data_quality_metrics          -- Data quality metrics
ops.pipeline_performance          -- Pipeline performance metrics
ops.operational_alerts            -- Alert management
ops.monitoring_metrics            -- System monitoring metrics
ops.audit_trail                   -- Processing audit trail
ops.error_logs                    -- Error tracking and logging
```

---

## 2) Column Naming Standards

### 2.1 Standard Column Names

#### Surrogate Keys
```sql
-- Primary Surrogate Keys
{entity}_sk STRING                    -- e.g., compute_sk, workflow_sk, user_sk
{entity}_pk STRING                    -- Alternative: compute_pk, workflow_pk

-- Composite Surrogate Keys
billing_sk STRING                     -- SHA2 hash of natural key
workflow_run_sk STRING                -- SHA2 hash of run identifier
query_sk STRING                       -- SHA2 hash of statement_id
```

#### Business Keys (Natural Keys)
```sql
-- Entity Identifiers
workspace_id STRING                   -- Always present
entity_id STRING                      -- e.g., cluster_id, job_id, warehouse_id
entity_type STRING                    -- e.g., 'CLUSTER', 'WAREHOUSE', 'JOB', 'PIPELINE'

-- Composite Business Keys
compute_key STRING                    -- workspace_id|compute_type|compute_id
workflow_key STRING                   -- workspace_id|workflow_type|workflow_id
```

#### SCD2 Columns
```sql
-- Standard SCD2 Columns
effective_start_ts TIMESTAMP         -- When record became effective
effective_end_ts TIMESTAMP           -- When record stopped being effective
is_current BOOLEAN                   -- TRUE for current version
record_hash STRING                   -- Hash for change detection
```

#### Audit Columns
```sql
-- Data Warehouse Audit Columns
dw_created_ts TIMESTAMP              -- When record was created in DW
dw_updated_ts TIMESTAMP              -- When record was last updated in DW
dw_created_by STRING                 -- Who created the record
dw_updated_by STRING                 -- Who last updated the record

-- Processing Audit Columns
processing_timestamp TIMESTAMP       -- When record was processed
source_file STRING                   -- Source file name
record_hash STRING                   -- Hash for deduplication
is_deleted BOOLEAN                   -- Soft delete flag
```

#### Time Columns
```sql
-- Event Timing
start_time TIMESTAMP                 -- Event start time
end_time TIMESTAMP                   -- Event end time
duration_ms BIGINT                   -- Duration in milliseconds
duration_seconds BIGINT              -- Duration in seconds

-- Business Time
usage_date DATE                      -- Business date for aggregation
usage_hour INT                       -- Hour of day (0-23)
usage_week INT                       -- Week of year
usage_month INT                      -- Month of year
usage_quarter INT                    -- Quarter of year
usage_year INT                       -- Year
```

#### Status Columns
```sql
-- Execution Status
execution_status STRING              -- 'SUCCESS', 'FAILED', 'RUNNING'
result_state STRING                  -- 'SUCCEEDED', 'FAILED', 'CANCELLED'
processing_status STRING             -- 'SUCCESS', 'FAILED', 'IN_PROGRESS'

-- Record Status
is_active BOOLEAN                    -- Active flag
is_deleted BOOLEAN                   -- Deleted flag
is_current BOOLEAN                   -- Current version flag
```

#### Tag Columns
```sql
-- Standard Tag Categories
cost_center STRING                   -- Cost center assignment
business_unit STRING                 -- Business unit
department STRING                    -- Department
data_product STRING                  -- Data product
environment STRING                   -- Environment (dev, staging, prod)
team STRING                          -- Team name
project STRING                       -- Project name

-- Tag Maps
tags MAP<STRING, STRING>             -- Raw tags from source
custom_tags MAP<STRING, STRING>      -- Custom tags from billing
```

#### Performance Columns
```sql
-- Resource Metrics
cpu_user_percent DOUBLE              -- CPU user percentage
cpu_system_percent DOUBLE            -- CPU system percentage
cpu_wait_percent DOUBLE              -- CPU wait percentage
mem_used_percent DOUBLE              -- Memory used percentage
mem_swap_percent DOUBLE              -- Swap memory percentage

-- Data Metrics
read_bytes BIGINT                    -- Bytes read
written_bytes BIGINT                 -- Bytes written
read_rows BIGINT                     -- Rows read
written_rows BIGINT                  -- Rows written
read_files BIGINT                    -- Files read
written_files BIGINT                 -- Files written

-- Network Metrics
network_sent_bytes BIGINT            -- Network bytes sent
network_received_bytes BIGINT        -- Network bytes received
shuffle_read_bytes BIGINT            -- Shuffle bytes read
```

#### Cost Columns
```sql
-- Billing Metrics
usage_quantity DECIMAL(18,6)         -- Usage quantity
usage_unit STRING                    -- Unit of measure
list_price DECIMAL(18,6)             -- List price per unit
total_cost DECIMAL(18,6)             -- Total cost (quantity * price)
cost_per_hour DECIMAL(18,6)          -- Cost per hour
cost_per_job DECIMAL(18,6)           -- Cost per job execution

-- Currency
currency_code STRING                 -- Currency code (USD, EUR, etc.)
```

---

## 3) Data Modeling Standards

### 3.1 Surrogate Key Generation
```sql
-- Standard Surrogate Key Pattern
SHA2(CONCAT(workspace_id, '|', entity_type, '|', entity_id), 256) AS entity_sk

-- Composite Surrogate Key Pattern
SHA2(CONCAT(workspace_id, '|', workflow_type, '|', workflow_id, '|', run_id), 256) AS workflow_run_sk

-- Business Key Pattern
CONCAT(workspace_id, '|', entity_type, '|', entity_id) AS business_key
```

### 3.2 SCD2 Implementation Standards
```sql
-- Standard SCD2 Columns
effective_start_ts TIMESTAMP,
effective_end_ts TIMESTAMP,
is_current BOOLEAN,
record_hash STRING,
dw_created_ts TIMESTAMP,
dw_updated_ts TIMESTAMP

-- SCD2 Merge Pattern
MERGE INTO {target_table} AS target
USING {source_table} AS source
ON target.{natural_key_columns} = source.{natural_key_columns}
  AND target.is_current = true
WHEN MATCHED AND target.record_hash != source.record_hash THEN
  UPDATE SET 
    effective_end_ts = source.change_time,
    is_current = false,
    dw_updated_ts = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
  INSERT ({columns})
  VALUES ({values});
```

### 3.3 Partitioning Standards
```sql
-- Time-based Partitioning
PARTITIONED BY (workspace_id, date(start_time))
PARTITIONED BY (workspace_id, usage_date)
PARTITIONED BY (workspace_id, run_date)

-- Entity-based Partitioning
PARTITIONED BY (workspace_id, entity_type)
PARTITIONED BY (workspace_id, workflow_type)
```

### 3.4 Clustering Standards
```sql
-- Clustering by Business Keys
CLUSTERED BY (entity_id) INTO 8 BUCKETS
CLUSTERED BY (workflow_id) INTO 8 BUCKETS
CLUSTERED BY (compute_id) INTO 8 BUCKETS

-- Clustering by Time
CLUSTERED BY (start_time) INTO 8 BUCKETS
CLUSTERED BY (usage_start_time) INTO 8 BUCKETS
```

---

## 4) Function Naming Standards

### 4.1 Data Processing Functions
```sql
-- Tag Extraction
extract_tag(tags MAP<STRING, STRING>, tag_name STRING) RETURNS STRING
extract_cost_center(tags MAP<STRING, STRING>) RETURNS STRING
extract_business_unit(tags MAP<STRING, STRING>) RETURNS STRING

-- Surrogate Key Generation
generate_entity_sk(workspace_id STRING, entity_type STRING, entity_id STRING) RETURNS STRING
generate_workflow_sk(workspace_id STRING, workflow_type STRING, workflow_id STRING) RETURNS STRING

-- Hash Generation
generate_record_hash(columns...) RETURNS STRING
generate_business_key(workspace_id STRING, entity_type STRING, entity_id STRING) RETURNS STRING
```

### 4.2 Data Quality Functions
```sql
-- Data Quality Checks
check_completeness(table_name STRING, column_name STRING) RETURNS DOUBLE
check_timeliness(table_name STRING, timestamp_column STRING) RETURNS DOUBLE
check_validity(table_name STRING, validation_rule STRING) RETURNS DOUBLE

-- Monitoring Functions
get_watermark(source_table STRING, target_table STRING) RETURNS TIMESTAMP
update_watermark(source_table STRING, target_table STRING, new_watermark TIMESTAMP)
check_data_quality(table_name STRING) RETURNS ARRAY<STRUCT<metric STRING, value DOUBLE>>
```

---

## 5) View Naming Standards

### 5.1 Business Views
```sql
-- Cost Analytics
gold.v_cost_by_team
gold.v_cost_by_project
gold.v_cost_trends
gold.v_resource_utilization

-- Performance Analytics
gold.v_query_performance
gold.v_job_performance
gold.v_workflow_performance
gold.v_resource_efficiency

-- Governance Views
gold.v_user_activity
gold.v_security_events
gold.v_data_access_patterns
gold.v_compliance_metrics
```

### 5.2 Operational Views
```sql
-- Data Quality
gold.v_data_quality_summary
gold.v_pipeline_performance
gold.v_processing_metrics
gold.v_error_summary

-- Monitoring
gold.v_system_health
gold.v_cost_alerts
gold.v_performance_alerts
gold.v_operational_metrics
```

---

## 6) File Naming Standards

### 6.1 SQL Files
```
01_setup/01_catalog_schemas.sql
01_setup/02_permissions_setup.sql
02_bronze/01_bronze_tables.sql
03_silver/01_silver_schemas.sql
04_gold/01_dimensions.sql
05_monitoring/01_data_quality_checks.sql
```

### 6.2 Python Files
```
02_bronze/01_bronze_ingestion.py
03_watermarks/01_watermark_functions.py
05_scd2/01_scd2_functions.py
06_processing/01_billing_processing.py
10_monitoring/01_quality_metrics.py
```

### 6.3 Configuration Files
```
config/database_config.yaml
config/table_config.yaml
config/processing_config.yaml
config/monitoring_config.yaml
```

---

## 7) Comment Standards

### 7.1 Table Comments
```sql
COMMENT ON TABLE silver.compute_entities IS 
'Unified compute entities (clusters and warehouses) with SCD2 history tracking';

COMMENT ON TABLE gold.fct_billing_usage_hourly IS 
'Hourly billing usage facts with cost attribution and tag extraction';
```

### 7.2 Column Comments
```sql
COMMENT ON COLUMN silver.compute_entities.compute_sk IS 
'Surrogate key: SHA2 hash of workspace_id|compute_type|compute_id';

COMMENT ON COLUMN silver.compute_entities.effective_start_ts IS 
'SCD2: When this version of the record became effective';

COMMENT ON COLUMN gold.fct_billing_usage_hourly.cost_center IS 
'Extracted from tags: cost center for cost allocation';
```

### 7.3 Function Comments
```sql
-- Function: extract_tag
-- Purpose: Extract standardized tag value from tag map
-- Parameters: tags - map of tag key-value pairs, tag_name - tag to extract
-- Returns: Standardized tag value or 'Unknown' if not found
-- Example: extract_tag(MAP('cost_center' -> 'finance'), 'cost_center') -> 'finance'
```

---

## 8) Data Type Standards

### 8.1 Standard Data Types
```sql
-- Identifiers
workspace_id STRING                  -- Always STRING for consistency
entity_id STRING                     -- Always STRING for consistency
user_id STRING                       -- Always STRING for consistency

-- Timestamps
TIMESTAMP                           -- For all timestamp columns
DATE                               -- For date-only columns

-- Numeric Types
BIGINT                             -- For counts and large numbers
DECIMAL(18,6)                      -- For monetary amounts and precise decimals
DOUBLE                             -- For percentages and ratios

-- Text Types
STRING                             -- For all text columns
MAP<STRING, STRING>                -- For tag maps
ARRAY<STRING>                      -- For arrays of strings
STRUCT<...>                        -- For complex nested structures

-- Boolean Types
BOOLEAN                            -- For flags and status indicators
```

### 8.2 Precision Standards
```sql
-- Monetary Values
DECIMAL(18,6)                      -- 18 total digits, 6 decimal places
-- Examples: 123456789012.123456

-- Percentages
DOUBLE                             -- For percentages (0.0 to 100.0)
-- Examples: 45.67, 99.99

-- Counts
BIGINT                             -- For large counts
-- Examples: 1234567890

-- Durations
BIGINT                             -- Milliseconds for precision
-- Examples: 1500 (1.5 seconds)
```

---

## 9) Indexing and Performance Standards

### 9.1 Clustering Keys
```sql
-- Primary Clustering
CLUSTERED BY (entity_id) INTO 8 BUCKETS
CLUSTERED BY (workspace_id, entity_id) INTO 8 BUCKETS

-- Time-based Clustering
CLUSTERED BY (start_time) INTO 8 BUCKETS
CLUSTERED BY (usage_start_time) INTO 8 BUCKETS

-- Composite Clustering
CLUSTERED BY (workspace_id, entity_type, entity_id) INTO 8 BUCKETS
```

### 9.2 Z-Ordering (Future)
```sql
-- Z-Order by frequently joined columns
ZORDER BY (workspace_id, entity_id, start_time)
ZORDER BY (workspace_id, sku_name, usage_start_time)
```

---

## 10) Validation Standards

### 10.1 Data Quality Rules
```sql
-- Completeness Rules
NOT NULL constraints on critical columns
CHECK constraints on valid values
FOREIGN KEY constraints on relationships

-- Consistency Rules
Cross-table validation rules
Business rule validation
Referential integrity checks
```

### 10.2 Performance Rules
```sql
-- Query Performance
< 30 seconds for standard reports
< 5 minutes for complex analytics
< 1 minute for operational dashboards

-- Data Freshness
< 1 hour lag for critical data
< 4 hours lag for standard data
< 24 hours lag for historical data
```

---

This comprehensive naming standard ensures consistency, maintainability, and clarity across the entire observability platform.
