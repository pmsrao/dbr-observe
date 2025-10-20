# Architecture and Design

> **Navigation**: [README](../README.md) | [Database Best Practices](database_best_practices.md) | [Tag Extraction Strategy](tag_extraction_strategy.md) | [Configuration Guide](config_folder_guide.md) | [Deployment Guide](deployment_guide.md) | [Documentation Reorganization Summary](documentation_reorganization_summary.md)

## 🏗️ **Architecture Design**

### **Data Lakehouse Architecture**
This platform implements a modern data lakehouse architecture with three layers:

- **Bronze Layer**: Raw system table data with minimal transformation and full data preservation
- **Silver Layer**: Curated data with SCD2 history tracking, data harmonization, and enhanced metrics
- **Gold Layer**: Analytics-ready dimensions and facts for business intelligence and reporting

### **Schema Organization**
```
obs.bronze          -- Raw system table data (6 tables)
obs.silver          -- Curated data with SCD2 + staging views (8 tables + 5 views)
obs.gold            -- Analytics-ready data (5 dimensions + 4 facts)
obs.meta            -- Metadata management (watermarks)
obs.ops             -- Operational monitoring (future)
```

### **Data Flow Architecture**
```
System Tables → Bronze → Silver → Gold
     ↓           ↓        ↓       ↓
  Raw Data   Preserved  SCD2   Analytics
  Ingestion  History   Metrics  Ready
```

## 📊 **Detailed Table Design**

### **Bronze Layer (Raw Data Preservation)**
- **`system_billing_usage`**: DBU consumption with metadata
- **`system_billing_list_prices`**: SKU pricing information
- **`system_compute_clusters`**: Cluster configurations and lifecycle
- **`system_compute_warehouses`**: SQL warehouse configurations
- **`system_compute_node_types`**: Node type specifications
- **`system_compute_node_timeline`**: Minute-level resource utilization
- **`system_lakeflow_jobs`**: Job definitions and lifecycle
- **`system_lakeflow_job_tasks`**: Individual task definitions
- **`system_lakeflow_job_run_timeline`**: Job execution details
- **`system_lakeflow_job_task_run_timeline`**: Task execution details
- **`system_lakeflow_pipelines`**: DLT pipeline definitions
- **`system_lakeflow_pipeline_update_timeline`**: Pipeline execution details
- **`system_query_history`**: Query execution details
- **`system_storage_ops`**: Storage optimization operations
- **`system_access_audit`**: Access and security events

### **Silver Layer (Curated Data with SCD2)**
- **`compute_entities`**: Unified compute entities (clusters + warehouses) with SCD2
- **`workflow_entities`**: Unified workflow entities (jobs + pipelines) with SCD2
- **`workflow_runs`**: Enhanced workflow runs with comprehensive runtime metrics
- **`job_task_runs`**: Individual task runs with detailed performance metrics
- **`billing_usage`**: Billing usage with upsert pattern using record_id
- **`query_history`**: Query history with compute linkage
- **`audit_log`**: Access audit log for governance
- **`node_usage_minutely`**: Node resource utilization metrics

### **Gold Layer (Analytics-Ready)**
- **Dimensions**: `dim_compute`, `dim_workflow`, `dim_user`, `dim_sku`, `dim_node_type`
- **Facts**: `fct_billing_usage_hourly`, `fct_workflow_runs`, `fct_query_performance`, `fct_node_usage_hourly`

## 🔧 **Key Design Decisions**

### **SCD2 Implementation**
- **Audit Columns**: `dw_created_ts`, `dw_updated_ts`, `effective_start_ts`, `effective_end_ts`, `is_current`
- **Change Detection**: Record hash for efficient change identification
- **Late-Arriving Data**: Handles dimension changes with proper historical tracking

### **Enhanced Runtime Metrics**
- **Resource Utilization**: CPU, memory, disk, network, GPU utilization percentages
- **Performance Metrics**: Throughput, efficiency, reliability scores
- **Error Analysis**: Categorization, severity, retry logic
- **Cost Attribution**: DBU consumption, cost per record, efficiency metrics

### **Tag Extraction Strategy**
- **Cost Allocation**: `cost_center`, `business_unit`, `department`
- **Operational**: `environment`, `team`, `project`
- **Data Governance**: `data_product`, `data_classification`, `retention_policy`
- **Flexible Structure**: `MAP<STRING, STRING>` for schema evolution protection

### **Watermark Management**
- **Incremental Processing**: 2-day lookback with configurable intervals
- **Source-Target Mapping**: Granular watermark tracking per source-target pair
- **Error Handling**: Processing status tracking with retry capabilities
- **Cleanup**: Automated cleanup of old watermark records

### **Partitioning Strategy**
- **Large Tables**: `PARTITIONED BY (workspace_id, date(start_time))`
- **Entity Tables**: `PARTITIONED BY (workspace_id)`
- **Small Tables**: No partitioning for optimal performance

## 🛠️ **Technology Choices and Monitoring Strategy**

### **Function Implementation**
- **SQL UDFs**: Recommended for performance and simplicity
- **Benefits**: Execute closer to data, reduce serialization overhead, no Python dependency management
- **Maintainability**: Version control with SQL scripts, unified SQL ecosystem

### **Error Handling**
- **Try-Catch Patterns**: Manual re-processing capability for failed records
- **Watermark Tracking**: Processing status tracking with error details
- **Retry Logic**: Configurable retry attempts with exponential backoff

### **Monitoring Strategy**
- **Hybrid Approach**: Built-in Databricks monitoring + custom observability layer
- **Built-in**: Leverage Databricks SQL dashboards and alerts
- **Custom**: Enhanced metrics calculation and cost attribution
- **Alerting**: Email and Slack notifications for pipeline failures

### **Data Quality**
- **Validation**: Schema validation and data quality checks
- **Lineage**: Data lineage tracking through all layers
- **Audit Trails**: Complete audit trails for all processing steps
- **PII Handling**: Future implementation for sensitive data masking

## 📊 **Expected Outcomes and KPIs**

### **Cost Allocation & Showback**
- **DBU Consumption**: By workspace, team, project with detailed breakdown
- **Cost Attribution**: Cost per job/workflow execution with efficiency metrics
- **Resource Utilization**: Analysis of cluster and warehouse usage patterns
- **Waste Analysis**: Identification of idle clusters, oversized resources, and optimization opportunities
- **Showback Metrics**: Cost per data product, cost per team, ROI analysis

### **Performance Monitoring**
- **Query Performance**: Execution time analysis, resource consumption per query
- **Job Success Rates**: By team/project with failure analysis and root cause identification
- **Resource Efficiency**: CPU/memory utilization, throughput metrics, cache hit ratios
- **SLA Compliance**: Job execution within defined SLAs, reliability scores
- **Bottleneck Identification**: Performance bottlenecks and optimization recommendations

### **Governance & Security**
- **User Activity Patterns**: Access patterns, usage trends, compliance monitoring
- **Security Events**: Failed access attempts, privilege escalations, data access violations
- **Data Access Patterns**: Who accessed what data when, data lineage tracking
- **Compliance Metrics**: Audit trail completeness, data retention compliance, access control effectiveness

### **Operational Excellence**
- **Pipeline Health**: Success rates, execution times, error patterns
- **Data Quality**: Completeness, accuracy, freshness scores
- **System Availability**: Uptime metrics, performance degradation detection
- **Alert Response**: Mean time to resolution, alert accuracy, escalation patterns
