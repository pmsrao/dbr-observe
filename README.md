# Databricks Observability Platform

A comprehensive observability solution for Databricks workloads, providing cost allocation, performance monitoring, and governance capabilities.

> **📚 Documentation Navigation**: [Architecture & Design](docs/architecture.md) | [Database Best Practices](docs/database_best_practices.md) | [Tag Extraction Strategy](docs/tag_extraction_strategy.md) | [Configuration Guide](docs/config_folder_guide.md) | [Deployment Guide](docs/deployment_guide.md)

## 🎯 **Solution Overview**

This observability platform leverages Databricks system tables to provide comprehensive insights into:
- **Cost Allocation & Showback**: DBU consumption by workspace, team, project with detailed cost attribution
- **Performance Monitoring**: Query performance, job success rates, resource efficiency metrics
- **Governance & Security**: User activity patterns, security events, data access patterns
- **Operational Excellence**: Pipeline health, data quality scores, system availability metrics

## 🏗️ **Architecture Overview**

This platform implements a modern data lakehouse architecture with three layers:
- **Bronze Layer**: Raw system table data with minimal transformation and full data preservation
- **Silver Layer**: Curated data with SCD2 history tracking, data harmonization, and enhanced metrics
- **Gold Layer**: Analytics-ready dimensions and facts for business intelligence and reporting

> **📖 For detailed architecture information, see [Architecture & Design](docs/architecture.md)**

madhu_obs_pat - 
## 📁 Project Structure

```
dbr-observe/
├── src/
│   ├── sql/
│   │   ├── ddl/                    # Data Definition Language scripts
│   │   │   ├── 01_catalog_schemas.sql      # Catalog and schema setup
│   │   │   ├── 02_permissions_setup.sql    # Access controls
│   │   │   ├── 03_watermark_table.sql      # Watermark management
│   │   │   ├── 11_bronze_billing_tables.sql    # Bronze: Billing tables
│   │   │   ├── 12_bronze_compute_tables.sql    # Bronze: Compute tables
│   │   │   ├── 13_bronze_lakeflow_tables.sql   # Bronze: Lakeflow tables
│   │   │   ├── 14_bronze_query_tables.sql      # Bronze: Query tables
│   │   │   ├── 15_bronze_storage_tables.sql    # Bronze: Storage tables
│   │   │   ├── 16_bronze_access_tables.sql     # Bronze: Access tables
│   │   │   ├── 21_silver_entities.sql          # Silver: Entity tables
│   │   │   ├── 22_silver_workflow_runs.sql     # Silver: Workflow runs
│   │   │   ├── 23_silver_billing_usage.sql     # Silver: Billing usage
│   │   │   ├── 24_silver_query_history.sql     # Silver: Query history
│   │   │   ├── 25_silver_audit_log.sql         # Silver: Audit log
│   │   │   ├── 26_silver_node_usage.sql        # Silver: Node usage
│   │   │   ├── 31_gold_dimensions.sql          # Gold: Dimension tables
│   │   │   └── 32_gold_fact_tables.sql         # Gold: Fact tables
│   │   └── transformations/        # Data transformation scripts
│   │       ├── 41_staging_views.sql            # Staging views
│   │       ├── 42_scd2_functions.sql           # SCD2 functions
│   │       ├── 43_tag_extraction_functions.sql # Tag extraction
│   │       ├── 44_bronze_to_silver_processing.sql # Bronze to Silver
│   │       ├── 45_silver_to_gold_processing.sql   # Silver to Gold
│   │       └── 46_metrics_calculation.sql         # Metrics calculation
│   ├── jobs/                       # Databricks job definitions
│   │   ├── 01_daily_observability_pipeline.py  # Main daily pipeline
│   │   └── 02_watermark_management.py          # Watermark management
│   ├── libraries/                  # Reusable code modules
│   │   └── observability_utils/    # Common utilities
│   └── notebooks/                  # Databricks notebooks
├── tests/                          # Unit and integration tests
├── config/                         # Configuration files
│   ├── environments.json           # Environment-specific settings
│   ├── watermark_config.json       # Watermark processing configuration
│   ├── tag_taxonomy.json          # Tag taxonomy and validation rules
│   └── processing_config.json     # Processing and performance settings
├── docs/                           # Project documentation
│   ├── database_best_practices.md  # Naming standards and best practices
│   ├── tag_extraction_strategy.md  # Tag extraction strategy
│   └── config_folder_guide.md      # Configuration folder guide
├── scripts/                        # Deployment and utility scripts
└── .cursor/                        # Cursor IDE configuration
```

## 🚀 Quick Start

### 1. Environment Setup
```bash
# Run setup scripts in order
src/sql/ddl/01_catalog_schemas.sql
src/sql/ddl/02_permissions_setup.sql
src/sql/ddl/03_watermark_table.sql
```

### 2. Table Creation
```bash
# Create bronze tables (11-16)
src/sql/ddl/11_bronze_billing_tables.sql
src/sql/ddl/12_bronze_compute_tables.sql
src/sql/ddl/13_bronze_lakeflow_tables.sql
src/sql/ddl/14_bronze_query_tables.sql
src/sql/ddl/15_bronze_storage_tables.sql
src/sql/ddl/16_bronze_access_tables.sql

# Create silver tables (21-26)
src/sql/ddl/21_silver_entities.sql
src/sql/ddl/22_silver_workflow_runs.sql
src/sql/ddl/23_silver_billing_usage.sql
src/sql/ddl/24_silver_query_history.sql
src/sql/ddl/25_silver_audit_log.sql
src/sql/ddl/26_silver_node_usage.sql

# Create gold tables (31-32)
src/sql/ddl/31_gold_dimensions.sql
src/sql/ddl/32_gold_fact_tables.sql
```

### 3. Staging and Functions
```bash
# Create staging views and functions (41-43)
src/sql/transformations/41_staging_views.sql
src/sql/transformations/42_scd2_functions.sql
src/sql/transformations/43_tag_extraction_functions.sql
```

### 4. Daily Pipeline Setup
```bash
# Deploy and schedule the daily pipeline
src/jobs/01_daily_observability_pipeline.py
```


## 🔧 Technology Stack

- **Storage**: Delta Lake with Unity Catalog
- **Processing**: Spark SQL and PySpark
- **Orchestration**: Databricks Workflows
- **Monitoring**: Databricks SQL + Custom dashboards
- **Version Control**: Git with Databricks Repos

## 📈 Expected Data Volumes

- **Workspaces**: 4-8 workspaces
- **Jobs**: 100+ jobs
- **Data Retention**: 1 year (bronze), 2 years (silver), 3 years (gold)
- **Processing**: Daily incremental with 2-day lookback

## 📚 Documentation

- [Architecture & Design](docs/architecture.md) - Complete architecture and design details
- [Database Best Practices](docs/database_best_practices.md) - Naming standards and best practices
- [Tag Extraction Strategy](docs/tag_extraction_strategy.md) - Tag extraction strategy
- [Configuration Guide](docs/config_folder_guide.md) - Configuration folder guide
- [Deployment Guide](docs/deployment_guide.md) - Deployment instructions and setup
- [Documentation Reorganization Summary](docs/documentation_reorganization_summary.md) - Documentation structure overview

## 🤝 Contributing

1. Follow the established naming conventions in `docs/database_best_practices.md`
2. Use the cursor rules in `.cursor/rules/dbr-observe.mdc`
3. Ensure all SQL scripts include proper comments and documentation
4. Test all changes in development environment before production deployment

## 📄 License

This project is proprietary and confidential.