# Databricks Observability Platform

A comprehensive observability solution for Databricks workloads, providing cost allocation, performance monitoring, and governance capabilities.

> **📚 Documentation Navigation**: [Architecture & Design](docs/architecture.md) | [Database Best Practices](docs/database_best_practices.md) | [Tag Extraction Strategy](docs/tag_extraction_strategy.md) | [Configuration Guide](docs/config_folder_guide.md) | [Deployment Guide](docs/deployment_guide.md) | [Daily Job Testing Guide](docs/daily_job_testing_guide.md) | [Project Summary](docs/project_summary.md)

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
│   ├── python/                     # PySpark functions and processing
│   │   ├── functions/              # PySpark function modules
│   │   │   ├── watermark_management.py    # Watermark management functions
│   │   │   ├── scd2_processing.py        # SCD2 processing functions
│   │   │   └── tag_extraction.py         # Tag extraction functions
│   │   └── processing/             # Daily processing pipeline
│   │       └── daily_observability_pipeline.py  # Main daily pipeline
│   ├── jobs/                       # Databricks job definitions (legacy)
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
├── notebooks/                       # Databricks testing notebooks
│   ├── 01_observability_platform_testing.py  # Comprehensive testing
│   ├── 02_quick_testing.py         # Quick health checks
│   ├── 03_daily_pipeline_execution.py        # Pipeline testing
│   └── README.md                   # Notebook usage guide
└── .cursor/                        # Cursor IDE configuration
```

## 🚀 Quick Start

### Phase 1: Initial Setup (Run Once)

#### 1. Environment Setup
```bash
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-access-token"
export DATABRICKS_WAREHOUSE_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
```

#### 2. Run Complete Setup
```bash
# Run all setup files (1-43, excludes processing files 44-46)
./scripts/run_sql_setup.sh

# Or run with Python connector
python scripts/run_all_sql.py
```

#### 3. Setup Files Included
- ✅ **Catalog & Schemas**: `01_catalog_schemas.sql`
- ✅ **Permissions**: `02_permissions_setup.sql`
- ✅ **Watermark Management**: `03_watermark_table.sql`
- ✅ **Bronze Tables**: `11_bronze_billing_tables.sql` through `16_bronze_access_tables.sql`
- ✅ **Silver Tables**: `21_silver_entities.sql` through `26_silver_node_usage.sql`
- ✅ **Gold Tables**: `31_gold_dimensions.sql` through `32_gold_fact_tables.sql`
- ✅ **Staging Views**: `41_staging_views.sql`
- ✅ **Function Documentation**: `42_scd2_functions.sql`, `43_tag_extraction_functions.sql`

### Phase 2: Daily Processing (Run Daily)

#### 1. PySpark Functions Available
- ✅ **Watermark Management**: `src/python/functions/watermark_management.py`
- ✅ **SCD2 Processing**: `src/python/functions/scd2_processing.py`
- ✅ **Tag Extraction**: `src/python/functions/tag_extraction.py`
- ✅ **Daily Pipeline**: `src/python/processing/daily_observability_pipeline.py`

#### 2. Daily Processing Options

**Option A: Complete Daily Pipeline**
```python
from src.python.processing.daily_observability_pipeline import DailyObservabilityPipeline
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("Daily Observability Pipeline").getOrCreate()

# Run complete pipeline
pipeline = DailyObservabilityPipeline(spark)
success = pipeline.run_daily_pipeline()
```

**Option B: Individual Functions**
```python
from src.python.functions.watermark_management import get_watermark, update_watermark
from src.python.functions.scd2_processing import merge_compute_entities_scd2
from src.python.functions.tag_extraction import extract_standard_tags

# Use individual functions as needed
watermark = get_watermark(spark, "system.compute.clusters", "obs.silver.compute_entities", "change_time")
```

### Phase 3: Testing in Databricks

#### 1. Comprehensive Testing (30-45 minutes)
```python
# Run: notebooks/01_observability_platform_testing.py
# Tests all components: environment, tables, functions, processing, quality
```

#### 2. Quick Health Check (5-10 minutes)
```python
# Run: notebooks/02_quick_testing.py  
# Fast verification of core functionality
```

#### 3. Pipeline Execution Testing (15-30 minutes)
```python
# Run: notebooks/03_daily_pipeline_execution.py
# End-to-end pipeline testing and production readiness
```

#### 4. Notebook Usage
- **Update Repository Path**: Change `/Workspace/Repos/your-username/dbr-observe/src` to your actual path
- **Run in Order**: Start with comprehensive testing, then quick checks, then pipeline testing
- **Monitor Results**: Check watermark status, data quality, and performance metrics

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