# Deployment Guide

> **Navigation**: [README](../README.md) | [Architecture & Design](architecture.md) | [Database Best Practices](database_best_practices.md) | [Tag Extraction Strategy](tag_extraction_strategy.md) | [Configuration Guide](config_folder_guide.md) | [Documentation Reorganization Summary](documentation_reorganization_summary.md)

## 🚀 **Platform Deployment**

### **Deployment Overview**
The observability platform has **two distinct phases**:
1. **Setup Phase** (Run Once): Create catalog, schemas, tables, and views
2. **Processing Phase** (Run Daily): Process data using PySpark functions

### **Phase 1: Initial Setup (Run Once)**

#### **Environment Setup**
```bash
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-access-token"
export DATABRICKS_WAREHOUSE_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
```

#### **Run Setup Script**
```bash
# Run complete setup (files 1-43, excludes processing files 44-46)
./scripts/run_sql_setup.sh

# Or run with Python connector
python scripts/run_all_sql.py
```

#### **Setup Files Included**
- ✅ **Catalog & Schemas**: `01_catalog_schemas.sql`
- ✅ **Permissions**: `02_permissions_setup.sql`
- ✅ **Watermark Management**: `03_watermark_table.sql`
- ✅ **Bronze Tables**: `11_bronze_billing_tables.sql` through `16_bronze_access_tables.sql`
- ✅ **Silver Tables**: `21_silver_entities.sql` through `26_silver_node_usage.sql`
- ✅ **Gold Tables**: `31_gold_dimensions.sql` through `32_gold_fact_tables.sql`
- ✅ **Staging Views**: `41_staging_views.sql`
- ✅ **Function Documentation**: `42_scd2_functions.sql`, `43_tag_extraction_functions.sql`

### **Phase 2: Daily Processing (Run Daily)**

#### **PySpark Functions Available**
- ✅ **Watermark Management**: `src/python/functions/watermark_management.py`
- ✅ **SCD2 Processing**: `src/python/functions/scd2_processing.py`
- ✅ **Tag Extraction**: `src/python/functions/tag_extraction.py`
- ✅ **Daily Pipeline**: `src/python/processing/daily_observability_pipeline.py`

#### **Daily Processing Options**

**Option 1: Complete Daily Pipeline**
```python
from src.python.processing.daily_observability_pipeline import DailyObservabilityPipeline
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("Daily Observability Pipeline").getOrCreate()

# Run complete pipeline
pipeline = DailyObservabilityPipeline(spark)
success = pipeline.run_daily_pipeline()
```

**Option 2: Individual Processing Steps**
```python
# Bronze to Silver processing
pipeline.process_bronze_to_silver()

# Silver to Gold processing  
pipeline.process_silver_to_gold()

# Metrics calculation
pipeline.calculate_metrics()
```

**Option 3: Individual Functions**
```python
from src.python.functions.watermark_management import get_watermark, update_watermark
from src.python.functions.scd2_processing import merge_compute_entities_scd2
from src.python.functions.tag_extraction import extract_standard_tags

# Use individual functions as needed
watermark = get_watermark(spark, "system.compute.clusters", "obs.silver.compute_entities", "change_time")
```

### **Processing Architecture**
- **Bronze to Silver**: SCD2 processing with PySpark functions and watermark tracking
- **Silver to Gold**: Star schema creation with tag extraction using PySpark
- **Metrics Calculation**: Enhanced runtime metrics using PySpark DataFrame operations
- **Watermark Management**: Incremental processing with PySpark watermark functions

### **PySpark Function Features**
- ✅ **Watermark Management**: `WatermarkManager` class with get/update operations
- ✅ **SCD2 Processing**: `SCD2Processor` class with merge operations for entities
- ✅ **Tag Extraction**: `TagExtractor` class with standard tag processing
- ✅ **Daily Pipeline**: `DailyObservabilityPipeline` class for orchestration
- ✅ **Error Handling**: Comprehensive logging and error management
- ✅ **Incremental Processing**: Watermark-based processing with resume capability

## 📊 **Expected Outcomes**

### **Setup Phase Results**
- ✅ Catalog and schemas created
- ✅ All bronze, silver, and gold tables created
- ✅ Staging views and documentation ready
- ✅ Watermark management table initialized
- ✅ Platform ready for daily processing

### **Daily Processing Results**
- ✅ Process 2 days of incremental data using PySpark
- ✅ Calculate enhanced runtime metrics with PySpark functions
- ✅ Update cost attribution with tag extraction
- ✅ Maintain data quality scores
- ✅ SCD2 processing for dimension changes

### **Data Volumes**
- **Workspaces**: 4-8 workspaces
- **Jobs**: 100+ jobs
- **Processing Time**: ~30 minutes daily (PySpark optimized)
- **Data Retention**: 1 year (bronze), 2 years (silver), 3 years (gold)

## 🔧 **Configuration**

### **Environment Settings**
- `config/environments.json` - Environment-specific settings
- `config/watermark_config.json` - Processing intervals and lookback periods
- `config/processing_config.json` - Performance and error handling settings

### **PySpark Configuration**
- Spark session configuration for optimal performance
- Adaptive query execution enabled
- Partition coalescing for better performance
- Error handling and retry logic

### **Tag Taxonomy**
- `config/tag_taxonomy.json` - Tag validation and allowed values
- Cost allocation tags (cost_center, business_unit, department)
- Operational tags (environment, team, project)
- PySpark tag extraction with variations support

## 🧪 **Testing & Debugging**

### **Quick Testing Commands**
```bash
# Check all prerequisites
./scripts/quick_test.sh check-all

# Test individual components
./scripts/quick_test.sh test-watermark
./scripts/quick_test.sh test-scd2
./scripts/quick_test.sh test-tags
./scripts/quick_test.sh test-bronze
./scripts/quick_test.sh test-silver
./scripts/quick_test.sh test-metrics
./scripts/quick_test.sh test-full

# Or use Python script directly
python scripts/test_daily_pipeline.py --component watermark
python scripts/test_daily_pipeline.py --check-tables
python scripts/test_daily_pipeline.py --check-data
```

### **Step-by-Step Testing**
For detailed testing instructions, see [Daily Job Testing Guide](daily_job_testing_guide.md).

## 📚 **Documentation**

- [README](README.md) - Main project overview
- [Architecture & Design](architecture.md) - Technical architecture
- [Database Best Practices](database_best_practices.md) - Naming standards
- [Tag Extraction Strategy](tag_extraction_strategy.md) - Tag processing
- [Configuration Guide](config_folder_guide.md) - Config management
- [Daily Job Testing Guide](daily_job_testing_guide.md) - Step-by-step testing

## 🎯 **Success Criteria**

### **Setup Phase**
- ✅ All SQL setup files execute successfully (files 1-43)
- ✅ Catalog, schemas, and tables created
- ✅ Staging views and documentation ready
- ✅ Watermark management system initialized

### **Daily Processing**
- ✅ PySpark functions execute successfully
- ✅ Bronze to silver processing with SCD2
- ✅ Silver to gold processing with tag extraction
- ✅ Enhanced metrics calculated
- ✅ Cost attribution working
- ✅ Data quality maintained
- ✅ Error handling and notifications working

The observability platform is ready for production use with PySpark functions! 🚀
