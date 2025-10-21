# Daily Job Testing Guide

> **Navigation**: [README](../README.md) | [Architecture & Design](architecture.md) | [Database Best Practices](database_best_practices.md) | [Tag Extraction Strategy](tag_extraction_strategy.md) | [Configuration Guide](config_folder_guide.md) | [Deployment Guide](deployment_guide.md)

## 🧪 **Step-by-Step Daily Job Testing**

This guide helps you test the daily observability pipeline step by step for quick iteration and debugging.

### **Prerequisites**
- ✅ Setup phase completed (catalog, schemas, tables created)
- ✅ Databricks workspace access
- ✅ Python environment with PySpark installed

## 🚀 **Step 1: Environment Setup**

### **1.1 Set Environment Variables**
```bash
# Required environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-access-token"
export DATABRICKS_WAREHOUSE_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"

# Optional: Set catalog name (defaults to 'obs')
export OBS_CATALOG="obs"
```

### **1.2 Verify Environment**
```bash
# Test connection to Databricks
python -c "
import os
from databricks import sql
print('✅ Environment variables set')
print(f'Host: {os.getenv(\"DATABRICKS_HOST\")}')
print(f'Token: {\"***\" + os.getenv(\"DATABRICKS_TOKEN\", \"\")[-4:]}')
print(f'Warehouse: {os.getenv(\"DATABRICKS_WAREHOUSE_HTTP_PATH\")}')
"
```

## 🔧 **Step 2: Test Individual PySpark Functions**

### **2.1 Test Watermark Management**
```python
# Create test_watermark_management.py
from pyspark.sql import SparkSession
from src.python.functions.watermark_management import WatermarkManager, get_watermark, update_watermark

# Initialize Spark
spark = SparkSession.builder \
    .appName("Test Watermark Management") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    # Test watermark manager
    manager = WatermarkManager(spark)
    print("✅ WatermarkManager initialized successfully")
    
    # Test getting watermark
    watermark = manager.get_watermark(
        "system.compute.clusters", 
        "obs.silver.compute_entities", 
        "change_time"
    )
    print(f"✅ Current watermark: {watermark}")
    
    # Test updating watermark
    success = manager.update_watermark(
        "system.compute.clusters",
        "obs.silver.compute_entities", 
        "change_time",
        "2024-01-01T00:00:00Z",
        "SUCCESS",
        None,
        100,
        5000
    )
    print(f"✅ Watermark update: {success}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

### **2.2 Test SCD2 Processing**
```python
# Create test_scd2_processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from src.python.functions.scd2_processing import SCD2Processor, merge_compute_entities_scd2

# Initialize Spark
spark = SparkSession.builder \
    .appName("Test SCD2 Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    # Create test data
    test_data = spark.createDataFrame([
        {
            "workspace_id": "test-workspace",
            "compute_type": "CLUSTER",
            "compute_id": "test-cluster-001",
            "name": "Test Cluster",
            "owner": "test-user@company.com",
            "change_time": current_timestamp(),
            "processing_timestamp": current_timestamp()
        }
    ])
    
    # Test SCD2 processor
    processor = SCD2Processor(spark)
    print("✅ SCD2Processor initialized successfully")
    
    # Test merge (this will fail if target table doesn't exist, which is expected)
    try:
        success = processor.merge_compute_entities_scd2(test_data, "obs.silver.compute_entities")
        print(f"✅ SCD2 merge: {success}")
    except Exception as e:
        print(f"⚠️ SCD2 merge failed (expected if target table empty): {str(e)}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

### **2.3 Test Tag Extraction**
```python
# Create test_tag_extraction.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from src.python.functions.tag_extraction import TagExtractor, extract_standard_tags

# Initialize Spark
spark = SparkSession.builder \
    .appName("Test Tag Extraction") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    # Create test data with tags
    test_data = spark.createDataFrame([
        {
            "workspace_id": "test-workspace",
            "tags": {
                "cost_center": "IT",
                "business_unit": "Data Engineering",
                "environment": "dev",
                "team": "Platform"
            }
        }
    ])
    
    # Test tag extractor
    extractor = TagExtractor(spark)
    print("✅ TagExtractor initialized successfully")
    
    # Test standard tag extraction
    result = extractor.extract_standard_tags(test_data, "tags")
    print("✅ Standard tags extracted successfully")
    result.show()
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

## 🔄 **Step 3: Test Daily Pipeline Components**

### **3.1 Test Bronze to Silver Processing**
```python
# Create test_bronze_to_silver.py
from pyspark.sql import SparkSession
from src.python.processing.daily_observability_pipeline import DailyObservabilityPipeline

# Initialize Spark
spark = SparkSession.builder \
    .appName("Test Bronze to Silver") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    # Initialize pipeline
    pipeline = DailyObservabilityPipeline(spark)
    print("✅ DailyObservabilityPipeline initialized successfully")
    
    # Test individual components
    print("🔄 Testing compute entities processing...")
    success = pipeline._process_compute_entities()
    print(f"✅ Compute entities: {success}")
    
    print("🔄 Testing workflow entities processing...")
    success = pipeline._process_workflow_entities()
    print(f"✅ Workflow entities: {success}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

### **3.2 Test Silver to Gold Processing**
```python
# Create test_silver_to_gold.py
from pyspark.sql import SparkSession
from src.python.processing.daily_observability_pipeline import DailyObservabilityPipeline

# Initialize Spark
spark = SparkSession.builder \
    .appName("Test Silver to Gold") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    # Initialize pipeline
    pipeline = DailyObservabilityPipeline(spark)
    print("✅ DailyObservabilityPipeline initialized successfully")
    
    # Test individual components
    print("🔄 Testing dimensions processing...")
    success = pipeline._process_dimensions()
    print(f"✅ Dimensions: {success}")
    
    print("🔄 Testing facts processing...")
    success = pipeline._process_facts()
    print(f"✅ Facts: {success}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

### **3.3 Test Metrics Calculation**
```python
# Create test_metrics_calculation.py
from pyspark.sql import SparkSession
from src.python.processing.daily_observability_pipeline import DailyObservabilityPipeline

# Initialize Spark
spark = SparkSession.builder \
    .appName("Test Metrics Calculation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    # Initialize pipeline
    pipeline = DailyObservabilityPipeline(spark)
    print("✅ DailyObservabilityPipeline initialized successfully")
    
    # Test metrics calculation
    print("🔄 Testing workflow runs metrics...")
    success = pipeline._calculate_workflow_runs_metrics()
    print(f"✅ Workflow runs metrics: {success}")
    
    print("🔄 Testing job task runs metrics...")
    success = pipeline._calculate_job_task_runs_metrics()
    print(f"✅ Job task runs metrics: {success}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

## 🎯 **Step 4: Test Complete Daily Pipeline**

### **4.1 Full Pipeline Test**
```python
# Create test_full_pipeline.py
from pyspark.sql import SparkSession
from src.python.processing.daily_observability_pipeline import DailyObservabilityPipeline

# Initialize Spark
spark = SparkSession.builder \
    .appName("Test Full Daily Pipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

try:
    # Initialize pipeline
    pipeline = DailyObservabilityPipeline(spark)
    print("✅ DailyObservabilityPipeline initialized successfully")
    
    # Run complete pipeline
    print("🔄 Running complete daily pipeline...")
    success = pipeline.run_daily_pipeline()
    
    if success:
        print("🎉 Daily pipeline completed successfully!")
    else:
        print("❌ Daily pipeline failed!")
        
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

## 🐛 **Step 5: Debugging Common Issues**

### **5.1 Check Table Existence**
```python
# Create check_tables.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Check Tables").getOrCreate()

try:
    # Check if tables exist
    tables_to_check = [
        "obs.bronze.system_compute_clusters",
        "obs.silver.compute_entities",
        "obs.silver.workflow_entities",
        "obs.gold.dim_compute",
        "obs.meta.watermarks"
    ]
    
    for table in tables_to_check:
        try:
            df = spark.table(table)
            print(f"✅ {table}: {df.count()} rows")
        except Exception as e:
            print(f"❌ {table}: {str(e)}")
            
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

### **5.2 Check Watermark Status**
```python
# Create check_watermarks.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Check Watermarks").getOrCreate()

try:
    # Check watermark table
    watermarks = spark.table("obs.meta.watermarks")
    print("📊 Current watermarks:")
    watermarks.show(20, False)
    
    # Check watermark counts
    print(f"📊 Total watermarks: {watermarks.count()}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

### **5.3 Test Data Availability**
```python
# Create check_data_availability.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Check Data Availability").getOrCreate()

try:
    # Check system tables
    system_tables = [
        "system.billing.usage",
        "system.compute.clusters", 
        "system.lakeflow.jobs",
        "system.query.history"
    ]
    
    for table in system_tables:
        try:
            df = spark.table(table)
            count = df.count()
            print(f"✅ {table}: {count} rows")
            
            # Show sample data
            if count > 0:
                print(f"📊 Sample from {table}:")
                df.limit(3).show(3, False)
                
        except Exception as e:
            print(f"❌ {table}: {str(e)}")
            
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()
```

## 🚀 **Step 6: Production Deployment**

### **6.1 Create Production Script**
```python
# Create run_daily_pipeline.py
#!/usr/bin/env python3
"""
Production Daily Observability Pipeline
=======================================
"""

import logging
import sys
from pyspark.sql import SparkSession
from src.python.processing.daily_observability_pipeline import DailyObservabilityPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for daily pipeline."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Daily Observability Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize pipeline
        pipeline = DailyObservabilityPipeline(spark)
        logger.info("Daily observability pipeline started")
        
        # Run daily pipeline
        success = pipeline.run_daily_pipeline()
        
        if success:
            logger.info("🎉 Daily observability pipeline completed successfully!")
        else:
            logger.error("❌ Daily observability pipeline failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Error in daily pipeline: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

### **6.2 Run Production Pipeline**
```bash
# Make script executable
chmod +x run_daily_pipeline.py

# Run daily pipeline
python run_daily_pipeline.py
```

## 📋 **Testing Checklist**

### **Pre-Testing**
- [ ] Environment variables set
- [ ] Setup phase completed
- [ ] Tables created successfully
- [ ] System tables accessible

### **Function Testing**
- [ ] Watermark management functions work
- [ ] SCD2 processing functions work
- [ ] Tag extraction functions work
- [ ] Individual pipeline components work

### **Integration Testing**
- [ ] Bronze to silver processing works
- [ ] Silver to gold processing works
- [ ] Metrics calculation works
- [ ] Complete pipeline runs successfully

### **Production Readiness**
- [ ] Error handling works
- [ ] Logging configured
- [ ] Performance acceptable
- [ ] Data quality maintained

## 🎯 **Quick Iteration Tips**

1. **Start Small**: Test individual functions first
2. **Check Data**: Verify system tables have data
3. **Monitor Logs**: Watch for errors and warnings
4. **Test Incrementally**: Add components one by one
5. **Debug Issues**: Use the debugging scripts provided
6. **Validate Results**: Check output tables after each step

## 🆘 **Troubleshooting**

### **Common Issues**
- **Table not found**: Check if setup phase completed
- **Permission denied**: Verify token has access
- **Data not found**: Check if system tables have data
- **Memory issues**: Adjust Spark configuration
- **Timeout errors**: Increase timeout settings

### **Debug Commands**
```bash
# Check Spark UI
# Navigate to: http://your-workspace.cloud.databricks.com/driver-proxy/

# Check logs
tail -f /path/to/spark/logs/

# Check table contents
# Use the check_tables.py script
```

This guide provides a comprehensive approach to testing and debugging the daily observability pipeline! 🚀
