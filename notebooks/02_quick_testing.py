# Databricks notebook source
# MAGIC %md
# MAGIC # Quick Testing - Observability Platform
# MAGIC 
# MAGIC Quick testing notebook for the observability platform components.
# MAGIC 
# MAGIC ## 🚀 **Quick Start Testing**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Environment Check

# COMMAND ----------

# Check current environment
print("🔍 Environment Check:")
print(f"Current Catalog: {spark.sql('SELECT current_catalog()').collect()[0][0]}")
print(f"Current Schema: {spark.sql('SELECT current_schema()').collect()[0][0]}")

# Set to obs catalog
spark.sql("USE CATALOG obs")
print("✅ Switched to 'obs' catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Table Existence Check

# COMMAND ----------

# Quick table check
tables_to_check = [
    "bronze.system_billing_usage",
    "silver.compute_entities", 
    "silver.workflow_entities",
    "gold.dim_compute",
    "meta.watermarks"
]

print("🔍 Quick Table Check:")
for table in tables_to_check:
    try:
        count = spark.table(table).count()
        print(f"✅ {table}: {count} rows")
    except Exception as e:
        print(f"❌ {table}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: System Table Access

# COMMAND ----------

# Check system tables
spark.sql("USE CATALOG system")
print("✅ Switched to system catalog")

system_tables = ["billing.usage", "compute.clusters", "query.history"]

print("🔍 System Table Check:")
for table in system_tables:
    try:
        count = spark.table(table).count()
        print(f"✅ system.{table}: {count} rows")
    except Exception as e:
        print(f"❌ system.{table}: {str(e)}")

# Switch back
spark.sql("USE CATALOG obs")
print("✅ Switched back to obs catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: PySpark Functions Test

# COMMAND ----------

# Import and test PySpark functions
import sys
sys.path.append('/Workspace/Repos/your-username/dbr-observe/src')

try:
    from python.functions.watermark_management import WatermarkManager
    from python.functions.scd2_processing import SCD2Processor
    from python.functions.tag_extraction import TagExtractor
    
    print("✅ PySpark functions imported successfully")
    
    # Test watermark manager
    watermark_manager = WatermarkManager(spark)
    print("✅ WatermarkManager initialized")
    
    # Test tag extractor
    tag_extractor = TagExtractor(spark)
    print("✅ TagExtractor initialized")
    
    # Test SCD2 processor
    scd2_processor = SCD2Processor(spark)
    print("✅ SCD2Processor initialized")
    
except Exception as e:
    print(f"❌ PySpark functions error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Watermark Status

# COMMAND ----------

# Check watermark status
try:
    watermarks_df = spark.table("meta.watermarks")
    print("📊 Watermark Status:")
    watermarks_df.show(10, False)
    
    # Check for errors
    error_count = watermarks_df.filter("processing_status = 'ERROR'").count()
    if error_count > 0:
        print(f"⚠️  Found {error_count} errors")
    else:
        print("✅ No watermark errors")
        
except Exception as e:
    print(f"❌ Watermark check error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Data Quality Check

# COMMAND ----------

# Quick data quality check
quality_checks = [
    ("silver.compute_entities", "compute_id"),
    ("silver.workflow_entities", "workflow_id"),
    ("silver.billing_usage", "record_id")
]

print("📊 Data Quality Check:")
for table, column in quality_checks:
    try:
        df = spark.table(table)
        total = df.count()
        nulls = df.filter(f"{column} IS NULL").count()
        pct = (nulls / total * 100) if total > 0 else 0
        print(f"📈 {table}.{column}: {nulls}/{total} nulls ({pct:.1f}%)")
    except Exception as e:
        print(f"❌ {table}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ **Quick Test Complete!**
# MAGIC 
# MAGIC This quick test has verified:
# MAGIC - ✅ Environment setup
# MAGIC - ✅ Table existence
# MAGIC - ✅ System table access
# MAGIC - ✅ PySpark functions
# MAGIC - ✅ Watermark status
# MAGIC - ✅ Data quality
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC 1. **For detailed testing**: Run `01_observability_platform_testing.py`
# MAGIC 2. **For production**: Deploy daily workflows
# MAGIC 3. **For monitoring**: Set up alerts and dashboards
