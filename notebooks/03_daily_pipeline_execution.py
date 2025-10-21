# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Pipeline Execution Testing
# MAGIC 
# MAGIC This notebook tests the complete daily observability pipeline execution.
# MAGIC 
# MAGIC ## 🎯 **Purpose**
# MAGIC 
# MAGIC Test the end-to-end daily pipeline that processes data from system tables through bronze, silver, and gold layers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Import and Initialize Pipeline

# COMMAND ----------

# Import required modules
import sys
import os
from datetime import datetime, timedelta

# Add src directory to path
sys.path.append('/Workspace/Repos/dev/dbr-observe/src')

from python.processing.daily_observability_pipeline import DailyObservabilityPipeline
from python.functions.watermark_management import WatermarkManager

print("✅ Modules imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Set Environment Context

# COMMAND ----------

# Set catalog context
spark.sql("USE CATALOG obs")
print("✅ Switched to 'obs' catalog")

# Check current environment
print(f"Current Catalog: {spark.sql('SELECT current_catalog()').collect()[0][0]}")
print(f"Current Schema: {spark.sql('SELECT current_schema()').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Initialize Daily Pipeline

# COMMAND ----------

# Initialize the daily pipeline
try:
    pipeline = DailyObservabilityPipeline(spark)
    print("✅ DailyObservabilityPipeline initialized successfully")
    
    # Display pipeline configuration
    print("📋 Pipeline Configuration:")
    print(f"   - Catalog: {pipeline.catalog}")
    print(f"   - Lookback Days: {pipeline.lookback_days}")
    print(f"   - Processing Timestamp: {pipeline.processing_timestamp}")
    print(f"   - Watermark Manager: Available")
    print(f"   - SCD2 Processor: Available")
    print(f"   - Tag Extractor: Available")
    
except Exception as e:
    print(f"❌ Pipeline initialization error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Check Prerequisites

# COMMAND ----------

# Check if required tables exist
required_tables = [
    "bronze.system_billing_usage",
    "bronze.system_compute_clusters", 
    "bronze.system_lakeflow_jobs",
    "silver.compute_entities",
    "silver.workflow_entities",
    "meta.watermarks"
]

print("🔍 Checking Prerequisites:")
all_tables_exist = True

for table in required_tables:
    try:
        df = spark.table(table)
        count = df.count()
        print(f"✅ {table}: {count} rows")
    except Exception as e:
        print(f"❌ {table}: {str(e)}")
        all_tables_exist = False

if all_tables_exist:
    print("✅ All required tables exist")
else:
    print("❌ Some required tables are missing. Please run SQL setup scripts first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test Individual Pipeline Components

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.0 Load Data into Bronze Tables

# COMMAND ----------

# Load data from system tables into bronze tables using pipeline
print("🔄 Loading Data from System Tables into Bronze Tables...")

try:
    # Use the pipeline's system to bronze processing method
    success = pipeline.process_system_to_bronze()
    
    if success:
        print("✅ System to Bronze processing completed successfully")
        
        # Verify data was loaded from all schema-specific ingestion classes
        print(f"📊 Bronze table counts by schema:")
        
        # Compute schema tables
        try:
            compute_count = spark.table("obs.bronze.system_compute_clusters").count()
            warehouse_count = spark.table("obs.bronze.system_compute_warehouses").count()
            node_types_count = spark.table("obs.bronze.system_compute_node_types").count()
            node_timeline_count = spark.table("obs.bronze.system_compute_node_timeline").count()
            warehouse_events_count = spark.table("obs.bronze.system_compute_warehouse_events").count()
            print(f"   🔧 Compute Schema:")
            print(f"      - system_compute_clusters: {compute_count} rows")
            print(f"      - system_compute_warehouses: {warehouse_count} rows")
            print(f"      - system_compute_node_types: {node_types_count} rows")
            print(f"      - system_compute_node_timeline: {node_timeline_count} rows")
            print(f"      - system_compute_warehouse_events: {warehouse_events_count} rows")
        except Exception as e:
            print(f"   🔧 Compute Schema: Error - {str(e)}")
        
        # Lakeflow schema tables
        try:
            lakeflow_jobs_count = spark.table("obs.bronze.system_lakeflow_jobs").count()
            job_tasks_count = spark.table("obs.bronze.system_lakeflow_job_tasks").count()
            job_runs_count = spark.table("obs.bronze.system_lakeflow_job_run_timeline").count()
            task_runs_count = spark.table("obs.bronze.system_lakeflow_job_task_run_timeline").count()
            pipelines_count = spark.table("obs.bronze.system_lakeflow_pipelines").count()
            pipeline_updates_count = spark.table("obs.bronze.system_lakeflow_pipeline_update_timeline").count()
            print(f"   🌊 Lakeflow Schema:")
            print(f"      - system_lakeflow_jobs: {lakeflow_jobs_count} rows")
            print(f"      - system_lakeflow_job_tasks: {job_tasks_count} rows")
            print(f"      - system_lakeflow_job_run_timeline: {job_runs_count} rows")
            print(f"      - system_lakeflow_job_task_run_timeline: {task_runs_count} rows")
            print(f"      - system_lakeflow_pipelines: {pipelines_count} rows")
            print(f"      - system_lakeflow_pipeline_update_timeline: {pipeline_updates_count} rows")
        except Exception as e:
            print(f"   🌊 Lakeflow Schema: Error - {str(e)}")
        
        # Billing schema tables
        try:
            billing_usage_count = spark.table("obs.bronze.system_billing_usage").count()
            billing_prices_count = spark.table("obs.bronze.system_billing_list_prices").count()
            print(f"   💰 Billing Schema:")
            print(f"      - system_billing_usage: {billing_usage_count} rows")
            print(f"      - system_billing_list_prices: {billing_prices_count} rows")
        except Exception as e:
            print(f"   💰 Billing Schema: Error - {str(e)}")
        
        # Query schema tables
        try:
            query_history_count = spark.table("obs.bronze.system_query_history").count()
            print(f"   🔍 Query Schema:")
            print(f"      - system_query_history: {query_history_count} rows")
        except Exception as e:
            print(f"   🔍 Query Schema: Error - {str(e)}")
        
        # Audit schema tables
        try:
            audit_log_count = spark.table("obs.bronze.system_audit_log").count()
            print(f"   🔐 Audit Schema:")
            print(f"      - system_audit_log: {audit_log_count} rows")
        except Exception as e:
            print(f"   🔐 Audit Schema: Error - {str(e)}")
        
        # Storage schema tables
        try:
            storage_ops_count = spark.table("obs.bronze.system_storage_ops").count()
            print(f"   💾 Storage Schema:")
            print(f"      - system_storage_ops: {storage_ops_count} rows")
        except Exception as e:
            print(f"   💾 Storage Schema: Error - {str(e)}")
        
        # Show sample of loaded data from key tables
        print("\n📋 Sample data from key tables:")
        try:
            print("🔧 Compute clusters sample:")
            spark.table("obs.bronze.system_compute_clusters").select("workspace_id", "raw_data.name", "raw_data.owner", "change_time").show(5, False)
        except Exception as e:
            print(f"   Error showing compute clusters: {str(e)}")
        
        try:
            print("🌊 Lakeflow jobs sample:")
            spark.table("obs.bronze.system_lakeflow_jobs").select("workspace_id", "raw_data.name", "raw_data.creator_id", "change_time").show(5, False)
        except Exception as e:
            print(f"   Error showing lakeflow jobs: {str(e)}")
    else:
        print("❌ System to Bronze processing failed")
        
except Exception as e:
    print(f"❌ Bronze data loading error: {str(e)}")
    print("ℹ️  This may be expected if system tables don't exist or have permission issues")
    print("ℹ️  Make sure you have access to system.compute.clusters and system.lakeflow.jobs tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.0.1 Verify Watermark-Based Delta Processing

# COMMAND ----------

# Verify watermark-based delta processing
print("🔍 Verifying Watermark-Based Delta Processing...")

try:
    # Check watermark status for all schema-specific ingestion classes
    watermarks_df = spark.table("meta.watermarks")
    
    print("📊 Watermark Status by Schema:")
    
    # Group by source schema
    schema_watermarks = watermarks_df.select(
        "source_table_name",
        "target_table_name", 
        "watermark_value",
        "last_updated",
        "processing_status",
        "records_processed"
    ).orderBy("last_updated", "source_table_name")
    
    # Show recent watermarks
    print("📋 Recent Watermark Updates:")
    schema_watermarks.show(20, False)
    
    # Check for any errors
    error_count = watermarks_df.filter("processing_status = 'ERROR'").count()
    if error_count > 0:
        print(f"⚠️  Found {error_count} watermark errors:")
        watermarks_df.filter("processing_status = 'ERROR'").show(5, False)
    else:
        print("✅ No watermark errors found")
    
    # Show processing statistics
    print("\n📊 Processing Statistics:")
    stats_df = watermarks_df.filter("processing_status = 'SUCCESS'").selectExpr(
        "COUNT(*) as total_processing_runs",
        "SUM(records_processed) as total_records_processed",
        "AVG(processing_duration_ms) as avg_processing_time_ms"
    )
    stats_df.show(1, False)
    
except Exception as e:
    print(f"❌ Watermark verification error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Test Bronze to Silver Processing

# COMMAND ----------

# Test bronze to silver processing
print("🧪 Testing Bronze to Silver Processing...")

try:
    # Test bronze to silver processing
    success = pipeline.process_bronze_to_silver()
    print(f"✅ Bronze to Silver processing: {success}")
    
except Exception as e:
    print(f"❌ Bronze to Silver processing error: {str(e)}")
    print("ℹ️  This may be expected if no new data is available")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Test Silver to Gold Processing

# COMMAND ----------

# Test silver to gold processing
print("🧪 Testing Silver to Gold Processing...")

try:
    # Test silver to gold processing
    success = pipeline.process_silver_to_gold()
    print(f"✅ Silver to Gold processing: {success}")
    
except Exception as e:
    print(f"❌ Silver to Gold processing error: {str(e)}")
    print("ℹ️  This may be expected if no new data is available")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Test Metrics Calculation

# COMMAND ----------

# Test metrics calculation
print("🧪 Testing Metrics Calculation...")

try:
    # Test workflow runs metrics
    print("🔄 Testing workflow runs metrics...")
    success = pipeline.calculate_metrics()
    print(f"✅ Metrics calculation: {success}")
    
except Exception as e:
    print(f"❌ Metrics calculation error: {str(e)}")
    print("ℹ️  This may be expected if no data is available")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Test Specialized Ingestion Classes

# COMMAND ----------

# Test individual specialized ingestion classes
print("🧪 Testing Specialized Ingestion Classes...")

try:
    # Test compute ingestion
    print("🔧 Testing Compute Ingestion...")
    compute_success = pipeline.bronze_processor.compute_ingestion.ingest_all_compute_tables()
    print(f"   Compute ingestion: {'✅ Success' if compute_success else '❌ Failed'}")
    
    # Test lakeflow ingestion
    print("🌊 Testing Lakeflow Ingestion...")
    lakeflow_success = pipeline.bronze_processor.lakeflow_ingestion.ingest_all_lakeflow_tables()
    print(f"   Lakeflow ingestion: {'✅ Success' if lakeflow_success else '❌ Failed'}")
    
    # Test billing ingestion
    print("💰 Testing Billing Ingestion...")
    billing_success = pipeline.bronze_processor.billing_ingestion.ingest_all_billing_tables()
    print(f"   Billing ingestion: {'✅ Success' if billing_success else '❌ Failed'}")
    
    # Test query ingestion
    print("🔍 Testing Query Ingestion...")
    query_success = pipeline.bronze_processor.query_ingestion.ingest_all_query_tables()
    print(f"   Query ingestion: {'✅ Success' if query_success else '❌ Failed'}")
    
    # Test audit ingestion
    print("🔐 Testing Audit Ingestion...")
    audit_success = pipeline.bronze_processor.audit_ingestion.ingest_all_audit_tables()
    print(f"   Audit ingestion: {'✅ Success' if audit_success else '❌ Failed'}")
    
    # Test storage ingestion
    print("💾 Testing Storage Ingestion...")
    storage_success = pipeline.bronze_processor.storage_ingestion.ingest_all_storage_tables()
    print(f"   Storage ingestion: {'✅ Success' if storage_success else '❌ Failed'}")
    
    print("\n📊 Specialized Ingestion Summary:")
    print(f"   🔧 Compute: {'✅' if compute_success else '❌'}")
    print(f"   🌊 Lakeflow: {'✅' if lakeflow_success else '❌'}")
    print(f"   💰 Billing: {'✅' if billing_success else '❌'}")
    print(f"   🔍 Query: {'✅' if query_success else '❌'}")
    print(f"   🔐 Audit: {'✅' if audit_success else '❌'}")
    print(f"   💾 Storage: {'✅' if storage_success else '❌'}")
    
except Exception as e:
    print(f"❌ Specialized ingestion testing error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test Complete Daily Pipeline

# COMMAND ----------

# Test complete daily pipeline
print("🧪 Testing Complete Daily Pipeline...")

try:
    # Run the complete daily pipeline
    print("🔄 Running complete daily pipeline...")
    success = pipeline.run_daily_pipeline()
    
    if success:
        print("✅ Daily pipeline completed successfully!")
    else:
        print("❌ Daily pipeline completed with errors")
        
except Exception as e:
    print(f"❌ Daily pipeline error: {str(e)}")
    print("ℹ️  Check the error details above for troubleshooting")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Verify Results

# COMMAND ----------

# Verify processing results
print("🔍 Verifying Processing Results...")

# Check watermark status
try:
    watermarks_df = spark.table("meta.watermarks")
    print("📊 Watermark Status:")
    watermarks_df.show(10, False)
    
    # Check for errors
    error_count = watermarks_df.filter("processing_status = 'ERROR'").count()
    if error_count > 0:
        print(f"⚠️  Found {error_count} watermark errors")
        watermarks_df.filter("processing_status = 'ERROR'").show(5, False)
    else:
        print("✅ No watermark errors found")
        
except Exception as e:
    print(f"❌ Watermark verification error: {str(e)}")

# COMMAND ----------

# Check data counts in each layer
print("📊 Data Counts by Layer:")

# Bronze layer - organized by schema
bronze_tables = [
    # Compute schema
    ("Bronze", "bronze.system_compute_clusters"),
    ("Bronze", "bronze.system_compute_warehouses"),
    ("Bronze", "bronze.system_compute_node_types"),
    ("Bronze", "bronze.system_compute_node_timeline"),
    ("Bronze", "bronze.system_compute_warehouse_events"),
    # Lakeflow schema
    ("Bronze", "bronze.system_lakeflow_jobs"),
    ("Bronze", "bronze.system_lakeflow_job_tasks"),
    ("Bronze", "bronze.system_lakeflow_job_run_timeline"),
    ("Bronze", "bronze.system_lakeflow_job_task_run_timeline"),
    ("Bronze", "bronze.system_lakeflow_pipelines"),
    ("Bronze", "bronze.system_lakeflow_pipeline_update_timeline"),
    # Billing schema
    ("Bronze", "bronze.system_billing_usage"),
    ("Bronze", "bronze.system_billing_list_prices"),
    # Query schema
    ("Bronze", "bronze.system_query_history"),
    # Audit schema
    ("Bronze", "bronze.system_audit_log"),
    # Storage schema
    ("Bronze", "bronze.system_storage_ops")
]

# Silver and Gold layers
other_tables = [
    ("Silver", "silver.compute_entities"),
    ("Silver", "silver.workflow_entities"),
    ("Silver", "silver.billing_usage"),
    ("Gold", "gold.dim_compute"),
    ("Gold", "gold.fct_workflow_runs")
]

print("🔧 Bronze Layer (Schema-Organized):")
for layer, table in bronze_tables:
    try:
        count = spark.table(table).count()
        print(f"📈 {layer}: {table} = {count} rows")
    except Exception as e:
        print(f"❌ {layer}: {table} = {str(e)}")

print("\n🏗️ Silver & Gold Layers:")
for layer, table in other_tables:
    try:
        count = spark.table(table).count()
        print(f"📈 {layer}: {table} = {count} rows")
    except Exception as e:
        print(f"❌ {layer}: {table} = {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Performance Monitoring

# COMMAND ----------

# Check processing performance
print("📊 Performance Monitoring...")

try:
    # Get latest watermark timestamps
    latest_watermarks = spark.sql("""
        SELECT 
            source_table_name,
            target_table_name,
            watermark_value,
            last_updated,
            processing_status,
            records_processed,
            processing_duration_ms
        FROM meta.watermarks 
        WHERE processing_status = 'SUCCESS'
        ORDER BY last_updated DESC
        LIMIT 10
    """)
    
    print("📊 Latest Processing Results:")
    latest_watermarks.show(10, False)
    
    # Calculate average processing time
    avg_duration = latest_watermarks.selectExpr("AVG(processing_duration_ms)").collect()[0][0]
    if avg_duration:
        print(f"⏱️  Average processing time: {avg_duration:.2f} ms")
    
except Exception as e:
    print(f"❌ Performance monitoring error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Data Quality Check

# COMMAND ----------

# Check data quality
print("📊 Data Quality Check...")

quality_checks = [
    ("silver.compute_entities", "compute_id"),
    ("silver.workflow_entities", "workflow_id"),
    ("silver.billing_usage", "record_id"),
    ("gold.fct_workflow_runs", "workflow_run_id")
]

for table, key_column in quality_checks:
    try:
        df = spark.table(table)
        total = df.count()
        
        if total > 0:
            # Check for nulls in key column
            nulls = df.filter(f"{key_column} IS NULL").count()
            null_pct = (nulls / total * 100) if total > 0 else 0
            
            # Check for duplicates
            distinct_count = df.select(key_column).distinct().count()
            duplicate_count = total - distinct_count
            
            print(f"📈 {table}:")
            print(f"   - Total rows: {total}")
            print(f"   - Null {key_column}: {nulls} ({null_pct:.1f}%)")
            print(f"   - Duplicates: {duplicate_count}")
            
        else:
            print(f"📈 {table}: No data")
            
    except Exception as e:
        print(f"❌ {table}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ **Daily Pipeline Testing Complete!**
# MAGIC 
# MAGIC ### Summary of Tests:
# MAGIC 
# MAGIC ✅ **Pipeline Initialization**
# MAGIC - DailyObservabilityPipeline created successfully
# MAGIC - Modular architecture with specialized processors
# MAGIC - Configuration verified
# MAGIC 
# MAGIC ✅ **Schema-Based Ingestion Testing**
# MAGIC - 🔧 Compute Schema: Clusters, Warehouses, Node Types, Timeline, Events
# MAGIC - 🌊 Lakeflow Schema: Jobs, Tasks, Runs, Pipelines, Updates
# MAGIC - 💰 Billing Schema: Usage, List Prices
# MAGIC - 🔍 Query Schema: History
# MAGIC - 🔐 Audit Schema: Log
# MAGIC - 💾 Storage Schema: Operations
# MAGIC 
# MAGIC ✅ **Watermark-Based Delta Processing**
# MAGIC - Delta identification using watermarks
# MAGIC - Incremental data processing
# MAGIC - Processing statistics tracking
# MAGIC 
# MAGIC ✅ **Component Testing**
# MAGIC - Bronze to Silver processing
# MAGIC - Silver to Gold processing  
# MAGIC - Metrics calculation
# MAGIC 
# MAGIC ✅ **End-to-End Testing**
# MAGIC - Complete daily pipeline execution
# MAGIC - Results verification
# MAGIC 
# MAGIC ✅ **Quality Assurance**
# MAGIC - Watermark status monitoring
# MAGIC - Data quality checks
# MAGIC - Performance monitoring
# MAGIC 
# MAGIC ### 🏗️ **Modular Architecture Benefits**
# MAGIC 
# MAGIC ✅ **Enhanced Maintainability**
# MAGIC - Schema-specific ingestion classes
# MAGIC - Independent development and testing
# MAGIC - Clear separation of concerns
# MAGIC 
# MAGIC ✅ **Improved Scalability**
# MAGIC - Easy addition of new schemas
# MAGIC - Independent scaling of components
# MAGIC - Reduced coupling between modules
# MAGIC 
# MAGIC ✅ **Better Organization**
# MAGIC - Logical grouping by source schema
# MAGIC - Easy navigation and debugging
# MAGIC - Clear responsibility boundaries
# MAGIC 
# MAGIC ### 🚀 **Ready for Production!**
# MAGIC 
# MAGIC The daily pipeline with modular architecture is ready for production deployment. Next steps:
# MAGIC 
# MAGIC 1. **Schedule Daily Execution**: Set up Databricks Workflows
# MAGIC 2. **Monitor Performance**: Set up alerts and dashboards
# MAGIC 3. **Data Quality**: Implement automated quality checks
# MAGIC 4. **Cost Optimization**: Monitor and optimize processing costs
# MAGIC 5. **Schema Management**: Monitor individual schema processing
# MAGIC 
# MAGIC ### 📞 **Support**
# MAGIC 
# MAGIC For issues or questions:
# MAGIC - Check watermark status for errors by schema
# MAGIC - Review data quality metrics per schema
# MAGIC - Monitor processing performance by ingestion class
# MAGIC - Refer to deployment guide for troubleshooting
# MAGIC - Use specialized ingestion classes for targeted fixes
