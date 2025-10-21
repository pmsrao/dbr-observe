# Databricks notebook source
"""
Simple Bronze Cleanup - Adhoc Use
=================================

Cell 1: Truncate bronze tables
Cell 2: Reset/Delete watermarks for bronze tables
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧹 Simple Bronze Cleanup
# MAGIC 
# MAGIC **Cell 1:** Truncate bronze tables  
# MAGIC **Cell 2:** Reset/Delete watermarks for bronze tables

# COMMAND ----------

# Cell 1: Truncate bronze tables
print("🧹 Truncating bronze tables...")

bronze_tables = [
    "obs.bronze.system_compute_clusters",
    "obs.bronze.system_compute_warehouses", 
    "obs.bronze.system_compute_node_types",
    "obs.bronze.system_compute_node_timeline",
    "obs.bronze.system_compute_warehouse_events",
    "obs.bronze.system_lakeflow_jobs",
    "obs.bronze.system_lakeflow_job_tasks",
    "obs.bronze.system_lakeflow_job_run_timeline",
    "obs.bronze.system_lakeflow_job_task_run_timeline",
    "obs.bronze.system_lakeflow_pipelines",
    "obs.bronze.system_lakeflow_pipeline_update_timeline",
    "obs.bronze.system_billing_usage",
    "obs.bronze.system_billing_list_prices",
    "obs.bronze.system_query_history",
    "obs.bronze.system_access_audit",
    "obs.bronze.system_storage_ops"
]

success_count = 0
error_count = 0

for table in bronze_tables:
    try:
        spark.sql(f"TRUNCATE TABLE {table}")
        print(f"✅ Truncated {table}")
        success_count += 1
    except Exception as e:
        print(f"❌ Error truncating {table}: {str(e)}")
        error_count += 1

print(f"📊 Summary: {success_count} tables truncated, {error_count} errors")

# COMMAND ----------

# Cell 2: Reset/Delete watermarks for bronze tables
print("🧹 Cleaning bronze watermarks...")

try:
    # Get all watermark records
    watermarks_df = spark.table("obs.meta.watermarks")
    
    # Filter for bronze table watermarks
    bronze_watermarks = watermarks_df.filter(
        col("target_table_name").like("obs.bronze.%")
    )
    
    # Count records to delete
    record_count = bronze_watermarks.count()
    print(f"📊 Found {record_count} bronze watermark records")
    
    if record_count > 0:
        # Get non-bronze watermarks
        other_watermarks = watermarks_df.filter(
            ~col("target_table_name").like("obs.bronze.%")
        )
        
        # Keep only non-bronze watermarks
        if other_watermarks.count() > 0:
            other_watermarks.write.mode("overwrite").saveAsTable("obs.meta.watermarks")
            print(f"✅ Deleted {record_count} bronze watermark records")
        else:
            # No other watermarks, create empty table
            spark.sql("DROP TABLE IF EXISTS obs.meta.watermarks")
            spark.sql("CREATE TABLE obs.meta.watermarks AS SELECT * FROM obs.meta.watermarks WHERE 1=0")
            print(f"✅ Deleted all {record_count} watermark records")
    else:
        print("ℹ️  No bronze watermarks found")
        
except Exception as e:
    print(f"❌ Error cleaning watermarks: {str(e)}")

print("✅ Bronze cleanup completed!")
