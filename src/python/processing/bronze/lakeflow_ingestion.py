"""
Databricks Observability Platform - Lakeflow Source Ingestion
============================================================

Bronze layer ingestion for lakeflow-related system tables.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws, struct
import logging

logger = logging.getLogger(__name__)


class LakeflowIngestion:
    """Lakeflow source ingestion for bronze layer."""
    
    def __init__(self, spark: SparkSession, catalog: str, watermark_manager):
        """
        Initialize the lakeflow ingestion processor.
        
        Args:
            spark: Spark session
            catalog: Catalog name
            watermark_manager: Watermark manager instance
        """
        self.spark = spark
        self.catalog = catalog
        self.watermark_manager = watermark_manager
    
    def ingest_all_lakeflow_tables(self) -> bool:
        """
        Ingest all lakeflow-related system tables.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting lakeflow tables ingestion...")
            
            success = True
            
            if not self.ingest_lakeflow_jobs():
                success = False
                
            if not self.ingest_lakeflow_job_tasks():
                success = False
                
            if not self.ingest_lakeflow_job_run_timeline():
                success = False
                
            if not self.ingest_lakeflow_job_task_run_timeline():
                success = False
                
            if not self.ingest_lakeflow_pipelines():
                success = False
                
            if not self.ingest_lakeflow_pipeline_update_timeline():
                success = False
            
            if success:
                logger.info("Lakeflow tables ingestion completed successfully")
            else:
                logger.error("Lakeflow tables ingestion completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in lakeflow tables ingestion: {str(e)}")
            return False
    
    def ingest_lakeflow_jobs(self) -> bool:
        """Ingest lakeflow jobs from system table to bronze."""
        try:
            logger.info("Ingesting lakeflow jobs...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.lakeflow.jobs",
                "obs.bronze.system_lakeflow_jobs", 
                "change_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            lakeflow_source = self.spark.table("system.lakeflow.jobs") \
                .filter(col("change_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            lakeflow_bronze = lakeflow_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("job_id").alias("job_id"),
                    col("workspace_id").alias("workspace_id"),
                    col("name").alias("job_name"),
                    col("creator_id").alias("creator_id"),
                    col("description").alias("description"),
                    lit(None).cast("string").alias("job_type"),
                    lit(None).cast("string").alias("schedule"),
                    lit(None).cast("int").alias("timeout_seconds"),
                    lit(None).cast("int").alias("max_concurrent_runs"),
                    col("tags").alias("tags"),
                    lit(None).cast("timestamp").alias("create_time"),
                    col("delete_time").alias("delete_time"),
                    col("change_time").alias("change_time")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("change_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.lakeflow.jobs").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("job_id"),
                        col("workspace_id"),
                        col("name"),
                        col("creator_id"),
                        col("description"),
                        lit("").alias("job_type"),
                        lit("").alias("schedule"),
                        lit("").alias("timeout_seconds"),
                        lit("").alias("max_concurrent_runs"),
                        col("tags").cast("string")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            lakeflow_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_lakeflow_jobs")
            
            # Update watermark if data was processed
            record_count = lakeflow_bronze.count()
            if record_count > 0:
                latest_timestamp = lakeflow_bronze.select("change_time").orderBy(col("change_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.lakeflow.jobs",
                        "obs.bronze.system_lakeflow_jobs",
                        "change_time",
                        latest_timestamp[0]["change_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for lakeflow jobs: {latest_timestamp[0]['change_time']}")
            
            logger.info(f"Lakeflow jobs ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting lakeflow jobs: {str(e)}")
            return False
    
    def ingest_lakeflow_job_tasks(self) -> bool:
        """Ingest lakeflow job tasks from system table to bronze."""
        try:
            logger.info("Ingesting lakeflow job tasks...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.lakeflow.job_tasks",
                "obs.bronze.system_lakeflow_job_tasks", 
                "change_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            tasks_source = self.spark.table("system.lakeflow.job_tasks") \
                .filter(col("change_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            tasks_bronze = tasks_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("job_id").alias("job_id"),
                    col("task_key").alias("task_key"),
                    col("workspace_id").alias("workspace_id"),
                    lit(None).cast("string").alias("task_type"),
                    col("depends_on_keys").alias("depends_on"),
                    lit(None).cast("map<string,string>").alias("task_parameters"),
                    lit(None).cast("timestamp").alias("create_time"),
                    col("change_time").alias("change_time")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("change_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.lakeflow.job_tasks").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("job_id"),
                        col("task_key"),
                        col("workspace_id"),
                        lit("").alias("task_type")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            tasks_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_lakeflow_job_tasks")
            
            # Update watermark if data was processed
            record_count = tasks_bronze.count()
            if record_count > 0:
                latest_timestamp = tasks_bronze.select("change_time").orderBy(col("change_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.lakeflow.job_tasks",
                        "obs.bronze.system_lakeflow_job_tasks",
                        "change_time",
                        latest_timestamp[0]["change_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for lakeflow job tasks: {latest_timestamp[0]['change_time']}")
            
            logger.info(f"Lakeflow job tasks ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting lakeflow job tasks: {str(e)}")
            return False
    
    def ingest_lakeflow_job_run_timeline(self) -> bool:
        """Ingest lakeflow job run timeline from system table to bronze."""
        try:
            logger.info("Ingesting lakeflow job run timeline...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.lakeflow.job_run_timeline",
                "obs.bronze.system_lakeflow_job_run_timeline", 
                "period_start_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            runs_source = self.spark.table("system.lakeflow.job_run_timeline") \
                .filter(col("period_start_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            runs_bronze = runs_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("workspace_id").alias("workspace_id"),
                    col("job_id").alias("job_id"),
                    col("run_id").alias("run_id"),
                    col("period_start_time").alias("start_time"),
                    col("period_end_time").alias("end_time"),
                    col("result_state").alias("result_state"),
                    col("termination_code").alias("termination_code"),
                    col("job_parameters").alias("job_parameters"),
                    col("parent_run_id").alias("parent_run_id")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("period_start_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.lakeflow.job_run_timeline").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("workspace_id"),
                        col("job_id"),
                        col("run_id"),
                        col("period_start_time").cast("string"),
                        col("period_end_time").cast("string"),
                        col("result_state")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            runs_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_lakeflow_job_run_timeline")
            
            # Update watermark if data was processed
            record_count = runs_bronze.count()
            if record_count > 0:
                latest_timestamp = runs_bronze.select("period_start_time").orderBy(col("period_start_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.lakeflow.job_run_timeline",
                        "obs.bronze.system_lakeflow_job_run_timeline",
                        "period_start_time",
                        latest_timestamp[0]["period_start_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for lakeflow job run timeline: {latest_timestamp[0]['start_time']}")
            
            logger.info(f"Lakeflow job run timeline ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting lakeflow job run timeline: {str(e)}")
            return False
    
    def ingest_lakeflow_job_task_run_timeline(self) -> bool:
        """Ingest lakeflow job task run timeline from system table to bronze."""
        try:
            logger.info("Ingesting lakeflow job task run timeline...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.lakeflow.job_task_run_timeline",
                "obs.bronze.system_lakeflow_job_task_run_timeline", 
                "start_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            task_runs_source = self.spark.table("system.lakeflow.job_task_run_timeline") \
                .filter(col("start_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            task_runs_bronze = task_runs_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("workspace_id").alias("workspace_id"),
                    col("job_id").alias("job_id"),
                    col("run_id").alias("run_id"),
                    col("task_key").alias("task_key"),
                    col("start_time").alias("start_time"),
                    col("end_time").alias("end_time"),
                    col("result_state").alias("result_state"),
                    col("termination_code").alias("termination_code"),
                    col("task_parameters").alias("task_parameters")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("start_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.lakeflow.job_task_run_timeline").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("workspace_id"),
                        col("job_id"),
                        col("run_id"),
                        col("task_key"),
                        col("start_time").cast("string"),
                        col("end_time").cast("string"),
                        col("result_state")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            task_runs_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_lakeflow_job_task_run_timeline")
            
            # Update watermark if data was processed
            record_count = task_runs_bronze.count()
            if record_count > 0:
                latest_timestamp = task_runs_bronze.select("start_time").orderBy(col("start_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.lakeflow.job_task_run_timeline",
                        "obs.bronze.system_lakeflow_job_task_run_timeline",
                        "start_time",
                        latest_timestamp[0]["start_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for lakeflow job task run timeline: {latest_timestamp[0]['start_time']}")
            
            logger.info(f"Lakeflow job task run timeline ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting lakeflow job task run timeline: {str(e)}")
            return False
    
    def ingest_lakeflow_pipelines(self) -> bool:
        """Ingest lakeflow pipelines from system table to bronze."""
        try:
            logger.info("Ingesting lakeflow pipelines...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.lakeflow.pipelines",
                "obs.bronze.system_lakeflow_pipelines", 
                "change_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            pipelines_source = self.spark.table("system.lakeflow.pipelines") \
                .filter(col("change_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            pipelines_bronze = pipelines_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("pipeline_id").alias("pipeline_id"),
                    col("workspace_id").alias("workspace_id"),
                    col("name").alias("name"),
                    col("description").alias("description"),
                    col("creator_id").alias("creator_id"),
                    col("run_as").alias("run_as"),
                    col("pipeline_parameters").alias("pipeline_parameters"),
                    col("tags").alias("tags"),
                    col("create_time").alias("create_time"),
                    col("delete_time").alias("delete_time"),
                    col("change_time").alias("change_time")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("change_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.lakeflow.pipelines").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("pipeline_id"),
                        col("workspace_id"),
                        col("name"),
                        col("description"),
                        col("creator_id"),
                        col("run_as")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            pipelines_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_lakeflow_pipelines")
            
            # Update watermark if data was processed
            record_count = pipelines_bronze.count()
            if record_count > 0:
                latest_timestamp = pipelines_bronze.select("change_time").orderBy(col("change_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.lakeflow.pipelines",
                        "obs.bronze.system_lakeflow_pipelines",
                        "change_time",
                        latest_timestamp[0]["change_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for lakeflow pipelines: {latest_timestamp[0]['change_time']}")
            
            logger.info(f"Lakeflow pipelines ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting lakeflow pipelines: {str(e)}")
            return False
    
    def ingest_lakeflow_pipeline_update_timeline(self) -> bool:
        """Ingest lakeflow pipeline update timeline from system table to bronze."""
        try:
            logger.info("Ingesting lakeflow pipeline update timeline...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.lakeflow.pipeline_update_timeline",
                "obs.bronze.system_lakeflow_pipeline_update_timeline", 
                "start_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            updates_source = self.spark.table("system.lakeflow.pipeline_update_timeline") \
                .filter(col("start_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            updates_bronze = updates_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("workspace_id").alias("workspace_id"),
                    col("pipeline_id").alias("pipeline_id"),
                    col("update_id").alias("update_id"),
                    col("start_time").alias("start_time"),
                    col("end_time").alias("end_time"),
                    col("result_state").alias("result_state"),
                    col("termination_code").alias("termination_code"),
                    col("pipeline_parameters").alias("pipeline_parameters"),
                    col("parent_update_id").alias("parent_update_id")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("start_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.lakeflow.pipeline_update_timeline").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("workspace_id"),
                        col("pipeline_id"),
                        col("update_id"),
                        col("start_time").cast("string"),
                        col("end_time").cast("string"),
                        col("result_state")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            updates_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_lakeflow_pipeline_update_timeline")
            
            # Update watermark if data was processed
            record_count = updates_bronze.count()
            if record_count > 0:
                latest_timestamp = updates_bronze.select("start_time").orderBy(col("start_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.lakeflow.pipeline_update_timeline",
                        "obs.bronze.system_lakeflow_pipeline_update_timeline",
                        "start_time",
                        latest_timestamp[0]["start_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for lakeflow pipeline update timeline: {latest_timestamp[0]['start_time']}")
            
            logger.info(f"Lakeflow pipeline update timeline ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting lakeflow pipeline update timeline: {str(e)}")
            return False
