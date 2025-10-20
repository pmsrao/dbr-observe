"""
Databricks Observability Platform - Daily Pipeline
==================================================

This job runs the complete observability pipeline daily to process
data from bronze to gold layers with enhanced metrics calculation.

Author: Data Platform Team
Date: December 2024
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ObservabilityPipeline:
    """Main pipeline class for daily observability processing."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the pipeline with Spark session and configuration.
        
        Args:
            spark: Spark session
            config: Pipeline configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.catalog = config.get('catalog', 'obs')
        self.environment = config.get('environment', 'prod')
        
        # Set up logging
        self.spark.sparkContext.setLogLevel("INFO")
        
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def update_watermark(self, source_table: str, target_table: str, 
                        watermark_column: str, watermark_value: str,
                        records_processed: int, processing_duration_ms: int,
                        status: str = 'SUCCESS', error_message: str = None):
        """Update watermark table with processing results."""
        try:
            watermark_data = [
                (source_table, target_table, watermark_column, watermark_value,
                 current_timestamp(), status, error_message, records_processed, processing_duration_ms)
            ]
            
            watermark_schema = StructType([
                StructField("source_table_name", StringType(), True),
                StructField("target_table_name", StringType(), True),
                StructField("watermark_column", StringType(), True),
                StructField("watermark_value", StringType(), True),
                StructField("last_updated", TimestampType(), True),
                StructField("processing_status", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("records_processed", LongType(), True),
                StructField("processing_duration_ms", LongType(), True)
            ])
            
            watermark_df = self.spark.createDataFrame(watermark_data, watermark_schema)
            
            # Upsert watermark
            watermark_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"{self.catalog}.meta.watermarks")
                
            logger.info(f"Updated watermark for {source_table} -> {target_table}")
            
        except Exception as e:
            logger.error(f"Failed to update watermark: {e}")
            raise
    
    def process_bronze_to_silver(self):
        """Process data from bronze to silver layer."""
        logger.info("Starting Bronze to Silver processing...")
        
        try:
            # Process compute entities with SCD2
            logger.info("Processing compute entities...")
            self.spark.sql("CALL obs.meta.merge_compute_entities_scd2()")
            
            # Process workflow entities with SCD2
            logger.info("Processing workflow entities...")
            self.spark.sql("CALL obs.meta.merge_workflow_entities_scd2()")
            
            # Process workflow runs
            logger.info("Processing workflow runs...")
            self.spark.sql("""
                INSERT INTO obs.silver.workflow_runs
                SELECT 
                    workspace_id, workflow_type, workflow_id, run_id, workflow_name,
                    start_time, end_time,
                    CASE 
                        WHEN end_time IS NOT NULL AND start_time IS NOT NULL 
                        THEN TIMESTAMPDIFF(MICROSECOND, start_time, end_time) / 1000
                        ELSE NULL 
                    END as duration_ms,
                    result_state, termination_code, job_parameters, parent_run_id,
                    trigger_type, update_type, performance_target,
                    -- Enhanced runtime attributes (to be calculated later)
                    NULL as cpu_utilization_percent,
                    NULL as memory_utilization_percent,
                    NULL as disk_utilization_percent,
                    NULL as network_utilization_percent,
                    NULL as gpu_utilization_percent,
                    NULL as records_processed,
                    NULL as data_volume_bytes,
                    NULL as data_volume_mb,
                    NULL as throughput_records_per_second,
                    NULL as throughput_mb_per_second,
                    NULL as execution_efficiency,
                    NULL as resource_efficiency,
                    NULL as retry_count,
                    NULL as max_retry_count,
                    NULL as error_message,
                    NULL as error_category,
                    NULL as error_severity,
                    NULL as stack_trace,
                    NULL as last_error_time,
                    NULL as cluster_size,
                    NULL as min_cluster_size,
                    NULL as max_cluster_size,
                    NULL as node_type,
                    NULL as driver_node_type,
                    NULL as worker_node_type,
                    NULL as auto_scaling_enabled,
                    NULL as spot_instances_used,
                    NULL as dbu_consumed,
                    NULL as dbu_per_hour,
                    NULL as cost_usd,
                    NULL as cost_per_hour_usd,
                    NULL as cost_per_record_usd,
                    NULL as cost_efficiency_score,
                    NULL as input_files_count,
                    NULL as output_files_count,
                    NULL as input_partitions,
                    NULL as output_partitions,
                    NULL as shuffle_read_bytes,
                    NULL as shuffle_write_bytes,
                    NULL as spilled_bytes,
                    NULL as cache_hit_ratio,
                    NULL as dependency_wait_time_ms,
                    NULL as queue_wait_time_ms,
                    NULL as resource_wait_time_ms,
                    NULL as parent_workflow_id,
                    NULL as child_workflow_count,
                    NULL as parallel_execution_count,
                    NULL as success_rate,
                    NULL as failure_rate,
                    NULL as avg_duration_ms,
                    NULL as duration_variance,
                    NULL as reliability_score,
                    NULL as sla_compliance,
                    processing_timestamp
                FROM obs.silver.workflow_runs_staging
            """)
            
            # Process billing usage with upsert
            logger.info("Processing billing usage...")
            self.spark.sql("""
                MERGE INTO obs.silver.billing_usage AS target
                USING (
                    SELECT 
                        record_id, workspace_id, sku_name, cloud,
                        usage_start_time, usage_end_time, usage_date,
                        usage_unit, usage_quantity, usage_type,
                        custom_tags, usage_metadata, identity_metadata,
                        record_type, ingestion_date, billing_origin_product,
                        product_features, processing_timestamp
                    FROM obs.silver.billing_usage_staging
                ) AS source
                ON target.record_id = source.record_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        usage_quantity = source.usage_quantity,
                        usage_metadata = source.usage_metadata,
                        processing_timestamp = source.processing_timestamp
                WHEN NOT MATCHED THEN 
                    INSERT (
                        record_id, workspace_id, sku_name, cloud, usage_start_time, usage_end_time,
                        usage_date, usage_unit, usage_quantity, usage_type, custom_tags, usage_metadata,
                        identity_metadata, record_type, ingestion_date, billing_origin_product,
                        product_features, processing_timestamp
                    )
                    VALUES (
                        source.record_id, source.workspace_id, source.sku_name, source.cloud, 
                        source.usage_start_time, source.usage_end_time, source.usage_date, 
                        source.usage_unit, source.usage_quantity, source.usage_type, 
                        source.custom_tags, source.usage_metadata, source.identity_metadata,
                        source.record_type, source.ingestion_date, source.billing_origin_product,
                        source.product_features, source.processing_timestamp
                    )
            """)
            
            # Process query history
            logger.info("Processing query history...")
            self.spark.sql("""
                INSERT INTO obs.silver.query_history
                SELECT 
                    workspace_id, statement_id, session_id, execution_status,
                    statement_text, statement_type, error_message,
                    executed_by_user_id, executed_by, executed_as, executed_as_user_id,
                    start_time, end_time, total_duration_ms,
                    waiting_for_compute_duration_ms, waiting_at_capacity_duration_ms,
                    execution_duration_ms, compilation_duration_ms, total_task_duration_ms,
                    result_fetch_duration_ms, read_partitions, pruned_files, read_files,
                    read_rows, produced_rows, read_bytes, read_io_cache_percent,
                    spilled_local_bytes, written_bytes, written_rows, written_files,
                    shuffle_read_bytes, from_result_cache, cache_origin_statement_id,
                    client_application, client_driver, query_source, query_parameters,
                    processing_timestamp
                FROM obs.silver.query_history_staging
            """)
            
            logger.info("Bronze to Silver processing completed successfully!")
            
        except Exception as e:
            logger.error(f"Bronze to Silver processing failed: {e}")
            raise
    
    def process_silver_to_gold(self):
        """Process data from silver to gold layer."""
        logger.info("Starting Silver to Gold processing...")
        
        try:
            # Process dimension tables
            logger.info("Processing dimension tables...")
            self.spark.sql("""
                INSERT INTO obs.gold.dim_compute
                SELECT 
                    SHA2(CONCAT(workspace_id, '|', compute_type, '|', compute_id), 256) as compute_sk,
                    workspace_id, compute_type, compute_id, name, owner,
                    driver_node_type, worker_node_type, worker_count,
                    min_autoscale_workers, max_autoscale_workers, auto_termination_minutes,
                    enable_elastic_disk, data_security_mode, policy_id, dbr_version,
                    cluster_source, warehouse_type, warehouse_size, warehouse_channel,
                    min_clusters, max_clusters, auto_stop_minutes, tags,
                    create_time, delete_time, effective_start_ts, effective_end_ts,
                    is_current, dw_created_ts, dw_updated_ts
                FROM obs.silver.compute_entities
                WHERE is_current = true
            """)
            
            # Process fact tables
            logger.info("Processing fact tables...")
            self.spark.sql("""
                INSERT INTO obs.gold.fct_billing_usage_hourly
                SELECT 
                    SHA2(CONCAT(record_id, '|', workspace_id, '|', usage_start_time), 256) as billing_sk,
                    SHA2(CONCAT(workspace_id, '|', 'CLUSTER', '|', COALESCE(usage_metadata['cluster_id'], 'unknown')), 256) as compute_sk,
                    SHA2(CONCAT(workspace_id, '|', 'JOB', '|', COALESCE(usage_metadata['job_id'], 'unknown')), 256) as workflow_sk,
                    SHA2(COALESCE(identity_metadata.run_as, identity_metadata.owned_by, identity_metadata.created_by), 256) as user_sk,
                    SHA2(CONCAT(sku_name, '|', cloud), 256) as sku_sk,
                    usage_date, HOUR(usage_start_time) as usage_hour,
                    usage_start_time, usage_end_time, workspace_id, sku_name, cloud,
                    usage_quantity, usage_unit, usage_type,
                    NULL as list_price, NULL as total_cost,
                    obs.meta.extract_cost_center(custom_tags) as cost_center,
                    obs.meta.extract_business_unit(custom_tags) as business_unit,
                    obs.meta.extract_department(custom_tags) as department,
                    obs.meta.extract_data_product(custom_tags) as data_product,
                    obs.meta.extract_environment(custom_tags) as environment,
                    obs.meta.extract_team(custom_tags) as team,
                    obs.meta.extract_project(custom_tags) as project,
                    record_type, billing_origin_product, processing_timestamp
                FROM obs.silver.billing_usage
            """)
            
            logger.info("Silver to Gold processing completed successfully!")
            
        except Exception as e:
            logger.error(f"Silver to Gold processing failed: {e}")
            raise
    
    def calculate_metrics(self):
        """Calculate enhanced metrics for runtime analysis."""
        logger.info("Starting metrics calculation...")
        
        try:
            # Calculate workflow run metrics
            logger.info("Calculating workflow run metrics...")
            self.spark.sql("""
                UPDATE obs.silver.workflow_runs
                SET 
                    cpu_utilization_percent = (
                        SELECT AVG(cpu_user_percent + cpu_system_percent)
                        FROM obs.silver.node_usage_minutely n
                        WHERE n.cluster_id = obs.silver.workflow_runs.workflow_id
                          AND n.start_time >= obs.silver.workflow_runs.start_time
                          AND n.start_time <= obs.silver.workflow_runs.end_time
                    ),
                    memory_utilization_percent = (
                        SELECT AVG(mem_used_percent)
                        FROM obs.silver.node_usage_minutely n
                        WHERE n.cluster_id = obs.silver.workflow_runs.workflow_id
                          AND n.start_time >= obs.silver.workflow_runs.start_time
                          AND n.start_time <= obs.silver.workflow_runs.end_time
                    ),
                    success_rate = (
                        CASE 
                            WHEN result_state = 'SUCCEEDED' THEN 1.0
                            WHEN result_state = 'FAILED' THEN 0.0
                            ELSE NULL
                        END
                    ),
                    reliability_score = (
                        CASE 
                            WHEN result_state = 'SUCCEEDED' THEN 1.0
                            WHEN result_state = 'FAILED' THEN 0.0
                            ELSE NULL
                        END
                    )
                WHERE processing_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 day'
            """)
            
            logger.info("Metrics calculation completed successfully!")
            
        except Exception as e:
            logger.error(f"Metrics calculation failed: {e}")
            raise
    
    def run_pipeline(self):
        """Run the complete daily observability pipeline."""
        start_time = datetime.now()
        logger.info(f"Starting daily observability pipeline at {start_time}")
        
        try:
            # Step 1: Bronze to Silver processing
            self.process_bronze_to_silver()
            
            # Step 2: Silver to Gold processing
            self.process_silver_to_gold()
            
            # Step 3: Metrics calculation
            self.calculate_metrics()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() * 1000
            
            logger.info(f"Daily observability pipeline completed successfully in {duration:.2f}ms")
            
            # Update pipeline status
            self.update_watermark(
                source_table="pipeline_status",
                target_table="pipeline_status",
                watermark_column="last_run_time",
                watermark_value=end_time.isoformat(),
                records_processed=0,
                processing_duration_ms=int(duration),
                status="SUCCESS"
            )
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() * 1000
            
            logger.error(f"Daily observability pipeline failed after {duration:.2f}ms: {e}")
            
            # Update pipeline status with error
            self.update_watermark(
                source_table="pipeline_status",
                target_table="pipeline_status",
                watermark_column="last_run_time",
                watermark_value=end_time.isoformat(),
                records_processed=0,
                processing_duration_ms=int(duration),
                status="FAILED",
                error_message=str(e)
            )
            
            raise


def main():
    """Main entry point for the daily observability pipeline."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Daily Observability Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Load configuration
        config = {
            "catalog": "obs",
            "environment": "prod"
        }
        
        # Initialize and run pipeline
        pipeline = ObservabilityPipeline(spark, config)
        pipeline.run_pipeline()
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
