"""
Databricks Observability Platform - SCD2 Processing Functions
============================================================

PySpark functions for Slowly Changing Dimension Type 2 (SCD2) processing.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, coalesce, 
    sha2, concat_ws, max as spark_max, min as spark_min
)
from pyspark.sql.types import TimestampType, StringType, BooleanType
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class SCD2Processor:
    """Handles SCD2 merge operations for dimension tables."""
    
    def __init__(self, spark: SparkSession, catalog: str = "obs"):
        """
        Initialize the SCD2 processor.
        
        Args:
            spark: Spark session
            catalog: Catalog name (default: obs)
        """
        self.spark = spark
        self.catalog = catalog
    
    def merge_compute_entities_scd2(self, 
                                   source_df: DataFrame,
                                   target_table: str = "obs.silver.compute_entities") -> bool:
        """
        Merge compute entities using SCD2 pattern.
        
        Args:
            source_df: Source DataFrame with compute entity data
            target_table: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate record hash for change detection
            source_with_hash = source_df.withColumn(
                "record_hash",
                sha2(
                    concat_ws("|",
                        coalesce(col("workspace_id"), lit("")),
                        coalesce(col("compute_type"), lit("")),
                        coalesce(col("compute_id"), lit("")),
                        coalesce(col("name"), lit("")),
                        coalesce(col("owner"), lit("")),
                        coalesce(col("driver_node_type"), lit("")),
                        coalesce(col("worker_node_type"), lit("")),
                        coalesce(col("worker_count").cast("string"), lit("")),
                        coalesce(col("min_autoscale_workers").cast("string"), lit("")),
                        coalesce(col("max_autoscale_workers").cast("string"), lit("")),
                        coalesce(col("auto_termination_minutes").cast("string"), lit("")),
                        coalesce(col("enable_elastic_disk").cast("string"), lit("")),
                        coalesce(col("data_security_mode"), lit("")),
                        coalesce(col("policy_id"), lit("")),
                        coalesce(col("dbr_version"), lit("")),
                        coalesce(col("cluster_source"), lit("")),
                        coalesce(col("warehouse_type"), lit("")),
                        coalesce(col("warehouse_size"), lit("")),
                        coalesce(col("warehouse_channel"), lit("")),
                        coalesce(col("min_clusters").cast("string"), lit("")),
                        coalesce(col("max_clusters").cast("string"), lit("")),
                        coalesce(col("auto_stop_minutes").cast("string"), lit("")),
                        coalesce(col("tags"), lit(""))
                    ), 256
                )
            )
            
            # Read existing target data
            target_df = self.spark.table(target_table)
            
            # Find records that need to be updated (changed)
            changed_records = source_with_hash.alias("source").join(
                target_df.alias("target"),
                (col("source.workspace_id") == col("target.workspace_id")) &
                (col("source.compute_type") == col("target.compute_type")) &
                (col("source.compute_id") == col("target.compute_id")) &
                (col("target.is_current") == True) &
                (col("source.record_hash") != col("target.record_hash")),
                "inner"
            ).select(
                col("source.*"),
                col("target.effective_start_ts").alias("old_effective_start_ts")
            )
            
            # Update existing records (close current records)
            if changed_records.count() > 0:
                # Close current records
                target_df.filter(
                    (col("workspace_id").isin([row["workspace_id"] for row in changed_records.collect()])) &
                    (col("compute_type").isin([row["compute_type"] for row in changed_records.collect()])) &
                    (col("compute_id").isin([row["compute_id"] for row in changed_records.collect()])) &
                    (col("is_current") == True)
                ).withColumn("effective_end_ts", col("change_time")) \
                 .withColumn("is_current", lit(False)) \
                 .withColumn("dw_updated_ts", current_timestamp()) \
                 .write.mode("append").saveAsTable(target_table)
            
            # Insert new records
            new_records = source_with_hash.withColumn("effective_start_ts", col("change_time")) \
                                        .withColumn("effective_end_ts", lit("9999-12-31").cast(TimestampType())) \
                                        .withColumn("is_current", lit(True)) \
                                        .withColumn("dw_created_ts", current_timestamp()) \
                                        .withColumn("dw_updated_ts", current_timestamp())
            
            new_records.write.mode("append").saveAsTable(target_table)
            
            logger.info(f"SCD2 merge completed for {target_table}")
            return True
            
        except Exception as e:
            logger.error(f"Error in SCD2 merge for {target_table}: {str(e)}")
            return False
    
    def merge_workflow_entities_scd2(self,
                                    source_df: DataFrame,
                                    target_table: str = "obs.silver.workflow_entities") -> bool:
        """
        Merge workflow entities using SCD2 pattern.
        
        Args:
            source_df: Source DataFrame with workflow entity data
            target_table: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate record hash for change detection
            source_with_hash = source_df.withColumn(
                "record_hash",
                sha2(
                    concat_ws("|",
                        coalesce(col("workspace_id"), lit("")),
                        coalesce(col("workflow_type"), lit("")),
                        coalesce(col("workflow_id"), lit("")),
                        coalesce(col("name"), lit("")),
                        coalesce(col("owner"), lit("")),
                        coalesce(col("run_as"), lit("")),
                        coalesce(col("settings"), lit("")),
                        coalesce(col("tags"), lit("")),
                        coalesce(col("create_time"), lit("")),
                        coalesce(col("delete_time"), lit(""))
                    ), 256
                )
            )
            
            # Read existing target data
            target_df = self.spark.table(target_table)
            
            # Find records that need to be updated (changed)
            changed_records = source_with_hash.alias("source").join(
                target_df.alias("target"),
                (col("source.workspace_id") == col("target.workspace_id")) &
                (col("source.workflow_type") == col("target.workflow_type")) &
                (col("source.workflow_id") == col("target.workflow_id")) &
                (col("target.is_current") == True) &
                (col("source.record_hash") != col("target.record_hash")),
                "inner"
            ).select(
                col("source.*"),
                col("target.effective_start_ts").alias("old_effective_start_ts")
            )
            
            # Update existing records (close current records)
            if changed_records.count() > 0:
                # Close current records
                target_df.filter(
                    (col("workspace_id").isin([row["workspace_id"] for row in changed_records.collect()])) &
                    (col("workflow_type").isin([row["workflow_type"] for row in changed_records.collect()])) &
                    (col("workflow_id").isin([row["workflow_id"] for row in changed_records.collect()])) &
                    (col("is_current") == True)
                ).withColumn("effective_end_ts", col("change_time")) \
                 .withColumn("is_current", lit(False)) \
                 .withColumn("dw_updated_ts", current_timestamp()) \
                 .write.mode("append").saveAsTable(target_table)
            
            # Insert new records
            new_records = source_with_hash.withColumn("effective_start_ts", col("change_time")) \
                                        .withColumn("effective_end_ts", lit("9999-12-31").cast(TimestampType())) \
                                        .withColumn("is_current", lit(True)) \
                                        .withColumn("dw_created_ts", current_timestamp()) \
                                        .withColumn("dw_updated_ts", current_timestamp())
            
            new_records.write.mode("append").saveAsTable(target_table)
            
            logger.info(f"SCD2 merge completed for {target_table}")
            return True
            
        except Exception as e:
            logger.error(f"Error in SCD2 merge for {target_table}: {str(e)}")
            return False


def merge_compute_entities_scd2(spark: SparkSession, source_df: DataFrame, target_table: str = "obs.silver.compute_entities") -> bool:
    """
    Convenience function to merge compute entities using SCD2.
    
    Args:
        spark: Spark session
        source_df: Source DataFrame with compute entity data
        target_table: Target table name
        
    Returns:
        True if successful, False otherwise
    """
    processor = SCD2Processor(spark)
    return processor.merge_compute_entities_scd2(source_df, target_table)


def merge_workflow_entities_scd2(spark: SparkSession, source_df: DataFrame, target_table: str = "obs.silver.workflow_entities") -> bool:
    """
    Convenience function to merge workflow entities using SCD2.
    
    Args:
        spark: Spark session
        source_df: Source DataFrame with workflow entity data
        target_table: Target table name
        
    Returns:
        True if successful, False otherwise
    """
    processor = SCD2Processor(spark)
    return processor.merge_workflow_entities_scd2(source_df, target_table)
