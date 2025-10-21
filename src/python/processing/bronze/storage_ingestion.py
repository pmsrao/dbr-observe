"""
Databricks Observability Platform - Storage Source Ingestion
===========================================================

Bronze layer ingestion for storage-related system tables.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws, struct
import logging

logger = logging.getLogger(__name__)


class StorageIngestion:
    """Storage source ingestion for bronze layer."""
    
    def __init__(self, spark: SparkSession, catalog: str, watermark_manager):
        """
        Initialize the storage ingestion processor.
        
        Args:
            spark: Spark session
            catalog: Catalog name
            watermark_manager: Watermark manager instance
        """
        self.spark = spark
        self.catalog = catalog
        self.watermark_manager = watermark_manager
    
    def ingest_all_storage_tables(self) -> bool:
        """
        Ingest all storage-related system tables.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting storage tables ingestion...")
            
            success = True
            
            if not self.ingest_storage_ops():
                success = False
            
            if success:
                logger.info("Storage tables ingestion completed successfully")
            else:
                logger.error("Storage tables ingestion completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in storage tables ingestion: {str(e)}")
            return False
    
    def ingest_storage_ops(self) -> bool:
        """Ingest storage operations from system table to bronze."""
        try:
            logger.info("Ingesting storage operations...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.storage.ops",
                "obs.bronze.system_storage_ops", 
                "start_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            storage_source = self.spark.table("system.storage.ops") \
                .filter(col("start_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            storage_bronze = storage_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("operation_id").alias("operation_id"),
                    col("workspace_id").alias("workspace_id"),
                    col("operation_type").alias("operation_type"),
                    col("table_name").alias("table_name"),
                    col("schema_name").alias("schema_name"),
                    col("catalog_name").alias("catalog_name"),
                    col("start_time").alias("start_time"),
                    col("end_time").alias("end_time"),
                    col("status").alias("status"),
                    col("details").alias("details")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("start_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.storage.ops").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("operation_id"),
                        col("workspace_id"),
                        col("operation_type"),
                        col("table_name"),
                        col("start_time").cast("string"),
                        col("status")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            storage_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_storage_ops")
            
            # Update watermark if data was processed
            record_count = storage_bronze.count()
            if record_count > 0:
                latest_timestamp = storage_bronze.select("start_time").orderBy(col("start_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.storage.ops",
                        "obs.bronze.system_storage_ops",
                        "start_time",
                        latest_timestamp[0]["start_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for storage operations: {latest_timestamp[0]['start_time']}")
            
            logger.info(f"Storage operations ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting storage operations: {str(e)}")
            return False
