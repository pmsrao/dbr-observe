"""
Databricks Observability Platform - Audit Source Ingestion
=========================================================

Bronze layer ingestion for audit and access-related system tables.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws, struct
import logging

logger = logging.getLogger(__name__)


class AuditIngestion:
    """Audit source ingestion for bronze layer."""
    
    def __init__(self, spark: SparkSession, catalog: str, watermark_manager):
        """
        Initialize the audit ingestion processor.
        
        Args:
            spark: Spark session
            catalog: Catalog name
            watermark_manager: Watermark manager instance
        """
        self.spark = spark
        self.catalog = catalog
        self.watermark_manager = watermark_manager
    
    def ingest_all_audit_tables(self) -> bool:
        """
        Ingest all audit and access-related system tables.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting audit tables ingestion...")
            
            success = True
            
            if not self.ingest_audit_log():
                success = False
            
            if success:
                logger.info("Audit tables ingestion completed successfully")
            else:
                logger.error("Audit tables ingestion completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in audit tables ingestion: {str(e)}")
            return False
    
    def ingest_audit_log(self) -> bool:
        """Ingest audit log from system table to bronze."""
        try:
            logger.info("Ingesting audit log...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.access.audit",
                "obs.bronze.system_access_audit", 
                "event_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            audit_source = self.spark.table("system.access.audit") \
                .filter(col("event_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            audit_bronze = audit_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("request_id").alias("request_id"),
                    col("workspace_id").alias("workspace_id"),
                    col("event_time").alias("timestamp"),
                    col("service_name").alias("service_name"),
                    col("action_name").alias("action_name"),
                    col("response").alias("result"),
                    col("user_identity").alias("user_identity"),
                    col("user_agent").alias("user_agent"),
                    col("source_ip_address").alias("source_ip_address"),
                    col("request_params").alias("request_params"),
                    col("response").alias("response"),
                    col("version").alias("version"),
                    col("account_id").alias("account_id"),
                    lit(None).cast("string").alias("request_context"),
                    col("session_id").alias("session_id"),
                    lit(None).cast("timestamp").alias("change_time")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("event_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.access.audit").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("request_id"),
                        col("workspace_id"),
                        col("event_time").cast("string"),
                        col("service_name"),
                        col("action_name"),
                        col("response").cast("string"),
                        col("user_identity"),
                        col("source_ip_address")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            audit_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_access_audit")
            
            # Update watermark if data was processed
            record_count = audit_bronze.count()
            if record_count > 0:
                latest_timestamp = audit_bronze.select("event_time").orderBy(col("event_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.access.audit",
                        "obs.bronze.system_access_audit",
                        "event_time",
                        latest_timestamp[0]["event_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for audit log: {latest_timestamp[0]['event_time']}")
            
            logger.info(f"Audit log ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting audit log: {str(e)}")
            return False
