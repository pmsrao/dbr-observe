"""
Databricks Observability Platform - Query Source Ingestion
=========================================================

Bronze layer ingestion for query-related system tables.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws, struct
import logging

logger = logging.getLogger(__name__)


class QueryIngestion:
    """Query source ingestion for bronze layer."""
    
    def __init__(self, spark: SparkSession, catalog: str, watermark_manager):
        """
        Initialize the query ingestion processor.
        
        Args:
            spark: Spark session
            catalog: Catalog name
            watermark_manager: Watermark manager instance
        """
        self.spark = spark
        self.catalog = catalog
        self.watermark_manager = watermark_manager
    
    def ingest_all_query_tables(self) -> bool:
        """
        Ingest all query-related system tables.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting query tables ingestion...")
            
            success = True
            
            if not self.ingest_query_history():
                success = False
            
            if success:
                logger.info("Query tables ingestion completed successfully")
            else:
                logger.error("Query tables ingestion completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in query tables ingestion: {str(e)}")
            return False
    
    def ingest_query_history(self) -> bool:
        """Ingest query history from system table to bronze."""
        try:
            logger.info("Ingesting query history...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.query.history",
                "obs.bronze.system_query_history", 
                "start_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            query_source = self.spark.table("system.query.history") \
                .filter(col("start_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            query_bronze = query_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("statement_id").alias("statement_id"),
                    col("workspace_id").alias("workspace_id"),
                    col("session_id").alias("session_id"),
                    col("execution_status").alias("execution_status"),
                    col("compute").alias("compute"),
                    col("executed_by_user_id").alias("executed_by_user_id"),
                    col("executed_by").alias("executed_by"),
                    col("executed_as").alias("executed_as"),
                    col("start_time").alias("start_time"),
                    col("end_time").alias("end_time"),
                    col("duration_ms").alias("duration_ms"),
                    col("query_text").alias("query_text"),
                    col("query_type").alias("query_type"),
                    col("query_parameters").cast("string").alias("query_parameters"),
                    col("error_message").alias("error_message"),
                    col("error_details").alias("error_details"),
                    col("spark_version").alias("spark_version"),
                    col("channel_name").alias("channel_name"),
                    col("channel_used").alias("channel_used"),
                    col("execution_engine").alias("execution_engine"),
                    col("warehouse_id").alias("warehouse_id"),
                    col("cluster_id").alias("cluster_id"),
                    col("query_id").alias("query_id"),
                    col("request_id").alias("request_id"),
                    col("user_agent").alias("user_agent"),
                    col("tags").alias("tags"),
                    col("change_time").alias("change_time")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("start_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.query.history").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("statement_id"),
                        col("workspace_id"),
                        col("session_id"),
                        col("execution_status"),
                        col("start_time").cast("string"),
                        col("end_time").cast("string"),
                        col("duration_ms").cast("string"),
                        col("query_type"),
                        col("warehouse_id"),
                        col("cluster_id")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            query_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_query_history")
            
            # Update watermark if data was processed
            record_count = query_bronze.count()
            if record_count > 0:
                latest_timestamp = query_bronze.select("start_time").orderBy(col("start_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.query.history",
                        "obs.bronze.system_query_history",
                        "start_time",
                        latest_timestamp[0]["start_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for query history: {latest_timestamp[0]['start_time']}")
            
            logger.info(f"Query history ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting query history: {str(e)}")
            return False
