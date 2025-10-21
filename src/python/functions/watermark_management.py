"""
Databricks Observability Platform - Watermark Management Functions
================================================================

PySpark functions for watermark management in the observability platform.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, coalesce, lit, when
from pyspark.sql.types import TimestampType, StringType
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class WatermarkManager:
    """Manages watermarks for incremental data processing."""
    
    def __init__(self, spark: SparkSession, catalog: str = "obs", schema: str = "meta"):
        """
        Initialize the watermark manager.
        
        Args:
            spark: Spark session
            catalog: Catalog name (default: obs)
            schema: Schema name (default: meta)
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.watermarks_table = f"{catalog}.{schema}.watermarks"
    
    def get_watermark(self, source_table: str, target_table: str, watermark_column: str) -> Optional[str]:
        """
        Get the latest watermark value for a source-target table pair.
        
        Args:
            source_table: Source table name
            target_table: Target table name  
            watermark_column: Column name to use as watermark
            
        Returns:
            Latest watermark value or None if not found
        """
        try:
            df = self.spark.table(self.watermarks_table)
            
            result = df.filter(
                (col("source_table_name") == source_table) &
                (col("target_table_name") == target_table) &
                (col("watermark_column") == watermark_column) &
                (col("processing_status") == "SUCCESS")
            ).orderBy(col("last_updated").desc()).limit(1)
            
            watermark_rows = result.collect()
            
            if watermark_rows:
                return watermark_rows[0]["watermark_value"]
            else:
                logger.info(f"No watermark found for {source_table} -> {target_table} ({watermark_column})")
                return None
                
        except Exception as e:
            logger.error(f"Error getting watermark for {source_table} -> {target_table}: {str(e)}")
            return None
    
    def update_watermark(self, 
                        source_table: str, 
                        target_table: str, 
                        watermark_column: str,
                        watermark_value: str,
                        processing_status: str = "SUCCESS",
                        error_message: Optional[str] = None,
                        records_processed: int = 0,
                        processing_duration_ms: int = 0) -> bool:
        """
        Update watermark value for a source-target table pair.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            watermark_column: Column name used as watermark
            watermark_value: New watermark value
            processing_status: Processing status (SUCCESS, FAILED)
            error_message: Error message if failed
            records_processed: Number of records processed
            processing_duration_ms: Processing duration in milliseconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create new watermark record
            new_record = self.spark.createDataFrame([{
                "source_table_name": source_table,
                "target_table_name": target_table,
                "watermark_column": watermark_column,
                "watermark_value": watermark_value,
                "last_updated": current_timestamp(),
                "processing_status": processing_status,
                "error_message": error_message,
                "records_processed": records_processed,
                "processing_duration_ms": processing_duration_ms,
                "created_by": "pyspark",
                "created_at": current_timestamp(),
                "updated_by": "pyspark",
                "updated_at": current_timestamp()
            }])
            
            # Insert new record
            new_record.write.mode("append").saveAsTable(self.watermarks_table)
            
            logger.info(f"Updated watermark for {source_table} -> {target_table}: {watermark_value}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating watermark for {source_table} -> {target_table}: {str(e)}")
            return False
    
    def update_watermark_error(self,
                              source_table: str,
                              target_table: str, 
                              watermark_column: str,
                              error_message: str) -> bool:
        """
        Update watermark with error status.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            watermark_column: Column name used as watermark
            error_message: Error message
            
        Returns:
            True if successful, False otherwise
        """
        return self.update_watermark(
            source_table=source_table,
            target_table=target_table,
            watermark_column=watermark_column,
            watermark_value="ERROR",  # Placeholder value
            processing_status="FAILED",
            error_message=error_message,
            records_processed=0,
            processing_duration_ms=0
        )


def get_watermark(spark: SparkSession, source_table: str, target_table: str, watermark_column: str) -> Optional[str]:
    """
    Convenience function to get watermark value.
    
    Args:
        spark: Spark session
        source_table: Source table name
        target_table: Target table name
        watermark_column: Column name to use as watermark
        
    Returns:
        Latest watermark value or None if not found
    """
    manager = WatermarkManager(spark)
    return manager.get_watermark(source_table, target_table, watermark_column)


def update_watermark(spark: SparkSession,
                    source_table: str,
                    target_table: str,
                    watermark_column: str,
                    watermark_value: str,
                    processing_status: str = "SUCCESS",
                    error_message: Optional[str] = None,
                    records_processed: int = 0,
                    processing_duration_ms: int = 0) -> bool:
    """
    Convenience function to update watermark value.
    
    Args:
        spark: Spark session
        source_table: Source table name
        target_table: Target table name
        watermark_column: Column name used as watermark
        watermark_value: New watermark value
        processing_status: Processing status (SUCCESS, FAILED)
        error_message: Error message if failed
        records_processed: Number of records processed
        processing_duration_ms: Processing duration in milliseconds
        
    Returns:
        True if successful, False otherwise
    """
    manager = WatermarkManager(spark)
    return manager.update_watermark(
        source_table, target_table, watermark_column, watermark_value,
        processing_status, error_message, records_processed, processing_duration_ms
    )
