"""
Databricks Observability Platform - Watermark Management Functions
================================================================

PySpark functions for watermark management in the observability platform.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, coalesce, lit, when
from pyspark.sql.types import TimestampType, StringType, StructType, StructField, IntegerType
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
        
        # Define schema for watermark records
        self.watermark_schema = StructType([
            StructField("source_table_name", StringType(), False),
            StructField("target_table_name", StringType(), False),
            StructField("watermark_column", StringType(), False),
            StructField("watermark_value", StringType(), False),
            StructField("last_updated", TimestampType(), False),
            StructField("processing_status", StringType(), False),
            StructField("error_message", StringType(), True),
            StructField("records_processed", IntegerType(), False),
            StructField("processing_duration_ms", IntegerType(), False),
            StructField("created_by", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("updated_by", StringType(), False),
            StructField("updated_at", TimestampType(), False)
        ])
    
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
            # Ensure all values are properly typed
            watermark_str = str(watermark_value) if watermark_value is not None else "1900-01-01 00:00:00"
            records_int = int(records_processed) if records_processed is not None else 0
            duration_int = int(processing_duration_ms) if processing_duration_ms is not None else 0
            error_str = str(error_message) if error_message is not None else None
            status_str = str(processing_status) if processing_status is not None else "UNKNOWN"
            
            print(f"🔄 DEBUG: Updating watermark - {source_table} -> {target_table}: {watermark_str}")
            print(f"🔄 DEBUG: Records: {records_int}, Duration: {duration_int}, Status: {status_str}")
            
            # Create new watermark record with explicit schema
            try:
                new_record = self.spark.createDataFrame([{
                    "source_table_name": str(source_table),
                    "target_table_name": str(target_table),
                    "watermark_column": str(watermark_column),
                    "watermark_value": watermark_str,
                    "last_updated": current_timestamp(),
                    "processing_status": status_str,
                    "error_message": error_str,
                    "records_processed": records_int,
                    "processing_duration_ms": duration_int,
                    "created_by": "pyspark",
                    "created_at": current_timestamp(),
                    "updated_by": "pyspark",
                    "updated_at": current_timestamp()
                }], schema=self.watermark_schema)
                
                print(f"🔄 DEBUG: DataFrame created successfully")
                
                # Insert new record
                new_record.write.mode("append").saveAsTable(self.watermarks_table)
                print(f"✅ DEBUG: Watermark record inserted successfully")
                
            except Exception as df_error:
                print(f"❌ DEBUG: DataFrame creation error: {str(df_error)}")
                # Try without explicit schema as fallback
                new_record = self.spark.createDataFrame([{
                    "source_table_name": str(source_table),
                    "target_table_name": str(target_table),
                    "watermark_column": str(watermark_column),
                    "watermark_value": watermark_str,
                    "last_updated": current_timestamp(),
                    "processing_status": status_str,
                    "error_message": error_str,
                    "records_processed": records_int,
                    "processing_duration_ms": duration_int,
                    "created_by": "pyspark",
                    "created_at": current_timestamp(),
                    "updated_by": "pyspark",
                    "updated_at": current_timestamp()
                }])
                
                new_record.write.mode("append").saveAsTable(self.watermarks_table)
                print(f"✅ DEBUG: Watermark record inserted with fallback method")
            
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
