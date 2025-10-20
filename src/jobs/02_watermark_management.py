"""
Databricks Observability Platform - Watermark Management
=======================================================

This job manages watermarks for incremental data processing,
ensuring proper tracking of data processing progress.

Author: Data Platform Team
Date: December 2024
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WatermarkManager:
    """Manages watermarks for incremental data processing."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the watermark manager.
        
        Args:
            spark: Spark session
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.catalog = config.get('catalog', 'obs')
        
    def get_watermark(self, source_table: str, target_table: str) -> str:
        """Get the current watermark value for a source-target pair."""
        try:
            watermark_df = self.spark.sql(f"""
                SELECT watermark_value
                FROM {self.catalog}.meta.watermarks
                WHERE source_table_name = '{source_table}'
                  AND target_table_name = '{target_table}'
                  AND processing_status = 'SUCCESS'
                ORDER BY last_updated DESC
                LIMIT 1
            """)
            
            if watermark_df.count() > 0:
                return watermark_df.collect()[0]['watermark_value']
            else:
                # Return default lookback timestamp
                lookback_days = self.config.get('default_lookback_days', 2)
                default_timestamp = (datetime.now() - timedelta(days=lookback_days)).isoformat()
                logger.info(f"No watermark found for {source_table} -> {target_table}, using default: {default_timestamp}")
                return default_timestamp
                
        except Exception as e:
            logger.error(f"Failed to get watermark for {source_table} -> {target_table}: {e}")
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
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"{self.catalog}.meta.watermarks")
                
            logger.info(f"Updated watermark for {source_table} -> {target_table}: {watermark_value}")
            
        except Exception as e:
            logger.error(f"Failed to update watermark: {e}")
            raise
    
    def get_processing_status(self) -> List[Dict[str, Any]]:
        """Get the current processing status for all source-target pairs."""
        try:
            status_df = self.spark.sql(f"""
                SELECT 
                    source_table_name,
                    target_table_name,
                    watermark_value,
                    last_updated,
                    processing_status,
                    records_processed,
                    processing_duration_ms,
                    error_message
                FROM {self.catalog}.meta.watermarks
                WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '7 days'
                ORDER BY last_updated DESC
            """)
            
            return [row.asDict() for row in status_df.collect()]
            
        except Exception as e:
            logger.error(f"Failed to get processing status: {e}")
            raise
    
    def cleanup_old_watermarks(self, retention_days: int = 30):
        """Clean up old watermark records to maintain table size."""
        try:
            self.spark.sql(f"""
                DELETE FROM {self.catalog}.meta.watermarks
                WHERE last_updated < CURRENT_TIMESTAMP - INTERVAL '{retention_days} days'
            """)
            
            logger.info(f"Cleaned up watermarks older than {retention_days} days")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old watermarks: {e}")
            raise
    
    def validate_watermarks(self) -> Dict[str, Any]:
        """Validate watermark consistency and identify issues."""
        try:
            validation_results = {
                "total_watermarks": 0,
                "successful_watermarks": 0,
                "failed_watermarks": 0,
                "stale_watermarks": 0,
                "issues": []
            }
            
            # Get all watermarks
            watermarks_df = self.spark.sql(f"""
                SELECT 
                    source_table_name,
                    target_table_name,
                    watermark_value,
                    last_updated,
                    processing_status,
                    error_message
                FROM {self.catalog}.meta.watermarks
                WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '7 days'
            """)
            
            watermarks = [row.asDict() for row in watermarks_df.collect()]
            validation_results["total_watermarks"] = len(watermarks)
            
            for watermark in watermarks:
                if watermark['processing_status'] == 'SUCCESS':
                    validation_results["successful_watermarks"] += 1
                elif watermark['processing_status'] == 'FAILED':
                    validation_results["failed_watermarks"] += 1
                    validation_results["issues"].append({
                        "type": "failed_processing",
                        "source_table": watermark['source_table_name'],
                        "target_table": watermark['target_table_name'],
                        "error": watermark['error_message'],
                        "last_updated": watermark['last_updated']
                    })
                
                # Check for stale watermarks (no updates in last 2 days)
                last_updated = datetime.fromisoformat(watermark['last_updated'].replace('Z', '+00:00'))
                if (datetime.now() - last_updated).days > 2:
                    validation_results["stale_watermarks"] += 1
                    validation_results["issues"].append({
                        "type": "stale_watermark",
                        "source_table": watermark['source_table_name'],
                        "target_table": watermark['target_table_name'],
                        "last_updated": watermark['last_updated']
                    })
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Failed to validate watermarks: {e}")
            raise


def main():
    """Main entry point for watermark management."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Watermark Management") \
        .getOrCreate()
    
    try:
        # Load configuration
        config = {
            "catalog": "obs",
            "default_lookback_days": 2
        }
        
        # Initialize watermark manager
        watermark_manager = WatermarkManager(spark, config)
        
        # Get processing status
        status = watermark_manager.get_processing_status()
        logger.info(f"Current processing status: {len(status)} watermarks found")
        
        # Validate watermarks
        validation_results = watermark_manager.validate_watermarks()
        logger.info(f"Watermark validation results: {validation_results}")
        
        # Cleanup old watermarks
        watermark_manager.cleanup_old_watermarks(retention_days=30)
        
        logger.info("Watermark management completed successfully!")
        
    except Exception as e:
        logger.error(f"Watermark management failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
