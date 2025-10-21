"""
Databricks Observability Platform - Billing Source Ingestion
===========================================================

Bronze layer ingestion for billing-related system tables.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws, struct
import logging

logger = logging.getLogger(__name__)


class BillingIngestion:
    """Billing source ingestion for bronze layer."""
    
    def __init__(self, spark: SparkSession, catalog: str, watermark_manager):
        """
        Initialize the billing ingestion processor.
        
        Args:
            spark: Spark session
            catalog: Catalog name
            watermark_manager: Watermark manager instance
        """
        self.spark = spark
        self.catalog = catalog
        self.watermark_manager = watermark_manager
    
    def ingest_all_billing_tables(self) -> bool:
        """
        Ingest all billing-related system tables.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting billing tables ingestion...")
            
            success = True
            
            if not self.ingest_billing_usage():
                success = False
                
            if not self.ingest_billing_list_prices():
                success = False
            
            if success:
                logger.info("Billing tables ingestion completed successfully")
            else:
                logger.error("Billing tables ingestion completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in billing tables ingestion: {str(e)}")
            return False
    
    def ingest_billing_usage(self) -> bool:
        """Ingest billing usage from system table to bronze."""
        try:
            logger.info("Ingesting billing usage...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.billing.usage",
                "obs.bronze.system_billing_usage", 
                "usage_date"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            billing_source = self.spark.table("system.billing.usage") \
                .filter(col("usage_date") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            billing_bronze = billing_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("record_id").alias("record_id"),
                    col("workspace_id").alias("workspace_id"),
                    col("sku_name").alias("sku_name"),
                    col("cloud").alias("cloud"),
                    col("usage_start_time").alias("usage_start_time"),
                    col("usage_end_time").alias("usage_end_time"),
                    col("usage_date").alias("usage_date"),
                    col("usage_unit").alias("usage_unit"),
                    col("usage_quantity").alias("usage_quantity"),
                    col("usage_type").alias("usage_type"),
                    col("custom_tags").alias("custom_tags"),
                    # Note: usage_metadata and identity_metadata are complex structs
                    col("usage_metadata").alias("usage_metadata"),
                    col("identity_metadata").alias("identity_metadata"),
                    col("record_type").alias("record_type"),
                    col("ingestion_date").alias("ingestion_date")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("usage_date"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.billing.usage").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("record_id"),
                        col("workspace_id"),
                        col("sku_name"),
                        col("cloud"),
                        col("usage_start_time").cast("string"),
                        col("usage_end_time").cast("string"),
                        col("usage_date").cast("string"),
                        col("usage_unit"),
                        col("usage_quantity").cast("string"),
                        col("usage_type"),
                        col("record_type")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            billing_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_billing_usage")
            
            # Update watermark if data was processed
            record_count = billing_bronze.count()
            if record_count > 0:
                latest_timestamp = billing_bronze.select("usage_date").orderBy(col("usage_date").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.billing.usage",
                        "obs.bronze.system_billing_usage",
                        "usage_date",
                        latest_timestamp[0]["usage_date"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for billing usage: {latest_timestamp[0]['usage_date']}")
            
            logger.info(f"Billing usage ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting billing usage: {str(e)}")
            return False
    
    def ingest_billing_list_prices(self) -> bool:
        """Ingest billing list prices from system table to bronze."""
        try:
            logger.info("Ingesting billing list prices...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.billing.list_prices",
                "obs.bronze.system_billing_list_prices", 
                "effective_date"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            prices_source = self.spark.table("system.billing.list_prices") \
                .filter(col("effective_date") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            prices_bronze = prices_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("sku_name").alias("sku_name"),
                    col("cloud").alias("cloud"),
                    col("region").alias("region"),
                    col("unit_price").alias("unit_price"),
                    col("currency").alias("currency"),
                    col("effective_date").alias("effective_date"),
                    col("expiration_date").alias("expiration_date"),
                    col("pricing_tier").alias("pricing_tier"),
                    col("description").alias("description")
                ).alias("raw_data"),
                # Bronze layer columns
                col("cloud"),
                col("effective_date"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.billing.list_prices").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("sku_name"),
                        col("cloud"),
                        col("region"),
                        col("effective_date").cast("string"),
                        col("unit_price").cast("string")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            prices_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_billing_list_prices")
            
            # Update watermark if data was processed
            record_count = prices_bronze.count()
            if record_count > 0:
                latest_timestamp = prices_bronze.select("effective_date").orderBy(col("effective_date").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.billing.list_prices",
                        "obs.bronze.system_billing_list_prices",
                        "effective_date",
                        latest_timestamp[0]["effective_date"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for billing list prices: {latest_timestamp[0]['effective_date']}")
            
            logger.info(f"Billing list prices ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting billing list prices: {str(e)}")
            return False
