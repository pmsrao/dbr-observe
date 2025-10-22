"""
Databricks Observability Platform - Bronze Layer Processor (Coordinator)
========================================================================

Bronze layer data processing coordinator that delegates to specialized processors.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
import logging

# Import specialized ingestion classes
from bronze.compute_ingestion import ComputeIngestion
from bronze.lakeflow_ingestion import LakeflowIngestion
from bronze.billing_ingestion import BillingIngestion
from bronze.query_ingestion import QueryIngestion
from bronze.audit_ingestion import AuditIngestion
from bronze.storage_ingestion import StorageIngestion

logger = logging.getLogger(__name__)


class BronzeProcessor:
    """Bronze layer data processor coordinator."""
    
    def __init__(self, spark: SparkSession, catalog: str, watermark_manager):
        """
        Initialize the bronze processor.
        
        Args:
            spark: Spark session
            catalog: Catalog name
            watermark_manager: Watermark manager instance
        """
        self.spark = spark
        self.catalog = catalog
        self.watermark_manager = watermark_manager
        
        # Initialize specialized ingestion classes
        self.compute_ingestion = ComputeIngestion(spark, catalog, watermark_manager)
        self.lakeflow_ingestion = LakeflowIngestion(spark, catalog, watermark_manager)
        self.billing_ingestion = BillingIngestion(spark, catalog, watermark_manager)
        self.query_ingestion = QueryIngestion(spark, catalog, watermark_manager)
        self.audit_ingestion = AuditIngestion(spark, catalog, watermark_manager)
        self.storage_ingestion = StorageIngestion(spark, catalog, watermark_manager)
    
    def process_system_to_bronze(self) -> bool:
        """
        Process system tables to bronze layer.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting system to bronze processing...")
            print("🔄 DEBUG: Starting system to bronze processing...")
            
            # Process each source schema using specialized processors
            success = True
            
            # Compute tables
            print("🔄 DEBUG: Processing compute tables...")
            if not self.compute_ingestion.ingest_all_compute_tables():
                success = False
                print("❌ DEBUG: Compute tables processing failed")
            else:
                print("✅ DEBUG: Compute tables processing completed")
                
            # Lakeflow tables
            print("🔄 DEBUG: Processing lakeflow tables...")
            if not self.lakeflow_ingestion.ingest_all_lakeflow_tables():
                success = False
                print("❌ DEBUG: Lakeflow tables processing failed")
            else:
                print("✅ DEBUG: Lakeflow tables processing completed")
                
            # Billing tables
            print("🔄 DEBUG: Processing billing tables...")
            if not self.billing_ingestion.ingest_all_billing_tables():
                success = False
                print("❌ DEBUG: Billing tables processing failed")
            else:
                print("✅ DEBUG: Billing tables processing completed")
                
            # Query tables
            print("🔄 DEBUG: Processing query tables...")
            if not self.query_ingestion.ingest_all_query_tables():
                success = False
                print("❌ DEBUG: Query tables processing failed")
            else:
                print("✅ DEBUG: Query tables processing completed")
                
            # Access tables
            print("🔄 DEBUG: Processing audit tables...")
            if not self.audit_ingestion.ingest_all_audit_tables():
                success = False
                print("❌ DEBUG: Audit tables processing failed")
            else:
                print("✅ DEBUG: Audit tables processing completed")
                
            # Storage tables
            print("🔄 DEBUG: Processing storage tables...")
            if not self.storage_ingestion.ingest_all_storage_tables():
                success = False
                print("❌ DEBUG: Storage tables processing failed")
            else:
                print("✅ DEBUG: Storage tables processing completed")
            
            if success:
                logger.info("System to bronze processing completed successfully")
                print("✅ DEBUG: System to bronze processing completed successfully")
            else:
                logger.error("System to bronze processing completed with errors")
                print("❌ DEBUG: System to bronze processing completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in system to bronze processing: {str(e)}")
            print(f"❌ DEBUG: Error in system to bronze processing: {str(e)}")
            return False
    
    # All ingestion methods are now handled by specialized classes
    # This file now acts as a coordinator that delegates to the specialized processors