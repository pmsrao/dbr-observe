"""
Databricks Observability Platform - Bronze Layer Processor
========================================================

Bronze layer data processing for system table ingestion.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
import logging

# Import specialized ingestion classes directly from files
import sys
import os

# Add the current directory to the path to find the bronze package
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Import from bronze package files directly
try:
    from bronze.compute_ingestion import ComputeIngestion
    from bronze.lakeflow_ingestion import LakeflowIngestion
    from bronze.billing_ingestion import BillingIngestion
    from bronze.audit_ingestion import AuditIngestion
    from bronze.query_ingestion import QueryIngestion
    from bronze.storage_ingestion import StorageIngestion
except ImportError as e:
    print(f"Warning: Could not import bronze package classes: {e}")
    # Create placeholder classes to avoid import errors
    class ComputeIngestion:
        def __init__(self, *args, **kwargs):
            pass
        def ingest_all_compute_tables(self):
            return False
    
    class LakeflowIngestion:
        def __init__(self, *args, **kwargs):
            pass
        def ingest_all_lakeflow_tables(self):
            return False
    
    class BillingIngestion:
        def __init__(self, *args, **kwargs):
            pass
        def ingest_all_billing_tables(self):
            return False
    
    class AuditIngestion:
        def __init__(self, *args, **kwargs):
            pass
        def ingest_all_audit_tables(self):
            return False
    
    class QueryIngestion:
        def __init__(self, *args, **kwargs):
            pass
        def ingest_all_query_tables(self):
            return False
    
    class StorageIngestion:
        def __init__(self, *args, **kwargs):
            pass
        def ingest_all_storage_tables(self):
            return False

logger = logging.getLogger(__name__)


class BronzeProcessor:
    """Bronze layer data processor for system table ingestion."""
    
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
        
        # Initialize specialized ingestion processors
        self.compute_ingestion = ComputeIngestion(spark, catalog, watermark_manager)
        self.lakeflow_ingestion = LakeflowIngestion(spark, catalog, watermark_manager)
        self.billing_ingestion = BillingIngestion(spark, catalog, watermark_manager)
        self.audit_ingestion = AuditIngestion(spark, catalog, watermark_manager)
        self.query_ingestion = QueryIngestion(spark, catalog, watermark_manager)
        self.storage_ingestion = StorageIngestion(spark, catalog, watermark_manager)
    
    def process_system_to_bronze(self) -> bool:
        """
        Process system tables to bronze layer.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting system to bronze processing...")
            
            # Process each source schema using specialized processors
            success = True
            
            # Compute tables
            if not self.compute_ingestion.ingest_all_compute_tables():
                success = False
                
            # Lakeflow tables
            if not self.lakeflow_ingestion.ingest_all_lakeflow_tables():
                success = False
                
            # Billing tables
            if not self.billing_ingestion.ingest_all_billing_tables():
                success = False
                
            # Query tables
            if not self.query_ingestion.ingest_all_query_tables():
                success = False
                
            # Access tables
            if not self.audit_ingestion.ingest_all_audit_tables():
                success = False
                
            # Storage tables
            if not self.storage_ingestion.ingest_all_storage_tables():
                success = False
            
            if success:
                logger.info("System to bronze processing completed successfully")
            else:
                logger.error("System to bronze processing completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in system to bronze processing: {str(e)}")
            return False
    
# All individual ingestion methods are now handled by specialized classes
# in the bronze package (compute_ingestion.py, lakeflow_ingestion.py, etc.)
