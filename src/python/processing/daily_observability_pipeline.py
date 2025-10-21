"""
Databricks Observability Platform - Daily Processing Pipeline
============================================================

Daily data processing pipeline for the observability platform using PySpark.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import logging
import sys
import os

# Add the functions directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'functions'))

from watermark_management import WatermarkManager
from scd2_processing import SCD2Processor
from tag_extraction import TagExtractor

# Import layer processors
from bronze_processor import BronzeProcessor
from silver_processor import SilverProcessor
from gold_processor import GoldProcessor
from metrics_processor import MetricsProcessor

logger = logging.getLogger(__name__)


class DailyObservabilityPipeline:
    """Daily processing pipeline for the observability platform."""
    
    def __init__(self, spark: SparkSession, catalog: str = "obs"):
        """
        Initialize the daily processing pipeline.
        
        Args:
            spark: Spark session
            catalog: Catalog name (default: obs)
        """
        self.spark = spark
        self.catalog = catalog
        
        # Initialize core components
        self.watermark_manager = WatermarkManager(spark, catalog)
        self.scd2_processor = SCD2Processor(spark, catalog)
        self.tag_extractor = TagExtractor(spark)
        
        # Initialize layer processors
        self.bronze_processor = BronzeProcessor(spark, catalog, self.watermark_manager)
        self.silver_processor = SilverProcessor(spark, catalog, self.watermark_manager, self.scd2_processor)
        self.gold_processor = GoldProcessor(spark, catalog)
        self.metrics_processor = MetricsProcessor(spark, catalog)
        
        # Processing configuration
        self.lookback_days = 2  # Process last 2 days of data
        self.processing_timestamp = current_timestamp()
    
    def process_system_to_bronze(self) -> bool:
        """
        Process data from system tables to bronze layer.
        
        Returns:
            True if successful, False otherwise
        """
        return self.bronze_processor.process_system_to_bronze()
    
    def process_bronze_to_silver(self) -> bool:
        """
        Process data from bronze to silver layer.
        
        Returns:
            True if successful, False otherwise
        """
        return self.silver_processor.process_bronze_to_silver()
    
    def process_silver_to_gold(self) -> bool:
        """
        Process data from silver to gold layer.
        
        Returns:
            True if successful, False otherwise
        """
        return self.gold_processor.process_silver_to_gold()
    
    def calculate_metrics(self) -> bool:
        """
        Calculate enhanced metrics for workflow runs and job task runs.
        
        Returns:
            True if successful, False otherwise
        """
        return self.metrics_processor.calculate_metrics()
    
    def run_daily_pipeline(self) -> bool:
        """
        Run the complete daily processing pipeline.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting daily observability pipeline...")
            
            # Step 1: System to Bronze processing
            if not self.process_system_to_bronze():
                logger.error("System to bronze processing failed")
                return False
            
            # Step 2: Bronze to Silver processing
            if not self.process_bronze_to_silver():
                logger.error("Bronze to silver processing failed")
                return False
            
            # Step 3: Silver to Gold processing
            if not self.process_silver_to_gold():
                logger.error("Silver to gold processing failed")
                return False
            
            # Step 4: Metrics calculation
            if not self.calculate_metrics():
                logger.error("Metrics calculation failed")
                return False
            
            logger.info("Daily observability pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error in daily pipeline: {str(e)}")
            return False
    
def main():
    """Main entry point for the daily processing pipeline."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Daily Observability Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize pipeline
        pipeline = DailyObservabilityPipeline(spark)
        
        # Run daily pipeline
        success = pipeline.run_daily_pipeline()
        
        if success:
            logger.info("🎉 Daily observability pipeline completed successfully!")
        else:
            logger.error("❌ Daily observability pipeline failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Error in daily pipeline: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
