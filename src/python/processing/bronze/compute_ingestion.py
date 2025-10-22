"""
Databricks Observability Platform - Compute Source Ingestion
===========================================================

Bronze layer ingestion for compute-related system tables.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws, struct
import logging

logger = logging.getLogger(__name__)


class ComputeIngestion:
    """Compute source ingestion for bronze layer."""
    
    def __init__(self, spark: SparkSession, catalog: str, watermark_manager):
        """
        Initialize the compute ingestion processor.
        
        Args:
            spark: Spark session
            catalog: Catalog name
            watermark_manager: Watermark manager instance
        """
        self.spark = spark
        self.catalog = catalog
        self.watermark_manager = watermark_manager
    
    def ingest_all_compute_tables(self) -> bool:
        """
        Ingest all compute-related system tables.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting compute tables ingestion...")
            
            success = True
            
            if not self.ingest_compute_clusters():
                success = False
                
            if not self.ingest_compute_warehouses():
                success = False
                
            if not self.ingest_compute_node_types():
                success = False
                
            if not self.ingest_compute_node_timeline():
                success = False
                
            if not self.ingest_compute_warehouse_events():
                success = False
            
            if success:
                logger.info("Compute tables ingestion completed successfully")
            else:
                logger.error("Compute tables ingestion completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in compute tables ingestion: {str(e)}")
            return False
    
    def ingest_compute_clusters(self) -> bool:
        """Ingest compute clusters from system table to bronze."""
        try:
            logger.info("Ingesting compute clusters...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.compute.clusters",
                "obs.bronze.system_compute_clusters", 
                "change_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            compute_source = self.spark.table("system.compute.clusters") \
                .filter(col("change_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            compute_bronze = compute_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("cluster_id").alias("cluster_id"),
                    col("workspace_id").alias("workspace_id"),
                    col("cluster_name").alias("name"),
                    col("owned_by").alias("owner"),
                    col("driver_node_type").alias("driver_node_type"),
                    col("worker_node_type").alias("worker_node_type"),
                    col("worker_count").cast("bigint").alias("worker_count"),
                    col("min_autoscale_workers").cast("bigint").alias("min_autoscale_workers"),
                    col("max_autoscale_workers").cast("bigint").alias("max_autoscale_workers"),
                    col("auto_termination_minutes").cast("bigint").alias("auto_termination_minutes"),
                    col("enable_elastic_disk").alias("enable_elastic_disk"),
                    # These columns don't exist in system table, use null values
                    lit(None).cast("string").alias("data_security_mode"),
                    lit(None).cast("string").alias("policy_id"),
                    # dbr_version doesn't exist in system table, use null
                    lit(None).cast("string").alias("dbr_version"),
                    col("cluster_source").alias("cluster_source"),
                    col("tags").alias("tags"),
                    col("create_time").alias("create_time"),
                    col("delete_time").alias("delete_time"),
                    col("change_time").alias("change_time")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("change_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.compute.clusters").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("workspace_id"),
                        col("cluster_id"),
                        col("cluster_name"),
                        col("owned_by"),
                        col("driver_node_type"),
                        col("worker_node_type"),
                        col("worker_count").cast("string"),
                        col("min_autoscale_workers").cast("string"),
                        col("max_autoscale_workers").cast("string"),
                        col("auto_termination_minutes").cast("string"),
                        col("enable_elastic_disk").cast("string"),
                        lit("").alias("data_security_mode"),
                        lit("").alias("policy_id"),
                        lit("").alias("dbr_version"),
                        col("cluster_source"),
                        col("tags").cast("string")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            compute_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_compute_clusters")
            
            # Update watermark if data was processed
            record_count = compute_bronze.count()
            if record_count > 0:
                latest_timestamp = compute_bronze.select("change_time").orderBy(col("change_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.compute.clusters",
                        "obs.bronze.system_compute_clusters",
                        "change_time",
                        latest_timestamp[0]["change_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0  # Duration would be calculated
                    )
                    logger.info(f"Updated watermark for compute clusters: {latest_timestamp[0]['change_time']}")
            
            logger.info(f"Compute clusters ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting compute clusters: {str(e)}")
            return False
    
    def ingest_compute_warehouses(self) -> bool:
        """Ingest compute warehouses from system table to bronze."""
        try:
            logger.info("Ingesting compute warehouses...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.compute.warehouses",
                "obs.bronze.system_compute_warehouses", 
                "change_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            warehouse_source = self.spark.table("system.compute.warehouses") \
                .filter(col("change_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            warehouse_bronze = warehouse_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("warehouse_id").alias("warehouse_id"),
                    col("workspace_id").alias("workspace_id"),
                    col("warehouse_name").alias("name"),
                    col("owned_by").alias("owner"),
                    col("warehouse_type").alias("warehouse_type"),
                    col("warehouse_size").alias("warehouse_size"),
                    col("warehouse_channel").alias("warehouse_channel"),
                    col("min_clusters").alias("min_clusters"),
                    col("max_clusters").alias("max_clusters"),
                    col("auto_stop_minutes").alias("auto_stop_minutes"),
                    col("tags").alias("tags"),
                    col("create_time").alias("create_time"),
                    col("delete_time").alias("delete_time"),
                    col("change_time").alias("change_time")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("change_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.compute.warehouses").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("warehouse_id"),
                        col("workspace_id"),
                        col("warehouse_name"),
                        col("owned_by"),
                        col("warehouse_type"),
                        col("warehouse_size"),
                        col("warehouse_channel"),
                        col("min_clusters").cast("string"),
                        col("max_clusters").cast("string"),
                        col("auto_stop_minutes").cast("string")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            warehouse_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_compute_warehouses")
            
            # Update watermark if data was processed
            record_count = warehouse_bronze.count()
            if record_count > 0:
                latest_timestamp = warehouse_bronze.select("change_time").orderBy(col("change_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.compute.warehouses",
                        "obs.bronze.system_compute_warehouses",
                        "change_time",
                        latest_timestamp[0]["change_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for compute warehouses: {latest_timestamp[0]['change_time']}")
            
            logger.info(f"Compute warehouses ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting compute warehouses: {str(e)}")
            return False
    
    def ingest_compute_node_types(self) -> bool:
        """Ingest compute node types from system table to bronze."""
        try:
            logger.info("Ingesting compute node types...")
            print("🔄 DEBUG: Starting compute node types ingestion...")
            
            # Read from system table (no watermark needed for reference table)
            try:
                node_types_source = self.spark.table("system.compute.node_types")
                print(f"🔄 DEBUG: Read from system.compute.node_types, checking record count...")
                source_count = node_types_source.count()
                print(f"🔄 DEBUG: Source table has {source_count} records")
            except Exception as e:
                logger.warning(f"System table system.compute.node_types not accessible: {str(e)}")
                print(f"❌ DEBUG: System table system.compute.node_types not accessible: {str(e)}")
                logger.info("Skipping compute node types ingestion - system table not available")
                return True
            
            if source_count == 0:
                print("ℹ️ DEBUG: No records to process")
                logger.info("No compute node types records to process")
                return True
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            node_types_bronze = node_types_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("node_type").alias("node_type"),
                    col("core_count").alias("core_count"),
                    col("memory_mb").alias("memory_mb"),
                    col("gpu_count").alias("gpu_count"),
                    lit(None).cast("string").alias("cloud"),  # cloud doesn't exist
                    lit(None).cast("string").alias("region"),  # region doesn't exist
                    lit(None).cast("array<string>").alias("availability_zones")  # availability_zones doesn't exist
                ).alias("raw_data"),
                # Bronze layer columns (no partitioning)
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.compute.node_types").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("node_type"),
                        lit("").alias("cloud"),  # Use empty string for missing cloud
                        lit("").alias("region"),  # Use empty string for missing region
                        col("core_count").cast("string"),
                        col("memory_mb").cast("string"),
                        col("gpu_count").cast("string")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            print("🔄 DEBUG: Writing to bronze table...")
            # Write to bronze table with mergeSchema option
            node_types_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_compute_node_types")
            
            # No watermark needed for reference table
            record_count = node_types_bronze.count()
            print(f"🔄 DEBUG: Written {record_count} records to bronze table")
            
            logger.info(f"Compute node types ingested successfully - {record_count} records")
            print(f"✅ DEBUG: Compute node types ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting compute node types: {str(e)}")
            return False
    
    def ingest_compute_node_timeline(self) -> bool:
        """Ingest compute node timeline from system table to bronze."""
        try:
            logger.info("Ingesting compute node timeline...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.compute.node_timeline",
                "obs.bronze.system_compute_node_timeline", 
                "start_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            node_source = self.spark.table("system.compute.node_timeline") \
                .filter(col("start_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            node_bronze = node_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("workspace_id").alias("workspace_id"),
                    col("cluster_id").alias("cluster_id"),
                    col("instance_id").alias("instance_id"),
                    col("start_time").alias("start_time"),
                    col("end_time").alias("end_time"),
                    col("driver").alias("driver"),
                    col("cpu_user_percent").alias("cpu_user_percent"),
                    col("cpu_system_percent").alias("cpu_system_percent"),
                    col("cpu_wait_percent").alias("cpu_wait_percent"),
                    col("mem_used_percent").alias("mem_used_percent"),
                    col("mem_swap_percent").alias("mem_swap_percent"),
                    col("network_sent_bytes").alias("network_sent_bytes"),
                    col("network_received_bytes").alias("network_received_bytes"),
                    col("disk_free_bytes_per_mount_point").alias("disk_free_bytes_per_mount_point")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("start_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.compute.node_timeline").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("workspace_id"),
                        col("cluster_id"),
                        col("instance_id"),
                        col("start_time").cast("string"),
                        col("end_time").cast("string"),
                        col("driver").cast("string")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            node_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_compute_node_timeline")
            
            # Update watermark if data was processed
            record_count = node_bronze.count()
            if record_count > 0:
                latest_timestamp = node_bronze.select("start_time").orderBy(col("start_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.compute.node_timeline",
                        "obs.bronze.system_compute_node_timeline",
                        "start_time",
                        latest_timestamp[0]["start_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for compute node timeline: {latest_timestamp[0]['start_time']}")
            
            logger.info(f"Compute node timeline ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting compute node timeline: {str(e)}")
            return False
    
    def ingest_compute_warehouse_events(self) -> bool:
        """Ingest compute warehouse events from system table to bronze."""
        try:
            logger.info("Ingesting compute warehouse events...")
            
            # Get watermark for delta processing
            watermark = self.watermark_manager.get_watermark(
                "system.compute.warehouse_events",
                "obs.bronze.system_compute_warehouse_events", 
                "event_time"
            )
            
            if watermark is None:
                watermark = "1900-01-01"
                logger.info(f"No watermark found, processing from {watermark}")
            else:
                logger.info(f"Processing delta from watermark: {watermark}")
            
            # Read from system table with watermark filter
            events_source = self.spark.table("system.compute.warehouse_events") \
                .filter(col("event_time") > watermark)
            
            # Transform to bronze format with raw_data structure matching exact bronze schema
            events_bronze = events_source.select(
                # Create raw_data struct matching bronze schema exactly
                struct(
                    col("workspace_id").alias("workspace_id"),
                    col("warehouse_id").alias("warehouse_id"),
                    col("event_type").alias("event_type"),
                    col("event_time").alias("event_time"),
                    col("cluster_count").alias("cluster_count"),
                    col("details").alias("details")
                ).alias("raw_data"),
                # Bronze layer columns
                col("workspace_id"),
                col("event_time"),
                current_timestamp().alias("ingestion_timestamp"),
                lit("system.compute.warehouse_events").alias("source_file"),
                # Generate record hash using available columns
                sha2(
                    concat_ws("|",
                        col("workspace_id"),
                        col("warehouse_id"),
                        col("event_type"),
                        col("event_time").cast("string"),
                        col("cluster_count").cast("string")
                    ), 256
                ).alias("record_hash"),
                lit(False).alias("is_deleted")
            )
            
            # Write to bronze table with mergeSchema option
            events_bronze.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.bronze.system_compute_warehouse_events")
            
            # Update watermark if data was processed
            record_count = events_bronze.count()
            if record_count > 0:
                latest_timestamp = events_bronze.select("event_time").orderBy(col("event_time").desc()).limit(1).collect()
                if latest_timestamp:
                    self.watermark_manager.update_watermark(
                        "system.compute.warehouse_events",
                        "obs.bronze.system_compute_warehouse_events",
                        "event_time",
                        latest_timestamp[0]["event_time"].isoformat(),
                        "SUCCESS",
                        None,
                        record_count,
                        0
                    )
                    logger.info(f"Updated watermark for compute warehouse events: {latest_timestamp[0]['event_time']}")
            
            logger.info(f"Compute warehouse events ingested successfully - {record_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting compute warehouse events: {str(e)}")
            return False
