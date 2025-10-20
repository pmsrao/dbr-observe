#!/usr/bin/env python3
"""
Databricks Observability Platform - SQL Setup Script
===================================================

This script runs all SQL setup files in the correct order with resume capability.

Author: Data Platform Team
Date: December 2024
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

try:
    from databricks import sql
except ImportError:
    print("❌ databricks-sql-connector not installed. Run: pip install databricks-sql-connector")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# SQL files in execution order
SQL_FILES = [
    "src/sql/ddl/01_catalog_schemas.sql",
    "src/sql/ddl/02_permissions_setup.sql", 
    "src/sql/ddl/03_watermark_table.sql",
    "src/sql/ddl/11_bronze_billing_tables.sql",
    "src/sql/ddl/12_bronze_compute_tables.sql",
    "src/sql/ddl/13_bronze_lakeflow_tables.sql",
    "src/sql/ddl/14_bronze_query_tables.sql",
    "src/sql/ddl/15_bronze_storage_tables.sql",
    "src/sql/ddl/16_bronze_access_tables.sql",
    "src/sql/ddl/21_silver_entities.sql",
    "src/sql/ddl/22_silver_workflow_runs.sql",
    "src/sql/ddl/23_silver_billing_usage.sql",
    "src/sql/ddl/24_silver_query_history.sql",
    "src/sql/ddl/25_silver_audit_log.sql",
    "src/sql/ddl/26_silver_node_usage.sql",
    "src/sql/ddl/31_gold_dimensions.sql",
    "src/sql/ddl/32_gold_fact_tables.sql",
    "src/sql/transformations/41_staging_views.sql",
    "src/sql/transformations/42_scd2_functions.sql",
    "src/sql/transformations/43_tag_extraction_functions.sql",
    "src/sql/transformations/44_bronze_to_silver_processing.sql",
    "src/sql/transformations/45_silver_to_gold_processing.sql",
    "src/sql/transformations/46_metrics_calculation.sql"
]

# Progress tracking file
PROGRESS_FILE = "sql_setup_progress.json"


class SQLSetupRunner:
    """Runs SQL setup files with resume capability."""
    
    def __init__(self, databricks_host: str, warehouse_http_path: str, token: str):
        """
        Initialize the SQL setup runner.
        
        Args:
            databricks_host: Databricks workspace URL
            warehouse_http_path: SQL Warehouse HTTP path
            token: Databricks access token
        """
        self.databricks_host = databricks_host.replace("https://", "")
        self.warehouse_http_path = warehouse_http_path
        self.token = token
        self.connection = None
        
        # Load progress
        self.progress = self.load_progress()
    
    def load_progress(self) -> Dict[str, Any]:
        """Load progress from file."""
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
        
        return {
            "completed_files": [],
            "failed_files": [],
            "current_file": None,
            "start_time": None
        }
    
    def save_progress(self):
        """Save progress to file."""
        try:
            with open(PROGRESS_FILE, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")
    
    def connect(self):
        """Establish connection to Databricks SQL Warehouse."""
        try:
            self.connection = sql.connect(
                server_hostname=self.databricks_host,
                http_path=self.warehouse_http_path,
                access_token=self.token
            )
            logger.info("✅ Connected to Databricks SQL Warehouse")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Databricks: {e}")
            raise
    
    def disconnect(self):
        """Close connection."""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Disconnected from Databricks")
    
    def execute_sql_file(self, file_path: str) -> bool:
        """
        Execute a single SQL file.
        
        Args:
            file_path: Path to SQL file
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(file_path):
            logger.error(f"❌ File not found: {file_path}")
            return False
        
        try:
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            if not sql_content.strip():
                logger.warning(f"⚠️  Empty file: {file_path}")
                return True
            
            logger.info(f"🔄 Executing: {file_path}")
            
            with self.connection.cursor() as cursor:
                # Split SQL content by semicolons and execute each statement
                statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                
                for i, statement in enumerate(statements):
                    if not statement:
                        continue
                    
                    try:
                        cursor.execute(statement)
                        # Fetch results if any (for SELECT statements)
                        try:
                            results = cursor.fetchall()
                            if results:
                                logger.debug(f"  Statement {i+1} returned {len(results)} rows")
                        except:
                            # No results to fetch (DDL statements)
                            pass
                            
                    except Exception as e:
                        logger.error(f"❌ Error in statement {i+1} of {file_path}: {e}")
                        logger.error(f"Statement: {statement[:200]}...")
                        raise
            
            logger.info(f"✅ Completed: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to execute {file_path}: {e}")
            return False
    
    def run_setup(self, start_from: str = None, skip_completed: bool = True):
        """
        Run all SQL setup files.
        
        Args:
            start_from: File to start from (resume point)
            skip_completed: Whether to skip already completed files
        """
        if not self.connection:
            self.connect()
        
        # Determine starting point
        start_index = 0
        if start_from:
            try:
                start_index = SQL_FILES.index(start_from)
                logger.info(f"🔄 Resuming from: {start_from}")
            except ValueError:
                logger.error(f"❌ Start file not found: {start_from}")
                return
        
        # Set start time
        if not self.progress["start_time"]:
            from datetime import datetime
            self.progress["start_time"] = datetime.now().isoformat()
        
        total_files = len(SQL_FILES)
        completed_count = len(self.progress["completed_files"])
        
        logger.info(f"🚀 Starting SQL setup: {total_files} files total")
        logger.info(f"📊 Progress: {completed_count}/{total_files} completed")
        
        for i, file_path in enumerate(SQL_FILES[start_index:], start_index):
            # Skip if already completed and skip_completed is True
            if skip_completed and file_path in self.progress["completed_files"]:
                logger.info(f"⏭️  Skipping completed file: {file_path}")
                continue
            
            # Update current file
            self.progress["current_file"] = file_path
            self.save_progress()
            
            logger.info(f"📁 Processing file {i+1}/{total_files}: {file_path}")
            
            # Execute the file
            success = self.execute_sql_file(file_path)
            
            if success:
                # Mark as completed
                if file_path not in self.progress["completed_files"]:
                    self.progress["completed_files"].append(file_path)
                
                # Remove from failed files if it was there
                if file_path in self.progress["failed_files"]:
                    self.progress["failed_files"].remove(file_path)
            else:
                # Mark as failed
                if file_path not in self.progress["failed_files"]:
                    self.progress["failed_files"].append(file_path)
                
                # Ask user what to do
                print(f"\n❌ Failed to execute: {file_path}")
                print("Options:")
                print("1. Retry this file")
                print("2. Skip this file and continue")
                print("3. Stop execution")
                
                while True:
                    choice = input("Enter your choice (1-3): ").strip()
                    if choice == "1":
                        # Retry
                        success = self.execute_sql_file(file_path)
                        if success:
                            if file_path not in self.progress["completed_files"]:
                                self.progress["completed_files"].append(file_path)
                            if file_path in self.progress["failed_files"]:
                                self.progress["failed_files"].remove(file_path)
                            break
                        else:
                            print("❌ Retry failed. Please fix the issue and run again.")
                            continue
                    elif choice == "2":
                        # Skip
                        break
                    elif choice == "3":
                        # Stop
                        logger.info("🛑 Execution stopped by user")
                        self.save_progress()
                        return
                    else:
                        print("Invalid choice. Please enter 1, 2, or 3.")
            
            # Save progress after each file
            self.save_progress()
        
        # Final summary
        completed = len(self.progress["completed_files"])
        failed = len(self.progress["failed_files"])
        
        logger.info(f"\n🎉 SQL Setup Complete!")
        logger.info(f"✅ Completed: {completed}/{total_files} files")
        
        if failed > 0:
            logger.warning(f"❌ Failed: {failed} files")
            logger.warning("Failed files:")
            for file_path in self.progress["failed_files"]:
                logger.warning(f"  - {file_path}")
            logger.warning("\nTo retry failed files, run:")
            logger.warning("python scripts/run_all_sql.py --retry-failed")
        else:
            logger.info("🎊 All files executed successfully!")
            # Clean up progress file
            if os.path.exists(PROGRESS_FILE):
                os.remove(PROGRESS_FILE)
                logger.info("🧹 Cleaned up progress file")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run SQL setup files for Databricks Observability Platform")
    parser.add_argument("--start-from", help="File to start from (resume point)")
    parser.add_argument("--retry-failed", action="store_true", help="Retry only failed files")
    parser.add_argument("--no-skip", action="store_true", help="Don't skip completed files")
    parser.add_argument("--warehouse-id", help="SQL Warehouse ID (alternative to http_path)")
    
    args = parser.parse_args()
    
    # Get configuration from environment variables
    databricks_host = os.getenv('DATABRICKS_HOST')
    databricks_token = os.getenv('DATABRICKS_TOKEN')
    warehouse_http_path = os.getenv('DATABRICKS_WAREHOUSE_HTTP_PATH')
    
    # Handle warehouse ID if provided
    if args.warehouse_id:
        warehouse_http_path = f"/sql/1.0/warehouses/{args.warehouse_id}"
    
    if not databricks_host or not databricks_token:
        logger.error("❌ DATABRICKS_HOST and DATABRICKS_TOKEN environment variables are required")
        sys.exit(1)
    
    if not warehouse_http_path:
        logger.error("❌ DATABRICKS_WAREHOUSE_HTTP_PATH environment variable or --warehouse-id is required")
        logger.error("💡 Get HTTP path from: Databricks UI > SQL Warehouses > Your Warehouse > Connection Details")
        sys.exit(1)
    
    try:
        # Initialize runner
        runner = SQLSetupRunner(databricks_host, warehouse_http_path, databricks_token)
        
        # Handle retry failed
        if args.retry_failed:
            if not runner.progress["failed_files"]:
                logger.info("✅ No failed files to retry")
                return
            
            logger.info(f"🔄 Retrying {len(runner.progress['failed_files'])} failed files")
            for file_path in runner.progress["failed_files"]:
                runner.execute_sql_file(file_path)
            return
        
        # Run setup
        runner.run_setup(
            start_from=args.start_from,
            skip_completed=not args.no_skip
        )
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        sys.exit(1)
    finally:
        if 'runner' in locals():
            runner.disconnect()


if __name__ == "__main__":
    main()
