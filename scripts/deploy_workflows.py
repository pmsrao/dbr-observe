#!/usr/bin/env python3
"""
Databricks Observability Platform - Workflow Deployment Script
==============================================================

This script deploys the observability workflows to Databricks.

Author: Data Platform Team
Date: December 2024
"""

import json
import logging
import os
import sys
from typing import Dict, Any

import requests
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from databricks_cli.workspace.api import WorkspaceApi

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WorkflowDeployer:
    """Deploys observability workflows to Databricks."""
    
    def __init__(self, databricks_host: str, token: str):
        """
        Initialize the workflow deployer.
        
        Args:
            databricks_host: Databricks workspace URL
            token: Databricks access token
        """
        self.databricks_host = databricks_host
        self.token = token
        
        # Initialize API client
        self.api_client = ApiClient(host=databricks_host, token=token)
        self.jobs_api = JobsApi(self.api_client)
        self.workspace_api = WorkspaceApi(self.api_client)
    
    def load_workflow_config(self, config_path: str) -> Dict[str, Any]:
        """Load workflow configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load workflow config from {config_path}: {e}")
            raise
    
    def deploy_workflow(self, workflow_name: str, workflow_config: Dict[str, Any]) -> str:
        """
        Deploy a workflow to Databricks.
        
        Args:
            workflow_name: Name of the workflow
            workflow_config: Workflow configuration
            
        Returns:
            Job ID of the deployed workflow
        """
        try:
            # Check if workflow already exists
            existing_jobs = self.jobs_api.list_jobs()
            existing_job = None
            
            for job in existing_jobs.get('jobs', []):
                if job['settings']['name'] == workflow_config['name']:
                    existing_job = job
                    break
            
            if existing_job:
                # Update existing workflow
                logger.info(f"Updating existing workflow: {workflow_config['name']}")
                job_id = existing_job['job_id']
                
                update_config = {
                    'job_id': job_id,
                    'new_settings': workflow_config
                }
                
                self.jobs_api.update_job(**update_config)
                logger.info(f"Updated workflow {workflow_config['name']} with job ID: {job_id}")
                
            else:
                # Create new workflow
                logger.info(f"Creating new workflow: {workflow_config['name']}")
                
                create_config = {
                    'name': workflow_config['name'],
                    'tasks': workflow_config['tasks'],
                    'email_notifications': workflow_config.get('email_notifications', {}),
                    'webhook_notifications': workflow_config.get('webhook_notifications', {}),
                    'max_concurrent_runs': workflow_config.get('max_concurrent_runs', 1),
                    'tags': workflow_config.get('tags', {}),
                    'schedule': workflow_config.get('schedule', {})
                }
                
                response = self.jobs_api.create_job(**create_config)
                job_id = response['job_id']
                logger.info(f"Created workflow {workflow_config['name']} with job ID: {job_id}")
            
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to deploy workflow {workflow_name}: {e}")
            raise
    
    def deploy_all_workflows(self, config_path: str):
        """Deploy all workflows from configuration file."""
        try:
            # Load workflow configuration
            config = self.load_workflow_config(config_path)
            workflows = config.get('workflows', {})
            
            deployed_workflows = {}
            
            for workflow_name, workflow_config in workflows.items():
                logger.info(f"Deploying workflow: {workflow_name}")
                job_id = self.deploy_workflow(workflow_name, workflow_config)
                deployed_workflows[workflow_name] = job_id
            
            logger.info(f"Successfully deployed {len(deployed_workflows)} workflows")
            return deployed_workflows
            
        except Exception as e:
            logger.error(f"Failed to deploy workflows: {e}")
            raise
    
    def validate_deployment(self, deployed_workflows: Dict[str, str]) -> bool:
        """Validate that all workflows were deployed successfully."""
        try:
            for workflow_name, job_id in deployed_workflows.items():
                # Get job details
                job_details = self.jobs_api.get_job(job_id)
                
                if not job_details:
                    logger.error(f"Workflow {workflow_name} (ID: {job_id}) not found after deployment")
                    return False
                
                logger.info(f"✅ Workflow {workflow_name} (ID: {job_id}) deployed successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate deployment: {e}")
            return False


def main():
    """Main entry point for workflow deployment."""
    # Get configuration from environment variables
    databricks_host = os.getenv('DATABRICKS_HOST')
    databricks_token = os.getenv('DATABRICKS_TOKEN')
    config_path = os.getenv('WORKFLOW_CONFIG_PATH', 'src/jobs/workflow_config.json')
    
    if not databricks_host or not databricks_token:
        logger.error("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables are required")
        sys.exit(1)
    
    try:
        # Initialize deployer
        deployer = WorkflowDeployer(databricks_host, databricks_token)
        
        # Deploy all workflows
        deployed_workflows = deployer.deploy_all_workflows(config_path)
        
        # Validate deployment
        if deployer.validate_deployment(deployed_workflows):
            logger.info("🎉 All workflows deployed successfully!")
            
            # Print deployment summary
            print("\n" + "="*50)
            print("DEPLOYMENT SUMMARY")
            print("="*50)
            for workflow_name, job_id in deployed_workflows.items():
                print(f"✅ {workflow_name}: Job ID {job_id}")
            print("="*50)
            
        else:
            logger.error("❌ Deployment validation failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
