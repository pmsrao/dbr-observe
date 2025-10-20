-- =============================================================================
-- Databricks Observability Platform - Permissions Setup
-- =============================================================================
-- Purpose: Configure access controls and permissions for observability platform
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. SERVICE PRINCIPAL PERMISSIONS
-- =============================================================================

-- Grant service principal access to system tables
-- Note: This should be configured through Databricks admin console
-- Service Principal: observability-service-principal@company.com

-- Grant catalog usage to service principal
GRANT USE CATALOG ON CATALOG obs TO `observability-service-principal@company.com`;

-- Grant schema usage to service principal
GRANT USE SCHEMA ON SCHEMA obs.bronze TO `observability-service-principal@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.silver TO `observability-service-principal@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.gold TO `observability-service-principal@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.meta TO `observability-service-principal@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.ops TO `observability-service-principal@company.com`;

-- Grant table permissions to service principal
-- Bronze layer - Read/Write access
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.bronze TO `observability-service-principal@company.com`;

-- Silver layer - Read/Write access
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.silver TO `observability-service-principal@company.com`;

-- Silver staging views are now in silver schema

-- Gold layer - Read/Write access
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.gold TO `observability-service-principal@company.com`;

-- Meta schema - Read/Write access
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.meta TO `observability-service-principal@company.com`;

-- Ops schema - Read/Write access
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.ops TO `observability-service-principal@company.com`;

-- =============================================================================
-- 2. PLATFORM TEAM PERMISSIONS
-- =============================================================================

-- Grant platform team access to observability layer
-- Platform Team: data-platform-team@company.com

-- Grant catalog usage to platform team
GRANT USE CATALOG ON CATALOG obs TO `data-platform-team@company.com`;

-- Grant schema usage to platform team
GRANT USE SCHEMA ON SCHEMA obs.bronze TO `data-platform-team@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.silver TO `data-platform-team@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.gold TO `data-platform-team@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.meta TO `data-platform-team@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.ops TO `data-platform-team@company.com`;

-- Grant full permissions to platform team
GRANT ALL PRIVILEGES ON SCHEMA obs.bronze TO `data-platform-team@company.com`;
GRANT ALL PRIVILEGES ON SCHEMA obs.silver TO `data-platform-team@company.com`;
GRANT ALL PRIVILEGES ON SCHEMA obs.gold TO `data-platform-team@company.com`;
GRANT ALL PRIVILEGES ON SCHEMA obs.meta TO `data-platform-team@company.com`;
GRANT ALL PRIVILEGES ON SCHEMA obs.ops TO `data-platform-team@company.com`;

-- =============================================================================
-- 3. BUSINESS USER PERMISSIONS
-- =============================================================================

-- Grant business users read-only access to gold layer
-- Business Users: business-analytics-team@company.com

-- Grant catalog usage to business users
GRANT USE CATALOG ON CATALOG obs TO `business-analytics-team@company.com`;

-- Grant schema usage to business users (gold layer only)
GRANT USE SCHEMA ON SCHEMA obs.gold TO `business-analytics-team@company.com`;

-- Grant read-only access to gold layer
GRANT SELECT ON SCHEMA obs.gold TO `business-analytics-team@company.com`;

-- =============================================================================
-- 4. DATA ENGINEER PERMISSIONS
-- =============================================================================

-- Grant data engineers access to bronze and silver layers
-- Data Engineers: data-engineering-team@company.com

-- Grant catalog usage to data engineers
GRANT USE CATALOG ON CATALOG obs TO `data-engineering-team@company.com`;

-- Grant schema usage to data engineers
GRANT USE SCHEMA ON SCHEMA obs.bronze TO `data-engineering-team@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.silver TO `data-engineering-team@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.gold TO `data-engineering-team@company.com`;

-- Grant read/write access to data engineers
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.bronze TO `data-engineering-team@company.com`;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.silver TO `data-engineering-team@company.com`;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.gold TO `data-engineering-team@company.com`;

-- =============================================================================
-- 5. MONITORING AND ALERTING PERMISSIONS
-- =============================================================================

-- Grant monitoring team access to ops schema
-- Monitoring Team: monitoring-team@company.com

-- Grant catalog usage to monitoring team
GRANT USE CATALOG ON CATALOG obs TO `monitoring-team@company.com`;

-- Grant schema usage to monitoring team
GRANT USE SCHEMA ON SCHEMA obs.ops TO `monitoring-team@company.com`;
GRANT USE SCHEMA ON SCHEMA obs.meta TO `monitoring-team@company.com`;

-- Grant read/write access to monitoring team
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.ops TO `monitoring-team@company.com`;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.meta TO `monitoring-team@company.com`;

-- =============================================================================
-- 6. ROW-LEVEL SECURITY (RLS) SETUP
-- =============================================================================

-- Enable row-level security for sensitive data
-- This will be implemented after table creation

-- Example RLS policy for workspace isolation
-- ALTER TABLE obs.silver.billing_usage ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY workspace_isolation ON obs.silver.billing_usage
--   FOR ALL TO `business-analytics-team@company.com`
--   USING (workspace_id IN (
--     SELECT workspace_id FROM user_workspace_access 
--     WHERE user_email = current_user()
--   ));

-- =============================================================================
-- 7. COLUMN-LEVEL SECURITY (CLS) SETUP
-- =============================================================================

-- Mask sensitive columns for business users
-- This will be implemented after table creation

-- Example CLS for PII masking
-- ALTER TABLE obs.silver.query_history 
--   ALTER COLUMN executed_by SET MASKING FUNCTION mask_email(executed_by);
-- ALTER TABLE obs.silver.query_history 
--   ALTER COLUMN statement_text SET MASKING FUNCTION mask_sql(statement_text);

-- =============================================================================
-- 8. VERIFICATION
-- =============================================================================

-- Verify permissions are set correctly
SHOW GRANTS ON CATALOG obs;
SHOW GRANTS ON SCHEMA obs.bronze;
SHOW GRANTS ON SCHEMA obs.silver;
SHOW GRANTS ON SCHEMA obs.gold;
SHOW GRANTS ON SCHEMA obs.meta;
SHOW GRANTS ON SCHEMA obs.ops;

-- =============================================================================
-- 9. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 03_watermark_table.sql - Create watermark management system
-- 2. Bronze table creation scripts
-- 3. Silver table creation scripts
-- 4. Gold table creation scripts
-- 5. Implement RLS and CLS policies after table creation

PRINT 'Permissions setup completed successfully!';
PRINT 'Service Principal: Full access to all schemas';
PRINT 'Platform Team: Full access to all schemas';
PRINT 'Business Users: Read-only access to gold layer';
PRINT 'Data Engineers: Read/Write access to bronze, silver, gold layers';
PRINT 'Monitoring Team: Read/Write access to ops and meta schemas';
PRINT 'Ready for watermark table creation.';
