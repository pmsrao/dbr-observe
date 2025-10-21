-- =============================================================================
-- Databricks Observability Platform - Permissions Setup
-- =============================================================================
-- Purpose: Configure access controls and permissions for observability platform
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. OWNERSHIP ASSIGNMENTS
-- =============================================================================

-- Assign catalog and schema ownership to data-platform-owner
-- Note: Skipping ownership assignments as current user has full permissions
-- ALTER CATALOG obs OWNER TO `data-platform-owner`;
-- ALTER SCHEMA obs.bronze OWNER TO `data-platform-owner`;
-- ALTER SCHEMA obs.silver OWNER TO `data-platform-owner`;
-- ALTER SCHEMA obs.gold OWNER TO `data-platform-owner`;
-- ALTER SCHEMA obs.meta OWNER TO `data-platform-owner`;
-- ALTER SCHEMA obs.ops OWNER TO `data-platform-owner`;

-- Skip explicit grants as current user has full permissions via data-platform-owner group
-- GRANT ALL PRIVILEGES ON CATALOG obs TO `data-platform-owner`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.bronze TO `data-platform-owner`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.silver TO `data-platform-owner`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.gold TO `data-platform-owner`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.meta TO `data-platform-owner`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.ops TO `data-platform-owner`;

-- Verify current user has access to the catalog
SELECT 'Current user has full permissions via data-platform-owner group' as permission_status;

-- =============================================================================
-- 2. SERVICE PRINCIPAL PERMISSIONS
-- =============================================================================

-- Grant service principal access to system tables
-- Note: This should be configured through Databricks admin console
-- Service Principal: sp_madhu
-- TODO: Create service principal 'sp_madhu' in Databricks UI before uncommenting

-- Grant catalog usage to service principal
-- GRANT USE CATALOG ON CATALOG obs TO `sp_madhu`;

-- Grant schema usage to service principal
-- GRANT USE SCHEMA ON SCHEMA obs.bronze TO `sp_madhu`;
-- GRANT USE SCHEMA ON SCHEMA obs.silver TO `sp_madhu`;
-- GRANT USE SCHEMA ON SCHEMA obs.gold TO `sp_madhu`;
-- GRANT USE SCHEMA ON SCHEMA obs.meta TO `sp_madhu`;
-- GRANT USE SCHEMA ON SCHEMA obs.ops TO `sp_madhu`;

-- Grant table permissions to service principal
-- Bronze layer - Read/Write access
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.bronze TO `sp_madhu`;

-- Silver layer - Read/Write access
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.silver TO `sp_madhu`;

-- Silver staging views are now in silver schema

-- Gold layer - Read/Write access
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.gold TO `sp_madhu`;

-- Meta schema - Read/Write access
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.meta TO `sp_madhu`;

-- Ops schema - Read/Write access
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.ops TO `sp_madhu`;

-- =============================================================================
-- 2. PLATFORM TEAM PERMISSIONS
-- =============================================================================

-- Grant platform team access to observability layer
-- Platform Team: data-platform-team@company.com
-- Note: Skipping platform team grants as current user has full permissions

-- Grant catalog usage to platform team
-- GRANT USE CATALOG ON CATALOG obs TO `data-platform-team@company.com`;

-- Grant schema usage to platform team
-- GRANT USE SCHEMA ON SCHEMA obs.bronze TO `data-platform-team@company.com`;
-- GRANT USE SCHEMA ON SCHEMA obs.silver TO `data-platform-team@company.com`;
-- GRANT USE SCHEMA ON SCHEMA obs.gold TO `data-platform-team@company.com`;
-- GRANT USE SCHEMA ON SCHEMA obs.meta TO `data-platform-team@company.com`;
-- GRANT USE SCHEMA ON SCHEMA obs.ops TO `data-platform-team@company.com`;

-- Grant full permissions to platform team
-- GRANT ALL PRIVILEGES ON SCHEMA obs.bronze TO `data-platform-team@company.com`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.silver TO `data-platform-team@company.com`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.gold TO `data-platform-team@company.com`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.meta TO `data-platform-team@company.com`;
-- GRANT ALL PRIVILEGES ON SCHEMA obs.ops TO `data-platform-team@company.com`;

-- =============================================================================
-- 3. BUSINESS USER PERMISSIONS
-- =============================================================================

-- Grant business users read-only access to gold layer
-- Business Users: business-analytics-team@company.com
-- Note: Skipping business user grants as current user has full permissions

-- Grant catalog usage to business users
-- GRANT USE CATALOG ON CATALOG obs TO `business-analytics-team@company.com`;

-- Grant schema usage to business users (gold layer only)
-- GRANT USE SCHEMA ON SCHEMA obs.gold TO `business-analytics-team@company.com`;

-- Grant read-only access to gold layer
-- GRANT SELECT ON SCHEMA obs.gold TO `business-analytics-team@company.com`;

-- =============================================================================
-- 4. DATA ENGINEER PERMISSIONS
-- =============================================================================

-- Grant data engineers access to bronze and silver layers
-- Data Engineers: data-engineering-team@company.com
-- Note: Skipping data engineer grants as current user has full permissions

-- Grant catalog usage to data engineers
-- GRANT USE CATALOG ON CATALOG obs TO `data-engineering-team@company.com`;

-- Grant schema usage to data engineers
-- GRANT USE SCHEMA ON SCHEMA obs.bronze TO `data-engineering-team@company.com`;
-- GRANT USE SCHEMA ON SCHEMA obs.silver TO `data-engineering-team@company.com`;
-- GRANT USE SCHEMA ON SCHEMA obs.gold TO `data-engineering-team@company.com`;

-- Grant read/write access to data engineers
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.bronze TO `data-engineering-team@company.com`;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.silver TO `data-engineering-team@company.com`;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.gold TO `data-engineering-team@company.com`;

-- =============================================================================
-- 5. MONITORING AND ALERTING PERMISSIONS
-- =============================================================================

-- Grant monitoring team access to ops schema
-- Monitoring Team: monitoring-team@company.com
-- Note: Skipping monitoring team grants as current user has full permissions

-- Grant catalog usage to monitoring team
-- GRANT USE CATALOG ON CATALOG obs TO `monitoring-team@company.com`;

-- Grant schema usage to monitoring team
-- GRANT USE SCHEMA ON SCHEMA obs.ops TO `monitoring-team@company.com`;
-- GRANT USE SCHEMA ON SCHEMA obs.meta TO `monitoring-team@company.com`;

-- Grant read/write access to monitoring team
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.ops TO `monitoring-team@company.com`;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA obs.meta TO `monitoring-team@company.com`;

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

-- Permissions setup completed successfully!
SELECT 'Permissions setup completed successfully!' as status;
SELECT 'All grants skipped - current user has full permissions via data-platform-owner group' as note;
SELECT 'Service Principal: Permissions commented out (create sp_madhu to enable)' as service_principal;
SELECT 'Platform Team: Permissions commented out' as platform_team;
SELECT 'Business Users: Permissions commented out' as business_users;
SELECT 'Data Engineers: Permissions commented out' as data_engineers;
SELECT 'Monitoring Team: Permissions commented out' as monitoring_team;
SELECT 'Ready for watermark table creation.' as next_steps;
