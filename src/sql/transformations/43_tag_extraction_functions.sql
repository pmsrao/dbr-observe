-- =============================================================================
-- Databricks Observability Platform - Tag Extraction Functions
-- =============================================================================
-- Purpose: Document tag extraction patterns for entity processing
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- Set catalog context
USE CATALOG obs;

-- =============================================================================
-- 1. TAG EXTRACTION PATTERNS
-- =============================================================================

-- Note: SQL functions are not supported in Databricks SQL
-- This file documents the tag extraction patterns for reference
-- The actual implementation will be done in Python processing scripts

-- Tag Extraction Pattern:
-- 1. Try exact tag name match
-- 2. Try uppercase tag name match
-- 3. Try lowercase tag name match
-- 4. Try title case tag name match
-- 5. Try underscore to dash replacement
-- 6. Try dash to underscore replacement
-- 7. Return 'Unknown' if no match found

-- =============================================================================
-- 2. TAG EXTRACTION LOGIC
-- =============================================================================

-- Tag Extraction Function Logic (for Python implementation):
-- def extract_tag(tags_map, tag_name):
--     return (
--         tags_map.get(tag_name) or
--         tags_map.get(tag_name.upper()) or
--         tags_map.get(tag_name.lower()) or
--         tags_map.get(tag_name.title()) or
--         tags_map.get(tag_name.replace('_', '-')) or
--         tags_map.get(tag_name.replace('-', '_')) or
--         'Unknown'
--     )

-- =============================================================================
-- 3. COMMON TAG PATTERNS
-- =============================================================================

-- Common tag names to extract:
-- - environment: dev, staging, prod
-- - team: data-engineering, analytics, platform
-- - project: project-name, project_id
-- - cost-center: cost-center-code
-- - owner: team-email, owner-name
-- - classification: public, internal, confidential
-- - lifecycle: active, deprecated, experimental

-- =============================================================================
-- 4. TAG EXTRACTION EXAMPLES
-- =============================================================================

-- Example tag extraction patterns:
-- 1. Environment tags: 'env', 'environment', 'ENV', 'Environment'
-- 2. Team tags: 'team', 'Team', 'TEAM', 'team-name'
-- 3. Project tags: 'project', 'Project', 'PROJECT', 'project-id'
-- 4. Cost center tags: 'cost-center', 'Cost-Center', 'COST_CENTER', 'cost_center'
-- 5. Owner tags: 'owner', 'Owner', 'OWNER', 'owner-name'
-- 6. Classification tags: 'classification', 'Classification', 'CLASSIFICATION'

-- =============================================================================
-- 5. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 04_processing_logic.sql - Create data processing logic
-- 2. 05_metrics_calculation_functions.sql - Create metrics calculation functions
-- 3. 06_workflow_deployment.sql - Deploy Databricks workflows

SELECT 'Tag extraction patterns documented successfully!' as message;
SELECT 'Note: SQL functions are not supported in Databricks SQL' as message;
SELECT 'Tag extraction implementation will be done in Python processing scripts' as message;
SELECT 'Ready for processing logic.' as message;