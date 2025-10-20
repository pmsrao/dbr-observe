-- =============================================================================
-- Databricks Observability Platform - Tag Extraction Functions
-- =============================================================================
-- Purpose: Create tag extraction functions for showback and cost allocation
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. STANDARD TAG EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_tag(
    tags MAP<STRING, STRING>,
    tag_name STRING
)
RETURNS STRING
LANGUAGE SQL
AS $$
    COALESCE(
        tags[tag_name],
        tags[UPPER(tag_name)],
        tags[LOWER(tag_name)],
        tags[INITCAP(tag_name)],
        tags[REPLACE(tag_name, '_', '-')],
        tags[REPLACE(tag_name, '-', '_')],
        'Unknown'
    )
$$;

-- =============================================================================
-- 2. COST CENTER EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_cost_center(
    tags MAP<STRING, STRING>
)
RETURNS STRING
LANGUAGE SQL
AS $$
    COALESCE(
        obs.meta.extract_tag(tags, 'cost_center'),
        obs.meta.extract_tag(tags, 'CostCenter'),
        obs.meta.extract_tag(tags, 'cost-center'),
        obs.meta.extract_tag(tags, 'Cost Center'),
        'Unknown'
    )
$$;

-- =============================================================================
-- 3. BUSINESS UNIT EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_business_unit(
    tags MAP<STRING, STRING>
)
RETURNS STRING
LANGUAGE SQL
AS $$
    COALESCE(
        obs.meta.extract_tag(tags, 'business_unit'),
        obs.meta.extract_tag(tags, 'BusinessUnit'),
        obs.meta.extract_tag(tags, 'business-unit'),
        obs.meta.extract_tag(tags, 'Business Unit'),
        'Unknown'
    )
$$;

-- =============================================================================
-- 4. DEPARTMENT EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_department(
    tags MAP<STRING, STRING>
)
RETURNS STRING
LANGUAGE SQL
AS $$
    COALESCE(
        obs.meta.extract_tag(tags, 'department'),
        obs.meta.extract_tag(tags, 'Department'),
        obs.meta.extract_tag(tags, 'dept'),
        obs.meta.extract_tag(tags, 'Dept'),
        'Unknown'
    )
$$;

-- =============================================================================
-- 5. ENVIRONMENT EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_environment(
    tags MAP<STRING, STRING>
)
RETURNS STRING
LANGUAGE SQL
AS $$
    COALESCE(
        obs.meta.extract_tag(tags, 'environment'),
        obs.meta.extract_tag(tags, 'Environment'),
        obs.meta.extract_tag(tags, 'env'),
        obs.meta.extract_tag(tags, 'Env'),
        'Unknown'
    )
$$;

-- =============================================================================
-- 6. TEAM EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_team(
    tags MAP<STRING, STRING>
)
RETURNS STRING
LANGUAGE SQL
AS $$
    COALESCE(
        obs.meta.extract_tag(tags, 'team'),
        obs.meta.extract_tag(tags, 'Team'),
        obs.meta.extract_tag(tags, 'owner_team'),
        obs.meta.extract_tag(tags, 'OwnerTeam'),
        'Unknown'
    )
$$;

-- =============================================================================
-- 7. PROJECT EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_project(
    tags MAP<STRING, STRING>
)
RETURNS STRING
LANGUAGE SQL
AS $$
    COALESCE(
        obs.meta.extract_tag(tags, 'project'),
        obs.meta.extract_tag(tags, 'Project'),
        obs.meta.extract_tag(tags, 'project_name'),
        obs.meta.extract_tag(tags, 'ProjectName'),
        'Unknown'
    )
$$;

-- =============================================================================
-- 8. DATA PRODUCT EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_data_product(
    tags MAP<STRING, STRING>
)
RETURNS STRING
LANGUAGE SQL
AS $$
    COALESCE(
        obs.meta.extract_tag(tags, 'data_product'),
        obs.meta.extract_tag(tags, 'DataProduct'),
        obs.meta.extract_tag(tags, 'data-product'),
        obs.meta.extract_tag(tags, 'Data Product'),
        'Unknown'
    )
$$;

-- =============================================================================
-- 9. COMPREHENSIVE TAG EXTRACTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.extract_all_tags(
    tags MAP<STRING, STRING>
)
RETURNS STRUCT<
    cost_center STRING,
    business_unit STRING,
    department STRING,
    environment STRING,
    team STRING,
    project STRING,
    data_product STRING
>
LANGUAGE SQL
AS $$
    STRUCT(
        obs.meta.extract_cost_center(tags) as cost_center,
        obs.meta.extract_business_unit(tags) as business_unit,
        obs.meta.extract_department(tags) as department,
        obs.meta.extract_environment(tags) as environment,
        obs.meta.extract_team(tags) as team,
        obs.meta.extract_project(tags) as project,
        obs.meta.extract_data_product(tags) as data_product
    )
$$;

-- =============================================================================
-- 10. TAG VALIDATION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.validate_tag_value(
    tag_value STRING,
    allowed_values ARRAY<STRING>
)
RETURNS STRING
LANGUAGE SQL
AS $$
    CASE 
        WHEN tag_value IN (SELECT value FROM UNNEST(allowed_values) AS t(value)) THEN tag_value
        ELSE 'Invalid'
    END
$$;

-- =============================================================================
-- 11. VERIFICATION
-- =============================================================================

-- Verify function creation
SHOW FUNCTIONS IN obs.meta LIKE 'extract_*';

-- Test tag extraction functions
SELECT obs.meta.extract_cost_center(MAP('cost_center', 'finance', 'team', 'data-eng')) as cost_center;
SELECT obs.meta.extract_environment(MAP('env', 'prod', 'environment', 'production')) as environment;
SELECT obs.meta.extract_all_tags(MAP('cost_center', 'finance', 'team', 'data-eng', 'env', 'prod')) as all_tags;

-- =============================================================================
-- 12. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 04_metrics_calculation_functions.sql - Create metrics calculation functions
-- 2. 05_processing_logic.sql - Create data processing logic
-- 3. 06_data_quality_functions.sql - Create data quality functions

PRINT 'Tag extraction functions created successfully!';
PRINT 'Functions: extract_tag, extract_cost_center, extract_business_unit, extract_department, extract_environment, extract_team, extract_project, extract_data_product, extract_all_tags';
PRINT 'Comprehensive tag extraction for showback and cost allocation';
PRINT 'Ready for metrics calculation functions.';
