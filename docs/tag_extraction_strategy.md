# Tag Extraction Strategy

> **Navigation**: [README](../README.md) | [Architecture & Design](architecture.md) | [Database Best Practices](database_best_practices.md) | [Configuration Guide](config_folder_guide.md) | [Deployment Guide](deployment_guide.md) | [Documentation Reorganization Summary](documentation_reorganization_summary.md)

# Tag Extraction and Processing Strategy

## 1) Tag Sources and Mapping

### 1.1 Primary Tag Sources
```sql
-- Billing Usage Tags
system.billing.usage.custom_tags
system.billing.usage.usage_metadata.tags

-- Compute Entity Tags  
system.compute.clusters.tags
system.compute.warehouses.tags

-- Workflow Entity Tags
system.lakeflow.jobs.tags
system.lakeflow.pipelines.tags

-- Query History Tags (from compute context)
system.query.history.compute.tags
```

### 1.2 Tag Extraction Hierarchy
```sql
-- Priority Order for Tag Resolution
1. Direct entity tags (highest priority)
2. Billing usage custom_tags
3. Compute context tags
4. Default values (lowest priority)
```

## 2) Standardized Tag Categories

### 2.1 Cost Allocation Tags
```sql
-- Primary Cost Tags
cost_center STRING           -- 'finance', 'engineering', 'marketing'
business_unit STRING         -- 'data-platform', 'analytics', 'ml-ops'
department STRING            -- 'data-engineering', 'business-intelligence'
data_product STRING          -- 'customer-360', 'recommendation-engine'

-- Secondary Cost Tags
project STRING               -- 'customer-analytics', 'fraud-detection'
cost_category STRING         -- 'compute', 'storage', 'networking'
chargeback_entity STRING     -- Entity responsible for costs
```

### 2.2 Operational Tags
```sql
-- Environment Tags
environment STRING           -- 'dev', 'staging', 'prod', 'test'
tier STRING                 -- 'bronze', 'silver', 'gold'
criticality STRING          -- 'critical', 'important', 'standard'

-- Team Tags
team STRING                 -- 'data-eng', 'analytics', 'ml-team'
owner STRING                -- Team or individual owner
steward STRING              -- Data steward
```

### 2.3 Technical Tags
```sql
-- Technical Classification
data_domain STRING          -- 'customer', 'product', 'financial'
data_classification STRING  -- 'public', 'internal', 'confidential', 'restricted'
retention_period STRING     -- '30d', '1y', '7y', 'permanent'
processing_type STRING      -- 'batch', 'streaming', 'real-time'
```

## 3) Tag Extraction Functions

### 3.1 Core Tag Extraction Function
```sql
-- Generic tag extraction with fallback logic
CREATE OR REPLACE FUNCTION extract_tag(
  tags MAP<STRING, STRING>, 
  tag_name STRING, 
  default_value STRING DEFAULT 'Unknown'
) RETURNS STRING
LANGUAGE SQL
AS $$
  COALESCE(
    tags[tag_name],                    -- Exact match
    tags[UPPER(tag_name)],            -- Uppercase match
    tags[LOWER(tag_name)],            -- Lowercase match
    tags[INITCAP(tag_name)],          -- Title case match
    tags[REPLACE(tag_name, '_', '-')], -- Hyphen variant
    tags[REPLACE(tag_name, '-', '_')], -- Underscore variant
    default_value
  )
$$;
```

### 3.2 Specific Tag Extraction Functions
```sql
-- Cost Center Extraction
CREATE OR REPLACE FUNCTION extract_cost_center(tags MAP<STRING, STRING>) 
RETURNS STRING
LANGUAGE SQL
AS $$
  extract_tag(tags, 'cost_center', 'Unknown')
$$;

-- Business Unit Extraction
CREATE OR REPLACE FUNCTION extract_business_unit(tags MAP<STRING, STRING>) 
RETURNS STRING
LANGUAGE SQL
AS $$
  extract_tag(tags, 'business_unit', 'Unknown')
$$;

-- Environment Extraction
CREATE OR REPLACE FUNCTION extract_environment(tags MAP<STRING, STRING>) 
RETURNS STRING
LANGUAGE SQL
AS $$
  extract_tag(tags, 'env', 'Unknown')
$$;

-- Team Extraction
CREATE OR REPLACE FUNCTION extract_team(tags MAP<STRING, STRING>) 
RETURNS STRING
LANGUAGE SQL
AS $$
  extract_tag(tags, 'team', 'Unknown')
$$;
```

## 4) Tag Processing Pipeline

### 4.1 Tag Harmonization Process
```sql
-- Step 1: Collect tags from all sources
WITH tag_sources AS (
  -- From compute entities
  SELECT workspace_id, compute_id, 'COMPUTE' as source_type, tags
  FROM bronze.system_compute_clusters
  WHERE tags IS NOT NULL
  
  UNION ALL
  
  SELECT workspace_id, warehouse_id as compute_id, 'WAREHOUSE' as source_type, tags
  FROM bronze.system_compute_warehouses
  WHERE tags IS NOT NULL
  
  UNION ALL
  
  -- From workflow entities
  SELECT workspace_id, job_id as workflow_id, 'JOB' as source_type, tags
  FROM bronze.system_lakeflow_jobs
  WHERE tags IS NOT NULL
  
  UNION ALL
  
  SELECT workspace_id, pipeline_id as workflow_id, 'PIPELINE' as source_type, tags
  FROM bronze.system_lakeflow_pipelines
  WHERE tags IS NOT NULL
),

-- Step 2: Harmonize tag values
harmonized_tags AS (
  SELECT 
    workspace_id,
    entity_id,
    source_type,
    extract_cost_center(tags) as cost_center,
    extract_business_unit(tags) as business_unit,
    extract_environment(tags) as environment,
    extract_team(tags) as team,
    extract_tag(tags, 'project', 'Unknown') as project,
    extract_tag(tags, 'data_product', 'Unknown') as data_product,
    extract_tag(tags, 'department', 'Unknown') as department,
    tags as raw_tags
  FROM tag_sources
)

-- Step 3: Create unified tag mapping
SELECT * FROM harmonized_tags;
```

### 4.2 Tag Validation and Standardization
```sql
-- Tag value standardization
CREATE OR REPLACE FUNCTION standardize_tag_value(
  tag_value STRING, 
  tag_type STRING
) RETURNS STRING
LANGUAGE SQL
AS $$
  CASE tag_type
    WHEN 'cost_center' THEN 
      CASE UPPER(TRIM(tag_value))
        WHEN 'FINANCE' THEN 'finance'
        WHEN 'ENGINEERING' THEN 'engineering'
        WHEN 'MARKETING' THEN 'marketing'
        WHEN 'SALES' THEN 'sales'
        ELSE LOWER(TRIM(tag_value))
      END
    WHEN 'environment' THEN
      CASE UPPER(TRIM(tag_value))
        WHEN 'DEV' THEN 'dev'
        WHEN 'DEVELOPMENT' THEN 'dev'
        WHEN 'STAGING' THEN 'staging'
        WHEN 'STAGE' THEN 'staging'
        WHEN 'PROD' THEN 'prod'
        WHEN 'PRODUCTION' THEN 'prod'
        WHEN 'TEST' THEN 'test'
        ELSE LOWER(TRIM(tag_value))
      END
    WHEN 'business_unit' THEN
      CASE UPPER(TRIM(tag_value))
        WHEN 'DATA-PLATFORM' THEN 'data-platform'
        WHEN 'DATA_PLATFORM' THEN 'data-platform'
        WHEN 'ANALYTICS' THEN 'analytics'
        WHEN 'ML-OPS' THEN 'ml-ops'
        WHEN 'ML_OPS' THEN 'ml-ops'
        ELSE LOWER(REPLACE(TRIM(tag_value), '_', '-'))
      END
    ELSE LOWER(TRIM(tag_value))
  END
$$;
```

## 5) Tag Quality and Governance

### 5.1 Tag Quality Metrics
```sql
-- Tag completeness metrics
CREATE OR REPLACE FUNCTION calculate_tag_completeness(
  cost_center STRING,
  business_unit STRING,
  environment STRING,
  team STRING
) RETURNS DOUBLE
LANGUAGE SQL
AS $$
  CASE 
    WHEN cost_center != 'Unknown' AND business_unit != 'Unknown' 
         AND environment != 'Unknown' AND team != 'Unknown' THEN 1.0
    WHEN cost_center != 'Unknown' AND business_unit != 'Unknown' 
         AND environment != 'Unknown' THEN 0.75
    WHEN cost_center != 'Unknown' AND business_unit != 'Unknown' THEN 0.5
    WHEN cost_center != 'Unknown' THEN 0.25
    ELSE 0.0
  END
$$;
```

### 5.2 Tag Validation Rules
```sql
-- Tag validation constraints
ALTER TABLE silver.compute_entities 
ADD CONSTRAINT check_cost_center 
CHECK (cost_center IN ('finance', 'engineering', 'marketing', 'sales', 'Unknown'));

ALTER TABLE silver.compute_entities 
ADD CONSTRAINT check_environment 
CHECK (environment IN ('dev', 'staging', 'prod', 'test', 'Unknown'));

ALTER TABLE silver.compute_entities 
ADD CONSTRAINT check_business_unit 
CHECK (business_unit IN ('data-platform', 'analytics', 'ml-ops', 'data-engineering', 'Unknown'));
```

## 6) Tag-Based Cost Attribution

### 6.1 Cost Attribution Logic
```sql
-- Cost attribution based on tag hierarchy
CREATE OR REPLACE FUNCTION get_cost_attribution(
  usage_metadata STRUCT<cluster_id STRING, job_id STRING, warehouse_id STRING>,
  compute_tags MAP<STRING, STRING>,
  workflow_tags MAP<STRING, STRING>
) RETURNS STRUCT<
  cost_center STRING,
  business_unit STRING,
  department STRING,
  data_product STRING,
  environment STRING,
  team STRING,
  project STRING
>
LANGUAGE SQL
AS $$
  STRUCT(
    -- Priority: Direct usage tags > Compute tags > Workflow tags > Default
    COALESCE(
      extract_tag(usage_metadata.tags, 'cost_center'),
      extract_cost_center(compute_tags),
      extract_cost_center(workflow_tags),
      'Unknown'
    ) as cost_center,
    
    COALESCE(
      extract_tag(usage_metadata.tags, 'business_unit'),
      extract_business_unit(compute_tags),
      extract_business_unit(workflow_tags),
      'Unknown'
    ) as business_unit,
    
    COALESCE(
      extract_tag(usage_metadata.tags, 'department'),
      extract_tag(compute_tags, 'department'),
      extract_tag(workflow_tags, 'department'),
      'Unknown'
    ) as department,
    
    COALESCE(
      extract_tag(usage_metadata.tags, 'data_product'),
      extract_tag(compute_tags, 'data_product'),
      extract_tag(workflow_tags, 'data_product'),
      'Unknown'
    ) as data_product,
    
    COALESCE(
      extract_tag(usage_metadata.tags, 'env'),
      extract_environment(compute_tags),
      extract_environment(workflow_tags),
      'Unknown'
    ) as environment,
    
    COALESCE(
      extract_tag(usage_metadata.tags, 'team'),
      extract_team(compute_tags),
      extract_team(workflow_tags),
      'Unknown'
    ) as team,
    
    COALESCE(
      extract_tag(usage_metadata.tags, 'project'),
      extract_tag(compute_tags, 'project'),
      extract_tag(workflow_tags, 'project'),
      'Unknown'
    ) as project
  )
$$;
```

## 7) Tag Monitoring and Alerting

### 7.1 Tag Quality Monitoring
```sql
-- Tag quality dashboard query
CREATE VIEW ops.tag_quality_metrics AS
SELECT 
  workspace_id,
  entity_type,
  COUNT(*) as total_entities,
  COUNT(CASE WHEN cost_center != 'Unknown' THEN 1 END) as cost_center_tagged,
  COUNT(CASE WHEN business_unit != 'Unknown' THEN 1 END) as business_unit_tagged,
  COUNT(CASE WHEN environment != 'Unknown' THEN 1 END) as environment_tagged,
  COUNT(CASE WHEN team != 'Unknown' THEN 1 END) as team_tagged,
  AVG(calculate_tag_completeness(cost_center, business_unit, environment, team)) as avg_completeness,
  COUNT(CASE WHEN calculate_tag_completeness(cost_center, business_unit, environment, team) = 1.0 THEN 1 END) as fully_tagged,
  COUNT(CASE WHEN calculate_tag_completeness(cost_center, business_unit, environment, team) = 0.0 THEN 1 END) as untagged
FROM silver.compute_entities
WHERE is_current = true
GROUP BY workspace_id, entity_type;
```

### 7.2 Tag Compliance Alerts
```sql
-- Alert for untagged resources
CREATE VIEW ops.untagged_resources_alert AS
SELECT 
  workspace_id,
  entity_type,
  entity_id,
  name,
  'UNTAGGED_RESOURCE' as alert_type,
  'Resource missing required tags' as alert_message,
  CURRENT_TIMESTAMP as alert_time
FROM silver.compute_entities
WHERE is_current = true
  AND calculate_tag_completeness(cost_center, business_unit, environment, team) < 0.5;
```

This comprehensive tag extraction strategy ensures consistent, high-quality tag data for cost attribution and governance across the observability platform.
