-- =============================================================================
-- Databricks Observability Platform - Silver Audit Log Table
-- =============================================================================
-- Purpose: Create silver audit log table for governance and security
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

CREATE TABLE IF NOT EXISTS obs.silver.audit_log (
    -- Basic identifiers
    workspace_id STRING NOT NULL,
    event_id STRING NOT NULL,
    
    -- Event details
    event_time TIMESTAMP NOT NULL,
    event_type STRING,
    action_name STRING,
    resource_type STRING,
    resource_id STRING,
    
    -- User context
    user_id STRING,
    user_name STRING,
    service_principal_name STRING,
    
    -- Request context
    request_id STRING,
    session_id STRING,
    ip_address STRING,
    user_agent STRING,
    
    -- Result
    result STRING,
    error_message STRING,
    
    -- Additional context
    request_params MAP<STRING, STRING>,
    response STRING,
    
    -- Audit
    processing_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (workspace_id, event_time);

-- Set properties
ALTER TABLE obs.silver.audit_log SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- Add primary key constraint (idempotent)
ALTER TABLE obs.silver.audit_log 
DROP CONSTRAINT IF EXISTS pk_audit_log;

ALTER TABLE obs.silver.audit_log 
ADD CONSTRAINT pk_audit_log 
PRIMARY KEY (workspace_id, event_id);

SELECT 'Silver audit log table created successfully!' as message;
