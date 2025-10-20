-- =============================================================================
-- Databricks Observability Platform - SCD2 Functions
-- =============================================================================
-- Purpose: Create SCD2 merge functions for entity processing
-- Author: Data Platform Architect
-- Date: December 2024
-- =============================================================================

-- =============================================================================
-- 1. COMPUTE ENTITIES SCD2 MERGE FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.merge_compute_entities_scd2()
RETURNS VOID
LANGUAGE SQL
AS $$
    MERGE INTO obs.silver.compute_entities AS target
    USING (
        SELECT 
            workspace_id,
            compute_type,
            compute_id,
            name,
            owner,
            driver_node_type,
            worker_node_type,
            worker_count,
            min_autoscale_workers,
            max_autoscale_workers,
            auto_termination_minutes,
            enable_elastic_disk,
            data_security_mode,
            policy_id,
            dbr_version,
            cluster_source,
            warehouse_type,
            warehouse_size,
            warehouse_channel,
            min_clusters,
            max_clusters,
            auto_stop_minutes,
            tags,
            create_time,
            delete_time,
            change_time,
            processing_timestamp,
            -- Generate record hash for change detection
            SHA2(CONCAT(
                COALESCE(workspace_id, ''), '|',
                COALESCE(compute_type, ''), '|',
                COALESCE(compute_id, ''), '|',
                COALESCE(name, ''), '|',
                COALESCE(owner, ''), '|',
                COALESCE(driver_node_type, ''), '|',
                COALESCE(worker_node_type, ''), '|',
                COALESCE(CAST(worker_count AS STRING), ''), '|',
                COALESCE(CAST(min_autoscale_workers AS STRING), ''), '|',
                COALESCE(CAST(max_autoscale_workers AS STRING), ''), '|',
                COALESCE(CAST(auto_termination_minutes AS STRING), ''), '|',
                COALESCE(CAST(enable_elastic_disk AS STRING), ''), '|',
                COALESCE(data_security_mode, ''), '|',
                COALESCE(policy_id, ''), '|',
                COALESCE(dbr_version, ''), '|',
                COALESCE(cluster_source, ''), '|',
                COALESCE(warehouse_type, ''), '|',
                COALESCE(warehouse_size, ''), '|',
                COALESCE(warehouse_channel, ''), '|',
                COALESCE(CAST(min_clusters AS STRING), ''), '|',
                COALESCE(CAST(max_clusters AS STRING), ''), '|',
                COALESCE(CAST(auto_stop_minutes AS STRING), ''), '|',
                COALESCE(TO_JSON_STRING(tags), '')
            ), 256) AS record_hash
        FROM obs.silver.compute_entities_staging
    ) AS source
    ON target.workspace_id = source.workspace_id
      AND target.compute_type = source.compute_type
      AND target.compute_id = source.compute_id
      AND target.is_current = true
    WHEN MATCHED AND target.record_hash != source.record_hash THEN
        UPDATE SET 
            effective_end_ts = source.change_time,
            is_current = false,
            dw_updated_ts = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (
            workspace_id, compute_type, compute_id,
            effective_start_ts, effective_end_ts, is_current,
            record_hash, dw_created_ts, dw_updated_ts, processing_timestamp,
            name, owner, driver_node_type, worker_node_type, worker_count,
            min_autoscale_workers, max_autoscale_workers, auto_termination_minutes,
            enable_elastic_disk, data_security_mode, policy_id, dbr_version,
            cluster_source, warehouse_type, warehouse_size, warehouse_channel,
            min_clusters, max_clusters, auto_stop_minutes, tags,
            create_time, delete_time
        )
        VALUES (
            source.workspace_id, source.compute_type, source.compute_id,
            source.change_time, TIMESTAMP('9999-12-31'), true,
            source.record_hash, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, source.processing_timestamp,
            source.name, source.owner, source.driver_node_type, source.worker_node_type, source.worker_count,
            source.min_autoscale_workers, source.max_autoscale_workers, source.auto_termination_minutes,
            source.enable_elastic_disk, source.data_security_mode, source.policy_id, source.dbr_version,
            source.cluster_source, source.warehouse_type, source.warehouse_size, source.warehouse_channel,
            source.min_clusters, source.max_clusters, source.auto_stop_minutes, source.tags,
            source.create_time, source.delete_time
        );
$$;

-- =============================================================================
-- 2. WORKFLOW ENTITIES SCD2 MERGE FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.merge_workflow_entities_scd2()
RETURNS VOID
LANGUAGE SQL
AS $$
    MERGE INTO obs.silver.workflow_entities AS target
    USING (
        SELECT 
            workspace_id,
            workflow_type,
            workflow_id,
            name,
            description,
            creator_id,
            run_as,
            pipeline_type,
            settings,
            tags,
            create_time,
            delete_time,
            change_time,
            processing_timestamp,
            -- Generate record hash for change detection
            SHA2(CONCAT(
                COALESCE(workspace_id, ''), '|',
                COALESCE(workflow_type, ''), '|',
                COALESCE(workflow_id, ''), '|',
                COALESCE(name, ''), '|',
                COALESCE(description, ''), '|',
                COALESCE(creator_id, ''), '|',
                COALESCE(run_as, ''), '|',
                COALESCE(pipeline_type, ''), '|',
                COALESCE(TO_JSON_STRING(settings), ''), '|',
                COALESCE(TO_JSON_STRING(tags), '')
            ), 256) AS record_hash
        FROM obs.silver.workflow_entities_staging
    ) AS source
    ON target.workspace_id = source.workspace_id
      AND target.workflow_type = source.workflow_type
      AND target.workflow_id = source.workflow_id
      AND target.is_current = true
    WHEN MATCHED AND target.record_hash != source.record_hash THEN
        UPDATE SET 
            effective_end_ts = source.change_time,
            is_current = false,
            dw_updated_ts = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (
            workspace_id, workflow_type, workflow_id,
            effective_start_ts, effective_end_ts, is_current,
            record_hash, dw_created_ts, dw_updated_ts, processing_timestamp,
            name, description, creator_id, run_as, pipeline_type, settings, tags,
            create_time, delete_time
        )
        VALUES (
            source.workspace_id, source.workflow_type, source.workflow_id,
            source.change_time, TIMESTAMP('9999-12-31'), true,
            source.record_hash, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, source.processing_timestamp,
            source.name, source.description, source.creator_id, source.run_as, source.pipeline_type, source.settings, source.tags,
            source.create_time, source.delete_time
        );
$$;

-- =============================================================================
-- 3. GENERIC SCD2 MERGE FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.merge_scd2_entity(
    entity_table STRING,
    staging_view STRING,
    natural_key_columns ARRAY<STRING>,
    business_columns ARRAY<STRING>
)
RETURNS VOID
LANGUAGE SQL
AS $$
    -- This is a template function that can be customized for specific entities
    -- Implementation would depend on the specific requirements
    SELECT 'Generic SCD2 merge function - to be implemented based on specific needs';
$$;

-- =============================================================================
-- 4. LATE-ARRIVING DATA HANDLING FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION obs.meta.handle_late_arriving_data(
    entity_table STRING,
    lookback_days INT
)
RETURNS VOID
LANGUAGE SQL
AS $$
    -- Function to handle late-arriving dimension changes
    -- This would check for changes in the last N days and reprocess them
    SELECT 'Late-arriving data handling function - to be implemented based on specific needs';
$$;

-- =============================================================================
-- 5. VERIFICATION
-- =============================================================================

-- Verify function creation
SHOW FUNCTIONS IN obs.meta;

-- Test SCD2 functions (these would be called during processing)
-- SELECT obs.meta.merge_compute_entities_scd2();
-- SELECT obs.meta.merge_workflow_entities_scd2();

-- =============================================================================
-- 6. NEXT STEPS
-- =============================================================================

-- After running this script, proceed with:
-- 1. 03_processing_logic.sql - Create data processing logic
-- 2. 04_tag_extraction_functions.sql - Create tag extraction functions
-- 3. 05_metrics_calculation_functions.sql - Create metrics calculation functions

PRINT 'SCD2 functions created successfully!';
PRINT 'Functions: merge_compute_entities_scd2, merge_workflow_entities_scd2';
PRINT 'Generic SCD2 merge pattern implemented';
PRINT 'Ready for processing logic.';
