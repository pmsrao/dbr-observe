# Databricks Observability Platform — Design (v3, HWM, No DLT)

**Author**: Data Platform Architect

> **What’s new in v3**
> - Explicitly included **`system.lakeflow.jobs`** (SCD2 in Silver).
> - **Unify at Silver** (not Gold):
>   - `silver.compute_entities` (SCD2) = clusters ∪ warehouses with a common key.
>   - `silver.workflow_entities` (SCD2) = jobs ∪ pipelines with harmonized attributes.
>   - `silver.workflow_runs` = job_run_timeline ∪ pipeline_update_timeline (run-level).
> - Gold dims (`dim_compute`, `dim_workflow`) are now thin views over the unified Silver entities.

---

## 0) Rationale — Why unify in Silver?

- **Conformed keys earlier** → All downstream curation and facts share the same surrogate keys, reducing edge-case joins.
- **One place for SCD2** → History semantics live in one table per entity class; Gold can be rebuilt without SCD logic.
- **Schema evolution friendly** → Keep entity-specific fields as nullable columns + a `_raw` struct to avoid lossiness.
- **Gold stays analytical** → Facts/dims are thin and faster to iterate.

Trade-offs: Silver gets slightly wider; we control this by strong naming, `_raw` preservation, and column documentation.

---

## 1) Sources (unchanged)
- Billing: `system.billing.usage`, `system.billing.list_prices` (if enabled)
- Lakeflow: `jobs`, `job_tasks`, `job_run_timeline`, `job_task_run_timeline`, `pipelines`, `pipeline_update_timeline`
- Compute: `clusters`, `node_types`, `node_timeline`
- Warehouses: `warehouses`, `warehouse_events`
- Storage: `predictive_optimization_operations_history`
- Access: `workspaces_latest`, `audit`
- Query: `history`

---

## 2) Silver (Curated) — Unified Entities & SCD2

### 2.1 Tables Overview
- **Billing**
  - `silver.billing_usage`
  - `silver.billing_list_prices`
- **Unified Compute (SCD2)**
  - `silver.compute_entities`  ←  clusters ∪ warehouses
  - `silver.node_usage_minutely`  ←  node_timeline
  - `silver.warehouse_events`     ←  warehouse_events
- **Unified Workflows (SCD2 where applicable)**
  - `silver.workflow_entities` (SCD2) ← jobs ∪ pipelines
  - `silver.workflow_runs`      ← job_run_timeline ∪ pipeline_update_timeline
  - `silver.job_task_runs`      ← job_task_run_timeline (no pipeline equivalent)
  - `silver.job_tasks` (SCD2)   ← job_tasks
- **Other**
  - `silver.workspaces` (SCD2-lite if needed), `silver.audit_log`, `silver.query_history`, `silver.storage_ops`

### 2.2 Keys and SCD2
- **Compute** natural key: `(workspace_id, compute_type, compute_id)` where:
  - clusters → `(workspace_id, 'CLUSTER', cluster_id)`
  - warehouses → `(workspace_id, 'WAREHOUSE', warehouse_id)`
- **Workflow** natural key: `(workspace_id, workflow_type, workflow_id)` where:
  - jobs → `(workspace_id, 'JOB', job_id)`
  - pipelines → `(workspace_id, 'PIPELINE', pipeline_id)`
- Use `effective_start_ts_utc`, `effective_end_ts_utc`, `is_current`.

### 2.3 Build — `silver.compute_entities` (SCD2)
**Staging (harmonize columns):**
```sql
CREATE OR REPLACE VIEW obs.silver_stg.compute_entities_h AS
SELECT
  workspace_id,
  'CLUSTER' AS compute_type,
  cluster_id AS compute_id,
  cluster_name, owned_by AS owner,
  driver_node_type, worker_node_type, policy_id, data_security_mode,
  tags, change_time AS change_ts, delete_time,
  named_struct('source','clusters','raw', to_json(named_struct('*', t.*))) AS _raw
FROM obs.bronze.system_compute_clusters t
UNION ALL
SELECT
  workspace_id,
  'WAREHOUSE' AS compute_type,
  warehouse_id AS compute_id,
  warehouse_name AS cluster_name, NULL AS owner,
  NULL AS driver_node_type, NULL AS worker_node_type, NULL AS policy_id, NULL AS data_security_mode,
  tags, change_time AS change_ts, delete_time,
  named_struct('source','warehouses','raw', to_json(named_struct('*', w.*))) AS _raw
FROM obs.bronze.system_compute_warehouses w;
```

**SCD2 Merge (sketch):**
```sql
MERGE INTO obs.silver.compute_entities t
USING obs.silver_stg.compute_entities_h s
ON  t.workspace_id = s.workspace_id
AND t.compute_type = s.compute_type
AND t.compute_id   = s.compute_id
AND t.is_current   = true
WHEN MATCHED AND (hash(t.cluster_name, t.owner, t.driver_node_type, t.worker_node_type, t.policy_id, t.data_security_mode, t.tags)
                  <> hash(s.cluster_name, s.owner, s.driver_node_type, s.worker_node_type, s.policy_id, s.data_security_mode, s.tags))
  THEN UPDATE SET t.effective_end_ts_utc = s.change_ts, t.is_current=false
WHEN NOT MATCHED THEN
  INSERT (workspace_id, compute_type, compute_id, cluster_name, owner, driver_node_type, worker_node_type, policy_id, data_security_mode, tags,
          effective_start_ts_utc, effective_end_ts_utc, is_current, _raw)
  VALUES (s.workspace_id, s.compute_type, s.compute_id, s.cluster_name, s.owner, s.driver_node_type, s.worker_node_type, s.policy_id, s.data_security_mode, s.tags,
          s.change_ts, TIMESTAMP('9999-12-31'), true, s._raw);
```

### 2.4 Build — `silver.workflow_entities` (SCD2) & `silver.workflow_runs`
**Workflow entities staging:**
```sql
CREATE OR REPLACE VIEW obs.silver_stg.workflow_entities_h AS
SELECT workspace_id, 'JOB' AS workflow_type, job_id   AS workflow_id, name, run_as, tags, change_time AS change_ts, delete_time,
       named_struct('source','jobs','raw', to_json(named_struct('*', j.*))) AS _raw
FROM obs.bronze.system_lakeflow_jobs j
UNION ALL
SELECT workspace_id, 'PIPELINE' AS workflow_type, pipeline_id AS workflow_id, name, NULL as run_as, NULL as tags, change_time AS change_ts, delete_time,
       named_struct('source','pipelines','raw', to_json(named_struct('*', p.*))) AS _raw
FROM obs.bronze.system_lakeflow_pipelines p;
```

**SCD2 merge is identical in pattern to compute_entities.**

**Unify runs:**
```sql
CREATE OR REPLACE VIEW obs.silver.workflow_runs AS
SELECT workspace_id, 'JOB' AS workflow_type, job_id AS workflow_id, job_run_id AS run_id,
       period_start_time AS start_time, period_end_time AS end_time, result_state, termination_code,
       job_parameters, parent_run_id
FROM obs.bronze.system_lakeflow_job_run_timeline
UNION ALL
SELECT workspace_id, 'PIPELINE' AS workflow_type, pipeline_id AS workflow_id, update_id AS run_id,
       period_start_time AS start_time, period_end_time AS end_time, result_state, termination_code,
       NULL AS job_parameters, NULL as parent_run_id
FROM obs.bronze.system_lakeflow_pipeline_update_timeline;
```

**Job task runs remain separate:** `silver.job_task_runs` from `job_task_run_timeline`.

---

## 3) Tag Dimensions (unchanged intent)
- `dim_cost_center`, `dim_business_unit`, `dim_department`, `dim_data_product` (nullable FKs).
- Extract from `billing_usage.custom_tags/tags` and `compute_entities.tags` and `workflow_entities.tags`.

---

## 4) Gold — Thin over Unified Silver

### 4.1 Dims
- `dim_compute`  → thin view over `silver.compute_entities` (current rows or full history).
- `dim_workflow` → thin view over `silver.workflow_entities` (current rows or full history).
- Others: `dim_workspace`, `dim_user`, `dim_node_type`, tag dims, `dim_date`.

### 4.2 Facts
- `fct_billing_usage_daily` (joins to `dim_compute` and tag dims; can also link to `dim_workflow` when job_id is present).
- `fct_workflow_runs` (from `silver.workflow_runs`, joins `dim_workflow`, `dim_compute` via compute hints if present).
- `fct_job_task_runs` (task-level).
- `fct_node_usage_hourly` (rollup from minutely).
- `fct_warehouse_events`.
- `fct_query_history`.

**Example: thin `dim_compute`**
```sql
CREATE OR REPLACE VIEW obs.gold.dim_compute AS
SELECT
  sha2(concat(compute_type, '|', workspace_id, '|', compute_id), 256) AS compute_sk,
  * EXCEPT (_raw)
FROM obs.silver.compute_entities
WHERE is_current = true;  -- or expose full history with SCD columns
```

---

## 5) README Pointers Updated
- Lakeflow **jobs are included** explicitly (SCD2 at Silver and part of `workflow_entities`).
- Unified at **Silver** for both compute and workflows to standardize keys and SCD behavior earlier.
- Gold is simplified to analytics-only views and star schemas.

---

## 6) Next Steps (if you want code stubs)
- Emit `silver_stg` views and SCD2 merge notebooks for compute & workflow entities.
- Update Gold SQL to consume unified Silver (dims/facts).
- Expand `fct_billing_usage_daily` with list price joins and tag FKs.
