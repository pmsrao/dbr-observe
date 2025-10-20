# SQL Setup Guide

This guide helps you run all SQL setup files for the Databricks Observability Platform.

## Prerequisites

1. **Install databricks-sql-connector**:
   ```bash
   pip install databricks-sql-connector
   ```

2. **Get your SQL Warehouse HTTP Path**:
   - Go to Databricks UI > SQL Warehouses
   - Click on your warehouse
   - Go to "Connection Details" tab
   - Copy the "HTTP path" (e.g., `/sql/1.0/warehouses/abc123def456`)

3. **Set environment variables**:
   ```bash
   export DATABRICKS_HOST="https://your-databricks-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-databricks-token"
   export DATABRICKS_WAREHOUSE_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
   ```

## Running the Setup

### Option 1: Using the shell script (recommended)
```bash
# Make executable (first time only)
chmod +x scripts/run_sql_setup.sh

# Run all SQL files
./scripts/run_sql_setup.sh

# Resume from a specific file
./scripts/run_sql_setup.sh --start-from "src/sql/ddl/21_silver_entities.sql"

# Retry only failed files
./scripts/run_sql_setup.sh --retry-failed
```

### Option 2: Using Python directly
```bash
# Run all SQL files
python3 scripts/run_all_sql.py

# Resume from a specific file
python3 scripts/run_all_sql.py --start-from "src/sql/ddl/21_silver_entities.sql"

# Retry only failed files
python3 scripts/run_all_sql.py --retry-failed

# Use warehouse ID instead of HTTP path
python3 scripts/run_all_sql.py --warehouse-id "your-warehouse-id"
```

## Features

- ✅ **Resume capability**: If execution fails, you can resume from where it left off
- ✅ **Progress tracking**: Saves progress to `sql_setup_progress.json`
- ✅ **Error handling**: Interactive prompts when files fail
- ✅ **Skip completed**: Automatically skips already completed files
- ✅ **Retry failed**: Option to retry only failed files

## File Execution Order

The script runs SQL files in this order:

1. **Setup** (01-03): Catalog, permissions, watermarks
2. **Bronze** (11-16): Raw data tables
3. **Silver** (21-26): Curated data tables
4. **Gold** (31-32): Analytics-ready tables
5. **Transformations** (41-46): Views, functions, processing

## Troubleshooting

### Connection Issues
- Verify your `DATABRICKS_HOST` URL is correct
- Check that your `DATABRICKS_TOKEN` is valid and not expired
- Ensure the SQL Warehouse is running and accessible

### Permission Issues
- Make sure your user has permissions to create catalogs and schemas
- Verify you have access to the SQL Warehouse

### File Not Found
- Run the script from the project root directory
- Ensure all SQL files exist in the expected locations

### Resume After Failure
```bash
# Check what failed
cat sql_setup_progress.json

# Resume from a specific file
./scripts/run_sql_setup.sh --start-from "src/sql/ddl/21_silver_entities.sql"

# Or retry only failed files
./scripts/run_sql_setup.sh --retry-failed
```

## Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | `https://dbc-xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Your personal access token | `dapi123...` |
| `DATABRICKS_WAREHOUSE_HTTP_PATH` | SQL Warehouse HTTP path | `/sql/1.0/warehouses/abc123` |
| `WAREHOUSE_ID` | Alternative to HTTP path | `abc123def456` |

## Success Indicators

When setup completes successfully, you should see:
- ✅ All 23 SQL files executed
- 🎉 "SQL Setup Complete!" message
- 🧹 Progress file cleaned up (if no failures)

The observability platform will be ready for data processing!
