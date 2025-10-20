#!/bin/bash
# Databricks Observability Platform - SQL Setup Runner
# ===================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Databricks Observability Platform - SQL Setup${NC}"
echo "=================================================="

# Check if required environment variables are set
if [ -z "$DATABRICKS_HOST" ]; then
    echo -e "${RED}❌ DATABRICKS_HOST environment variable is required${NC}"
    echo "Example: export DATABRICKS_HOST='https://dbc-xxxx.cloud.databricks.com'"
    exit 1
fi

if [ -z "$DATABRICKS_TOKEN" ]; then
    echo -e "${RED}❌ DATABRICKS_TOKEN environment variable is required${NC}"
    echo "Example: export DATABRICKS_TOKEN='your_personal_access_token'"
    exit 1
fi

if [ -z "$DATABRICKS_WAREHOUSE_HTTP_PATH" ] && [ -z "$WAREHOUSE_ID" ]; then
    echo -e "${RED}❌ DATABRICKS_WAREHOUSE_HTTP_PATH or WAREHOUSE_ID environment variable is required${NC}"
    echo "Options:"
    echo "1. export DATABRICKS_WAREHOUSE_HTTP_PATH='/sql/1.0/warehouses/your-warehouse-id'"
    echo "2. export WAREHOUSE_ID='your-warehouse-id'"
    echo ""
    echo "💡 Get HTTP path from: Databricks UI > SQL Warehouses > Your Warehouse > Connection Details"
    exit 1
fi

# Check if databricks-sql-connector is installed
if ! python3 -c "import databricks.sql" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  databricks-sql-connector not found. Installing...${NC}"
    pip install databricks-sql-connector
fi

# Set warehouse HTTP path if WAREHOUSE_ID is provided
if [ -n "$WAREHOUSE_ID" ] && [ -z "$DATABRICKS_WAREHOUSE_HTTP_PATH" ]; then
    export DATABRICKS_WAREHOUSE_HTTP_PATH="/sql/1.0/warehouses/$WAREHOUSE_ID"
fi

echo -e "${GREEN}✅ Environment variables configured${NC}"
echo "Host: $DATABRICKS_HOST"
echo "Warehouse HTTP Path: $DATABRICKS_WAREHOUSE_HTTP_PATH"
echo ""

# Run the Python script
echo -e "${BLUE}🔄 Starting SQL setup...${NC}"
python3 scripts/run_all_sql.py "$@"

echo -e "${GREEN}🎉 SQL setup completed!${NC}"
