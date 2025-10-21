#!/bin/bash
#
# Quick Testing Script for Daily Pipeline
# ======================================
#
# This script provides quick commands to test the daily observability pipeline.
#
# Usage:
#     ./scripts/quick_test.sh check-all          # Check all prerequisites
#     ./scripts/quick_test.sh test-watermark     # Test watermark management
#     ./scripts/quick_test.sh test-scd2          # Test SCD2 processing
#     ./scripts/quick_test.sh test-tags          # Test tag extraction
#     ./scripts/quick_test.sh test-bronze        # Test bronze to silver
#     ./scripts/quick_test.sh test-silver        # Test silver to gold
#     ./scripts/quick_test.sh test-metrics       # Test metrics calculation
#     ./scripts/quick_test.sh test-full          # Test complete pipeline
#

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if environment variables are set
check_environment() {
    print_status "Checking environment variables..."
    
    # For local PySpark testing, we don't need Databricks environment variables
    # These are only needed for actual Databricks cluster execution
    if [ -z "$DATABRICKS_HOST" ] && [ -z "$DATABRICKS_TOKEN" ]; then
        print_warning "Databricks environment variables not set - using local PySpark mode"
        print_warning "For production deployment, set DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_WAREHOUSE_HTTP_PATH"
    else
        print_success "Databricks environment variables are set"
    fi
    
    return 0
}

# Check all prerequisites
check_all() {
    print_status "Running all prerequisite checks..."
    
    # Check environment
    if ! check_environment; then
        print_error "Environment check failed"
        return 1
    fi
    
    # Check tables
    print_status "Checking table existence..."
    python scripts/test_daily_pipeline.py --check-tables
    
    # Check watermarks
    print_status "Checking watermark status..."
    python scripts/test_daily_pipeline.py --check-watermarks
    
    # Check data availability
    print_status "Checking data availability..."
    python scripts/test_daily_pipeline.py --check-data
    
    print_success "All checks completed"
}

# Test individual components
test_watermark() {
    print_status "Testing watermark management..."
    python scripts/test_daily_pipeline.py --component watermark
}

test_scd2() {
    print_status "Testing SCD2 processing..."
    python scripts/test_daily_pipeline.py --component scd2
}

test_tags() {
    print_status "Testing tag extraction..."
    python scripts/test_daily_pipeline.py --component tags
}

test_bronze() {
    print_status "Testing bronze to silver processing..."
    python scripts/test_daily_pipeline.py --component bronze-to-silver
}

test_silver() {
    print_status "Testing silver to gold processing..."
    python scripts/test_daily_pipeline.py --component silver-to-gold
}

test_metrics() {
    print_status "Testing metrics calculation..."
    python scripts/test_daily_pipeline.py --component metrics
}

test_full() {
    print_status "Testing complete daily pipeline..."
    python scripts/test_daily_pipeline.py --component full
}

# Main script logic
case "$1" in
    "check-all")
        check_all
        ;;
    "test-watermark")
        test_watermark
        ;;
    "test-scd2")
        test_scd2
        ;;
    "test-tags")
        test_tags
        ;;
    "test-bronze")
        test_bronze
        ;;
    "test-silver")
        test_silver
        ;;
    "test-metrics")
        test_metrics
        ;;
    "test-full")
        test_full
        ;;
    *)
        echo "Usage: $0 {check-all|test-watermark|test-scd2|test-tags|test-bronze|test-silver|test-metrics|test-full}"
        echo ""
        echo "Commands:"
        echo "  check-all      - Check all prerequisites"
        echo "  test-watermark - Test watermark management"
        echo "  test-scd2      - Test SCD2 processing"
        echo "  test-tags      - Test tag extraction"
        echo "  test-bronze    - Test bronze to silver processing"
        echo "  test-silver    - Test silver to gold processing"
        echo "  test-metrics   - Test metrics calculation"
        echo "  test-full      - Test complete pipeline"
        exit 1
        ;;
esac
