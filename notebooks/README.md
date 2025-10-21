# Databricks Notebooks for Observability Platform Testing

This directory contains Databricks notebooks for testing the observability platform components.

## 📚 **Notebook Overview**

### 1. **Comprehensive Testing** - `01_observability_platform_testing.py`
- **Purpose**: Complete step-by-step testing of all platform components
- **Duration**: ~30-45 minutes
- **Use Case**: Initial setup verification and comprehensive testing

**Tests Include**:
- Environment setup and validation
- Table structure validation (Bronze, Silver, Gold, Meta)
- PySpark functions testing (Watermark, SCD2, Tag extraction)
- Data processing pipeline testing
- Data quality and metrics testing
- Performance and monitoring

### 2. **Quick Testing** - `02_quick_testing.py`
- **Purpose**: Fast verification of core functionality
- **Duration**: ~5-10 minutes
- **Use Case**: Quick health checks and troubleshooting

**Tests Include**:
- Environment check
- Table existence verification
- System table access
- PySpark functions import
- Watermark status
- Data quality check

### 3. **Daily Pipeline Execution** - `03_daily_pipeline_execution.py`
- **Purpose**: Test the complete daily data processing pipeline
- **Duration**: ~15-30 minutes
- **Use Case**: End-to-end pipeline testing and production readiness

**Tests Include**:
- Pipeline initialization
- Prerequisites verification
- Individual component testing
- Complete pipeline execution
- Results verification
- Performance monitoring
- Data quality assurance

## 🚀 **Getting Started**

### Prerequisites
1. **Unity Catalog Enabled**: Ensure Unity Catalog is enabled in your workspace
2. **System Tables Access**: Verify access to system tables
3. **SQL Setup Complete**: Run SQL setup scripts (01-43) first
4. **Repository Cloned**: Ensure the observability platform repository is cloned

### Step-by-Step Usage

#### 1. **Initial Setup Verification**
```python
# Run the comprehensive testing notebook first
# File: 01_observability_platform_testing.py
# This will verify all components are set up correctly
```

#### 2. **Quick Health Check**
```python
# Run the quick testing notebook for fast verification
# File: 02_quick_testing.py
# Use this for regular health checks
```

#### 3. **Pipeline Testing**
```python
# Run the daily pipeline execution notebook
# File: 03_daily_pipeline_execution.py
# Test the complete data processing pipeline
```

## 🔧 **Configuration**

### Update Repository Path
Before running the notebooks, update the repository path in each notebook:

```python
# Change this line in each notebook:
sys.path.append('/Workspace/Repos/your-username/dbr-observe/src')

# To your actual repository path:
sys.path.append('/Workspace/Repos/your-actual-username/dbr-observe/src')
```

### Environment Variables
Set these environment variables if needed:
```python
# For production deployment
os.environ['DATABRICKS_HOST'] = 'your-workspace-url'
os.environ['DATABRICKS_TOKEN'] = 'your-token'
```

## 📊 **Expected Results**

### Successful Test Results
- ✅ All tables exist and are accessible
- ✅ PySpark functions import and initialize correctly
- ✅ System tables are accessible
- ✅ Watermark management works
- ✅ Data processing completes without errors
- ✅ Data quality metrics are within acceptable ranges

### Common Issues and Solutions

#### 1. **Table Not Found Errors**
```
❌ bronze.system_billing_usage: Table not found
```
**Solution**: Run SQL setup scripts (01-43) first

#### 2. **Permission Denied Errors**
```
❌ system.billing.usage: Permission denied
```
**Solution**: Verify Unity Catalog permissions and system table access

#### 3. **Import Errors**
```
❌ ModuleNotFoundError: No module named 'python.functions'
```
**Solution**: Update the repository path in the notebook

#### 4. **Watermark Errors**
```
⚠️ Found 3 watermark errors
```
**Solution**: Check watermark table for error details and fix underlying issues

## 🎯 **Testing Scenarios**

### Scenario 1: Initial Setup
1. Run `01_observability_platform_testing.py`
2. Verify all components are working
3. Check for any missing tables or permissions

### Scenario 2: Regular Health Check
1. Run `02_quick_testing.py`
2. Verify core functionality
3. Check watermark status

### Scenario 3: Pipeline Testing
1. Run `03_daily_pipeline_execution.py`
2. Test complete data processing
3. Verify results and performance

### Scenario 4: Troubleshooting
1. Run `02_quick_testing.py` for quick diagnosis
2. Check specific error messages
3. Refer to deployment guide for solutions

## 📈 **Performance Expectations**

### Processing Times
- **Quick Testing**: 5-10 minutes
- **Comprehensive Testing**: 30-45 minutes
- **Pipeline Execution**: 15-30 minutes (depending on data volume)

### Data Volume Expectations
- **Bronze Layer**: Raw system table data
- **Silver Layer**: Processed and enriched data
- **Gold Layer**: Analytics-ready aggregated data

### Resource Requirements
- **Cluster Size**: 2-4 workers recommended
- **Memory**: 8-16 GB per worker
- **Storage**: Delta Lake with Unity Catalog

## 🔍 **Monitoring and Alerts**

### Key Metrics to Monitor
1. **Watermark Status**: Processing success/failure rates
2. **Data Quality**: Null values, duplicates, completeness
3. **Processing Performance**: Duration, throughput
4. **Error Rates**: Failed processing attempts

### Recommended Alerts
1. **Watermark Errors**: Alert on processing failures
2. **Data Quality**: Alert on quality threshold breaches
3. **Performance**: Alert on processing time increases
4. **Availability**: Alert on pipeline execution failures

## 📞 **Support and Troubleshooting**

### Documentation References
- **Architecture**: `docs/architecture.md`
- **Deployment Guide**: `docs/deployment_guide.md`
- **Database Best Practices**: `docs/database_best_practices.md`
- **Tag Extraction Strategy**: `docs/tag_extraction_strategy.md`

### Common Troubleshooting Steps
1. **Check Environment**: Verify catalog and schema context
2. **Check Permissions**: Ensure Unity Catalog access
3. **Check Data**: Verify system tables have data
4. **Check Logs**: Review processing logs for errors
5. **Check Watermarks**: Verify watermark status

### Getting Help
1. Review error messages in notebooks
2. Check watermark table for processing errors
3. Verify data quality metrics
4. Refer to deployment guide
5. Contact data platform team

## 🎉 **Success Criteria**

### Complete Success
- ✅ All notebooks run without errors
- ✅ All tables are accessible and populated
- ✅ PySpark functions work correctly
- ✅ Data processing completes successfully
- ✅ Data quality metrics are acceptable
- ✅ Performance is within expected ranges

### Ready for Production
- ✅ SQL setup scripts completed
- ✅ All components tested and working
- ✅ Daily pipeline execution successful
- ✅ Monitoring and alerts configured
- ✅ Documentation reviewed and understood

The observability platform is ready for production deployment! 🚀
