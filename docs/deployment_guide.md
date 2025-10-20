# Deployment Guide

> **Navigation**: [README](../README.md) | [Architecture & Design](architecture.md) | [Database Best Practices](database_best_practices.md) | [Tag Extraction Strategy](tag_extraction_strategy.md) | [Configuration Guide](config_folder_guide.md) | [Documentation Reorganization Summary](documentation_reorganization_summary.md)

## 🚀 **Daily Pipeline Deployment**

### **Pipeline Overview**
The observability platform runs **once daily** to process data from bronze to gold layers with enhanced metrics calculation.

### **Workflow Schedule**
- **Main Pipeline**: Daily at 2:00 AM UTC
- **Watermark Management**: Daily at 1:30 AM UTC (runs before main pipeline)

### **Deployment Steps**

#### 1. **Environment Setup**
```bash
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-access-token"

# Deploy workflows
python scripts/deploy_workflows.py
```

#### 2. **Manual Execution**
```bash
# Run main pipeline
python src/jobs/01_daily_observability_pipeline.py

# Run watermark management
python src/jobs/02_watermark_management.py
```

### **Pipeline Architecture**
- **Bronze to Silver**: SCD2 processing with watermark tracking
- **Silver to Gold**: Star schema creation with tag extraction
- **Metrics Calculation**: Enhanced runtime metrics and cost attribution

### **Monitoring**
- Email notifications on success/failure
- Slack webhook integration
- Watermark tracking for incremental processing
- Error handling with retry logic

## 📊 **Expected Outcomes**

### **Daily Processing**
- ✅ Process 2 days of incremental data
- ✅ Calculate enhanced runtime metrics
- ✅ Update cost attribution with tags
- ✅ Maintain data quality scores

### **Data Volumes**
- **Workspaces**: 4-8 workspaces
- **Jobs**: 100+ jobs
- **Processing Time**: ~30 minutes daily
- **Data Retention**: 1 year (bronze), 2 years (silver), 3 years (gold)

## 🔧 **Configuration**

### **Environment Settings**
- `config/environments.json` - Environment-specific settings
- `config/watermark_config.json` - Processing intervals and lookback periods
- `config/processing_config.json` - Performance and error handling settings

### **Tag Taxonomy**
- `config/tag_taxonomy.json` - Tag validation and allowed values
- Cost allocation tags (cost_center, business_unit, department)
- Operational tags (environment, team, project)

## 📚 **Documentation**

- [README](README.md) - Main project overview
- [Database Best Practices](docs/database_best_practices.md) - Naming standards
- [Tag Extraction Strategy](docs/tag_extraction_strategy.md) - Tag processing
- [Configuration Guide](docs/config_folder_guide.md) - Config management

## 🎯 **Success Criteria**

- ✅ Daily pipeline runs successfully
- ✅ All data layers updated with latest data
- ✅ Enhanced metrics calculated
- ✅ Cost attribution working
- ✅ Data quality maintained
- ✅ Error handling and notifications working

The observability platform is ready for daily production use! 🚀
