# Configuration Folder Guide

> **Navigation**: [README](../README.md) | [Architecture & Design](architecture.md) | [Database Best Practices](database_best_practices.md) | [Tag Extraction Strategy](tag_extraction_strategy.md) | [Configuration Guide](config_folder_guide.md) | [Deployment Guide](deployment_guide.md) | [Documentation Reorganization Summary](documentation_reorganization_summary.md)

## 📁 Purpose of the `config/` Folder

The `config/` folder contains environment-specific configuration files that control various aspects of the observability platform. These files allow for easy customization and deployment across different environments (dev, staging, prod).

## 📋 Configuration Files

### 1. `environments.json`
**Purpose**: Environment-specific settings for different deployment stages

**Contents**:
- Catalog names for each environment
- Workspace URLs
- Data retention policies

**Example Usage**:
```json
{
  "environments": {
    "dev": {
      "catalog": "obs_dev",
      "workspace_url": "https://dev-workspace.cloud.databricks.com",
      "retention_days": {
        "bronze": 90,
        "silver": 180,
        "gold": 365
      }
    }
  }
}
```

### 2. `watermark_config.json`
**Purpose**: Configuration for incremental data processing and watermark management

**Contents**:
- Default lookback periods
- Processing intervals
- Source table specific settings
- Watermark column mappings

**Example Usage**:
```json
{
  "watermark_config": {
    "default_lookback_days": 2,
    "max_lookback_days": 7,
    "processing_intervals": {
      "bronze_to_silver": "15 minutes",
      "silver_to_gold": "1 hour"
    }
  }
}
```

### 3. `tag_taxonomy.json`
**Purpose**: Tag taxonomy and validation rules for cost allocation and governance

**Contents**:
- Required vs optional tags
- Allowed values for each tag
- Default values
- Tag categories (cost_allocation, operational, data_governance)

**Example Usage**:
```json
{
  "tag_taxonomy": {
    "cost_allocation": {
      "cost_center": {
        "required": true,
        "allowed_values": ["finance", "engineering", "marketing"],
        "default": "unknown"
      }
    }
  }
}
```

### 4. `processing_config.json`
**Purpose**: Processing performance and error handling configuration

**Contents**:
- Batch sizes for different processing stages
- Parallelism settings
- Error handling and retry logic
- Performance tuning parameters
- Monitoring and alerting thresholds

**Example Usage**:
```json
{
  "processing_config": {
    "batch_sizes": {
      "bronze_to_silver": 10000,
      "silver_to_gold": 50000
    },
    "error_handling": {
      "max_retries": 3,
      "retry_delay_seconds": 60
    }
  }
}
```

## 🔧 Additional Configuration Files (Recommended)

### 5. `workspace_config.json` (Future)
**Purpose**: Workspace-specific settings
- Workspace IDs and names
- Service principal configurations
- Network and security settings
- Resource quotas and limits

### 6. `monitoring_config.json` (Future)
**Purpose**: Monitoring and alerting configuration
- Dashboard configurations
- Alert rules and thresholds
- Notification channels
- SLA definitions

### 7. `security_config.json` (Future)
**Purpose**: Security and compliance settings
- Access control policies
- Data classification rules
- Audit requirements
- Compliance frameworks

## 🚀 Usage in Code

### Python/PySpark Example (reading env settings):
```python
import json

# Load environment configuration
with open('config/environments.json', 'r') as f:
    env_config = json.load(f)

# Get current environment settings
current_env = env_config['environments']['prod']
catalog_name = current_env['catalog']

# Use in SQL (schema locations default to catalog storage)
spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalog_name}
""")
```

### SQL Example:
```sql
-- Load watermark configuration
-- (Would be implemented as a function or parameterized query)
SELECT 
    watermark_column,
    lookback_days
FROM config.watermark_config
WHERE source_table = 'system.billing.usage'
```

## 📝 Best Practices

### 1. Environment Separation
- Keep environment-specific values in separate files
- Use environment variables for sensitive data
- Never commit production credentials

### 2. Validation
- Validate configuration files against schemas
- Use default values for optional settings
- Implement configuration validation in deployment scripts

### 3. Version Control
- Track all configuration changes
- Use meaningful commit messages
- Tag configuration versions with releases

### 4. Security
- Encrypt sensitive configuration data
- Use least-privilege access principles
- Regularly rotate secrets and credentials

## 🔄 Configuration Management

### Development Workflow:
1. **Local Development**: Use `dev` environment settings
2. **Testing**: Use `staging` environment settings
3. **Production**: Use `prod` environment settings

### Deployment Process:
1. Validate configuration files
2. Apply environment-specific settings
3. Deploy with appropriate configuration
4. Verify configuration is applied correctly

## 📊 Configuration Validation

### Schema Validation:
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "environments": {
      "type": "object",
      "patternProperties": {
        "^[a-z]+$": {
          "type": "object",
          "required": ["catalog", "workspace_url"]
        }
      }
    }
  }
}
```

### Validation Script:
```python
import jsonschema
import json

def validate_config(config_file, schema_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    
    jsonschema.validate(config, schema)
    print(f"✅ {config_file} is valid")
```

## 🎯 Summary

The `config/` folder provides:
- ✅ **Environment Management**: Easy deployment across environments
- ✅ **Flexibility**: Customizable settings without code changes
- ✅ **Maintainability**: Centralized configuration management
- ✅ **Security**: Separation of configuration from code
- ✅ **Validation**: Schema-based configuration validation

This approach ensures the observability platform can be easily deployed and maintained across different environments while maintaining consistency and security.
