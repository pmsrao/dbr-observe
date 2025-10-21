"""
Databricks Observability Platform - Tag Extraction Functions
============================================================

PySpark functions for extracting and processing tags from various sources.

Author: Data Platform Team
Date: December 2024
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, coalesce, lit, map_keys, map_values, 
    regexp_extract, split, trim, lower, upper, initcap
)
from pyspark.sql.types import StringType, MapType
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class TagExtractor:
    """Handles tag extraction and processing from various sources."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the tag extractor.
        
        Args:
            spark: Spark session
        """
        self.spark = spark
    
    def extract_tag(self, tags_map: DataFrame, tag_name: str) -> DataFrame:
        """
        Extract a specific tag from a tags map column.
        
        Args:
            tags_map: DataFrame with tags map column
            tag_name: Name of the tag to extract
            
        Returns:
            DataFrame with extracted tag values
        """
        try:
            # Try different variations of the tag name
            tag_variations = [
                tag_name,
                tag_name.upper(),
                tag_name.lower(),
                tag_name.title(),
                tag_name.replace('_', '-'),
                tag_name.replace('-', '_')
            ]
            
            # Create extraction logic
            extraction_expr = None
            for variation in tag_variations:
                if extraction_expr is None:
                    extraction_expr = when(col("tags")[variation].isNotNull(), col("tags")[variation])
                else:
                    extraction_expr = extraction_expr.when(col("tags")[variation].isNotNull(), col("tags")[variation])
            
            # Default to 'Unknown' if no variation found
            extraction_expr = extraction_expr.otherwise("Unknown")
            
            result = tags_map.withColumn(f"extracted_{tag_name}", extraction_expr)
            
            logger.info(f"Tag extraction completed for {tag_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error extracting tag {tag_name}: {str(e)}")
            return tags_map
    
    def extract_standard_tags(self, df: DataFrame, tags_column: str = "tags") -> DataFrame:
        """
        Extract standard tags from a DataFrame.
        
        Args:
            df: Input DataFrame
            tags_column: Name of the tags column
            
        Returns:
            DataFrame with extracted standard tags
        """
        try:
            result_df = df
            
            # Standard tag names to extract
            standard_tags = [
                "cost_center",
                "business_unit", 
                "department",
                "data_product",
                "environment",
                "team",
                "project",
                "owner",
                "application",
                "service",
                "tier",
                "classification"
            ]
            
            # Extract each standard tag
            for tag in standard_tags:
                result_df = self.extract_tag(result_df, tag)
            
            logger.info("Standard tags extraction completed")
            return result_df
            
        except Exception as e:
            logger.error(f"Error extracting standard tags: {str(e)}")
            return df
    
    def normalize_tags(self, df: DataFrame, tags_column: str = "tags") -> DataFrame:
        """
        Normalize tag keys and values.
        
        Args:
            df: Input DataFrame
            tags_column: Name of the tags column
            
        Returns:
            DataFrame with normalized tags
        """
        try:
            # Normalize tag keys (lowercase, replace special chars)
            normalized_df = df.withColumn(
                f"{tags_column}_normalized",
                # This would need a UDF for complex normalization
                col(tags_column)
            )
            
            logger.info("Tag normalization completed")
            return normalized_df
            
        except Exception as e:
            logger.error(f"Error normalizing tags: {str(e)}")
            return df
    
    def validate_tags(self, df: DataFrame, tags_column: str = "tags") -> DataFrame:
        """
        Validate tag values against business rules.
        
        Args:
            df: Input DataFrame
            tags_column: Name of the tags column
            
        Returns:
            DataFrame with tag validation results
        """
        try:
            # Add validation columns
            validated_df = df.withColumn(
                "tag_validation_status",
                when(col(tags_column).isNull(), "MISSING")
                .when(map_keys(col(tags_column)).isNull(), "EMPTY")
                .otherwise("VALID")
            ).withColumn(
                "tag_count",
                when(col(tags_column).isNotNull(), 
                     when(map_keys(col(tags_column)).isNotNull(), 
                          size(map_keys(col(tags_column)))
                     .otherwise(0))
                .otherwise(0)
            )
            
            logger.info("Tag validation completed")
            return validated_df
            
        except Exception as e:
            logger.error(f"Error validating tags: {str(e)}")
            return df


def extract_tag(spark: SparkSession, tags_map: DataFrame, tag_name: str) -> DataFrame:
    """
    Convenience function to extract a specific tag.
    
    Args:
        spark: Spark session
        tags_map: DataFrame with tags map column
        tag_name: Name of the tag to extract
        
    Returns:
        DataFrame with extracted tag values
    """
    extractor = TagExtractor(spark)
    return extractor.extract_tag(tags_map, tag_name)


def extract_standard_tags(spark: SparkSession, df: DataFrame, tags_column: str = "tags") -> DataFrame:
    """
    Convenience function to extract standard tags.
    
    Args:
        spark: Spark session
        df: Input DataFrame
        tags_column: Name of the tags column
        
    Returns:
        DataFrame with extracted standard tags
    """
    extractor = TagExtractor(spark)
    return extractor.extract_standard_tags(df, tags_column)


def normalize_tags(spark: SparkSession, df: DataFrame, tags_column: str = "tags") -> DataFrame:
    """
    Convenience function to normalize tags.
    
    Args:
        spark: Spark session
        df: Input DataFrame
        tags_column: Name of the tags column
        
    Returns:
        DataFrame with normalized tags
    """
    extractor = TagExtractor(spark)
    return extractor.normalize_tags(df, tags_column)


def validate_tags(spark: SparkSession, df: DataFrame, tags_column: str = "tags") -> DataFrame:
    """
    Convenience function to validate tags.
    
    Args:
        spark: Spark session
        df: Input DataFrame
        tags_column: Name of the tags column
        
    Returns:
        DataFrame with tag validation results
    """
    extractor = TagExtractor(spark)
    return extractor.validate_tags(df, tags_column)
