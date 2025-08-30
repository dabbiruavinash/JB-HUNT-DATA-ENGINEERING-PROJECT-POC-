from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when

class DataQualityModule:
    """Module 41: Data quality checks and monitoring"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def check_data_quality(self, df: DataFrame) -> dict:
        """Perform comprehensive data quality checks"""
        
        quality_metrics = {}
        
        # Completeness checks
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            total_count = df.count()
            completeness = 1 - (null_count / total_count) if total_count > 0 else 0
            
            quality_metrics[f"{column}_completeness"] = completeness
        
        # Uniqueness checks
        unique_metrics = {}
        for column in ["shipment_id"]:  # Columns that should be unique
            distinct_count = df.select(column).distinct().count()
            total_count = df.count()
            uniqueness = distinct_count / total_count if total_count > 0 else 0
            
            quality_metrics[f"{column}_uniqueness"] = uniqueness
        
        return quality_metrics