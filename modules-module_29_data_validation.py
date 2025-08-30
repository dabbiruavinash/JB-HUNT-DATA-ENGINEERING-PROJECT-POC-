from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count

class DataValidationModule:
    """Module 29: Data validation and quality checks"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def validate_data(self, df: DataFrame, rules: dict) -> dict:
        """Validate DataFrame against rules"""
        results = {}
        
        for column, rule in rules.items():
            if rule.get('not_null'):
                null_count = df.filter(col(column).isNull()).count()
                results[f"{column}_null_count"] = null_count
            
            if rule.get('unique'):
                total_count = df.count()
                distinct_count = df.select(column).distinct().count()
                results[f"{column}_duplicate_count"] = total_count - distinct_count
        
        return results