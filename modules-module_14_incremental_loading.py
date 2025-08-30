from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max

class IncrementalLoadingModule:
    """Module 14: Incremental loading without last modified date"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def incremental_load(self, full_df: DataFrame, existing_df: DataFrame, key_column: str) -> DataFrame:
        """Load only new or changed records"""
        
        # Get maximum key from existing data
        if existing_df.count() > 0:
            max_key = existing_df.agg(spark_max(col(key_column))).collect()[0][0]
            new_data = full_df.filter(col(key_column) > max_key)
        else:
            new_data = full_df
        
        return new_data