from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, mean, stddev, min, max

class DataProfilingModule:
    """Module 38: Data profiling and statistics"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def profile_data(self, df: DataFrame) -> dict:
        """Generate data profile statistics"""
        
        profile = {}
        
        for column in df.columns:
            column_stats = df.agg(
                count(col(column)).alias("count"),
                mean(col(column)).alias("mean"),
                stddev(col(column)).alias("stddev"),
                min(col(column)).alias("min"),
                max(col(column)).alias("max")
            ).collect()[0]
            
            profile[column] = {
                "count": column_stats["count"],
                "mean": column_stats["mean"],
                "stddev": column_stats["stddev"],
                "min": column_stats["min"],
                "max": column_stats["max"]
            }
        
        return profile