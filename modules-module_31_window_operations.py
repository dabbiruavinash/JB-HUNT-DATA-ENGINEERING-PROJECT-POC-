from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, avg, sum
from pyspark.sql.window import Window

class WindowOperationsModule:
    """Module 31: Window functions and operations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def apply_window_functions(self, df: DataFrame) -> DataFrame:
        """Apply window functions for analytics"""
        
        window_spec = Window.partitionBy("origin").orderBy("weight")
        
        result_df = df.withColumn("row_number", row_number().over(window_spec)) \
                     .withColumn("avg_weight_by_origin", avg("weight").over(Window.partitionBy("origin"))) \
                     .withColumn("total_weight_by_origin", sum("weight").over(Window.partitionBy("origin")))
        
        return result_df