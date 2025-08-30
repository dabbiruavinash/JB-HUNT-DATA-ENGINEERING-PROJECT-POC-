from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import broadcast

class BroadcastVariablesModule:
    """Module 16: Broadcast variables usage"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def join_with_broadcast(self, large_df: DataFrame, small_df: DataFrame, join_key: str):
        """Join using broadcast variables for small DataFrame"""
        return large_df.join(broadcast(small_df), join_key)