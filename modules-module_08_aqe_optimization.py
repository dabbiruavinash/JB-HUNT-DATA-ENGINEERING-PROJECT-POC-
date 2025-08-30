from pyspark.sql import SparkSession

class AQEOptimizationModule:
    """Module 8: Adaptive Query Execution Optimization"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def enable_aqe(self):
        """Enable Adaptive Query Execution"""
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    def disable_aqe(self):
        """Disable Adaptive Query Execution"""
        self.spark.conf.set("spark.sql.adaptive.enabled", "false")