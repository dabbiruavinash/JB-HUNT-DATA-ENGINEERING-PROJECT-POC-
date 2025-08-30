from pyspark.sql import SparkSession, DataFrame

class DeltaReaderModule:
    """Module 22: Reading Delta files"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_delta(self, path: str, **options) -> DataFrame:
        return self.spark.read \
            .format("delta") \
            .options(**options) \
            .load(path)