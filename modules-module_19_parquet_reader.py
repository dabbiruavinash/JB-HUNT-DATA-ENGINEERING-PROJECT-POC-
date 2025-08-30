from pyspark.sql import SparkSession, DataFrame

class ParquetReaderModule:
    """Module 19: Reading Parquet files"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_parquet(self, path: str, **options) -> DataFrame:
        return self.spark.read \
            .format("parquet") \
            .options(**options) \
            .load(path)