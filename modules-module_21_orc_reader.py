from pyspark.sql import SparkSession, DataFrame

class ORCReaderModule:
    """Module 21: Reading ORC files"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_orc(self, path: str, **options) -> DataFrame:
        return self.spark.read \
            .format("orc") \
            .options(**options) \
            .load(path)