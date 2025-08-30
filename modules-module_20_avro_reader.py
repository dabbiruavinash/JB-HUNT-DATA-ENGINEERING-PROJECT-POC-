from pyspark.sql import SparkSession, DataFrame

class AvroReaderModule:
    """Module 20: Reading Avro files"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_avro(self, path: str, **options) -> DataFrame:
        return self.spark.read \
            .format("avro") \
            .options(**options) \
            .load(path)