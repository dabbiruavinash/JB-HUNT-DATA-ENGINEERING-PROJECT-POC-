from pyspark.sql import SparkSession, DataFrame

class AppendModeModule:
    """Module 27: Writing with append mode"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def write_append(self, df: DataFrame, path: str, format_type: str = "parquet", **options):
        df.write \
            .format(format_type) \
            .mode("append") \
            .options(**options) \
            .save(path)