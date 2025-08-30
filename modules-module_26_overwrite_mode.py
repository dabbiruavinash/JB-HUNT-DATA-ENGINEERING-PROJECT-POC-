from pyspark.sql import SparkSession, DataFrame

class OverwriteModeModule:
    """Module 26: Writing with overwrite mode"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def write_overwrite(self, df: DataFrame, path: str, format_type: str = "parquet", **options):
        df.write \
            .format(format_type) \
            .mode("overwrite") \
            .options(**options) \
            .save(path)