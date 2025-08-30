from pyspark.sql import SparkSession, DataFrame

class DirectoryReaderModule:
    """Module 24: Reading directories with wildcards"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_directory(self, base_path: str, pattern: str = "*", format_type: str = "parquet", **options) -> DataFrame:
        path = f"{base_path}/{pattern}"
        return self.spark.read \
            .format(format_type) \
            .options(**options) \
            .load(path)