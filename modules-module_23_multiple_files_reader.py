from pyspark.sql import SparkSession, DataFrame
from typing import List

class MultipleFilesReaderModule:
    """Module 23: Reading multiple files"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_multiple_files(self, paths: List[str], format_type: str = "parquet", **options) -> DataFrame:
        return self.spark.read \
            .format(format_type) \
            .options(**options) \
            .load(paths)