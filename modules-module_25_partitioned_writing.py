from pyspark.sql import SparkSession, DataFrame
from typing import List

class PartitionedWritingModule:
    """Module 25: Writing files with partitioning"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def write_partitioned(self, df: DataFrame, path: str, partition_cols: List[str], 
                         format_type: str = "parquet", mode: str = "overwrite", **options):
        df.write \
            .format(format_type) \
            .partitionBy(*partition_cols) \
            .mode(mode) \
            .options(**options) \
            .save(path)