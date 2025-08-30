from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, split

class FlatMapOperationsModule:
    """Module 10: FlatMap operations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def process_with_flatmap(self, df: DataFrame, column: str) -> DataFrame:
        """Process data using flatMap operations"""
        # Split and explode array-like data
        return df.withColumn("exploded_data", explode(split(col(column), ",")))