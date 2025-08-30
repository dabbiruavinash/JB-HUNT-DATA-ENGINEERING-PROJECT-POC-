from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

class MapOperationsModule:
    """Module 9: Map operations with UDFs"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def process_with_map(self, df: DataFrame) -> DataFrame:
        """Process data using map operations"""
        
        # Define UDF for status mapping
        @udf(StringType())
        def map_status(status):
            status_mapping = {
                'DELIVERED': 'COMPLETED',
                'IN_TRANSIT': 'ACTIVE',
                'PENDING': 'QUEUED'
            }
            return status_mapping.get(status, 'UNKNOWN')
        
        return df.withColumn("mapped_status", map_status(col("status")))