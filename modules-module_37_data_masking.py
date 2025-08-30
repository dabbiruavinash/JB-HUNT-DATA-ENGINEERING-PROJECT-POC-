from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import hashlib

class DataMaskingModule:
    """Module 37: Data masking and anonymization"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def mask_sensitive_data(self, df: DataFrame) -> DataFrame:
        """Mask sensitive information in the data"""
        
        # UDF for masking
        @udf(StringType())
        def mask_string(value):
            if value:
                return hashlib.sha256(value.encode()).hexdigest()[:16]
            return None
        
        masked_df = df.withColumn("shipment_id_masked", mask_string(col("shipment_id")))
        
        return masked_df