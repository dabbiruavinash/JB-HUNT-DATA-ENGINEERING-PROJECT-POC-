from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

class DataTransformationModule:
    """Module 30: Complex data transformations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def transform_shipment_data(self, df: DataFrame) -> DataFrame:
        """Apply complex transformations to shipment data"""
        
        # Add derived columns
        transformed_df = df.withColumn("weight_category", 
                                     when(col("weight") < 1000, "LIGHT")
                                     .when(col("weight") < 5000, "MEDIUM")
                                     .otherwise("HEAVY"))
        
        # Clean and standardize data
        transformed_df = transformed_df.withColumn("origin_clean", 
                                                udf(lambda x: x.upper().strip() if x else None, StringType())(col("origin")))
        
        return transformed_df