from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, regexp_replace

class DataCleaningModule:
    """Module 33: Data cleaning and preprocessing"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def clean_shipment_data(self, df: DataFrame) -> DataFrame:
        """Clean and preprocess shipment data"""
        
        cleaned_df = df \
            .na.fill({"status": "UNKNOWN", "weight": 0}) \
            .withColumn("origin_clean", regexp_replace(col("origin"), "[^a-zA-Z0-9\\s]", "")) \
            .withColumn("destination_clean", regexp_replace(col("destination"), "[^a-zA-Z0-9\\s]", "")) \
            .filter(col("weight") >= 0)
        
        return cleaned_df