from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when

class DataEnrichmentModule:
    """Module 34: Data enrichment with external sources"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def enrich_shipment_data(self, df: DataFrame, enrichment_df: DataFrame) -> DataFrame:
        """Enrich shipment data with additional information"""
        
        enriched_df = df.join(enrichment_df, "shipment_id", "left")
        
        # Add derived enrichment columns
        enriched_df = enriched_df.withColumn("priority_level",
                                           when(col("weight") > 5000, "HIGH")
                                           .when(col("weight") > 1000, "MEDIUM")
                                           .otherwise("LOW"))
        
        return enriched_df