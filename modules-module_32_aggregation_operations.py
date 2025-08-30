from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, avg, max, min

class AggregationModule:
    """Module 32: Aggregation operations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def aggregate_shipment_data(self, df: DataFrame) -> DataFrame:
        """Aggregate shipment data by various dimensions"""
        
        aggregated_df = df.groupBy("origin", "destination", "status") \
                         .agg(
                             count("*").alias("total_shipments"),
                             sum("weight").alias("total_weight"),
                             avg("weight").alias("avg_weight"),
                             max("weight").alias("max_weight"),
                             min("weight").alias("min_weight")
                         )
        
        return aggregated_df