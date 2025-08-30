from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

class KafkaConsumerModule:
    """Module 1: Kafka Consumer with structured streaming"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def consume_shipment_data(self, bootstrap_servers: str, topic: str):
        schema = StructType([
            StructField("shipment_id", StringType(), True),
            StructField("origin", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("weight", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
        
        return parsed_df