from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_json, struct

class KafkaProducerModule:
    """Module 2: Kafka Producer"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def produce_to_kafka(self, df: DataFrame, bootstrap_servers: str, topic: str):
        kafka_df = df.select(
            to_json(struct([col for col in df.columns])).alias("value"))
        
        query = kafka_df \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", "/tmp/kafka_checkpoint") \
            .start()
        
        return query