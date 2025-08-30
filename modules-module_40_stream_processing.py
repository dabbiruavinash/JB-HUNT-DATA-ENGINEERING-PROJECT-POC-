from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamWriter

class StreamProcessingModule:
    """Module 40: Stream processing framework"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def process_stream(self, stream_df: DataFrame, output_path: str) -> DataStreamWriter:
        """Process data in streaming mode"""
        
        processed_stream = stream_df \
            .writeStream \
            .format("parquet") \
            .option("checkpointLocation", f"{output_path}/_checkpoints") \
            .option("path", output_path) \
            .outputMode("append")
        
        return processed_stream