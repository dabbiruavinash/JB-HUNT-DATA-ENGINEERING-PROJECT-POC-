from pyspark.sql import SparkSession, DataFrame

class GCSReaderModule:
    """Module 5: Reading from Google Cloud Storage"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_from_gcs(self, bucket: str, path: str, format_type: str = "parquet") -> DataFrame:
        gcs_path = f"gs://{bucket}/{path}"
        
        return self.spark.read \
            .format(format_type) \
            .load(gcs_path)