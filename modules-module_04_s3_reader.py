from pyspark.sql import SparkSession, DataFrame

class S3ReaderModule:
    """Module 4: Reading from AWS S3"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_from_s3(self, bucket: str, path: str, format_type: str = "parquet") -> DataFrame:
        s3_path = f"s3a://{bucket}/{path}"
        
        return self.spark.read \
            .format(format_type) \
            .load(s3_path)