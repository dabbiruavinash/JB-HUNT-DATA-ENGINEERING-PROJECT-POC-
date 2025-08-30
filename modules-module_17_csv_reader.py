from pyspark.sql import SparkSession, DataFrame

class CSVReaderModule:
    """Module 17: Reading CSV files"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_csv(self, path: str, **options) -> DataFrame:
        default_options = {
            "header": "true",
            "inferSchema": "true",
            "delimiter": ","
        }
        default_options.update(options)
        
        return self.spark.read \
            .format("csv") \
            .options(**default_options) \
            .load(path)