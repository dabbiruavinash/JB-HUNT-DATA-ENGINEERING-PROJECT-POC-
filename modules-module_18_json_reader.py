from pyspark.sql import SparkSession, DataFrame

class JSONReaderModule:
    """Module 18: Reading JSON files"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_json(self, path: str, **options) -> DataFrame:
        default_options = {
            "multiline": "true"
        }
        default_options.update(options)
        
        return self.spark.read \
            .format("json") \
            .options(**default_options) \
            .load(path)