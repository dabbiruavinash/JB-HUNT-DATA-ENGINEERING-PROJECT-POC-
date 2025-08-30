from pyspark.sql import SparkSession, DataFrame

class ADLSReaderModule:
    """Module 3: Reading from Azure Data Lake Storage"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_from_adls(self, storage_account: str, container: str, path: str, 
                      format_type: str = "parquet") -> DataFrame:
        adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"
        
        return self.spark.read \
            .format(format_type) \
            .load(adls_path)