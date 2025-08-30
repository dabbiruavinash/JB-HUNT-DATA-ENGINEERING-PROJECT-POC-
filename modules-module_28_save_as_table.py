from pyspark.sql import SparkSession, DataFrame

class SaveAsTableModule:
    """Module 28: Save as table in metastore"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def save_as_table(self, df: DataFrame, table_name: str, mode: str = "overwrite", **options):
        df.write \
            .format("delta") \
            .mode(mode) \
            .options(**options) \
            .saveAsTable(table_name)