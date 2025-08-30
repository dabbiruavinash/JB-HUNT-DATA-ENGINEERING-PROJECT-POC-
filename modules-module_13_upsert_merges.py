from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

class UpsertMergeModule:
    """Module 13: Upserts and MERGE operations with Delta Lake"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def merge_data(self, delta_table_path: str, updates_df: DataFrame, key_column: str):
        """Perform MERGE operation (upsert)"""
        
        delta_table = DeltaTable.forPath(self.spark, delta_table_path)
        
        merge_condition = f"target.{key_column} = source.{key_column}"
        
        delta_table.alias("target").merge(
            updates_df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()