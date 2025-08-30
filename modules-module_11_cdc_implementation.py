from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, current_timestamp

class CDCModule:
    """Module 11: Change Data Capture Implementation"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def detect_changes(self, old_df: DataFrame, new_df: DataFrame, key_columns: list) -> DataFrame:
        """Detect changes between two DataFrames"""
        
        old_df = old_df.withColumn("source", lit("old"))
        new_df = new_df.withColumn("source", lit("new"))
        
        # Union both DataFrames
        union_df = old_df.unionByName(new_df, allowMissingColumns=True)
        
        # Group by key columns and detect changes
        change_cols = [c for c in union_df.columns if c not in key_columns + ["source"]]
        
        change_exprs = []
        for col_name in change_cols:
            change_exprs.append(
                when(col(f"old.{col_name}") != col(f"new.{col_name}"), lit("UPDATED"))
                .otherwise(lit("UNCHANGED"))
                .alias(f"{col_name}_change")
            )
        
        changes_df = union_df.groupBy(key_columns).agg(
            *[first(col).alias(f"old_{col}") for col in change_cols],
            *[last(col).alias(f"new_{col}") for col in change_cols]
        )
        
        return changes_df