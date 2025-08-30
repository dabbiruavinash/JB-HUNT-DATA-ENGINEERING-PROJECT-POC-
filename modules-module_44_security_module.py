from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

class SecurityModule:
    """Module 44: Data security and access control"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def apply_security(self, df: DataFrame, user_permissions: dict) -> DataFrame:
        """Apply security filters based on user permissions"""
        
        # Apply row-level security
        if 'origin' in user_permissions:
            allowed_origins = user_permissions['origin']
            df = df.filter(col("origin").isin(allowed_origins))
        
        # Apply column-level security
        if 'masked_columns' in user_permissions:
            for column in user_permissions['masked_columns']:
                if column in df.columns:
                    df = df.withColumn(column, col(column).cast("string").substr(1, 1) + "***")
        
        return df