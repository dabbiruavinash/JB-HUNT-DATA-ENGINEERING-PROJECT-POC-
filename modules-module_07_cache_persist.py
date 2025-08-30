from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel

class CachePersistModule:
    """Module 7: Cache and Persist operations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def cache_dataframe(self, df: DataFrame) -> DataFrame:
        """Cache DataFrame in memory"""
        return df.cache()
    
    def persist_dataframe(self, df: DataFrame, storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK) -> DataFrame:
        """Persist DataFrame with specific storage level"""
        return df.persist(storage_level)
    
    def unpersist_dataframe(self, df: DataFrame) -> DataFrame:
        """Unpersist DataFrame"""
        return df.unpersist()