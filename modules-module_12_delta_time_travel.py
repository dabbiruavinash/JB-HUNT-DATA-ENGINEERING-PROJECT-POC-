from pyspark.sql import SparkSession, DataFrame

class DeltaTimeTravelModule:
    """Module 12: Delta Lake Time Travel"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def time_travel_read(self, delta_path: str, version: int = None, timestamp: str = None) -> DataFrame:
        """Read data from specific version or timestamp"""
        reader = self.spark.read.format("delta")
        
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)
        
        return reader.load(delta_path)
    
    def get_history(self, delta_path: str) -> DataFrame:
        """Get history of Delta table"""
        return self.spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")