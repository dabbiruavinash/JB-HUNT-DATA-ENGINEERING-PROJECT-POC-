from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import unittest

class TestingModule:
    """Module 47: Data testing framework"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_test_data(self) -> DataFrame:
        """Create test data for unit testing"""
        
        schema = StructType([
            StructField("shipment_id", StringType(), True),
            StructField("origin", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("weight", DoubleType(), True),
            StructField("status", StringType(), True)
        ])
        
        test_data = [
            ("SHIP001", "Chicago", "New York", 1000.0, "DELIVERED"),
            ("SHIP002", "Los Angeles", "Seattle", 2500.0, "IN_TRANSIT"),
            ("SHIP003", "Miami", "Atlanta", 500.0, "PENDING")
        ]
        
        return self.spark.createDataFrame(test_data, schema)
    
    def assert_dataframe_equals(self, actual_df: DataFrame, expected_df: DataFrame):
        """Assert that two DataFrames are equal"""
        
        actual_data = actual_df.collect()
        expected_data = expected_df.collect()
        
        assert len(actual_data) == len(expected_data), "DataFrame row count mismatch"
        
        for actual_row, expected_row in zip(actual_data, expected_data):
            assert actual_row.asDict() == expected_row.asDict(), "DataFrame content mismatch"