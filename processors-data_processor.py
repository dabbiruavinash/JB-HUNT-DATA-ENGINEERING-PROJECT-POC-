from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Any

class DataProcessor(ABC):
    """Abstract base class for data processing"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    @abstractmethod
    def process(self, data: DataFrame) -> DataFrame:
        """Process data - to be implemented by subclasses"""
        pass

class ShipmentProcessor(DataProcessor):
    """Concrete implementation for shipment data processing"""
    
    def process(self, data: DataFrame) -> DataFrame:
        # Add business logic for shipment processing
        processed_data = data.filter(data.status.isin(['DELIVERED', 'IN_TRANSIT']))
        return processed_data

class DriverProcessor(DataProcessor):
    """Concrete implementation for driver data processing"""
    
    def process(self, data: DataFrame) -> DataFrame:
        # Add business logic for driver processing
        processed_data = data.filter(data.license_status == 'VALID')
        return processed_data