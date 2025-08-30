from pyspark.sql import SparkSession, DataFrame
import time
from datetime import datetime

class MonitoringModule:
    """Module 45: Performance monitoring and metrics"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.metrics = {}
    
    def start_monitoring(self, operation_name: str):
        """Start monitoring an operation"""
        
        self.metrics[operation_name] = {
            "start_time": time.time(),
            "start_datetime": datetime.now().isoformat()
        }
    
    def stop_monitoring(self, operation_name: str, additional_metrics: dict = None):
        """Stop monitoring and record metrics"""
        
        if operation_name in self.metrics:
            end_time = time.time()
            start_time = self.metrics[operation_name]["start_time"]
            
            self.metrics[operation_name].update({
                "end_time": end_time,
                "end_datetime": datetime.now().isoformat(),
                "duration_seconds": end_time - start_time,
                "additional_metrics": additional_metrics or {}
            })
    
    def get_metrics(self) -> dict:
        """Get all collected metrics"""
        return self.metrics