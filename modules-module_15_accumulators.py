from pyspark import AccumulatorParam
from pyspark.sql import SparkSession

class StringAccumulatorParam(AccumulatorParam):
    """Custom accumulator for string concatenation"""
    
    def zero(self, initialValue):
        return initialValue
    
    def addInPlace(self, v1, v2):
        return v1 + v2

class AccumulatorsModule:
    """Module 15: Accumulators usage"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.total_weight_acc = spark.sparkContext.accumulator(0.0)
        self.shipment_count_acc = spark.sparkContext.accumulator(0)
        self.status_acc = spark.sparkContext.accumulator("", StringAccumulatorParam())
    
    def process_with_accumulators(self, rdd):
        """Process RDD with accumulators"""
        
        def process_record(record):
            self.total_weight_acc.add(record['weight'])
            self.shipment_count_acc.add(1)
            self.status_acc.add(record['status'] + ",")
            return record
        
        return rdd.map(process_record)