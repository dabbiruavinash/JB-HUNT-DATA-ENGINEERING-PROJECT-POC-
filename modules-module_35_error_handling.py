from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when
import logging

class ErrorHandlingModule:
    """Module 35: Error handling and logging"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def handle_errors(self, df: DataFrame) -> DataFrame:
        """Handle errors and invalid data"""
        
        try:
            # Validate and handle errors
            validated_df = df.withColumn("is_valid",
                                      when(col("shipment_id").isNull(), False)
                                      .when(col("weight").isNull() | (col("weight") < 0), False)
                                      .otherwise(True))
            
            # Log invalid records
            invalid_count = validated_df.filter(~col("is_valid")).count()
            if invalid_count > 0:
                self.logger.warning(f"Found {invalid_count} invalid records")
            
            return validated_df.filter(col("is_valid"))
            
        except Exception as e:
            self.logger.error(f"Error in data processing: {str(e)}")
            raise