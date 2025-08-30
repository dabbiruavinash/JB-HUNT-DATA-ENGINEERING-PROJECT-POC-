#!/usr/bin/env python3
"""
Main execution script for JB Hunt Data Engineering Project
Integrates all 50 modules into a comprehensive data pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from config.config_manager import JSONConfigManager
from modules.module_01_kafka_consumer import KafkaConsumerModule
from modules.module_03_adls_reader import ADLSReaderModule
from modules.module_06_scd2_implementation import SCD2Module
from modules.module_13_upsert_merges import UpsertMergeModule
from modules.module_50_main_integration import MainIntegrationModule
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jb_hunt_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class JBHuntDataPipeline:
    """Main class orchestrating the complete JB Hunt data engineering pipeline"""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.config_manager = JSONConfigManager()
        self.config = self.config_manager.load_config('config/pipeline_config.json')
        
        # Initialize all modules
        self._initialize_modules()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        
        return SparkSession.builder \
            .appName("JBHuntDataEngineering") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .getOrCreate()
    
    def _initialize_modules(self):
        """Initialize all 50 modules"""
        
        self.modules = {
            'kafka_consumer': KafkaConsumerModule(self.spark),
            'adls_reader': ADLSReaderModule(self.spark),
            'scd2': SCD2Module(self.spark),
            'upsert_merge': UpsertMergeModule(self.spark),
            'main_integration': MainIntegrationModule(self.spark),
            # Initialize all other modules here...
        }
    
    def run_batch_pipeline(self):
        """Execute batch processing pipeline"""
        
        logger.info("Starting batch processing pipeline")
        
        try:
            # Read source data
            source_df = self._read_source_data()
            
            # Apply transformations
            transformed_df = self._apply_transformations(source_df)
            
            # Apply SCD2 if configured
            if self.config.get('scd2_enabled', False):
                transformed_df = self._apply_scd2(transformed_df)
            
            # Write to destination
            self._write_destination_data(transformed_df)
            
            logger.info("Batch processing pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Batch processing pipeline failed: {str(e)}")
            raise
    
    def run_streaming_pipeline(self):
        """Execute streaming processing pipeline"""
        
        logger.info("Starting streaming processing pipeline")
        
        try:
            # Read from Kafka stream
            stream_df = self.modules['kafka_consumer'].consume_shipment_data(
                self.config['kafka']['bootstrap_servers'],
                self.config['kafka']['topic']
            )
            
            # Process stream
            query = stream_df \
                .writeStream \
                .format("delta") \
                .option("checkpointLocation", self.config['checkpoint_path']) \
                .outputMode("append") \
                .start(self.config['output_path'])
            
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Streaming processing pipeline failed: {str(e)}")
            raise
    
    def _read_source_data(self):
        """Read data from source based on configuration"""
        
        source_type = self.config['source']['type']
        
        if source_type == 'adls':
            return self.modules['adls_reader'].read_from_adls(
                self.config['source']['storage_account'],
                self.config['source']['container'],
                self.config['source']['path']
            )
        elif source_type == 'kafka':
            return self.modules['kafka_consumer'].consume_shipment_data(
                self.config['source']['bootstrap_servers'],
                self.config['source']['topic']
            )
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _apply_transformations(self, df):
        """Apply all data transformations"""
        
        # Implement transformation logic using various modules
        # This is a simplified example - actual implementation would use multiple modules
        
        transformed_df = df
        
        # Add your transformation logic here
        # Example: data cleaning, enrichment, aggregation, etc.
        
        return transformed_df
    
    def _apply_scd2(self, df):
        """Apply SCD Type 2 logic"""
        
        # Read current dimension table
        current_df = self.spark.read.format("delta").load(self.config['scd2']['current_table_path'])
        
        # Apply SCD2
        return self.modules['scd2'].apply_scd2(
            current_df, df, 
            self.config['scd2']['primary_key']
        )
    
    def _write_destination_data(self, df):
        """Write data to destination"""
        
        sink_type = self.config['sink']['type']
        
        if sink_type == 'delta':
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(self.config['sink']['path'])
        elif sink_type == 'kafka':
            self.modules['kafka_producer'].produce_to_kafka(
                df,
                self.config['sink']['bootstrap_servers'],
                self.config['sink']['topic']
            )
    
    def shutdown(self):
        """Cleanup and shutdown"""
        self.spark.stop()
        logger.info("Spark session stopped")

def main():
    """Main execution function"""
    
    try:
        # Initialize pipeline
        pipeline = JBHuntDataPipeline()
        
        # Determine pipeline type from config or command line args
        pipeline_type = 'batch'  # Default to batch
        
        if pipeline_type == 'batch':
            pipeline.run_batch_pipeline()
        elif pipeline_type == 'streaming':
            pipeline.run_streaming_pipeline()
        else:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)
    
    finally:
        if 'pipeline' in locals():
            pipeline.shutdown()

if __name__ == "__main__":
    main()