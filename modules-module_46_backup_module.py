from pyspark.sql import SparkSession, DataFrame
import shutil
import os

class BackupModule:
    """Module 46: Data backup and recovery"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_backup(self, source_path: str, backup_path: str):
        """Create backup of data"""
        
        # For local file system (in production, use cloud storage operations)
        if os.path.exists(source_path):
            if os.path.exists(backup_path):
                shutil.rmtree(backup_path)
            shutil.copytree(source_path, backup_path)
    
    def restore_backup(self, backup_path: str, restore_path: str):
        """Restore data from backup"""
        
        if os.path.exists(backup_path):
            if os.path.exists(restore_path):
                shutil.rmtree(restore_path)
            shutil.copytree(backup_path, restore_path)