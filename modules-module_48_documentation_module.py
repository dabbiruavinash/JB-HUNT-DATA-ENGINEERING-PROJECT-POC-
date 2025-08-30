from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
import json

class DocumentationModule:
    """Module 48: Automated documentation generation"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.documentation = {}
    
    def document_operation(self, operation_name: str, description: str, 
                          parameters: Dict[str, Any], output_schema: StructType = None):
        """Document a data operation"""
        
        doc_entry = {
            "operation": operation_name,
            "description": description,
            "parameters": parameters,
            "timestamp": datetime.now().isoformat(),
            "output_schema": str(output_schema) if output_schema else None
        }
        
        self.documentation[operation_name] = doc_entry
    
    def export_documentation(self, output_path: str):
        """Export documentation to file"""
        
        with open(output_path, 'w') as f:
            json.dump(self.documentation, f, indent=2)