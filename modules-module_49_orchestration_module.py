from pyspark.sql import SparkSession, DataFrame
from typing import List, Callable, Dict, Any

class OrchestrationModule:
    """Module 49: Data pipeline orchestration"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.pipeline_steps = []
    
    def add_step(self, step_name: str, step_function: Callable, 
                dependencies: List[str] = None, parameters: Dict[str, Any] = None):
        """Add a step to the pipeline"""
        
        self.pipeline_steps.append({
            "name": step_name,
            "function": step_function,
            "dependencies": dependencies or [],
            "parameters": parameters or {},
            "status": "PENDING"
        })
    
    def execute_pipeline(self):
        """Execute the entire pipeline"""
        
        executed_steps = set()
        
        while len(executed_steps) < len(self.pipeline_steps):
            for step in self.pipeline_steps:
                if step["name"] not in executed_steps:
                    # Check if dependencies are satisfied
                    dependencies_satisfied = all(
                        dep in executed_steps for dep in step["dependencies"]
                    )
                    
                    if dependencies_satisfied or not step["dependencies"]:
                        try:
                            # Execute step
                            step["function"](**step["parameters"])
                            step["status"] = "COMPLETED"
                            executed_steps.add(step["name"])
                        except Exception as e:
                            step["status"] = "FAILED"
                            raise Exception(f"Step {step['name']} failed: {str(e)}")