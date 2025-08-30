from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, current_timestamp

class SCD2Module:
    """Module 6: SCD Type 2 Implementation"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def apply_scd2(self, current_df: DataFrame, new_df: DataFrame, 
                  primary_key: str, effective_date_col: str = "effective_date",
                  end_date_col: str = "end_date", is_current_col: str = "is_current"):
        """
        Apply SCD Type 2 logic to merge new data with current data
        """
        # Create temporary views
        current_df.createOrReplaceTempView("current_data")
        new_df.createOrReplaceTempView("new_data")
        
        scd2_sql = f"""
        WITH updated_records AS (
            SELECT 
                c.*,
                CASE 
                    WHEN c.{primary_key} IS NOT NULL AND n.{primary_key} IS NOT NULL 
                         AND (c.origin != n.origin OR c.destination != n.destination OR c.status != n.status) 
                    THEN false 
                    ELSE c.{is_current_col} 
                END as {is_current_col},
                CASE 
                    WHEN c.{primary_key} IS NOT NULL AND n.{primary_key} IS NOT NULL 
                         AND (c.origin != n.origin OR c.destination != n.destination OR c.status != n.status) 
                    THEN current_timestamp() 
                    ELSE c.{end_date_col} 
                END as {end_date_col}
            FROM current_data c
            LEFT JOIN new_data n ON c.{primary_key} = n.{primary_key}
        ),
        new_records AS (
            SELECT 
                n.*,
                current_timestamp() as {effective_date_col},
                null as {end_date_col},
                true as {is_current_col}
            FROM new_data n
            LEFT JOIN current_data c ON n.{primary_key} = c.{primary_key}
            WHERE c.{primary_key} IS NULL
        ),
        unchanged_records AS (
            SELECT c.*
            FROM current_data c
            LEFT JOIN new_data n ON c.{primary_key} = n.{primary_key}
            WHERE n.{primary_key} IS NULL
        )
        
        SELECT * FROM updated_records
        UNION ALL
        SELECT * FROM new_records
        UNION ALL
        SELECT * FROM unchanged_records
        """
        
        return self.spark.sql(scd2_sql)