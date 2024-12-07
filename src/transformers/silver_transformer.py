from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr
from ..core.base_transformer import BaseTransformer
from ..config.pipeline_config import PipelineConfig

class ColumnTransformation:
    def __init__(self, source_col: str, target_col: str, transform_type: str, 
                 cast_type: str = None, expression: str = None):
        self.source_col = source_col
        self.target_col = target_col
        self.transform_type = transform_type
        self.cast_type = cast_type
        self.expression = expression

class SilverTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession, config: PipelineConfig, 
                 transformations: List[ColumnTransformation]):
        super().__init__(spark, config)
        self.transformations = transformations

    def read_bronze(self) -> DataFrame:
        """Read data from bronze layer"""
        return self.spark.read.format(self.config.storage_format.value)\
            .load(f"{self.config.target_location}/bronze")

    def apply_transformations(self, df: DataFrame) -> DataFrame:
        """Apply the configured transformations"""
        for transform in self.transformations:
            if transform.transform_type == "direct":
                df = df.withColumn(transform.target_col, col(transform.source_col))
            elif transform.transform_type == "cast":
                df = df.withColumn(transform.target_col, 
                                 col(transform.source_col).cast(transform.cast_type))
            elif transform.transform_type == "expression":
                df = df.withColumn(transform.target_col, expr(transform.expression))
        return df

    def transform(self, input_df: DataFrame) -> DataFrame:
        """
        Silver layer transformation:
        1. Apply column transformations
        2. Validate data quality
        3. Update audit columns
        """
        # Apply configured transformations
        df = self.apply_transformations(input_df)
        
        # Update audit columns
        df = df.withColumn("silver_processed_timestamp", 
                          self.spark.sql.functions.current_timestamp())
        
        return df

    def process(self) -> None:
        """Execute the silver layer processing"""
        bronze_df = self.read_bronze()
        transformed_df = self.transform(bronze_df)
        self.write(transformed_df, f"{self.config.target_location}/silver")
