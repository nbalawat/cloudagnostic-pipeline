from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from ..core.base_transformer import BaseTransformer
from ..config.pipeline_config import PipelineConfig

class BronzeTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        super().__init__(spark, config)
        
    def read_source(self) -> DataFrame:
        """Read data from landing zone based on input format"""
        reader = self.spark.read.format(self.config.input_format.value)
        if self.config.input_format.value == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")
        return reader.load(self.config.landing_location)

    def transform(self, input_df: DataFrame) -> DataFrame:
        """
        Bronze layer transformation:
        1. Add metadata columns
        2. Encrypt sensitive columns
        3. Add audit columns
        """
        # Add metadata columns
        df = input_df.withColumn("source_file", self.spark.sql.functions.input_file_name())
        df = df.withColumn("ingestion_timestamp", self.spark.sql.functions.current_timestamp())
        
        # Encrypt sensitive columns if specified
        if self.config.encrypted_columns:
            df = self._encrypt_columns(df, self.config.encrypted_columns)
            
        # Add audit columns
        df = df.withColumn("record_status", self.spark.sql.functions.lit("active"))
        df = df.withColumn("processed_timestamp", self.spark.sql.functions.current_timestamp())
        
        return df

    def process(self) -> None:
        """Execute the bronze layer processing"""
        input_df = self.read_source()
        transformed_df = self.transform(input_df)
        self.write(transformed_df, f"{self.config.target_location}/bronze")
