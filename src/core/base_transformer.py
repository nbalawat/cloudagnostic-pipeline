from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from ..config.pipeline_config import PipelineConfig, StorageFormat

class BaseTransformer(ABC):
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        
    def _get_storage_writer(self, df: DataFrame, path: str) -> DataFrame:
        """Get the appropriate storage writer based on configuration"""
        if self.config.storage_format == StorageFormat.DELTA:
            return df.write.format("delta")
        elif self.config.storage_format == StorageFormat.ICEBERG:
            return df.write.format("iceberg")
        elif self.config.storage_format == StorageFormat.HUDI:
            return df.write.format("hudi")
        else:
            raise ValueError(f"Unsupported storage format: {self.config.storage_format}")

    def _encrypt_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """Encrypt specified columns using spark built-in encryption"""
        if not columns:
            return df
        for column in columns:
            if column in df.columns:
                df = df.withColumn(column, self.spark.sql.functions.sha2(column, 256))
        return df

    @abstractmethod
    def transform(self, input_df: DataFrame) -> DataFrame:
        """Transform the input dataframe according to the layer's requirements"""
        pass

    def write(self, df: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Write the dataframe to the specified location using configured format"""
        writer = self._get_storage_writer(df, path)
        writer.mode(mode).save(path)

    def validate_schema(self, df: DataFrame, expected_schema: Dict) -> bool:
        """Validate if the dataframe schema matches expected schema"""
        current_schema = {field.name: field.dataType for field in df.schema.fields}
        return current_schema == expected_schema
