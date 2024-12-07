from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, expr, sum, avg, count
from ..core.base_transformer import BaseTransformer
from ..config.pipeline_config import PipelineConfig

class AggregationRule:
    def __init__(self, name: str, source_cols: List[str], agg_type: str, 
                 group_by_cols: List[str] = None, having_clause: str = None):
        self.name = name
        self.source_cols = source_cols
        self.agg_type = agg_type
        self.group_by_cols = group_by_cols or []
        self.having_clause = having_clause

class GoldTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession, config: PipelineConfig, 
                 aggregation_rules: List[AggregationRule]):
        super().__init__(spark, config)
        self.aggregation_rules = aggregation_rules

    def read_silver(self) -> DataFrame:
        """Read data from silver layer"""
        return self.spark.read.format(self.config.storage_format.value)\
            .load(f"{self.config.target_location}/silver")

    def apply_aggregations(self, df: DataFrame) -> DataFrame:
        """Apply the configured aggregations"""
        for rule in self.aggregation_rules:
            agg_exprs = []
            for col_name in rule.source_cols:
                if rule.agg_type == "sum":
                    agg_exprs.append(sum(col(col_name)).alias(f"{rule.name}_{col_name}_sum"))
                elif rule.agg_type == "avg":
                    agg_exprs.append(avg(col(col_name)).alias(f"{rule.name}_{col_name}_avg"))
                elif rule.agg_type == "count":
                    agg_exprs.append(count(col(col_name)).alias(f"{rule.name}_{col_name}_count"))

            if rule.group_by_cols:
                df = df.groupBy(rule.group_by_cols).agg(*agg_exprs)
                if rule.having_clause:
                    df = df.filter(rule.having_clause)

        return df

    def transform(self, input_df: DataFrame) -> DataFrame:
        """
        Gold layer transformation:
        1. Apply aggregations
        2. Create business metrics
        3. Update audit columns
        """
        # Apply configured aggregations
        df = self.apply_aggregations(input_df)
        
        # Update audit columns
        df = df.withColumn("gold_processed_timestamp", 
                          self.spark.sql.functions.current_timestamp())
        
        return df

    def process(self) -> None:
        """Execute the gold layer processing"""
        silver_df = self.read_silver()
        transformed_df = self.transform(silver_df)
        self.write(transformed_df, f"{self.config.target_location}/gold")
