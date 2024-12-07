from typing import Dict, List, Optional
import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, input_file_name, col, expr, 
    when, lit, regexp_replace, upper, trim
)
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MedallionProcessor:
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or SparkSession.builder \
            .appName("MedallionProcessor") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        self.quality_metrics = {}
        
    def load_config(self, config_path: str) -> Dict:
        """Load YAML configuration file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def process_bronze_layer(self, config_path: str):
        """Process data from landing to bronze layer"""
        config = self.load_config(config_path)
        
        for source_config in config['source_configurations']:
            try:
                # Read from landing zone
                landing_path = source_config['landing_zone']['path']
                file_pattern = source_config['landing_zone']['file_pattern']
                input_format = source_config['format']
                
                input_path = f"{landing_path}/{file_pattern}"
                df = self._read_source(input_path, input_format, source_config)
                
                if df is None:
                    continue
                
                # Apply bronze processing
                df = self._apply_bronze_processing(df, source_config)
                
                # Quality checks
                if not self._apply_quality_checks(df, source_config['quality_checks']):
                    self._handle_quality_failure(source_config)
                    continue
                
                # Write to bronze zone
                bronze_path = source_config['bronze_zone']['path']
                partition_by = source_config['bronze_zone'].get('partition_by', [])
                
                self._write_bronze(df, bronze_path, partition_by)
                
                logger.info(f"Successfully processed {source_config['name']} to bronze layer")
                
            except Exception as e:
                logger.error(f"Error processing {source_config['name']}: {str(e)}")
                self._handle_processing_error(source_config, e)

    def process_silver_layer(self, config_path: str):
        """Process data from bronze to silver layer"""
        config = self.load_config(config_path)
        
        for source_config in config['source_configurations']:
            try:
                # Read from bronze
                input_path = source_config['input']['path']
                df = self.spark.read.format("delta").load(input_path)
                
                # Apply transformations
                df = self._apply_silver_transformations(df, source_config['transformations'])
                
                # Quality checks
                if not self._apply_quality_checks(df, source_config['quality_checks']):
                    self._handle_quality_failure(source_config)
                    continue
                
                # Write to silver
                output_config = source_config['output']
                self._write_silver(
                    df, 
                    output_config['path'],
                    output_config['partition_by']
                )
                
                logger.info(f"Successfully processed {source_config['name']} to silver layer")
                
            except Exception as e:
                logger.error(f"Error processing {source_config['name']}: {str(e)}")
                self._handle_processing_error(source_config, e)

    def process_gold_layer(self, config_path: str):
        """Process data from silver to gold layer"""
        config = self.load_config(config_path)
        
        for agg_config in config['aggregations']:
            try:
                # Read source tables
                if isinstance(agg_config['source_table'], str):
                    df = self.spark.read.table(agg_config['source_table'])
                else:
                    # Handle multiple source tables
                    dfs = [self.spark.read.table(t) for t in agg_config['source_tables']]
                    df = self._join_tables(dfs, agg_config.get('join_conditions', []))
                
                # Apply time window aggregations
                df = self._apply_gold_aggregations(df, agg_config)
                
                # Quality checks
                if not self._apply_quality_checks(df, agg_config['quality_checks']):
                    self._handle_quality_failure(agg_config)
                    continue
                
                # Write to gold
                output_config = agg_config['output']
                self._write_gold(
                    df,
                    output_config['table'],
                    output_config['mode'],
                    output_config.get('merge_key', [])
                )
                
                logger.info(f"Successfully processed {agg_config['name']} to gold layer")
                
            except Exception as e:
                logger.error(f"Error processing {agg_config['name']}: {str(e)}")
                self._handle_processing_error(agg_config, e)

    def _read_source(self, path: str, format: str, config: Dict) -> Optional[DataFrame]:
        """Read source data with schema validation"""
        try:
            reader = self.spark.read.format(format)
            
            # Apply schema if provided
            if 'schema_validation' in config:
                schema_location = config['schema_validation']['schema_location']
                with open(schema_location, 'r') as f:
                    schema = yaml.safe_load(f)
                reader = reader.schema(schema)
            
            return reader.load(path)
            
        except Exception as e:
            logger.error(f"Error reading {path}: {str(e)}")
            return None

    def _apply_bronze_processing(self, df: DataFrame, config: Dict) -> DataFrame:
        """Apply bronze layer processing rules"""
        # Add metadata columns
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file", input_file_name())
        
        # Clean column names if configured
        if config['processing'].get('clean_column_names', False):
            for col_name in df.columns:
                clean_name = regexp_replace(col_name, "[^a-zA-Z0-9_]", "_")
                df = df.withColumnRenamed(col_name, clean_name)
        
        # Handle duplicates
        dedup_mode = config['processing'].get('handle_duplicates', 'keep_latest')
        if dedup_mode == 'keep_latest':
            df = df.dropDuplicates()
        
        return df

    def _apply_silver_transformations(self, df: DataFrame, transformations: List[Dict]) -> DataFrame:
        """Apply silver layer transformations"""
        for transform in transformations:
            if transform['type'] == 'standardize_names':
                for col_name in transform['columns']:
                    df = df.withColumn(
                        col_name,
                        trim(upper(col(col_name)))
                    )
            elif transform['type'] == 'validate_email':
                for col_name in transform['columns']:
                    df = df.withColumn(
                        f"{col_name}_valid",
                        expr(f"regexp_extract({col_name}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{{2,}}$$', 0) != ''")
                    )
        
        return df

    def _apply_gold_aggregations(self, df: DataFrame, config: Dict) -> DataFrame:
        """Apply gold layer aggregations"""
        for window in config['time_window']:
            for metric in window['metrics']:
                df = df.withColumn(
                    metric['name'],
                    expr(metric['expr'])
                )
        
        if 'dimensions' in config:
            df = df.groupBy(config['dimensions'])
        
        return df

    def _apply_quality_checks(self, df: DataFrame, checks: List[Dict]) -> bool:
        """Apply quality checks and return pass/fail"""
        for check in checks:
            if check['type'] == 'row_count':
                count = df.count()
                if count < check['min_threshold']:
                    logger.error(f"Row count check failed. Got {count}, expected >= {check['min_threshold']}")
                    return False
            elif check['type'] == 'null_check':
                for column in check['columns']:
                    null_count = df.filter(col(column).isNull()).count()
                    null_percentage = (null_count / df.count()) * 100
                    if null_percentage > check['max_nulls_percentage']:
                        logger.error(f"Null check failed for {column}. Got {null_percentage}%, max allowed {check['max_nulls_percentage']}%")
                        return False
        
        return True

    def _write_bronze(self, df: DataFrame, path: str, partition_by: List[str]):
        """Write to bronze layer"""
        writer = df.write.format("delta").mode("append")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)

    def _write_silver(self, df: DataFrame, path: str, partition_by: List[str]):
        """Write to silver layer"""
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)

    def _write_gold(self, df: DataFrame, table: str, mode: str, merge_key: List[str]):
        """Write to gold layer"""
        if mode == "merge" and merge_key:
            delta_table = DeltaTable.forName(self.spark, table)
            delta_table.alias("target").merge(
                df.alias("source"),
                " AND ".join([f"target.{k} = source.{k}" for k in merge_key])
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df.write.format("delta").mode(mode).saveAsTable(table)

    def _handle_quality_failure(self, config: Dict):
        """Handle quality check failures"""
        logger.error(f"Quality checks failed for {config['name']}")
        # Implement notification logic here

    def _handle_processing_error(self, config: Dict, error: Exception):
        """Handle processing errors"""
        logger.error(f"Processing error for {config['name']}: {str(error)}")
        # Implement error handling logic here

    def _join_tables(self, dfs: List[DataFrame], join_conditions: List[Dict]) -> DataFrame:
        """Join multiple DataFrames based on conditions"""
        if not dfs:
            return None
        
        result = dfs[0]
        for i, condition in enumerate(join_conditions):
            result = result.join(
                dfs[i + 1],
                on=condition['on'],
                how=condition.get('how', 'inner')
            )
        
        return result
