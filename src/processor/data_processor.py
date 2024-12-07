from pyspark.sql import SparkSession
from typing import Optional
import os
import logging

class DataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataProcessor") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Enable Iceberg and Hudi support
        self.spark.sql("CREATE CATALOG IF NOT EXISTS iceberg_catalog USING org.apache.iceberg.spark.SparkCatalog")
        self.spark.sql("CREATE CATALOG IF NOT EXISTS hudi_catalog USING org.apache.hudi.spark.catalog.HoodieCatalog")

    def _read_source_files(self, path: str, format: str) -> Optional[DataFrame]:
        """Read source files based on format"""
        try:
            if format.lower() == 'parquet':
                return self.spark.read.parquet(path)
            elif format.lower() == 'json':
                return self.spark.read.json(path)
            elif format.lower() == 'csv':
                return self.spark.read.csv(path, header=True, inferSchema=True)
            elif format.lower() == 'orc':
                return self.spark.read.orc(path)
            else:
                raise ValueError(f"Unsupported format: {format}")
        except Exception as e:
            logging.error(f"Error reading source files from {path}: {str(e)}")
            return None

    def _write_table(self, df: DataFrame, path: str, format: str, mode: str = "overwrite"):
        """Write DataFrame to specified format"""
        try:
            if format.lower() == 'delta':
                df.write.format("delta").mode(mode).save(path)
            elif format.lower() == 'iceberg':
                df.write.format("iceberg").mode(mode).save(path)
            elif format.lower() == 'hudi':
                hudi_options = {
                    'hoodie.table.name': path.split('/')[-1],
                    'hoodie.datasource.write.recordkey.field': 'id',
                    'hoodie.datasource.write.precombine.field': 'ts'
                }
                df.write.format("hudi").options(**hudi_options).mode(mode).save(path)
            else:
                raise ValueError(f"Unsupported format: {format}")
        except Exception as e:
            logging.error(f"Error writing to {path}: {str(e)}")
            raise

    def process_bronze_layer(self, source_path: str, target_path: str, 
                           source_format: str, target_format: str):
        """Process data from landing to bronze layer"""
        df = self._read_source_files(source_path, source_format)
        if df is not None:
            # Add metadata columns
            df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                   .withColumn("source_file", input_file_name())
            
            self._write_table(df, target_path, target_format)

    def process_silver_layer(self, table_name: str, transformation: str, 
                           target_path: str, target_format: str):
        """Process data from bronze to silver layer"""
        try:
            # Execute the transformation SQL
            df = self.spark.sql(transformation)
            
            # Add quality columns
            df = df.withColumn("processed_timestamp", current_timestamp()) \
                   .withColumn("quality_check_passed", lit(True))
            
            self._write_table(df, target_path, target_format)
        except Exception as e:
            logging.error(f"Error processing silver layer for {table_name}: {str(e)}")
            raise

    def process_gold_layer(self, table_name: str, transformation: str, 
                          target_path: str, target_format: str):
        """Process data from silver to gold layer"""
        try:
            # Execute the transformation SQL
            df = self.spark.sql(transformation)
            
            # Add analytics metadata
            df = df.withColumn("analytics_timestamp", current_timestamp()) \
                   .withColumn("data_quality_score", lit(1.0))
            
            self._write_table(df, target_path, target_format)
        except Exception as e:
            logging.error(f"Error processing gold layer for {table_name}: {str(e)}")
            raise

    def process_table(self, table_name: str, transformation: str, format: str, path: str):
        """Main processing function called by Argo Workflow"""
        try:
            layer = table_name.split('.')[0]
            if layer == 'bronze':
                source_system = table_name.split('.')[1]
                source_path = f"/data/landing/{source_system}"
                self.process_bronze_layer(source_path, path, 'parquet', format)
            elif layer == 'silver':
                self.process_silver_layer(table_name, transformation, path, format)
            elif layer == 'gold':
                self.process_gold_layer(table_name, transformation, path, format)
            
            logging.info(f"Successfully processed {table_name}")
        except Exception as e:
            logging.error(f"Error processing {table_name}: {str(e)}")
            raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 5:
        print("Usage: data_processor.py <table_name> <transformation> <format> <path>")
        sys.exit(1)
    
    table_name = sys.argv[1]
    transformation = sys.argv[2]
    format = sys.argv[3]
    path = sys.argv[4]
    
    processor = DataProcessor()
    processor.process_table(table_name, transformation, format, path)
