from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (col, count, avg, stddev, min, max, 
                                 percentile_approx, lag, expr, sum, 
                                 when, row_number, collect_list)
from pyspark.ml.feature import VectorAssembler, IsolationForest
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
from dataclasses import dataclass
from enum import Enum

class AnomalyType(Enum):
    VOLUME = "volume"
    PROCESSING_TIME = "processing_time"
    ERROR_RATE = "error_rate"
    QUALITY_FAILURE = "quality_failure"
    GROUP_PATTERN = "group_pattern"

@dataclass
class AnomalyConfig:
    detection_method: str  # zscore, iqr, isolation_forest, moving_average
    lookback_window: int  # days
    threshold: float
    min_samples: int = 1000
    sensitivity: float = 1.0

class ProcessingAnomalyDetector:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def detect_volume_anomalies(self, df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Detect anomalies in data volume processing."""
        window = Window.orderBy("processing_date").rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn("volume", count("*").over(Window.partitionBy("processing_date"))) \
            .withColumn("avg_volume", avg("volume").over(window)) \
            .withColumn("stddev_volume", stddev("volume").over(window)) \
            .withColumn("volume_zscore", 
                       (col("volume") - col("avg_volume")) / col("stddev_volume")) \
            .withColumn("is_volume_anomaly", 
                       abs(col("volume_zscore")) > config.threshold)

    def detect_processing_time_anomalies(self, df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Detect anomalies in processing time."""
        window = Window.orderBy("start_time").rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn("processing_time", 
                           (col("end_time").cast("long") - col("start_time").cast("long"))) \
            .withColumn("avg_time", avg("processing_time").over(window)) \
            .withColumn("stddev_time", stddev("processing_time").over(window)) \
            .withColumn("time_zscore", 
                       (col("processing_time") - col("avg_time")) / col("stddev_time")) \
            .withColumn("is_time_anomaly", 
                       abs(col("time_zscore")) > config.threshold)

    def detect_error_rate_spikes(self, df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Detect spikes in error rates."""
        window = Window.orderBy("processing_date").rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn("error_rate", 
                           sum(when(col("status") == "ERROR", 1).otherwise(0)) \
                           .over(Window.partitionBy("processing_date")) / count("*") \
                           .over(Window.partitionBy("processing_date"))) \
            .withColumn("avg_error_rate", avg("error_rate").over(window)) \
            .withColumn("stddev_error_rate", stddev("error_rate").over(window)) \
            .withColumn("error_rate_zscore", 
                       (col("error_rate") - col("avg_error_rate")) / col("stddev_error_rate")) \
            .withColumn("is_error_rate_anomaly", 
                       abs(col("error_rate_zscore")) > config.threshold)

    def detect_quality_failure_patterns(self, df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Detect patterns in data quality failures."""
        window = Window.orderBy("check_date").rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn("failure_rate", 
                           sum(when(col("check_status") == "FAILED", 1).otherwise(0)) \
                           .over(Window.partitionBy("check_date", "check_type")) / 
                           count("*").over(Window.partitionBy("check_date", "check_type"))) \
            .withColumn("avg_failure_rate", avg("failure_rate").over(window)) \
            .withColumn("stddev_failure_rate", stddev("failure_rate").over(window)) \
            .withColumn("failure_pattern_score", 
                       (col("failure_rate") - col("avg_failure_rate")) / 
                       col("stddev_failure_rate")) \
            .withColumn("is_failure_pattern_anomaly", 
                       abs(col("failure_pattern_score")) > config.threshold)

class GroupBasedAnomalyDetector:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def detect_regional_variations(self, df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Detect anomalies in regional patterns."""
        window = Window.partitionBy("region").orderBy("processing_date") \
                      .rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn("avg_amount", avg("amount").over(window)) \
            .withColumn("stddev_amount", stddev("amount").over(window)) \
            .withColumn("q1", expr(f"percentile_approx(amount, 0.25)").over(window)) \
            .withColumn("q3", expr(f"percentile_approx(amount, 0.75)").over(window)) \
            .withColumn("iqr", col("q3") - col("q1")) \
            .withColumn("is_regional_anomaly", 
                       (col("amount") < col("q1") - config.sensitivity * col("iqr")) | 
                       (col("amount") > col("q3") + config.sensitivity * col("iqr")))

    def detect_segment_patterns(self, df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Detect anomalies in segment-based patterns."""
        # Prepare features for Isolation Forest
        assembler = VectorAssembler(
            inputCols=["credit_score", "income", "debt_ratio"],
            outputCol="features"
        )
        
        vector_df = assembler.transform(df)
        
        # Train Isolation Forest model
        isolation_forest = IsolationForest(
            featuresCol="features",
            contamination=0.1,  # Expected proportion of anomalies
            numTrees=100
        )
        
        model = isolation_forest.fit(vector_df)
        
        # Predict anomalies
        return model.transform(vector_df) \
            .withColumn("is_segment_anomaly", 
                       col("prediction") == 1)  # 1 indicates anomaly

    def detect_employment_income_anomalies(self, df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Detect anomalies in employment status vs income patterns."""
        window = Window.partitionBy("employment_status").orderBy("processing_date") \
                      .rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn("avg_income", avg("income").over(window)) \
            .withColumn("stddev_income", stddev("income").over(window)) \
            .withColumn("income_zscore", 
                       (col("income") - col("avg_income")) / col("stddev_income")) \
            .withColumn("is_employment_income_anomaly", 
                       abs(col("income_zscore")) > config.threshold)

    def detect_approval_rate_variations(self, df: DataFrame, config: AnomalyConfig) -> DataFrame:
        """Detect anomalies in state-wise approval rates."""
        window = Window.partitionBy("state").orderBy("processing_date") \
                      .rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn("approval_rate", 
                           sum(when(col("status") == "APPROVED", 1).otherwise(0)) \
                           .over(Window.partitionBy("state", "processing_date")) / 
                           count("*").over(Window.partitionBy("state", "processing_date"))) \
            .withColumn("avg_approval_rate", avg("approval_rate").over(window)) \
            .withColumn("stddev_approval_rate", stddev("approval_rate").over(window)) \
            .withColumn("approval_rate_zscore", 
                       (col("approval_rate") - col("avg_approval_rate")) / 
                       col("stddev_approval_rate")) \
            .withColumn("is_approval_rate_anomaly", 
                       abs(col("approval_rate_zscore")) > config.threshold)

class MultiMethodAnomalyDetector:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def zscore_detection(self, df: DataFrame, column: str, config: AnomalyConfig) -> DataFrame:
        """Z-score based anomaly detection."""
        window = Window.orderBy("processing_date").rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn(f"avg_{column}", avg(column).over(window)) \
            .withColumn(f"stddev_{column}", stddev(column).over(window)) \
            .withColumn(f"{column}_zscore", 
                       (col(column) - col(f"avg_{column}")) / col(f"stddev_{column}")) \
            .withColumn(f"is_{column}_zscore_anomaly", 
                       abs(col(f"{column}_zscore")) > config.threshold)

    def iqr_detection(self, df: DataFrame, column: str, config: AnomalyConfig) -> DataFrame:
        """IQR-based anomaly detection."""
        window = Window.orderBy("processing_date").rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn(f"q1_{column}", 
                           expr(f"percentile_approx({column}, 0.25)").over(window)) \
            .withColumn(f"q3_{column}", 
                       expr(f"percentile_approx({column}, 0.75)").over(window)) \
            .withColumn(f"iqr_{column}", 
                       col(f"q3_{column}") - col(f"q1_{column}")) \
            .withColumn(f"is_{column}_iqr_anomaly", 
                       (col(column) < col(f"q1_{column}") - 
                        config.sensitivity * col(f"iqr_{column}")) | 
                       (col(column) > col(f"q3_{column}") + 
                        config.sensitivity * col(f"iqr_{column}")))

    def isolation_forest_detection(self, df: DataFrame, 
                                 feature_cols: List[str], 
                                 config: AnomalyConfig) -> DataFrame:
        """Isolation Forest based anomaly detection."""
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        vector_df = assembler.transform(df)
        
        isolation_forest = IsolationForest(
            featuresCol="features",
            contamination=0.1,
            numTrees=100
        )
        
        model = isolation_forest.fit(vector_df)
        
        return model.transform(vector_df) \
            .withColumn("is_isolation_forest_anomaly", col("prediction") == 1)

    def moving_average_detection(self, df: DataFrame, 
                               column: str, 
                               config: AnomalyConfig) -> DataFrame:
        """Moving average based anomaly detection."""
        window = Window.orderBy("processing_date").rowsBetween(-config.lookback_window, 0)
        
        return df.withColumn(f"ma_{column}", avg(column).over(window)) \
            .withColumn(f"{column}_ma_diff", 
                       abs(col(column) - col(f"ma_{column}")) / col(f"ma_{column}")) \
            .withColumn(f"is_{column}_ma_anomaly", 
                       col(f"{column}_ma_diff") > config.threshold)

class AnomalyDetectionOrchestrator:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.processing_detector = ProcessingAnomalyDetector(spark)
        self.group_detector = GroupBasedAnomalyDetector(spark)
        self.multi_method_detector = MultiMethodAnomalyDetector(spark)

    def detect_all_anomalies(self, df: DataFrame, config: AnomalyConfig) -> Dict[str, DataFrame]:
        """Run all anomaly detection methods and return results."""
        results = {}
        
        # Processing Anomalies
        results['volume'] = self.processing_detector.detect_volume_anomalies(df, config)
        results['processing_time'] = self.processing_detector.detect_processing_time_anomalies(df, config)
        results['error_rate'] = self.processing_detector.detect_error_rate_spikes(df, config)
        results['quality_failure'] = self.processing_detector.detect_quality_failure_patterns(df, config)
        
        # Group-based Anomalies
        results['regional'] = self.group_detector.detect_regional_variations(df, config)
        results['segment'] = self.group_detector.detect_segment_patterns(df, config)
        results['employment_income'] = self.group_detector.detect_employment_income_anomalies(df, config)
        results['approval_rate'] = self.group_detector.detect_approval_rate_variations(df, config)
        
        # Multi-method Detection
        feature_cols = ["amount", "credit_score", "income"]
        results['zscore'] = self.multi_method_detector.zscore_detection(df, "amount", config)
        results['iqr'] = self.multi_method_detector.iqr_detection(df, "amount", config)
        results['isolation_forest'] = self.multi_method_detector.isolation_forest_detection(
            df, feature_cols, config)
        results['moving_average'] = self.multi_method_detector.moving_average_detection(
            df, "amount", config)
        
        return results

    def get_anomaly_summary(self, results: Dict[str, DataFrame]) -> Dict[str, Dict]:
        """Generate summary of detected anomalies."""
        summary = {}
        
        for anomaly_type, df in results.items():
            anomaly_cols = [col for col in df.columns if col.startswith("is_") and col.endswith("anomaly")]
            
            summary[anomaly_type] = {
                "total_records": df.count(),
                "anomaly_count": df.filter(" OR ".join([f"{col} == True" for col in anomaly_cols])).count(),
                "detection_timestamp": datetime.now().isoformat()
            }
        
        return summary
