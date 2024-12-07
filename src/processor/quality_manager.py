from typing import Dict, List, Optional
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull

logger = logging.getLogger(__name__)

class QualityManager:
    def __init__(self):
        self.quality_metrics = {}
        
    def check_completeness(self, df: DataFrame, columns: List[str], threshold: float) -> bool:
        """Check column completeness against threshold"""
        total_rows = df.count()
        
        for column in columns:
            null_count = df.filter(
                isnull(col(column)) | isnan(col(column))
            ).count()
            
            completeness = 1 - (null_count / total_rows)
            self.quality_metrics[f"{column}_completeness"] = completeness
            
            if completeness < threshold:
                logger.error(
                    f"Completeness check failed for {column}. "
                    f"Got {completeness:.2%}, expected >= {threshold:.2%}"
                )
                return False
        
        return True
    
    def check_uniqueness(self, df: DataFrame, columns: List[str]) -> bool:
        """Check column uniqueness"""
        total_rows = df.count()
        distinct_rows = df.select(columns).distinct().count()
        
        uniqueness = distinct_rows / total_rows
        self.quality_metrics["uniqueness"] = uniqueness
        
        return uniqueness == 1.0
    
    def check_referential_integrity(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        source_columns: List[str],
        target_columns: List[str]
    ) -> bool:
        """Check referential integrity between dataframes"""
        # Create views for SQL
        source_df.createOrReplaceTempView("source")
        target_df.createOrReplaceTempView("target")
        
        # Build join condition
        join_conditions = []
        for sc, tc in zip(source_columns, target_columns):
            join_conditions.append(f"source.{sc} = target.{tc}")
        
        join_condition = " AND ".join(join_conditions)
        
        # Count orphaned records
        orphaned_count = source_df.sparkSession.sql(f"""
            SELECT COUNT(*) as count
            FROM source
            LEFT JOIN target ON {join_condition}
            WHERE {" OR ".join([f"target.{tc} IS NULL" for tc in target_columns])}
        """).collect()[0]["count"]
        
        integrity = 1 - (orphaned_count / source_df.count())
        self.quality_metrics["referential_integrity"] = integrity
        
        return orphaned_count == 0
    
    def check_value_distribution(
        self,
        df: DataFrame,
        column: str,
        expected_values: Dict[str, float]
    ) -> bool:
        """Check value distribution against expected ratios"""
        total_rows = df.count()
        
        # Calculate actual distribution
        value_counts = df.groupBy(column).count().collect()
        actual_distribution = {
            row[column]: row["count"] / total_rows 
            for row in value_counts
        }
        
        # Compare with expected
        for value, expected_ratio in expected_values.items():
            actual_ratio = actual_distribution.get(value, 0)
            self.quality_metrics[f"{column}_{value}_distribution"] = actual_ratio
            
            if abs(actual_ratio - expected_ratio) > 0.1:  # 10% tolerance
                logger.error(
                    f"Distribution check failed for {column}={value}. "
                    f"Got {actual_ratio:.2%}, expected {expected_ratio:.2%}"
                )
                return False
        
        return True
    
    def check_freshness(self, df: DataFrame, timestamp_column: str, max_delay_hours: int) -> bool:
        """Check data freshness"""
        max_timestamp = df.agg({timestamp_column: "max"}).collect()[0][0]
        
        if not max_timestamp:
            return False
        
        from datetime import datetime, timedelta
        current_time = datetime.utcnow()
        max_age = current_time - max_timestamp
        
        freshness = 1 - (max_age.total_seconds() / (max_delay_hours * 3600))
        self.quality_metrics["freshness"] = freshness
        
        return max_age <= timedelta(hours=max_delay_hours)
    
    def check_anomalies(
        self,
        df: DataFrame,
        column: str,
        algorithm: str = "zscore",
        threshold: float = 3.0
    ) -> bool:
        """Check for anomalies using specified algorithm"""
        if algorithm == "zscore":
            # Calculate z-score
            stats = df.select(column).summary("mean", "stddev").collect()
            mean = float(stats[0][1])
            stddev = float(stats[1][1])
            
            anomaly_count = df.filter(
                abs(col(column) - mean) > (threshold * stddev)
            ).count()
            
            anomaly_ratio = anomaly_count / df.count()
            self.quality_metrics[f"{column}_anomaly_ratio"] = anomaly_ratio
            
            return anomaly_ratio <= 0.01  # Allow up to 1% anomalies
        
        else:
            raise ValueError(f"Unsupported anomaly detection algorithm: {algorithm}")
    
    def get_metrics(self) -> Dict[str, float]:
        """Get collected quality metrics"""
        return self.quality_metrics.copy()
    
    def reset_metrics(self):
        """Reset quality metrics"""
        self.quality_metrics.clear()
