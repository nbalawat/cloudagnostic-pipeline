from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, avg, stddev, expr
from typing import Dict, List, Optional
from datetime import datetime
import json
from abc import ABC, abstractmethod

class QualityRule(ABC):
    def __init__(self, rule_config: Dict):
        self.rule_id = rule_config['rule_id']
        self.version = rule_config['version']
        self.threshold = rule_config.get('threshold', 1.0)
        self.severity = rule_config.get('severity', 'ERROR')

    @abstractmethod
    def evaluate(self, df: DataFrame) -> Dict:
        pass

class CompletenessRule(QualityRule):
    def __init__(self, rule_config: Dict):
        super().__init__(rule_config)
        self.columns = rule_config['columns']

    def evaluate(self, df: DataFrame) -> Dict:
        results = {}
        total_rows = df.count()
        
        for column in self.columns:
            null_count = df.filter(col(column).isNull()).count()
            completeness_ratio = 1 - (null_count / total_rows)
            passed = completeness_ratio >= self.threshold
            
            results[column] = {
                'passed': passed,
                'completeness_ratio': completeness_ratio,
                'null_count': null_count,
                'total_rows': total_rows
            }
        
        return results

class UniquenessRule(QualityRule):
    def __init__(self, rule_config: Dict):
        super().__init__(rule_config)
        self.columns = rule_config['columns']

    def evaluate(self, df: DataFrame) -> Dict:
        results = {}
        total_rows = df.count()
        
        for column_set in self.columns:
            if isinstance(column_set, str):
                column_set = [column_set]
                
            distinct_count = df.select(column_set).distinct().count()
            uniqueness_ratio = distinct_count / total_rows
            passed = uniqueness_ratio >= self.threshold
            
            column_key = '_'.join(column_set)
            results[column_key] = {
                'passed': passed,
                'uniqueness_ratio': uniqueness_ratio,
                'duplicate_count': total_rows - distinct_count
            }
        
        return results

class NumericRule(QualityRule):
    def __init__(self, rule_config: Dict):
        super().__init__(rule_config)
        self.columns = rule_config['columns']
        self.min_value = rule_config.get('min_value')
        self.max_value = rule_config.get('max_value')

    def evaluate(self, df: DataFrame) -> Dict:
        results = {}
        
        for column in self.columns:
            stats = df.select(
                min(col(column)).alias('min'),
                max(col(column)).alias('max'),
                avg(col(column)).alias('avg'),
                stddev(col(column)).alias('stddev')
            ).collect()[0]
            
            passed = True
            if self.min_value is not None:
                passed &= stats['min'] >= self.min_value
            if self.max_value is not None:
                passed &= stats['max'] <= self.max_value
                
            results[column] = {
                'passed': passed,
                'statistics': {
                    'min': float(stats['min']),
                    'max': float(stats['max']),
                    'avg': float(stats['avg']),
                    'stddev': float(stats['stddev'])
                }
            }
        
        return results

class AnomalyRule(QualityRule):
    def __init__(self, rule_config: Dict):
        super().__init__(rule_config)
        self.columns = rule_config['columns']
        self.zscore_threshold = rule_config.get('zscore_threshold', 3.0)
        self.lookback_days = rule_config.get('lookback_days', 30)

    def evaluate(self, df: DataFrame) -> Dict:
        results = {}
        
        for column in self.columns:
            # Calculate historical statistics
            historical_stats = df.select(
                avg(col(column)).alias('avg'),
                stddev(col(column)).alias('stddev')
            ).collect()[0]
            
            # Identify anomalies using Z-score
            anomaly_df = df.withColumn(
                'zscore',
                abs((col(column) - historical_stats['avg']) / historical_stats['stddev'])
            )
            
            anomalies = anomaly_df.filter(col('zscore') > self.zscore_threshold)
            anomaly_count = anomalies.count()
            passed = anomaly_count <= (df.count() * (1 - self.threshold))
            
            results[column] = {
                'passed': passed,
                'anomaly_count': anomaly_count,
                'anomaly_samples': anomalies.limit(5).collect()
            }
        
        return results

class ReferentialIntegrityRule(QualityRule):
    def __init__(self, rule_config: Dict):
        super().__init__(rule_config)
        self.source_table = rule_config['source_table']
        self.target_table = rule_config['target_table']
        self.source_columns = rule_config['source_columns']
        self.target_columns = rule_config['target_columns']

    def evaluate(self, source_df: DataFrame, target_df: DataFrame) -> Dict:
        # Count orphaned records
        joined_df = source_df.join(
            target_df,
            [source_df[sc] == target_df[tc] for sc, tc in zip(self.source_columns, self.target_columns)],
            'left_anti'
        )
        
        orphaned_count = joined_df.count()
        total_count = source_df.count()
        integrity_ratio = 1 - (orphaned_count / total_count)
        passed = integrity_ratio >= self.threshold
        
        return {
            'passed': passed,
            'integrity_ratio': integrity_ratio,
            'orphaned_count': orphaned_count,
            'orphaned_samples': joined_df.limit(5).collect()
        }

class QualityRuleEngine:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.rule_types = {
            'completeness': CompletenessRule,
            'uniqueness': UniquenessRule,
            'numeric': NumericRule,
            'anomaly': AnomalyRule,
            'referential_integrity': ReferentialIntegrityRule
        }

    def create_rule(self, rule_config: Dict) -> QualityRule:
        rule_type = rule_config['rule_type']
        if rule_type not in self.rule_types:
            raise ValueError(f"Unsupported rule type: {rule_type}")
        
        return self.rule_types[rule_type](rule_config)

    def evaluate_rules(self, df: DataFrame, rules: List[Dict]) -> Dict:
        results = {
            'execution_time': datetime.now().isoformat(),
            'total_rows': df.count(),
            'rules_evaluated': len(rules),
            'results': {}
        }
        
        for rule_config in rules:
            rule = self.create_rule(rule_config)
            rule_results = rule.evaluate(df)
            
            results['results'][rule.rule_id] = {
                'version': rule.version,
                'type': rule_config['rule_type'],
                'severity': rule.severity,
                'evaluation_results': rule_results
            }
        
        return results

    def save_results(self, results: Dict, table_name: str) -> None:
        # Convert results to DataFrame and save to quality results table
        results_df = self.spark.createDataFrame([{
            'table_name': table_name,
            'execution_time': results['execution_time'],
            'results': json.dumps(results)
        }])
        
        results_df.write.format("delta") \
            .mode("append") \
            .saveAsTable("quality_results")
