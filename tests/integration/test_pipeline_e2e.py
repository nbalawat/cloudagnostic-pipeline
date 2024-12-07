import pytest
import os
from pathlib import Path
import shutil
from pyspark.sql import SparkSession
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List

from data_generators.sample_data_generator import TestDataGenerator
from quality.anomaly_detection import (
    AnomalyDetectionOrchestrator,
    AnomalyConfig
)
from messaging.event_publisher import (
    EventPublisher,
    KafkaConfig,
    PipelineEvent,
    EventType
)
from processor.data_processor import DataProcessor
from workflow.dag_generator import DAGGenerator

class TestE2EPipeline:
    @pytest.fixture(scope="class")
    def spark(self):
        spark = SparkSession.builder \
            .appName("PipelineTest") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture(scope="class")
    def test_data_path(self):
        path = Path("/tmp/pipeline_test_data")
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True)
        yield str(path)
        shutil.rmtree(path)

    @pytest.fixture(scope="class")
    def kafka_config(self):
        return KafkaConfig({
            'bootstrap_servers': 'localhost:9092',
            'security_protocol': 'PLAINTEXT',
            'pipeline_topic': 'test_pipeline_events',
            'quality_topic': 'test_quality_events',
            'alert_topic': 'test_pipeline_alerts'
        })

    @pytest.fixture(scope="class")
    def kafka_admin(self, kafka_config):
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_config.bootstrap_servers,
            security_protocol=kafka_config.security_protocol
        )
        
        # Create test topics
        topics = [
            NewTopic(name=kafka_config.pipeline_topic, num_partitions=1, replication_factor=1),
            NewTopic(name=kafka_config.quality_topic, num_partitions=1, replication_factor=1),
            NewTopic(name=kafka_config.alert_topic, num_partitions=1, replication_factor=1)
        ]
        
        try:
            admin_client.create_topics(topics)
        except Exception as e:
            print(f"Topics might already exist: {e}")
        
        yield admin_client
        
        # Cleanup topics
        admin_client.delete_topics([t.name for t in topics])
        admin_client.close()

    @pytest.fixture(scope="class")
    def test_data(self, test_data_path):
        generator = TestDataGenerator(test_data_path)
        generator.generate_all_data()
        return generator

    def test_data_generation(self, test_data):
        """Verify test data generation"""
        assert test_data.customers is not None
        assert test_data.transactions is not None
        assert test_data.products is not None
        assert test_data.orders is not None
        assert test_data.quality_metrics is not None
        
        # Check for intentional anomalies
        assert (test_data.customers['credit_score'] < 500).sum() > 0
        assert (test_data.transactions['processing_time'] > 400).sum() > 0

    def test_file_format_compatibility(self, spark, test_data_path):
        """Test reading different file formats"""
        formats = ['csv', 'json', 'parquet', 'delta']
        domains = ['finance', 'sales', 'quality']
        
        for fmt in formats:
            for domain in domains:
                path = Path(test_data_path) / 'landing' / domain
                if not path.exists():
                    continue
                    
                files = list(path.glob(f'*.{fmt}'))
                for file_path in files:
                    df = spark.read.format(fmt).load(str(file_path))
                    assert df.count() > 0

    def test_anomaly_detection(self, spark, test_data_path):
        """Test anomaly detection across different patterns"""
        # Load test data
        transactions_df = spark.read.parquet(
            str(Path(test_data_path) / 'landing' / 'finance' / 'transactions.parquet')
        )
        
        config = AnomalyConfig(
            detection_method='zscore',
            lookback_window=7,
            threshold=3.0,
            min_samples=100
        )
        
        orchestrator = AnomalyDetectionOrchestrator(spark)
        results = orchestrator.detect_all_anomalies(transactions_df, config)
        
        # Verify detection results
        assert 'volume' in results
        assert 'processing_time' in results
        assert 'error_rate' in results
        
        # Check for detected anomalies
        summary = orchestrator.get_anomaly_summary(results)
        assert any(s['anomaly_count'] > 0 for s in summary.values())

    def test_kafka_integration(self, kafka_config, kafka_admin):
        """Test Kafka event publishing and consumption"""
        publisher = EventPublisher(kafka_config)
        
        # Publish test events
        test_events = [
            PipelineEvent(
                event_type=EventType.PIPELINE_START,
                pipeline_id="test_pipeline_1",
                table_name="customers",
                layer="bronze",
                timestamp=datetime.now().isoformat(),
                details={"status": "starting"},
                severity="INFO"
            ),
            PipelineEvent(
                event_type=EventType.QUALITY_CHECK_FAILURE,
                pipeline_id="test_pipeline_1",
                table_name="transactions",
                layer="silver",
                timestamp=datetime.now().isoformat(),
                details={"error": "Data quality threshold breach"},
                severity="ERROR"
            )
        ]
        
        for event in test_events:
            publisher.publish_event(event)
        
        # Verify event consumption
        consumer = KafkaConsumer(
            kafka_config.pipeline_topic,
            bootstrap_servers=kafka_config.bootstrap_servers,
            security_protocol=kafka_config.security_protocol,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        messages = []
        start_time = time.time()
        while len(messages) < len(test_events) and time.time() - start_time < 10:
            for msg in consumer:
                messages.append(msg.value)
                if len(messages) >= len(test_events):
                    break
        
        assert len(messages) == len(test_events)
        consumer.close()

    def test_end_to_end_pipeline(self, spark, test_data_path, kafka_config):
        """Test complete pipeline execution"""
        # 1. Setup pipeline configuration
        config_path = Path(test_data_path) / "config"
        config_path.mkdir(exist_ok=True)
        
        # 2. Generate DAG
        dag_generator = DAGGenerator(str(config_path / "test_pipeline.yaml"))
        dag = dag_generator.generate_dag()
        
        # 3. Process data through layers
        processor = DataProcessor(spark)
        
        # Process Bronze layer
        bronze_df = processor.process_bronze_layer(
            source_path=str(Path(test_data_path) / "landing" / "finance" / "transactions.parquet"),
            target_path=str(Path(test_data_path) / "bronze" / "finance" / "transactions"),
            format="delta"
        )
        
        # Process Silver layer
        silver_df = processor.process_silver_layer(
            source_path=str(Path(test_data_path) / "bronze" / "finance" / "transactions"),
            target_path=str(Path(test_data_path) / "silver" / "finance" / "transactions"),
            format="delta"
        )
        
        # Process Gold layer
        gold_df = processor.process_gold_layer(
            source_path=str(Path(test_data_path) / "silver" / "finance" / "transactions"),
            target_path=str(Path(test_data_path) / "gold" / "finance" / "transactions_summary"),
            format="delta"
        )
        
        # 4. Verify data quality
        config = AnomalyConfig(
            detection_method='zscore',
            lookback_window=7,
            threshold=3.0
        )
        
        orchestrator = AnomalyDetectionOrchestrator(spark)
        quality_results = orchestrator.detect_all_anomalies(gold_df, config)
        
        # 5. Verify results
        assert bronze_df.count() > 0
        assert silver_df.count() > 0
        assert gold_df.count() > 0
        assert quality_results is not None

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
