from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from typing import Dict, List, Optional
import json
from datetime import datetime
import logging
from enum import Enum
from dataclasses import dataclass
import threading

class EventType(Enum):
    PIPELINE_START = "pipeline_start"
    PIPELINE_COMPLETE = "pipeline_complete"
    PIPELINE_ERROR = "pipeline_error"
    QUALITY_CHECK_START = "quality_check_start"
    QUALITY_CHECK_COMPLETE = "quality_check_complete"
    QUALITY_CHECK_FAILURE = "quality_check_failure"
    DATA_ANOMALY = "data_anomaly"
    THRESHOLD_BREACH = "threshold_breach"

@dataclass
class PipelineEvent:
    event_type: EventType
    pipeline_id: str
    table_name: str
    layer: str
    timestamp: str
    details: Dict
    severity: str = "INFO"

class KafkaConfig:
    def __init__(self, config: Dict):
        self.bootstrap_servers = config['bootstrap_servers']
        self.security_protocol = config.get('security_protocol', 'PLAINTEXT')
        self.sasl_mechanism = config.get('sasl_mechanism')
        self.sasl_username = config.get('sasl_username')
        self.sasl_password = config.get('sasl_password')
        self.ssl_cafile = config.get('ssl_cafile')
        
        # Topic configurations
        self.pipeline_topic = config.get('pipeline_topic', 'pipeline_events')
        self.quality_topic = config.get('quality_topic', 'quality_events')
        self.alert_topic = config.get('alert_topic', 'pipeline_alerts')

class EventPublisher:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = self._create_producer()
        self.admin_client = self._create_admin_client()
        self._ensure_topics_exist()
        
        # Start alert consumer
        self.alert_consumer = AlertConsumer(config)
        self.alert_consumer.start()

    def _create_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            security_protocol=self.config.security_protocol,
            sasl_mechanism=self.config.sasl_mechanism,
            sasl_plain_username=self.config.sasl_username,
            sasl_plain_password=self.config.sasl_password,
            ssl_cafile=self.config.ssl_cafile,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def _create_admin_client(self) -> KafkaAdminClient:
        return KafkaAdminClient(
            bootstrap_servers=self.config.bootstrap_servers,
            security_protocol=self.config.security_protocol,
            sasl_mechanism=self.config.sasl_mechanism,
            sasl_plain_username=self.config.sasl_username,
            sasl_plain_password=self.config.sasl_password,
            ssl_cafile=self.config.ssl_cafile
        )

    def _ensure_topics_exist(self):
        existing_topics = self.admin_client.list_topics()
        required_topics = [
            self.config.pipeline_topic,
            self.config.quality_topic,
            self.config.alert_topic
        ]
        
        topics_to_create = []
        for topic in required_topics:
            if topic not in existing_topics:
                topics_to_create.append(NewTopic(
                    name=topic,
                    num_partitions=3,
                    replication_factor=3
                ))
        
        if topics_to_create:
            self.admin_client.create_topics(topics_to_create)

    def publish_event(self, event: PipelineEvent):
        topic = self._get_topic_for_event(event.event_type)
        
        event_data = {
            'event_type': event.event_type.value,
            'pipeline_id': event.pipeline_id,
            'table_name': event.table_name,
            'layer': event.layer,
            'timestamp': event.timestamp,
            'details': event.details,
            'severity': event.severity
        }
        
        future = self.producer.send(topic, value=event_data)
        try:
            future.get(timeout=10)
            logging.info(f"Published event: {event_data}")
        except Exception as e:
            logging.error(f"Error publishing event: {str(e)}")
            raise

    def _get_topic_for_event(self, event_type: EventType) -> str:
        if event_type in [EventType.QUALITY_CHECK_START, 
                         EventType.QUALITY_CHECK_COMPLETE,
                         EventType.QUALITY_CHECK_FAILURE]:
            return self.config.quality_topic
        elif event_type in [EventType.DATA_ANOMALY, 
                          EventType.THRESHOLD_BREACH]:
            return self.config.alert_topic
        else:
            return self.config.pipeline_topic

class AlertConsumer(threading.Thread):
    def __init__(self, config: KafkaConfig):
        super().__init__()
        self.config = config
        self.consumer = self._create_consumer()
        self.running = True

    def _create_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.config.alert_topic,
            bootstrap_servers=self.config.bootstrap_servers,
            security_protocol=self.config.security_protocol,
            sasl_mechanism=self.config.sasl_mechanism,
            sasl_plain_username=self.config.sasl_username,
            sasl_plain_password=self.config.sasl_password,
            ssl_cafile=self.config.ssl_cafile,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='pipeline_alert_group',
            auto_offset_reset='latest'
        )

    def run(self):
        while self.running:
            try:
                for message in self.consumer:
                    self._handle_alert(message.value)
            except Exception as e:
                logging.error(f"Error processing alerts: {str(e)}")
                continue

    def _handle_alert(self, alert: Dict):
        severity = alert.get('severity', 'INFO')
        if severity in ['ERROR', 'CRITICAL']:
            self._send_notification(alert)
        self._store_alert(alert)

    def _send_notification(self, alert: Dict):
        # Implement notification logic (email, Slack, etc.)
        logging.critical(f"ALERT: {alert}")

    def _store_alert(self, alert: Dict):
        # Store alert in database for tracking
        pass

    def stop(self):
        self.running = False
        self.consumer.close()

class MetricsCollector:
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.metrics = {}

    def record_pipeline_metrics(self, pipeline_id: str, metrics: Dict):
        self.metrics[pipeline_id] = metrics
        
        if self._should_raise_alert(metrics):
            self.publisher.publish_event(PipelineEvent(
                event_type=EventType.THRESHOLD_BREACH,
                pipeline_id=pipeline_id,
                table_name=metrics.get('table_name', ''),
                layer=metrics.get('layer', ''),
                timestamp=datetime.now().isoformat(),
                details=metrics,
                severity="ERROR"
            ))

    def _should_raise_alert(self, metrics: Dict) -> bool:
        # Implement alert threshold logic
        return metrics.get('error_count', 0) > 0 or \
               metrics.get('quality_score', 1.0) < 0.9
