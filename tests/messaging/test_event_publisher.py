import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.messaging.event_publisher import EventPublisher

@pytest.fixture
def mock_kafka_producer():
    with patch('kafka.KafkaProducer') as mock:
        producer = Mock()
        mock.return_value = producer
        yield producer

@pytest.fixture
def event_publisher(mock_kafka_producer):
    return EventPublisher(
        bootstrap_servers='localhost:9092',
        topic_prefix='test'
    )

class TestEventPublisher:
    def test_publish_event_success(self, event_publisher, mock_kafka_producer):
        # Test event data
        event_data = {
            'pipeline_id': 'test-pipeline',
            'step_id': 'step1',
            'status': 'success',
            'timestamp': datetime.utcnow().isoformat()
        }

        # Publish event
        event_publisher.publish_event('pipeline.step.completed', event_data)

        # Verify Kafka producer was called
        mock_kafka_producer.send.assert_called_once()
        args = mock_kafka_producer.send.call_args
        assert args[0][0] == 'test.pipeline.step.completed'  # Topic
        assert b'test-pipeline' in args[0][1]  # Message contains pipeline ID
        assert b'success' in args[0][1]  # Message contains status

    def test_publish_event_with_key(self, event_publisher, mock_kafka_producer):
        event_data = {
            'pipeline_id': 'test-pipeline',
            'error': 'Test error',
            'timestamp': datetime.utcnow().isoformat()
        }

        # Publish event with key
        event_publisher.publish_event(
            'pipeline.error',
            event_data,
            key='test-pipeline'
        )

        # Verify Kafka producer was called with key
        mock_kafka_producer.send.assert_called_once()
        args = mock_kafka_producer.send.call_args
        assert args[1]['key'] == b'test-pipeline'

    def test_publish_event_batch(self, event_publisher, mock_kafka_producer):
        # Create batch of events
        events = [
            {
                'type': 'pipeline.step.completed',
                'data': {
                    'pipeline_id': 'test-pipeline',
                    'step_id': f'step{i}',
                    'status': 'success',
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            for i in range(3)
        ]

        # Publish batch
        event_publisher.publish_event_batch(events)

        # Verify all events were published
        assert mock_kafka_producer.send.call_count == 3

    def test_publish_event_error_handling(self, event_publisher, mock_kafka_producer):
        # Setup producer to simulate error
        mock_kafka_producer.send.side_effect = Exception('Kafka error')

        # Test event data
        event_data = {
            'pipeline_id': 'test-pipeline',
            'error': 'Test error'
        }

        # Verify error is caught and logged
        with pytest.raises(Exception) as exc_info:
            event_publisher.publish_event('pipeline.error', event_data)
        assert 'Failed to publish event' in str(exc_info.value)

    def test_close_publisher(self, event_publisher, mock_kafka_producer):
        # Close publisher
        event_publisher.close()

        # Verify producer was flushed and closed
        mock_kafka_producer.flush.assert_called_once()
        mock_kafka_producer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_publish_event(self, event_publisher, mock_kafka_producer):
        # Test event data
        event_data = {
            'pipeline_id': 'test-pipeline',
            'metric': 'records_processed',
            'value': 1000,
            'timestamp': datetime.utcnow().isoformat()
        }

        # Publish event asynchronously
        await event_publisher.async_publish_event('pipeline.metric', event_data)

        # Verify Kafka producer was called
        mock_kafka_producer.send.assert_called_once()
        args = mock_kafka_producer.send.call_args
        assert args[0][0] == 'test.pipeline.metric'
        assert b'records_processed' in args[0][1]

    @pytest.mark.asyncio
    async def test_async_publish_event_batch(self, event_publisher, mock_kafka_producer):
        # Create batch of metric events
        events = [
            {
                'type': 'pipeline.metric',
                'data': {
                    'pipeline_id': 'test-pipeline',
                    'metric': f'metric{i}',
                    'value': i * 100,
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            for i in range(3)
        ]

        # Publish batch asynchronously
        await event_publisher.async_publish_event_batch(events)

        # Verify all events were published
        assert mock_kafka_producer.send.call_count == 3
