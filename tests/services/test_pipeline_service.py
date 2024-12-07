import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.services.pipeline_service import PipelineService
from src.providers.base import (
    StorageProvider,
    OrchestratorProvider,
    MonitoringProvider,
    EncryptionProvider
)

class MockStorageProvider(StorageProvider):
    async def read_file(self, path: str) -> bytes:
        return b'test data'
    
    async def write_file(self, path: str, content: bytes) -> bool:
        return True
    
    async def list_files(self, prefix: str) -> list:
        return ['test1.txt', 'test2.txt']
    
    async def delete_file(self, path: str) -> bool:
        return True

class MockOrchestratorProvider(OrchestratorProvider):
    async def create_job(self, job_config: dict) -> str:
        return 'job-123'
    
    async def start_job(self, job_id: str) -> bool:
        return True
    
    async def get_job_status(self, job_id: str) -> dict:
        return {
            'status': 'running',
            'start_time': datetime.utcnow()
        }
    
    async def cancel_job(self, job_id: str) -> bool:
        return True

class MockMonitoringProvider(MonitoringProvider):
    async def log_metric(self, metric_name: str, value: float, tags: dict) -> bool:
        return True
    
    async def create_alert(self, alert_config: dict) -> str:
        return 'alert-123'
    
    async def get_metrics(self, metric_name: str, start_time: datetime, end_time: datetime) -> list:
        return [{'timestamp': datetime.utcnow(), 'value': 1.0}]

class MockEncryptionProvider(EncryptionProvider):
    async def encrypt_data(self, data: bytes, key_id: str) -> bytes:
        return b'encrypted'
    
    async def decrypt_data(self, encrypted_data: bytes, key_id: str) -> bytes:
        return b'decrypted'
    
    async def rotate_key(self, old_key_id: str) -> str:
        return 'new-key-123'
    
    async def get_key_metadata(self, key_id: str) -> dict:
        return {
            'id': key_id,
            'status': 'active'
        }

@pytest.fixture
def mock_providers():
    with patch('src.providers.factory.ProviderFactory') as factory_mock:
        factory_mock.get_storage_provider.return_value = MockStorageProvider({})
        factory_mock.get_orchestrator_provider.return_value = MockOrchestratorProvider({})
        factory_mock.get_monitoring_provider.return_value = MockMonitoringProvider({})
        factory_mock.get_encryption_provider.return_value = MockEncryptionProvider({})
        yield factory_mock

@pytest.fixture
def pipeline_service(mock_providers):
    config = {
        'cloud_provider': 'aws',
        'storage': {'bucket': 'test-bucket'},
        'orchestrator': {'job_queue': 'test-queue'},
        'monitoring': {'namespace': 'test'},
        'encryption': {'key_id': 'test-key'}
    }
    return PipelineService(config)

class TestPipelineService:
    @pytest.mark.asyncio
    async def test_execute_pipeline_success(self, pipeline_service):
        pipeline_config = {
            'id': 'test-pipeline',
            'steps': [
                {
                    'name': 'step1',
                    'type': 'transform',
                    'config': {}
                }
            ]
        }
        job_id = await pipeline_service.execute_pipeline(pipeline_config)
        assert job_id == 'job-123'

    @pytest.mark.asyncio
    async def test_get_pipeline_status_success(self, pipeline_service):
        status = await pipeline_service.get_pipeline_status('job-123')
        assert status['status'] == 'running'

    @pytest.mark.asyncio
    async def test_store_pipeline_data_success(self, pipeline_service):
        result = await pipeline_service.store_pipeline_data(
            b'test data',
            'test.txt',
            encrypt=True
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_read_pipeline_data_success(self, pipeline_service):
        data = await pipeline_service.read_pipeline_data(
            'test.txt',
            decrypt=True
        )
        assert data == b'decrypted'

    @pytest.mark.asyncio
    async def test_execute_pipeline_error_monitoring(self, pipeline_service):
        pipeline_config = {
            'id': 'test-pipeline',
            'steps': []
        }
        with patch.object(
            pipeline_service.orchestrator,
            'create_job',
            side_effect=Exception('Test error')
        ):
            with pytest.raises(Exception):
                await pipeline_service.execute_pipeline(pipeline_config)

    @pytest.mark.asyncio
    async def test_store_pipeline_data_encryption_error(self, pipeline_service):
        with patch.object(
            pipeline_service.encryption,
            'encrypt_data',
            side_effect=Exception('Encryption error')
        ):
            with pytest.raises(Exception):
                await pipeline_service.store_pipeline_data(
                    b'test data',
                    'test.txt',
                    encrypt=True
                )
