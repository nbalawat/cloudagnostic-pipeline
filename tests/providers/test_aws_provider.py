import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from botocore.exceptions import ClientError

from src.providers.aws import (
    AWSStorageProvider,
    AWSOrchestratorProvider,
    AWSMonitoringProvider,
    AWSEncryptionProvider
)

@pytest.fixture
def s3_mock():
    with patch('boto3.client') as mock:
        yield mock.return_value

@pytest.fixture
def batch_mock():
    with patch('boto3.client') as mock:
        yield mock.return_value

@pytest.fixture
def cloudwatch_mock():
    with patch('boto3.client') as mock:
        yield mock.return_value

@pytest.fixture
def kms_mock():
    with patch('boto3.client') as mock:
        yield mock.return_value

class TestAWSStorageProvider:
    @pytest.fixture
    def provider(self, s3_mock):
        return AWSStorageProvider({'bucket': 'test-bucket'})

    @pytest.mark.asyncio
    async def test_read_file_success(self, provider, s3_mock):
        s3_mock.get_object.return_value = {
            'Body': Mock(read=lambda: b'test data')
        }
        result = await provider.read_file('test.txt')
        assert result == b'test data'
        s3_mock.get_object.assert_called_with(
            Bucket='test-bucket',
            Key='test.txt'
        )

    @pytest.mark.asyncio
    async def test_read_file_error(self, provider, s3_mock):
        s3_mock.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}},
            'GetObject'
        )
        with pytest.raises(Exception):
            await provider.read_file('test.txt')

class TestAWSOrchestratorProvider:
    @pytest.fixture
    def provider(self, batch_mock):
        return AWSOrchestratorProvider({
            'job_queue': 'test-queue',
            'job_definition': 'test-job-def'
        })

    @pytest.mark.asyncio
    async def test_create_job_success(self, provider, batch_mock):
        batch_mock.submit_job.return_value = {'jobId': 'job-123'}
        job_config = {
            'name': 'test-job',
            'command': ['python', 'test.py'],
            'environment': {'ENV': 'test'}
        }
        result = await provider.create_job(job_config)
        assert result == 'job-123'
        batch_mock.submit_job.assert_called_once()

class TestAWSMonitoringProvider:
    @pytest.fixture
    def provider(self, cloudwatch_mock):
        return AWSMonitoringProvider({'namespace': 'TestNamespace'})

    @pytest.mark.asyncio
    async def test_log_metric_success(self, provider, cloudwatch_mock):
        result = await provider.log_metric(
            'test_metric',
            1.0,
            {'tag': 'value'}
        )
        assert result is True
        cloudwatch_mock.put_metric_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_alert_success(self, provider, cloudwatch_mock):
        alert_config = {
            'name': 'test-alert',
            'metric_name': 'test_metric',
            'threshold': 1.0,
            'operator': 'GreaterThanThreshold'
        }
        result = await provider.create_alert(alert_config)
        assert result == 'test-alert'
        cloudwatch_mock.put_metric_alarm.assert_called_once()

class TestAWSEncryptionProvider:
    @pytest.fixture
    def provider(self, kms_mock):
        return AWSEncryptionProvider({'key_alias': 'alias/test-key'})

    @pytest.mark.asyncio
    async def test_encrypt_data_success(self, provider, kms_mock):
        kms_mock.encrypt.return_value = {
            'CiphertextBlob': b'encrypted'
        }
        result = await provider.encrypt_data(b'test', 'key-123')
        assert result == b'encrypted'
        kms_mock.encrypt.assert_called_with(
            KeyId='key-123',
            Plaintext=b'test'
        )

    @pytest.mark.asyncio
    async def test_decrypt_data_success(self, provider, kms_mock):
        kms_mock.decrypt.return_value = {
            'Plaintext': b'decrypted'
        }
        result = await provider.decrypt_data(b'encrypted', 'key-123')
        assert result == b'decrypted'
        kms_mock.decrypt.assert_called_with(
            KeyId='key-123',
            CiphertextBlob=b'encrypted'
        )
