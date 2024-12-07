from typing import Dict, List, Optional
from datetime import datetime
import json

from ..providers.factory import ProviderFactory
from ..providers.base import StorageProvider, OrchestratorProvider, MonitoringProvider, EncryptionProvider

class PipelineService:
    def __init__(self, config: Dict[str, any]):
        self.config = config
        self.cloud_provider = config['cloud_provider']
        
        # Initialize providers
        self.storage = ProviderFactory.get_storage_provider(
            self.cloud_provider,
            config.get('storage', {})
        )
        self.orchestrator = ProviderFactory.get_orchestrator_provider(
            self.cloud_provider,
            config.get('orchestrator', {})
        )
        self.monitoring = ProviderFactory.get_monitoring_provider(
            self.cloud_provider,
            config.get('monitoring', {})
        )
        self.encryption = ProviderFactory.get_encryption_provider(
            self.cloud_provider,
            config.get('encryption', {})
        )
    
    async def execute_pipeline(self, pipeline_config: Dict[str, any]) -> str:
        """Execute a data pipeline with the given configuration."""
        try:
            # Create job configuration
            job_config = {
                'name': f"pipeline-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
                'command': ['python', 'pipeline_runner.py'],
                'environment': {
                    'PIPELINE_CONFIG': json.dumps(pipeline_config),
                    'CLOUD_PROVIDER': self.cloud_provider
                }
            }
            
            # Create and start job
            job_id = await self.orchestrator.create_job(job_config)
            await self.orchestrator.start_job(job_id)
            
            # Log pipeline start
            await self.monitoring.log_metric(
                'pipeline_start',
                1.0,
                {
                    'pipeline_id': pipeline_config['id'],
                    'job_id': job_id
                }
            )
            
            return job_id
            
        except Exception as e:
            await self.monitoring.log_metric(
                'pipeline_error',
                1.0,
                {
                    'pipeline_id': pipeline_config['id'],
                    'error': str(e)
                }
            )
            raise
    
    async def get_pipeline_status(self, job_id: str) -> Dict[str, any]:
        """Get the status of a running pipeline."""
        try:
            status = await self.orchestrator.get_job_status(job_id)
            
            # Log status check
            await self.monitoring.log_metric(
                'pipeline_status_check',
                1.0,
                {
                    'job_id': job_id,
                    'status': status['status']
                }
            )
            
            return status
            
        except Exception as e:
            await self.monitoring.log_metric(
                'pipeline_status_error',
                1.0,
                {
                    'job_id': job_id,
                    'error': str(e)
                }
            )
            raise
    
    async def store_pipeline_data(self, data: bytes, path: str, encrypt: bool = True) -> bool:
        """Store pipeline data with optional encryption."""
        try:
            if encrypt:
                # Get encryption key
                key_id = self.config['encryption']['key_id']
                
                # Encrypt data
                encrypted_data = await self.encryption.encrypt_data(data, key_id)
                
                # Store encrypted data
                success = await self.storage.write_file(path, encrypted_data)
            else:
                # Store unencrypted data
                success = await self.storage.write_file(path, data)
            
            # Log data storage
            await self.monitoring.log_metric(
                'pipeline_data_store',
                1.0,
                {
                    'path': path,
                    'encrypted': str(encrypt),
                    'size': len(data)
                }
            )
            
            return success
            
        except Exception as e:
            await self.monitoring.log_metric(
                'pipeline_data_store_error',
                1.0,
                {
                    'path': path,
                    'error': str(e)
                }
            )
            raise
    
    async def read_pipeline_data(self, path: str, decrypt: bool = True) -> bytes:
        """Read pipeline data with optional decryption."""
        try:
            # Read data from storage
            data = await self.storage.read_file(path)
            
            if decrypt:
                # Get encryption key
                key_id = self.config['encryption']['key_id']
                
                # Decrypt data
                decrypted_data = await self.encryption.decrypt_data(data, key_id)
                data = decrypted_data
            
            # Log data read
            await self.monitoring.log_metric(
                'pipeline_data_read',
                1.0,
                {
                    'path': path,
                    'encrypted': str(decrypt),
                    'size': len(data)
                }
            )
            
            return data
            
        except Exception as e:
            await self.monitoring.log_metric(
                'pipeline_data_read_error',
                1.0,
                {
                    'path': path,
                    'error': str(e)
                }
            )
            raise
