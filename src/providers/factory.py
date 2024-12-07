from typing import Dict, Optional, Type
from .base import StorageProvider, OrchestratorProvider, MonitoringProvider, EncryptionProvider
from .aws import AWSStorageProvider, AWSOrchestratorProvider, AWSMonitoringProvider, AWSEncryptionProvider
from .gcp import GCPStorageProvider, GCPOrchestratorProvider, GCPMonitoringProvider, GCPEncryptionProvider
from .azure import AzureStorageProvider, AzureOrchestratorProvider, AzureMonitoringProvider, AzureEncryptionProvider

class ProviderFactory:
    _storage_providers: Dict[str, Type[StorageProvider]] = {
        'aws': AWSStorageProvider,
        'gcp': GCPStorageProvider,
        'azure': AzureStorageProvider
    }
    
    _orchestrator_providers: Dict[str, Type[OrchestratorProvider]] = {
        'aws': AWSOrchestratorProvider,
        'gcp': GCPOrchestratorProvider,
        'azure': AzureOrchestratorProvider
    }
    
    _monitoring_providers: Dict[str, Type[MonitoringProvider]] = {
        'aws': AWSMonitoringProvider,
        'gcp': GCPMonitoringProvider,
        'azure': AzureMonitoringProvider
    }
    
    _encryption_providers: Dict[str, Type[EncryptionProvider]] = {
        'aws': AWSEncryptionProvider,
        'gcp': GCPEncryptionProvider,
        'azure': AzureEncryptionProvider
    }
    
    @classmethod
    def get_storage_provider(cls, provider: str, config: Optional[Dict] = None) -> StorageProvider:
        if provider not in cls._storage_providers:
            raise ValueError(f"Unsupported storage provider: {provider}")
        return cls._storage_providers[provider](config or {})
    
    @classmethod
    def get_orchestrator_provider(cls, provider: str, config: Optional[Dict] = None) -> OrchestratorProvider:
        if provider not in cls._orchestrator_providers:
            raise ValueError(f"Unsupported orchestrator provider: {provider}")
        return cls._orchestrator_providers[provider](config or {})
    
    @classmethod
    def get_monitoring_provider(cls, provider: str, config: Optional[Dict] = None) -> MonitoringProvider:
        if provider not in cls._monitoring_providers:
            raise ValueError(f"Unsupported monitoring provider: {provider}")
        return cls._monitoring_providers[provider](config or {})
    
    @classmethod
    def get_encryption_provider(cls, provider: str, config: Optional[Dict] = None) -> EncryptionProvider:
        if provider not in cls._encryption_providers:
            raise ValueError(f"Unsupported encryption provider: {provider}")
        return cls._encryption_providers[provider](config or {})
