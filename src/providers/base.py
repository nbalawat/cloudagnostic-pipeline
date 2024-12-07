from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime

class StorageProvider(ABC):
    @abstractmethod
    async def read_file(self, path: str) -> bytes:
        pass
    
    @abstractmethod
    async def write_file(self, path: str, content: bytes) -> bool:
        pass
    
    @abstractmethod
    async def list_files(self, prefix: str) -> List[str]:
        pass
    
    @abstractmethod
    async def delete_file(self, path: str) -> bool:
        pass

class OrchestratorProvider(ABC):
    @abstractmethod
    async def create_job(self, job_config: Dict[str, Any]) -> str:
        pass
    
    @abstractmethod
    async def start_job(self, job_id: str) -> bool:
        pass
    
    @abstractmethod
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        pass

class MonitoringProvider(ABC):
    @abstractmethod
    async def log_metric(self, metric_name: str, value: float, tags: Dict[str, str]) -> bool:
        pass
    
    @abstractmethod
    async def create_alert(self, alert_config: Dict[str, Any]) -> str:
        pass
    
    @abstractmethod
    async def get_metrics(self, metric_name: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        pass

class EncryptionProvider(ABC):
    @abstractmethod
    async def encrypt_data(self, data: bytes, key_id: str) -> bytes:
        pass
    
    @abstractmethod
    async def decrypt_data(self, encrypted_data: bytes, key_id: str) -> bytes:
        pass
    
    @abstractmethod
    async def rotate_key(self, old_key_id: str) -> str:
        pass
    
    @abstractmethod
    async def get_key_metadata(self, key_id: str) -> Dict[str, Any]:
        pass
