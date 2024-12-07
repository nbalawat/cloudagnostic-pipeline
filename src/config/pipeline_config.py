from dataclasses import dataclass
from typing import List, Optional
from enum import Enum

class StorageFormat(Enum):
    DELTA = "delta"
    ICEBERG = "iceberg"
    HUDI = "hudi"

class InputFormat(Enum):
    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"
    ORC = "orc"

@dataclass
class PipelineConfig:
    pipeline_name: str
    landing_location: str
    target_location: str
    input_format: InputFormat
    storage_format: StorageFormat
    kafka_input_topic: Optional[str] = None
    kafka_output_topic: Optional[str] = None
    encrypted_columns: List[str] = None

    def validate(self) -> bool:
        """Validate pipeline configuration"""
        if not all([self.pipeline_name, self.landing_location, self.target_location]):
            return False
        if self.input_format not in InputFormat:
            return False
        if self.storage_format not in StorageFormat:
            return False
        return True

class DatabaseConfig:
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
