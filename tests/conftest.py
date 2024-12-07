import pytest
from datetime import datetime
from typing import Dict, Any

import boto3
from moto import mock_s3, mock_cloudwatch, mock_kms
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.db.models import Base
from src.providers.aws import AWSStorageProvider, AWSMonitoringProvider, AWSEncryptionProvider

@pytest.fixture(scope="session")
def engine():
    """Create a test database engine."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return engine

@pytest.fixture
def db_session(engine):
    """Create a new database session for a test."""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.rollback()
        session.close()

@pytest.fixture
def aws_credentials():
    """Mock AWS Credentials for moto."""
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture
def s3(aws_credentials):
    """Create a mock S3 service."""
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")

@pytest.fixture
def cloudwatch(aws_credentials):
    """Create a mock CloudWatch service."""
    with mock_cloudwatch():
        yield boto3.client("cloudwatch", region_name="us-east-1")

@pytest.fixture
def kms(aws_credentials):
    """Create a mock KMS service."""
    with mock_kms():
        yield boto3.client("kms", region_name="us-east-1")

@pytest.fixture
def test_bucket(s3):
    """Create a test S3 bucket."""
    bucket_name = "test-bucket"
    s3.create_bucket(Bucket=bucket_name)
    return bucket_name

@pytest.fixture
def storage_provider(test_bucket):
    """Create a test storage provider."""
    return AWSStorageProvider({"bucket": test_bucket})

@pytest.fixture
def monitoring_provider():
    """Create a test monitoring provider."""
    return AWSMonitoringProvider({"namespace": "TestNamespace"})

@pytest.fixture
def encryption_provider(kms):
    """Create a test encryption provider."""
    return AWSEncryptionProvider({"key_alias": "alias/test-key"})

@pytest.fixture
def sample_pipeline_config() -> Dict[str, Any]:
    """Create a sample pipeline configuration."""
    return {
        "id": "test-pipeline",
        "name": "Test Pipeline",
        "version": "1.0.0",
        "steps": [
            {
                "id": "step1",
                "type": "extract",
                "source": {
                    "type": "s3",
                    "path": "input/data.csv"
                }
            },
            {
                "id": "step2",
                "type": "transform",
                "sql": "SELECT * FROM step1 WHERE value > 0"
            },
            {
                "id": "step3",
                "type": "load",
                "target": {
                    "type": "s3",
                    "path": "output/result.parquet"
                }
            }
        ],
        "quality_rules": [
            {
                "id": "rule1",
                "type": "null_check",
                "columns": ["id", "value"],
                "threshold": 0.99
            }
        ]
    }

@pytest.fixture
def sample_quality_result() -> Dict[str, Any]:
    """Create a sample quality check result."""
    return {
        "rule_id": "rule1",
        "passed": True,
        "metrics": {
            "total_records": 1000,
            "failed_records": 5,
            "pass_rate": 0.995
        },
        "timestamp": datetime.utcnow().isoformat()
    }

@pytest.fixture
def sample_pipeline_run() -> Dict[str, Any]:
    """Create a sample pipeline run configuration."""
    return {
        "id": "run-123",
        "pipeline_id": "test-pipeline",
        "status": "running",
        "start_time": datetime.utcnow().isoformat(),
        "metrics": {
            "records_processed": 0,
            "bytes_processed": 0
        }
    }
