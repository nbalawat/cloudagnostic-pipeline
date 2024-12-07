import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from src.processor.data_processor import DataProcessor
from src.messaging.event_publisher import EventPublisher

@pytest.fixture
def mock_event_publisher():
    return Mock(spec=EventPublisher)

@pytest.fixture
def data_processor(storage_provider, mock_event_publisher):
    return DataProcessor(
        storage_provider=storage_provider,
        event_publisher=mock_event_publisher
    )

class TestDataProcessor:
    @pytest.mark.asyncio
    async def test_process_data_success(self, data_processor, sample_pipeline_config, test_bucket, s3):
        # Setup test data
        test_data = b"id,value\n1,100\n2,200\n3,300\n"
        s3.put_object(
            Bucket=test_bucket,
            Key="input/data.csv",
            Body=test_data
        )

        # Process data
        result = await data_processor.process_data(
            pipeline_config=sample_pipeline_config,
            step_id="step1"
        )

        # Verify results
        assert result["success"] is True
        assert result["records_processed"] == 3
        assert result["quality_checks"]["rule1"]["passed"] is True

        # Verify events were published
        data_processor.event_publisher.publish_event.assert_called_with(
            "pipeline.step.completed",
            {
                "pipeline_id": sample_pipeline_config["id"],
                "step_id": "step1",
                "status": "success",
                "metrics": {
                    "records_processed": 3,
                    "quality_checks_passed": 1
                }
            }
        )

    @pytest.mark.asyncio
    async def test_process_data_quality_failure(self, data_processor, sample_pipeline_config, test_bucket, s3):
        # Setup test data with null values
        test_data = b"id,value\n1,\n2,200\n3,\n"
        s3.put_object(
            Bucket=test_bucket,
            Key="input/data.csv",
            Body=test_data
        )

        # Process data
        result = await data_processor.process_data(
            pipeline_config=sample_pipeline_config,
            step_id="step1"
        )

        # Verify quality check failure
        assert result["success"] is False
        assert result["quality_checks"]["rule1"]["passed"] is False
        assert result["quality_checks"]["rule1"]["metrics"]["pass_rate"] < 0.99

        # Verify error event was published
        data_processor.event_publisher.publish_event.assert_called_with(
            "pipeline.step.failed",
            {
                "pipeline_id": sample_pipeline_config["id"],
                "step_id": "step1",
                "error": "Quality check failed: rule1",
                "details": {
                    "rule_id": "rule1",
                    "pass_rate": result["quality_checks"]["rule1"]["metrics"]["pass_rate"]
                }
            }
        )

    @pytest.mark.asyncio
    async def test_process_data_transform(self, data_processor, sample_pipeline_config, test_bucket, s3):
        # Setup input data
        test_data = b"id,value\n1,100\n2,-50\n3,300\n"
        s3.put_object(
            Bucket=test_bucket,
            Key="input/data.csv",
            Body=test_data
        )

        # Process transform step
        result = await data_processor.process_data(
            pipeline_config=sample_pipeline_config,
            step_id="step2"
        )

        # Verify transformation results
        assert result["success"] is True
        assert result["records_processed"] == 2  # Only positive values
        
        # Verify transformed data was written
        response = s3.get_object(
            Bucket=test_bucket,
            Key="output/result.parquet"
        )
        assert response["ContentLength"] > 0

    @pytest.mark.asyncio
    async def test_process_data_error_handling(self, data_processor, sample_pipeline_config):
        # Test with invalid SQL
        sample_pipeline_config["steps"][1]["sql"] = "INVALID SQL"
        
        result = await data_processor.process_data(
            pipeline_config=sample_pipeline_config,
            step_id="step2"
        )

        assert result["success"] is False
        assert "error" in result
        
        # Verify error event was published
        data_processor.event_publisher.publish_event.assert_called_with(
            "pipeline.step.failed",
            {
                "pipeline_id": sample_pipeline_config["id"],
                "step_id": "step2",
                "error": "SQL execution failed",
                "details": {
                    "sql": "INVALID SQL",
                    "error_message": result["error"]
                }
            }
        )

    @pytest.mark.asyncio
    async def test_process_data_metrics(self, data_processor, sample_pipeline_config, test_bucket, s3):
        # Setup test data
        test_data = b"id,value\n" + b"1,100\n" * 1000
        s3.put_object(
            Bucket=test_bucket,
            Key="input/data.csv",
            Body=test_data
        )

        # Process data
        start_time = datetime.utcnow()
        result = await data_processor.process_data(
            pipeline_config=sample_pipeline_config,
            step_id="step1"
        )
        end_time = datetime.utcnow()

        # Verify metrics
        assert "metrics" in result
        assert result["metrics"]["duration_ms"] > 0
        assert result["metrics"]["duration_ms"] == pytest.approx(
            (end_time - start_time).total_seconds() * 1000,
            rel=1e-1
        )
        assert result["metrics"]["records_processed"] == 1000
        assert result["metrics"]["bytes_processed"] > 0

        # Verify metrics event was published
        data_processor.event_publisher.publish_event.assert_any_call(
            "pipeline.metrics",
            {
                "pipeline_id": sample_pipeline_config["id"],
                "step_id": "step1",
                "metrics": result["metrics"]
            }
        )
