# Cloud-Agnostic Data Pipeline Framework

A robust, extensible framework for building and running data pipelines across multiple cloud providers.

## Features

- **Cloud Provider Support**
  - AWS (Amazon Web Services)
  - GCP (Google Cloud Platform)
  - Azure (Microsoft Azure)

- **Core Components**
  - Storage Management
  - Pipeline Orchestration
  - Monitoring & Alerting
  - Data Encryption

- **Key Capabilities**
  - Database-driven configuration
  - Dynamic DAG generation
  - Quality rule enforcement
  - Error recovery
  - Version control
  - Migration management

## Architecture

### Provider Abstraction
- Abstract base classes define interfaces for:
  - Storage operations
  - Job orchestration
  - Monitoring & metrics
  - Encryption & security

### Database Schema
- Pipeline configurations
- Layer mappings
- Quality rules
- Execution tracking
- Error management

### Service Layer
- Pipeline execution
- Data management
- Monitoring integration
- Security implementation

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/cloud-agnostic-pipeline.git
cd cloud-agnostic-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Create a `config.yaml` file:

```yaml
cloud_provider: aws  # or 'gcp' or 'azure'
storage:
  bucket: my-pipeline-data
orchestrator:
  job_queue: pipeline-queue
  job_definition: pipeline-job-def
monitoring:
  namespace: MyPipeline
encryption:
  key_id: alias/pipeline-key
```

## Usage

### Basic Pipeline Execution

```python
from src.services.pipeline_service import PipelineService

# Initialize service
with open('config.yaml') as f:
    config = yaml.safe_load(f)
pipeline_service = PipelineService(config)

# Execute pipeline
job_id = await pipeline_service.execute_pipeline({
    'id': 'pipeline-001',
    'steps': [
        {
            'name': 'extract',
            'type': 'source',
            'config': {...}
        },
        {
            'name': 'transform',
            'type': 'sql',
            'config': {...}
        }
    ]
})

# Check status
status = await pipeline_service.get_pipeline_status(job_id)
```

### Quality Rules

```python
# Define quality rule
quality_rule = {
    'name': 'null_check',
    'columns': ['id', 'name'],
    'condition': 'IS NOT NULL',
    'threshold': 0.99
}

# Apply to pipeline
pipeline_config = {
    'id': 'pipeline-001',
    'quality_rules': [quality_rule],
    'steps': [...]
}
```

## Development

### Setting Up Development Environment

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest

# Run linting
flake8 src tests
```

### Adding a New Cloud Provider

1. Create provider implementation:
```python
from src.providers.base import StorageProvider

class NewStorageProvider(StorageProvider):
    async def read_file(self, path: str) -> bytes:
        # Implementation
        pass
```

2. Register in factory:
```python
class ProviderFactory:
    _storage_providers = {
        'new_provider': NewStorageProvider
    }
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run specific test category
pytest tests/test_providers/
pytest tests/test_services/

# Run with coverage
pytest --cov=src tests/
```

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Project Structure

```
cloud-agnostic-pipeline/
├── src/
│   ├── api/              # API endpoints
│   ├── db/              # Database models and migrations
│   ├── providers/       # Cloud provider implementations
│   ├── services/        # Business logic
│   └── workflow/        # Pipeline execution
├── tests/
│   ├── providers/       # Provider tests
│   ├── services/        # Service tests
│   └── workflow/        # Workflow tests
├── config/             # Configuration templates
└── docs/              # Documentation
