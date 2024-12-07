# Cloud-Agnostic Data Pipeline Framework

A comprehensive, cloud-agnostic data processing framework supporting multiple storage formats, quality checks, and advanced orchestration capabilities.

## Table of Contents
- [Features](#features)
- [Architecture](#architecture)
- [Module Overview](#module-overview)
- [Local Development Setup](#local-development-setup)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Monitoring & Metrics](#monitoring--metrics)
- [Contributing](#contributing)

## Features

### Core Capabilities
- Multi-cloud support (AWS, Azure, GCP)
- Multiple storage formats (Parquet, JSON, CSV, ORC)
- Delta Lake, Apache Iceberg, Apache Hudi support
- Medallion architecture (Bronze, Silver, Gold)
- Advanced quality framework
- Event-driven processing
- Comprehensive monitoring

### Data Processing
- Configurable transformations
- Schema evolution
- Data validation
- Error handling
- Audit logging
- Metadata management

### Quality Framework
- Rule-based validation
- Anomaly detection
- Pattern matching
- Threshold management
- Quality metrics tracking

### Security
- Column-level encryption
- Role-based access control
- Audit logging
- Secure configuration
- Key management

## Architecture

### Core Components
1. **API Layer**
   - FastAPI-based REST API
   - Configuration management
   - Pipeline control
   - Status monitoring

2. **Processing Engine**
   - Spark-based processing
   - Multiple format support
   - Transformation framework
   - Quality checks

3. **Orchestration**
   - Argo Workflows
   - DAG management
   - Event triggers
   - Error handling

4. **Monitoring**
   - Metrics collection
   - Alert management
   - Dashboard integration
   - Performance tracking

## Module Overview

### src/api/
- `main.py`: FastAPI application entry point
- `models/`: Database models and schemas
- `crud/`: Database operations
- `routes/`: API endpoints

### src/processor/
- `data_processor.py`: Core processing logic
- `transformers/`: Data transformation modules
- `validators/`: Data validation rules
- `utils/`: Helper functions

### src/quality/
- `rule_engine.py`: Quality rule processing
- `anomaly_detection.py`: Anomaly detection system
- `metrics/`: Quality metrics collection
- `validators/`: Quality validation rules

### src/workflow/
- `dag_generator.py`: DAG generation logic
- `run_pipeline.py`: Pipeline execution
- `error_handler.py`: Error management
- `status_tracker.py`: Pipeline status tracking

### src/messaging/
- `event_publisher.py`: Kafka event publishing
- `consumers/`: Event consumers
- `handlers/`: Event handling logic

### src/security/
- `encryption.py`: Data encryption
- `key_manager.py`: Key management
- `rbac/`: Access control

## Local Development Setup

### Prerequisites
```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install required tools
brew install python@3.9
brew install apache-spark
brew install kafka
brew install postgresql
brew install kubectl
brew install helm
brew install k9s  # Optional, for K8s management
```

### Environment Setup
```bash
# Clone repository
git clone https://github.com/yourusername/cloud-agnostic-pipeline.git
cd cloud-agnostic-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Start Local Services
```bash
# Start Docker services
docker-compose up -d

# Verify services are running
docker-compose ps

# Initialize database
python src/api/init_db.py

# Generate sample data
python tests/data_generators/sample_data_generator.py
```

### Start Development Server
```bash
# Start API server
uvicorn src.api.main:app --reload --port 8000

# In another terminal, start the pipeline
python src/workflow/run_pipeline.py
```

### Run Tests
```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Generate test coverage report
pytest --cov=src tests/
```

## Configuration

### Environment Variables
```bash
# Create .env file
cp .env.example .env

# Edit variables
POSTGRES_HOST=localhost
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SPARK_MASTER_URL=local[*]
```

### Pipeline Configuration
```yaml
# Edit src/config/pipeline_config.yaml
source_systems:
  - name: sales_data
    type: csv
    path: /data/landing/sales
  - name: customer_data
    type: parquet
    path: /data/landing/customers
```

## Usage Examples

### Run Simple Pipeline
```bash
# Using CLI
python src/cli/pipeline.py run --config config/simple_pipeline.yaml

# Using API
curl -X POST http://localhost:8000/api/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{"config_path": "config/simple_pipeline.yaml"}'
```

### Monitor Pipeline
```bash
# View pipeline status
curl http://localhost:8000/api/v1/pipelines/status/{pipeline_id}

# View quality metrics
curl http://localhost:8000/api/v1/quality/metrics/{pipeline_id}
```

## Monitoring & Metrics

### Available Metrics
- Pipeline execution time
- Record counts by layer
- Quality scores
- Error rates
- Resource utilization

### Dashboards
- Grafana dashboards available in `monitoring/dashboards/`
- Prometheus configurations in `monitoring/prometheus/`

## Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
MIT License - see LICENSE file for details
