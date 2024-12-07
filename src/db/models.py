from datetime import datetime
import uuid
from sqlalchemy import Column, String, Text, Boolean, Float, Integer, DateTime, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class PipelineConfiguration(Base):
    __tablename__ = 'pipeline_configurations'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_name = Column(String(255), nullable=False)
    config_json = Column(JSONB)
    status = Column(String(50))

class PipelineVersion(Base):
    __tablename__ = 'pipeline_versions'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey('pipeline_configurations.id'))
    version = Column(String(50))
    parent_version = Column(String(50))
    created_at = Column(DateTime(timezone=True))
    status = Column(String(50))

class PipelineLayerMapping(Base):
    __tablename__ = 'pipeline_layer_mappings'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey('pipeline_configurations.id'))
    layer_name = Column(String(50))
    mapping_id = Column(UUID(as_uuid=True))
    mapping_version = Column(String(50))
    status = Column(String(50))
    effective_from = Column(DateTime(timezone=True))
    effective_to = Column(DateTime(timezone=True))

class SourceTargetMapping(Base):
    __tablename__ = 'source_target_mappings'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    mapping_id = Column(UUID(as_uuid=True))
    version = Column(String(50))
    status = Column(String(50))
    mapping_definition = Column(JSONB)
    created_at = Column(DateTime(timezone=True))

class QualityRuleVersion(Base):
    __tablename__ = 'quality_rule_versions'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id = Column(UUID(as_uuid=True))
    version = Column(String(50))
    definition = Column(JSONB)
    status = Column(String(50))
    created_at = Column(DateTime(timezone=True))
    previous_id = Column(UUID(as_uuid=True))

class MappingQualityRule(Base):
    __tablename__ = 'mapping_quality_rules'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    mapping_version = Column(String(50))
    rule_version = Column(String(50))
    threshold_config = Column(JSONB)
    failure_metrics = Column(JSONB)
    created_at = Column(DateTime(timezone=True))

class QualityRuleThreshold(Base):
    __tablename__ = 'quality_rule_thresholds'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id = Column(UUID(as_uuid=True))
    threshold_type = Column(String(50))
    threshold_config = Column(JSONB)
    effective_from = Column(DateTime(timezone=True))
    effective_to = Column(DateTime(timezone=True))

class DataQualityResult(Base):
    __tablename__ = 'data_quality_results'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True))
    executed = Column(Boolean)
    passed = Column(Boolean)
    failed_records = Column(Integer)
    created_at = Column(DateTime(timezone=True))

class MappingQualityResult(Base):
    __tablename__ = 'mapping_quality_results'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    execution_id = Column(UUID(as_uuid=True))
    mapping_version = Column(String(50))
    execution_phase = Column(String(50))
    execution_timestamp = Column(DateTime(timezone=True))
    passed = Column(Boolean)
    metrics = Column(JSONB)
    failed_records_sample = Column(JSONB)
    remediation_action = Column(String(255))

class PipelineRun(Base):
    __tablename__ = 'pipeline_runs'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey('pipeline_configurations.id'))
    layer = Column(String(50))
    status = Column(String(50))
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    records_processed = Column(Integer)
    error_message = Column(Text)

class PipelineError(Base):
    __tablename__ = 'pipeline_errors'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey('pipeline_configurations.id'))
    step_id = Column(String(50))
    severity = Column(String(20))
    error_message = Column(Text)
    stack_trace = Column(Text)
    created_at = Column(DateTime(timezone=True))
    status = Column(String(50))

class PipelineMigration(Base):
    __tablename__ = 'pipeline_migrations'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    from_version = Column(String(50))
    to_version = Column(String(50))
    migration_script = Column(Text)
    executed_at = Column(DateTime(timezone=True))
    status = Column(String(50))

class RecoveryAttempt(Base):
    __tablename__ = 'recovery_attempts'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    error_id = Column(UUID(as_uuid=True), ForeignKey('pipeline_errors.id'))
    action_type = Column(String(50))
    parameters = Column(JSONB)
    attempt_number = Column(Integer)
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    status = Column(String(50))
    result = Column(Text)
