from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Boolean, Text, Float, JSON, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from .base import Base, TimestampMixin
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

class PipelineConfiguration(Base, TimestampMixin):
    __tablename__ = "pipeline_configurations"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_name = Column(String(50))
    config_json = Column(JSONB)
    status = Column(String(20), default="active")
    
    layer_mappings = relationship("PipelineLayerMapping", back_populates="pipeline")
    pipeline_runs = relationship("PipelineRun", back_populates="pipeline")
    versions = relationship("PipelineVersion", back_populates="pipeline")

class PipelineLayerMapping(Base, TimestampMixin):
    __tablename__ = "pipeline_layer_mappings"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_configurations.id"))
    layer_name = Column(String(20))
    mapping_id = Column(UUID(as_uuid=True), ForeignKey("source_target_mappings.id"))
    mapping_version = Column(String(50))
    status = Column(String(20))
    effective_from = Column(DateTime(timezone=True))
    effective_to = Column(DateTime(timezone=True))
    
    pipeline = relationship("PipelineConfiguration", back_populates="layer_mappings")
    mapping = relationship("SourceTargetMapping")

class SourceTargetMapping(Base, TimestampMixin):
    __tablename__ = "source_target_mappings"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    mapping_id = Column(UUID(as_uuid=True))
    version = Column(String(50))
    status = Column(String(20))
    mapping_definition = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    
    quality_rules = relationship("MappingQualityRule", back_populates="mapping")

class QualityRuleVersion(Base, TimestampMixin):
    __tablename__ = "quality_rule_versions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id = Column(UUID(as_uuid=True))
    version = Column(String(50))
    definition = Column(JSONB)
    status = Column(String(20))
    previous_id = Column(UUID(as_uuid=True))
    created_at = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    
    thresholds = relationship("QualityRuleThreshold", back_populates="rule")
    mapping_rules = relationship("MappingQualityRule", back_populates="rule_version")

class MappingQualityRule(Base, TimestampMixin):
    __tablename__ = "mapping_quality_rules"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    mapping_version = Column(String(50))
    rule_version = Column(String(50))
    threshold_config = Column(JSONB)
    failure_metrics = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    
    mapping = relationship("SourceTargetMapping", back_populates="quality_rules")
    rule_version = relationship("QualityRuleVersion", back_populates="mapping_rules")
    results = relationship("MappingQualityResult", back_populates="rule")

class QualityRuleThreshold(Base, TimestampMixin):
    __tablename__ = "quality_rule_thresholds"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id = Column(UUID(as_uuid=True), ForeignKey("quality_rule_versions.id"))
    threshold_type = Column(String(50))
    threshold_config = Column(JSONB)
    effective_from = Column(DateTime(timezone=True))
    effective_to = Column(DateTime(timezone=True))
    
    rule = relationship("QualityRuleVersion", back_populates="thresholds")

class PipelineRun(Base, TimestampMixin):
    __tablename__ = "pipeline_runs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_configurations.id"))
    layer = Column(String(20))
    status = Column(String(20))
    records_processed = Column(Integer)
    error_message = Column(Text)
    start_time = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    end_time = Column(DateTime(timezone=True))
    
    pipeline = relationship("PipelineConfiguration", back_populates="pipeline_runs")
    quality_results = relationship("DataQualityResult", back_populates="run")
    errors = relationship("PipelineError", back_populates="run")

class DataQualityResult(Base, TimestampMixin):
    __tablename__ = "data_quality_results"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_runs.id"))
    executed = Column(Boolean)
    passed = Column(Boolean)
    failed_records = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    
    run = relationship("PipelineRun", back_populates="quality_results")
    rule = relationship("QualityRuleVersion")

class PipelineVersion(Base, TimestampMixin):
    __tablename__ = "pipeline_versions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_configurations.id"))
    version = Column(String(50))
    parent_version = Column(String(50))
    status = Column(String(20))
    created_at = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    
    pipeline = relationship("PipelineConfiguration", back_populates="versions")
    migrations_from = relationship("PipelineMigration", foreign_keys="PipelineMigration.from_version", back_populates="from_ver")
    migrations_to = relationship("PipelineMigration", foreign_keys="PipelineMigration.to_version", back_populates="to_ver")

class PipelineMigration(Base, TimestampMixin):
    __tablename__ = "pipeline_migrations"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    from_version = Column(String(50), ForeignKey("pipeline_versions.version"))
    to_version = Column(String(50), ForeignKey("pipeline_versions.version"))
    migration_script = Column(Text)
    status = Column(String(20))
    executed_at = Column(DateTime(timezone=True))
    
    from_ver = relationship("PipelineVersion", foreign_keys=[from_version], back_populates="migrations_from")
    to_ver = relationship("PipelineVersion", foreign_keys=[to_version], back_populates="migrations_to")

class MappingQualityResult(Base, TimestampMixin):
    __tablename__ = "mapping_quality_results"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    execution_id = Column(UUID(as_uuid=True))
    mapping_version = Column(String(50))
    execution_phase = Column(String(50))
    passed = Column(Boolean)
    metrics = Column(JSONB)
    failed_records_sample = Column(JSONB)
    remediation_action = Column(String(50))
    execution_timestamp = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    
    rule = relationship("MappingQualityRule", back_populates="results")

class PipelineError(Base, TimestampMixin):
    __tablename__ = "pipeline_errors"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_configurations.id"))
    step_id = Column(String(50))
    severity = Column(String(20))
    error_message = Column(String(200))
    stack_trace = Column(Text)
    status = Column(String(20))
    created_at = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    
    run = relationship("PipelineRun", back_populates="errors")

class RecoveryAttempt(Base, TimestampMixin):
    __tablename__ = "recovery_attempts"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    error_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_errors.id"))
    action_type = Column(String(50))
    parameters = Column(JSONB)
    attempt_number = Column(Integer)
    status = Column(String(20))
    result = Column(String(50))
    start_time = Column(DateTime(timezone=True), server_default=datetime.utcnow)
    end_time = Column(DateTime(timezone=True))
