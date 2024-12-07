from pydantic import BaseModel, UUID4
from typing import Optional, List, Dict, Any
from datetime import datetime

class PipelineConfigBase(BaseModel):
    config_json: Dict[str, Any]

class PipelineConfigCreate(PipelineConfigBase):
    pipeline_id: str

class PipelineConfig(PipelineConfigBase):
    pipeline_id: str
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        orm_mode = True

class LayerMappingBase(BaseModel):
    layer_name: str
    mapping_id: str
    mapping_version: int
    status: str
    effective_from: datetime
    effective_to: Optional[datetime]

class LayerMappingCreate(LayerMappingBase):
    pass

class LayerMapping(LayerMappingBase):
    pipeline_id: str

    class Config:
        orm_mode = True

class QualityRuleBase(BaseModel):
    rule_type: str
    definition: Dict[str, Any]
    dependent_rules: Optional[Dict[str, Any]]
    status: str

class QualityRuleCreate(QualityRuleBase):
    rule_id: str
    version: int
    created_by: str

class QualityRule(QualityRuleBase):
    rule_id: str
    version: int
    created_at: datetime
    created_by: str

    class Config:
        orm_mode = True

class PipelineRunBase(BaseModel):
    pipeline_id: str
    layer: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    records_processed: Optional[int]
    error_message: Optional[str]

class PipelineRunCreate(PipelineRunBase):
    pass

class PipelineRun(PipelineRunBase):
    run_id: UUID4
    created_at: datetime

    class Config:
        orm_mode = True

class QualityResultBase(BaseModel):
    rule_id: str
    passed: bool
    failed_records: int
    error_samples: Optional[Dict[str, Any]]

class QualityResultCreate(QualityResultBase):
    run_id: UUID4

class QualityResult(QualityResultBase):
    run_id: UUID4
    created_at: datetime

    class Config:
        orm_mode = True

class PipelineVersionBase(BaseModel):
    pipeline_id: str
    parent_version: Optional[str]
    config: Dict[str, Any]
    status: str
    created_by: str

class PipelineVersionCreate(PipelineVersionBase):
    version_id: str

class PipelineVersion(PipelineVersionBase):
    version_id: str
    created_at: datetime

    class Config:
        orm_mode = True
