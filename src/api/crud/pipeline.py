from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from ..models.pipeline import (
    PipelineConfiguration, PipelineLayerMapping, QualityRuleVersion,
    PipelineRun, DataQualityResult, PipelineVersion
)
from ..schemas import pipeline as schemas

class PipelineCRUD:
    @staticmethod
    def create_pipeline(db: Session, pipeline: schemas.PipelineConfigCreate) -> PipelineConfiguration:
        db_pipeline = PipelineConfiguration(
            pipeline_id=pipeline.pipeline_id,
            config_json=pipeline.config_json
        )
        db.add(db_pipeline)
        db.commit()
        db.refresh(db_pipeline)
        return db_pipeline

    @staticmethod
    def get_pipeline(db: Session, pipeline_id: str) -> Optional[PipelineConfiguration]:
        return db.query(PipelineConfiguration).filter(
            PipelineConfiguration.pipeline_id == pipeline_id
        ).first()

    @staticmethod
    def get_pipelines(db: Session, skip: int = 0, limit: int = 100) -> List[PipelineConfiguration]:
        return db.query(PipelineConfiguration).offset(skip).limit(limit).all()

    @staticmethod
    def update_pipeline(
        db: Session, pipeline_id: str, pipeline: schemas.PipelineConfigBase
    ) -> Optional[PipelineConfiguration]:
        db_pipeline = PipelineCRUD.get_pipeline(db, pipeline_id)
        if db_pipeline:
            db_pipeline.config_json = pipeline.config_json
            db_pipeline.updated_at = datetime.utcnow()
            db.commit()
            db.refresh(db_pipeline)
        return db_pipeline

    @staticmethod
    def delete_pipeline(db: Session, pipeline_id: str) -> bool:
        db_pipeline = PipelineCRUD.get_pipeline(db, pipeline_id)
        if db_pipeline:
            db.delete(db_pipeline)
            db.commit()
            return True
        return False

class LayerMappingCRUD:
    @staticmethod
    def create_layer_mapping(
        db: Session, pipeline_id: str, mapping: schemas.LayerMappingCreate
    ) -> PipelineLayerMapping:
        db_mapping = PipelineLayerMapping(
            pipeline_id=pipeline_id,
            **mapping.dict()
        )
        db.add(db_mapping)
        db.commit()
        db.refresh(db_mapping)
        return db_mapping

    @staticmethod
    def get_layer_mappings(
        db: Session, pipeline_id: str
    ) -> List[PipelineLayerMapping]:
        return db.query(PipelineLayerMapping).filter(
            PipelineLayerMapping.pipeline_id == pipeline_id
        ).all()

    @staticmethod
    def update_layer_mapping(
        db: Session, pipeline_id: str, layer_name: str, mapping: schemas.LayerMappingBase
    ) -> Optional[PipelineLayerMapping]:
        db_mapping = db.query(PipelineLayerMapping).filter(
            PipelineLayerMapping.pipeline_id == pipeline_id,
            PipelineLayerMapping.layer_name == layer_name
        ).first()
        if db_mapping:
            for key, value in mapping.dict().items():
                setattr(db_mapping, key, value)
            db.commit()
            db.refresh(db_mapping)
        return db_mapping

class QualityRuleCRUD:
    @staticmethod
    def create_rule(db: Session, rule: schemas.QualityRuleCreate) -> QualityRuleVersion:
        db_rule = QualityRuleVersion(**rule.dict())
        db.add(db_rule)
        db.commit()
        db.refresh(db_rule)
        return db_rule

    @staticmethod
    def get_rule(db: Session, rule_id: str, version: int) -> Optional[QualityRuleVersion]:
        return db.query(QualityRuleVersion).filter(
            QualityRuleVersion.rule_id == rule_id,
            QualityRuleVersion.version == version
        ).first()

    @staticmethod
    def get_latest_rule_version(db: Session, rule_id: str) -> Optional[QualityRuleVersion]:
        return db.query(QualityRuleVersion).filter(
            QualityRuleVersion.rule_id == rule_id
        ).order_by(QualityRuleVersion.version.desc()).first()

class PipelineRunCRUD:
    @staticmethod
    def create_run(db: Session, run: schemas.PipelineRunCreate) -> PipelineRun:
        db_run = PipelineRun(**run.dict())
        db.add(db_run)
        db.commit()
        db.refresh(db_run)
        return db_run

    @staticmethod
    def update_run_status(
        db: Session, run_id: str, status: str, end_time: Optional[datetime] = None,
        error_message: Optional[str] = None
    ) -> Optional[PipelineRun]:
        db_run = db.query(PipelineRun).filter(PipelineRun.run_id == run_id).first()
        if db_run:
            db_run.status = status
            if end_time:
                db_run.end_time = end_time
            if error_message:
                db_run.error_message = error_message
            db.commit()
            db.refresh(db_run)
        return db_run

    @staticmethod
    def get_pipeline_runs(
        db: Session, pipeline_id: str, skip: int = 0, limit: int = 100
    ) -> List[PipelineRun]:
        return db.query(PipelineRun).filter(
            PipelineRun.pipeline_id == pipeline_id
        ).offset(skip).limit(limit).all()

class QualityResultCRUD:
    @staticmethod
    def create_result(db: Session, result: schemas.QualityResultCreate) -> DataQualityResult:
        db_result = DataQualityResult(**result.dict())
        db.add(db_result)
        db.commit()
        db.refresh(db_result)
        return db_result

    @staticmethod
    def get_run_results(db: Session, run_id: str) -> List[DataQualityResult]:
        return db.query(DataQualityResult).filter(
            DataQualityResult.run_id == run_id
        ).all()
