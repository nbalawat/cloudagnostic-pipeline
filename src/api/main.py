from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from .database import get_db
from .crud import pipeline as crud
from .schemas import pipeline as schemas
import uvicorn

app = FastAPI(title="Pipeline Configuration API")

# Pipeline Configuration Routes
@app.post("/pipelines/", response_model=schemas.PipelineConfig)
def create_pipeline(pipeline: schemas.PipelineConfigCreate, db: Session = Depends(get_db)):
    return crud.PipelineCRUD.create_pipeline(db, pipeline)

@app.get("/pipelines/{pipeline_id}", response_model=schemas.PipelineConfig)
def get_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    pipeline = crud.PipelineCRUD.get_pipeline(db, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline

@app.get("/pipelines/", response_model=List[schemas.PipelineConfig])
def list_pipelines(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.PipelineCRUD.get_pipelines(db, skip, limit)

@app.put("/pipelines/{pipeline_id}", response_model=schemas.PipelineConfig)
def update_pipeline(
    pipeline_id: str, pipeline: schemas.PipelineConfigBase, db: Session = Depends(get_db)
):
    updated = crud.PipelineCRUD.update_pipeline(db, pipeline_id, pipeline)
    if not updated:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return updated

@app.delete("/pipelines/{pipeline_id}")
def delete_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    if not crud.PipelineCRUD.delete_pipeline(db, pipeline_id):
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return {"status": "success"}

# Layer Mapping Routes
@app.post("/pipelines/{pipeline_id}/mappings/", response_model=schemas.LayerMapping)
def create_layer_mapping(
    pipeline_id: str, mapping: schemas.LayerMappingCreate, db: Session = Depends(get_db)
):
    return crud.LayerMappingCRUD.create_layer_mapping(db, pipeline_id, mapping)

@app.get("/pipelines/{pipeline_id}/mappings/", response_model=List[schemas.LayerMapping])
def get_layer_mappings(pipeline_id: str, db: Session = Depends(get_db)):
    return crud.LayerMappingCRUD.get_layer_mappings(db, pipeline_id)

# Quality Rule Routes
@app.post("/quality-rules/", response_model=schemas.QualityRule)
def create_quality_rule(rule: schemas.QualityRuleCreate, db: Session = Depends(get_db)):
    return crud.QualityRuleCRUD.create_rule(db, rule)

@app.get("/quality-rules/{rule_id}/latest", response_model=schemas.QualityRule)
def get_latest_rule_version(rule_id: str, db: Session = Depends(get_db)):
    rule = crud.QualityRuleCRUD.get_latest_rule_version(db, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule

# Pipeline Run Routes
@app.post("/pipeline-runs/", response_model=schemas.PipelineRun)
def create_pipeline_run(run: schemas.PipelineRunCreate, db: Session = Depends(get_db)):
    return crud.PipelineRunCRUD.create_run(db, run)

@app.get("/pipelines/{pipeline_id}/runs/", response_model=List[schemas.PipelineRun])
def get_pipeline_runs(
    pipeline_id: str, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)
):
    return crud.PipelineRunCRUD.get_pipeline_runs(db, pipeline_id, skip, limit)

# Quality Result Routes
@app.post("/quality-results/", response_model=schemas.QualityResult)
def create_quality_result(result: schemas.QualityResultCreate, db: Session = Depends(get_db)):
    return crud.QualityResultCRUD.create_result(db, result)

@app.get("/pipeline-runs/{run_id}/quality-results/", response_model=List[schemas.QualityResult])
def get_run_quality_results(run_id: str, db: Session = Depends(get_db)):
    return crud.QualityResultCRUD.get_run_results(db, run_id)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
