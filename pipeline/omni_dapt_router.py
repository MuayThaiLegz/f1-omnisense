"""OmniDapt APIRouter — model adaptation, training, registry, and scheduling.

Endpoints:
    POST /api/omni/dapt/datasets                    — create a dataset
    GET  /api/omni/dapt/datasets                    — list datasets
    GET  /api/omni/dapt/datasets/{name}             — get dataset info
    GET  /api/omni/dapt/datasets/{name}/stats       — dataset statistics
    GET  /api/omni/dapt/datasets/{name}/validate    — validate dataset quality
    DELETE /api/omni/dapt/datasets/{name}           — delete a dataset

    POST /api/omni/dapt/train                       — submit a training job
    GET  /api/omni/dapt/jobs                        — list training jobs
    GET  /api/omni/dapt/jobs/{job_id}               — get job status
    POST /api/omni/dapt/jobs/{job_id}/cancel        — cancel a running job

    GET  /api/omni/dapt/models                      — list model versions
    GET  /api/omni/dapt/models/{name}/versions      — version history
    GET  /api/omni/dapt/models/{name}/production    — get production model
    POST /api/omni/dapt/models/{name}/promote       — promote a version
    POST /api/omni/dapt/models/{name}/rollback      — rollback to previous
    GET  /api/omni/dapt/models/{name}/metrics       — metrics history
    POST /api/omni/dapt/models/{name}/register-vis  — register with OmniVis

    GET  /api/omni/dapt/health                      — all model health reports
    GET  /api/omni/dapt/health/{name}               — single model health
    POST /api/omni/dapt/drift/{name}                — detect drift

    GET  /api/omni/dapt/scheduler                   — scheduler status
    POST /api/omni/dapt/scheduler/start             — start scheduler
    POST /api/omni/dapt/scheduler/stop              — stop scheduler
"""

from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/omni/dapt", tags=["OmniDapt"])


# ── Request models ───────────────────────────────────────────────────────

class CreateDatasetRequest(BaseModel):
    name: str
    domain: str  # "cv" | "audio"
    description: str = ""
    labels: Optional[List[str]] = None


class TrainRequest(BaseModel):
    domain: str  # "cv" | "audio"
    dataset_name: str
    model_name: str = ""
    base_model: str = ""
    epochs: int = 50
    batch_size: int = 16
    learning_rate: float = 0.01
    image_size: int = 640
    patience: int = 10
    device: str = ""


class PromoteRequest(BaseModel):
    version: str
    to_status: str  # "validated" | "production" | "archived"


class DriftRequest(BaseModel):
    mAP50: float = 0.0
    f1_score: float = 0.0
    accuracy: float = 0.0
    precision: float = 0.0
    recall: float = 0.0


# ── Dataset endpoints ────────────────────────────────────────────────────

@router.post("/datasets")
def create_dataset(req: CreateDatasetRequest):
    """Create a new dataset for training."""
    from omnidapt import get_dataset_manager

    mgr = get_dataset_manager()
    ds = mgr.create_dataset(
        name=req.name,
        domain=req.domain,
        description=req.description,
        labels=req.labels,
    )
    return ds.to_dict()


@router.get("/datasets")
def list_datasets(domain: Optional[str] = Query(None)):
    """List all datasets, optionally filtered by domain."""
    from omnidapt import get_dataset_manager, ModelDomain

    mgr = get_dataset_manager()
    d = ModelDomain(domain) if domain else None
    datasets = mgr.list_datasets(domain=d)
    return {"datasets": [ds.to_dict() for ds in datasets]}


@router.get("/datasets/{name}")
def get_dataset(name: str):
    """Get dataset info."""
    from omnidapt import get_dataset_manager

    mgr = get_dataset_manager()
    ds = mgr.get_dataset(name)
    if not ds:
        raise HTTPException(404, f"Dataset '{name}' not found")
    return ds.to_dict()


@router.get("/datasets/{name}/stats")
def dataset_stats(name: str):
    """Get dataset statistics."""
    from omnidapt import get_dataset_manager

    mgr = get_dataset_manager()
    try:
        stats = mgr.get_stats(name)
        return stats.to_dict()
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.get("/datasets/{name}/validate")
def validate_dataset(name: str):
    """Validate dataset quality for training readiness."""
    from omnidapt import get_dataset_manager

    mgr = get_dataset_manager()
    return mgr.validate_dataset(name)


@router.delete("/datasets/{name}")
def delete_dataset(name: str):
    """Delete a dataset and all its samples."""
    from omnidapt import get_dataset_manager

    mgr = get_dataset_manager()
    mgr.delete_dataset(name)
    return {"deleted": name}


# ── Training endpoints ───────────────────────────────────────────────────

@router.post("/train")
def submit_training(req: TrainRequest):
    """Submit a background training job (YOLO or AST)."""
    from omnidapt import get_job_manager, train_yolo, train_ast, TrainingConfig, ModelDomain

    config = TrainingConfig(
        domain=ModelDomain(req.domain),
        dataset_name=req.dataset_name,
        model_name=req.model_name or req.dataset_name,
        base_model=req.base_model,
        epochs=req.epochs,
        batch_size=req.batch_size,
        learning_rate=req.learning_rate,
        image_size=req.image_size,
        patience=req.patience,
        device=req.device,
    )

    func = train_yolo if config.domain == ModelDomain.CV else train_ast
    jm = get_job_manager()
    job_id = jm.submit(func, config=config, kwargs={"config": config})

    return {"job_id": job_id, "status": "queued", "config": config.to_dict()}


@router.get("/jobs")
def list_jobs(status: Optional[str] = Query(None)):
    """List training jobs, optionally filtered by status."""
    from omnidapt import get_job_manager, TrainingStatus

    jm = get_job_manager()
    s = TrainingStatus(status) if status else None
    jobs = jm.list_jobs(status=s)
    return {"jobs": [j.to_dict() for j in jobs]}


@router.get("/jobs/{job_id}")
def get_job(job_id: str):
    """Get training job status and progress."""
    from omnidapt import get_job_manager

    jm = get_job_manager()
    job = jm.get(job_id)
    if not job:
        raise HTTPException(404, f"Job '{job_id}' not found")
    return job.to_dict()


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    """Cancel a running training job."""
    from omnidapt import get_job_manager

    jm = get_job_manager()
    success = jm.cancel(job_id)
    if not success:
        raise HTTPException(400, f"Cannot cancel job '{job_id}'")
    return {"job_id": job_id, "status": "cancelled"}


# ── Model registry endpoints ────────────────────────────────────────────

@router.get("/models")
def list_models():
    """List all registered model versions."""
    from omnidapt import get_model_registry

    registry = get_model_registry()
    all_docs = registry._storage.find_documents("model_versions")
    model_names = set(d["name"] for d in all_docs)
    result = {}
    for name in sorted(model_names):
        versions = registry.list_versions(name)
        result[name] = [v.to_dict() for v in versions]
    return {"models": result}


@router.get("/models/{name}/versions")
def model_versions(name: str):
    """List all versions of a model."""
    from omnidapt import get_model_registry

    registry = get_model_registry()
    versions = registry.list_versions(name)
    if not versions:
        raise HTTPException(404, f"No versions found for model '{name}'")
    return {"name": name, "versions": [v.to_dict() for v in versions]}


@router.get("/models/{name}/production")
def get_production_model(name: str):
    """Get the current production version of a model."""
    from omnidapt import get_model_registry

    registry = get_model_registry()
    prod = registry.get_production(name)
    if not prod:
        raise HTTPException(404, f"No production version for '{name}'")
    return prod.to_dict()


@router.post("/models/{name}/promote")
def promote_model(name: str, req: PromoteRequest):
    """Promote a model version (draft→validated→production→archived)."""
    from omnidapt import get_model_registry, ModelStatus

    registry = get_model_registry()
    success = registry.promote(name, req.version, ModelStatus(req.to_status))
    if not success:
        raise HTTPException(400, f"Cannot promote {name} v{req.version} to {req.to_status}")
    return {"name": name, "version": req.version, "status": req.to_status}


@router.post("/models/{name}/rollback")
def rollback_model(name: str):
    """Rollback: archive production, promote latest validated."""
    from omnidapt import get_model_registry

    registry = get_model_registry()
    rolled_back = registry.rollback(name)
    if not rolled_back:
        raise HTTPException(400, f"No validated version to rollback to for '{name}'")
    return {"name": name, "rolled_back_to": rolled_back.version}


@router.get("/models/{name}/metrics")
def metrics_history(name: str):
    """Get metrics history across all versions."""
    from omnidapt import get_model_registry

    registry = get_model_registry()
    history = registry.get_metrics_history(name)
    return {"name": name, "history": history}


@router.post("/models/{name}/register-vis")
def register_with_omnivis(name: str):
    """Register the production model with OmniVis for inference."""
    from omnidapt import get_model_registry

    registry = get_model_registry()
    success = registry.register_with_omnivis(name)
    if not success:
        raise HTTPException(400, f"Failed to register '{name}' with OmniVis")
    return {"name": name, "registered": True}


# ── Health & drift endpoints ────────────────────────────────────────────

@router.get("/health")
def all_health():
    """Health check all registered models."""
    from omnidapt import get_scheduler

    scheduler = get_scheduler()
    reports = scheduler.check_all_health()
    return {"reports": [r.to_dict() for r in reports]}


@router.get("/health/{name}")
def model_health(name: str):
    """Health check a specific model."""
    from omnidapt import get_scheduler

    scheduler = get_scheduler()
    report = scheduler.check_model_health(name)
    return report.to_dict()


@router.post("/drift/{name}")
def detect_drift(name: str, req: DriftRequest):
    """Detect performance drift by comparing recent metrics against production baseline."""
    from omnidapt import get_scheduler, TrainingMetrics

    scheduler = get_scheduler()
    recent = TrainingMetrics(
        mAP50=req.mAP50,
        f1_score=req.f1_score,
        accuracy=req.accuracy,
        precision=req.precision,
        recall=req.recall,
    )
    result = scheduler.detect_drift(name, recent)
    return result


# ── Scheduler endpoints ─────────────────────────────────────────────────

@router.get("/scheduler")
def scheduler_status():
    """Get scheduler status."""
    from omnidapt import get_scheduler

    scheduler = get_scheduler()
    return scheduler.get_status()


@router.post("/scheduler/start")
async def start_scheduler():
    """Start the automatic retraining scheduler."""
    from omnidapt import get_scheduler

    scheduler = get_scheduler()
    await scheduler.start()
    return {"status": "started"}


@router.post("/scheduler/stop")
async def stop_scheduler():
    """Stop the automatic retraining scheduler."""
    from omnidapt import get_scheduler

    scheduler = get_scheduler()
    await scheduler.stop()
    return {"status": "stopped"}
