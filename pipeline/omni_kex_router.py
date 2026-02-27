"""OmniKeX APIRouter — NL insight extraction from analytics data via WISE framework.

Endpoints:
    POST /api/omni/kex/extract               — extract insight from uploaded CSV or driver data
    POST /api/omni/kex/extract/driver/{code}  — extract insight from driver telemetry
    POST /api/omni/kex/report/{driver_code}   — full autonomous extraction from HealthReport
    GET  /api/omni/kex/providers              — list available LLM providers
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/omni/kex", tags=["OmniKeX"])

# ── Lazy singletons ─────────────────────────────────────────────────────

_health_chain_ready = False


def _load_driver_data(driver_code: str):
    """Load and merge car + bio telemetry for a driver."""
    from pipeline.anomaly.run_f1_anomaly import (
        load_car_race_data,
        load_bio_race_data,
        merge_telemetry,
    )
    car_df = load_car_race_data(driver_code)
    bio_df = load_bio_race_data(driver_code)
    return merge_telemetry(car_df, bio_df)


# ── Request/Response models ─────────────────────────────────────────────

class ExtractRequest(BaseModel):
    question: Optional[str] = None
    pillar: Optional[str] = None  # "realtime" | "anomaly" | "forecast"
    provider: Optional[str] = None  # "groq" | "ollama" | "openai" | "anthropic" | "auto"
    persona: Optional[str] = None  # "CEO" | "analyst" | etc.
    response_length: str = "medium"  # "short" | "medium" | "long"
    verify_grounding: bool = True


# ── Endpoints ───────────────────────────────────────────────────────────

@router.post("/extract/driver/{driver_code}")
def extract_driver(driver_code: str, req: ExtractRequest):
    """Extract NL insights from a driver's telemetry data."""
    from omnikex import extract

    drivers = {"NOR": "Lando Norris", "PIA": "Oscar Piastri"}
    if driver_code not in drivers:
        raise HTTPException(404, f"Unknown driver: {driver_code}")

    df = _load_driver_data(driver_code)

    kwargs = {
        "data": df,
        "question": req.question or f"Analyze {drivers[driver_code]}'s telemetry performance patterns",
        "response_length": req.response_length,
        "verify_grounding": req.verify_grounding,
    }
    if req.pillar:
        from omnikex import InsightPillar
        kwargs["pillar"] = InsightPillar(req.pillar)
    if req.provider:
        from omnikex import LLMProvider
        kwargs["llm_provider"] = LLMProvider(req.provider)
    if req.persona:
        kwargs["persona"] = req.persona

    insight = extract(**kwargs)
    return insight.to_dict()


@router.post("/report/{driver_code}")
def extract_report(driver_code: str, req: ExtractRequest):
    """Full autonomous extraction: health assess → 3 pillars (realtime, anomaly, forecast)."""
    from omnikex import extract_report as _extract_report
    from omnihealth import assess

    drivers = {"NOR": "Lando Norris", "PIA": "Oscar Piastri"}
    if driver_code not in drivers:
        raise HTTPException(404, f"Unknown driver: {driver_code}")

    df = _load_driver_data(driver_code)

    # Build component map matching omni_health_router
    component_map = {
        "Power Unit": [c for c in ["RPM_mean", "RPM_max", "RPM_std", "nGear_mean"] if c in df.columns],
        "Brakes": [c for c in ["Brake_pct", "Speed_mean", "Speed_std"] if c in df.columns],
        "Drivetrain": [c for c in ["Throttle_mean", "Throttle_max", "DRS_pct"] if c in df.columns],
        "Suspension": [c for c in ["Speed_mean", "Speed_max", "Distance_mean"] if c in df.columns],
        "Thermal": [c for c in ["HeartRate_bpm_mean", "CockpitTemp_C_mean", "AirTemp_C_mean", "TrackTemp_C_mean"] if c in df.columns],
        "Electronics": [c for c in ["DRS_pct", "RPM_mean", "nGear_mean"] if c in df.columns],
    }

    health_report = assess(df, component_map, horizon=10)

    kwargs = {
        "data": df,
        "health_report": health_report,
    }
    if req.question:
        kwargs["question"] = req.question
    if req.provider:
        from omnikex import LLMProvider
        kwargs["llm_provider"] = LLMProvider(req.provider)

    result = _extract_report(**kwargs)
    return result.to_dict()


@router.post("/extract")
async def extract_uploaded(
    file: UploadFile = File(...),
    question: str = Form(None),
    pillar: str = Form(None),
    provider: str = Form(None),
    persona: str = Form(None),
    response_length: str = Form("medium"),
):
    """Extract NL insight from an uploaded CSV/JSON file."""
    import io
    import pandas as pd
    from omnikex import extract

    content = await file.read()
    filename = file.filename or "data.csv"

    if filename.endswith(".json"):
        df = pd.read_json(io.BytesIO(content))
    else:
        df = pd.read_csv(io.BytesIO(content))

    kwargs = {
        "data": df,
        "question": question or "Analyze the key patterns and anomalies in this data",
        "response_length": response_length,
        "verify_grounding": True,
    }
    if pillar:
        from omnikex import InsightPillar
        kwargs["pillar"] = InsightPillar(pillar)
    if provider:
        from omnikex import LLMProvider
        kwargs["llm_provider"] = LLMProvider(provider)
    if persona:
        kwargs["persona"] = persona

    insight = extract(**kwargs)
    return insight.to_dict()


@router.get("/providers")
def list_providers():
    """List available LLM providers for insight generation."""
    providers = []
    try:
        if os.getenv("GROQ_API_KEY"):
            providers.append("groq")
    except Exception:
        pass
    try:
        import requests
        r = requests.get("http://localhost:11434/api/tags", timeout=2)
        if r.ok:
            providers.append("ollama")
    except Exception:
        pass
    if os.getenv("OPENAI_API_KEY"):
        providers.append("openai")
    if os.getenv("ANTHROPIC_API_KEY"):
        providers.append("anthropic")
    return {"providers": providers}
