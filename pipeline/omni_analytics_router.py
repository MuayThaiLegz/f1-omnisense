"""OmniAnalytics APIRouter — anomaly detection and forecasting for F1 telemetry.

Endpoints:
    POST /api/omni/analytics/anomaly/{driver_code}   — run anomaly ensemble
    POST /api/omni/analytics/forecast/{driver_code}   — forecast a telemetry column
"""

from __future__ import annotations

import logging
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from pipeline.anomaly.run_f1_anomaly import load_car_race_data
from omnidata._types import TabularDataset, DatasetProfile, ColumnProfile, ColumnRole, DType
from omnianalytics import AnomalyEnsemble, forecast

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/omni/analytics", tags=["OmniAnalytics"])

DRIVERS = {"NOR": "Lando Norris", "PIA": "Oscar Piastri"}


def _build_dataset(driver_code: str) -> TabularDataset:
    """Load race telemetry and wrap in a TabularDataset for omnianalytics."""
    df = load_car_race_data(driver_code)
    if df.empty:
        raise HTTPException(404, f"No telemetry data for driver {driver_code}")

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    col_profiles = [
        ColumnProfile(
            name=c,
            dtype=DType.FLOAT,
            role=ColumnRole.METRIC,
            null_count=int(df[c].isna().sum()),
            unique_count=int(df[c].nunique()),
        )
        for c in numeric_cols
    ]
    profile = DatasetProfile(
        row_count=len(df),
        column_count=len(numeric_cols),
        columns=col_profiles,
        metric_cols=numeric_cols,
        timestamp_col=None,
    )
    return TabularDataset(df=df, profile=profile)


class AnomalyRequest(BaseModel):
    columns: Optional[List[str]] = None


@router.post("/anomaly/{driver_code}")
def detect_anomalies(driver_code: str, body: AnomalyRequest = AnomalyRequest()):
    """Run the OmniAnalytics anomaly ensemble on a driver's race telemetry."""
    driver_code = driver_code.upper()
    if driver_code not in DRIVERS:
        raise HTTPException(404, f"Unknown driver: {driver_code}")

    ds = _build_dataset(driver_code)
    ensemble = AnomalyEnsemble()
    result = ensemble.run(ds, columns=body.columns)
    return {
        "driver": DRIVERS[driver_code],
        "code": driver_code,
        **result.to_dict(),
    }


@router.post("/forecast/{driver_code}")
def forecast_column(
    driver_code: str,
    column: str = Query(..., description="Telemetry column to forecast"),
    horizon: int = Query(5, ge=1, le=50),
    method: str = Query("auto", description="auto, arima, linear, or lightgbm"),
):
    """Forecast a specific telemetry column for a driver."""
    driver_code = driver_code.upper()
    if driver_code not in DRIVERS:
        raise HTTPException(404, f"Unknown driver: {driver_code}")

    ds = _build_dataset(driver_code)
    if column not in ds.df.columns:
        raise HTTPException(400, f"Column '{column}' not found. Available: {list(ds.df.columns)}")

    result = forecast(ds, column=column, horizon=horizon, method=method)
    return {
        "driver": DRIVERS[driver_code],
        "code": driver_code,
        **result.to_dict(),
    }
