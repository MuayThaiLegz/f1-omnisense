"""OmniData APIRouter — tabular data loading, profiling, preprocessing, and feature engineering.

Endpoints:
    POST /api/omni/data/load                   — load + profile a CSV/JSON/Excel file
    POST /api/omni/data/profile/{driver_code}  — profile driver telemetry data
    POST /api/omni/data/preprocess             — preprocess uploaded dataset
    POST /api/omni/data/features               — engineer features from uploaded dataset
    POST /api/omni/data/split                  — train/test split for ML
"""

from __future__ import annotations

import io
import logging
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/omni/data", tags=["OmniData"])


def _load_driver_dataset(driver_code: str):
    """Load driver telemetry as a TabularDataset."""
    from pipeline.anomaly.run_f1_anomaly import (
        load_car_race_data,
        load_bio_race_data,
        merge_telemetry,
    )
    from omnidata import load, profile

    car_df = load_car_race_data(driver_code)
    bio_df = load_bio_race_data(driver_code)
    merged = merge_telemetry(car_df, bio_df)

    # Build TabularDataset from the merged DataFrame
    from omnidata._types import TabularDataset, DatasetProfile, ColumnProfile, ColumnRole, DType
    import pandas as pd

    columns = []
    for col in merged.columns:
        dtype = DType.FLOAT if pd.api.types.is_numeric_dtype(merged[col]) else DType.STRING
        role = ColumnRole.METRIC if dtype == DType.FLOAT else ColumnRole.CATEGORICAL
        cp = ColumnProfile(name=col, dtype=dtype, role=role)
        if dtype == DType.FLOAT:
            cp.null_count = int(merged[col].isna().sum())
            cp.null_pct = round(cp.null_count / len(merged) * 100, 2) if len(merged) > 0 else 0
            cp.unique_count = int(merged[col].nunique())
            cp.min = float(merged[col].min()) if not merged[col].isna().all() else None
            cp.max = float(merged[col].max()) if not merged[col].isna().all() else None
            cp.mean = float(merged[col].mean()) if not merged[col].isna().all() else None
            cp.std = float(merged[col].std()) if not merged[col].isna().all() else None
        columns.append(cp)

    ds_profile = DatasetProfile(
        row_count=len(merged),
        column_count=len(merged.columns),
        columns=columns,
        metric_cols=[c.name for c in columns if c.role == ColumnRole.METRIC],
    )

    return TabularDataset(df=merged, profile=ds_profile, source=f"{driver_code}_telemetry")


# ── Endpoints ───────────────────────────────────────────────────────────

@router.post("/load")
async def load_file(
    file: UploadFile = File(...),
    sample: Optional[int] = Form(None),
):
    """Load and profile a CSV/JSON/Excel file."""
    from omnidata import load, profile

    content = await file.read()
    filename = file.filename or "data.csv"

    ds = load(content, filename=filename, sample=sample)
    ds.profile = profile(ds)

    return ds.to_dict(include_data=True, max_rows=100)


@router.post("/profile/{driver_code}")
def profile_driver(driver_code: str):
    """Profile driver telemetry data — column stats, roles, distributions."""
    drivers = {"NOR": "Lando Norris", "PIA": "Oscar Piastri"}
    if driver_code not in drivers:
        raise HTTPException(404, f"Unknown driver: {driver_code}")

    ds = _load_driver_dataset(driver_code)
    return {
        "driver": drivers[driver_code],
        "code": driver_code,
        "profile": ds.profile.to_dict(),
    }


@router.post("/preprocess")
async def preprocess_file(
    file: UploadFile = File(...),
    coerce_types: bool = Form(True),
    normalize_time: bool = Form(True),
    fill_strategy: str = Form("median"),
):
    """Load, profile, and preprocess a dataset."""
    from omnidata import load, profile, preprocess

    content = await file.read()
    filename = file.filename or "data.csv"

    ds = load(content, filename=filename)
    ds.profile = profile(ds)
    ds = preprocess(ds, coerce_types=coerce_types, normalize_time=normalize_time, fill_strategy=fill_strategy)

    return ds.to_dict(include_data=True, max_rows=100)


@router.post("/features")
async def engineer_features(
    file: UploadFile = File(...),
    temporal: bool = Form(True),
    rolling_window: int = Form(3),
    sequence_context: bool = Form(True),
):
    """Load, preprocess, and engineer features for ML."""
    from omnidata import load, profile, preprocess, engineer

    content = await file.read()
    filename = file.filename or "data.csv"

    ds = load(content, filename=filename)
    ds.profile = profile(ds)
    ds = preprocess(ds)
    ds, new_cols = engineer(ds, temporal=temporal, rolling_window=rolling_window, sequence_context=sequence_context)

    return {
        **ds.to_dict(include_data=True, max_rows=100),
        "new_columns": new_cols,
    }


@router.post("/split")
async def train_test_split_endpoint(
    file: UploadFile = File(...),
    target_col: Optional[str] = Form(None),
    test_ratio: float = Form(0.2),
    scale: bool = Form(True),
):
    """Load, preprocess, and split data for ML training."""
    from omnidata import load, profile, preprocess
    from omnidata.features import train_test_split

    content = await file.read()
    filename = file.filename or "data.csv"

    ds = load(content, filename=filename)
    ds.profile = profile(ds)
    ds = preprocess(ds)

    result = train_test_split(
        ds.df,
        target_col=target_col,
        test_ratio=test_ratio,
        scale=scale,
    )

    return {
        "train_shape": list(result["X_train"].shape),
        "test_shape": list(result["X_test"].shape),
        "columns": list(result["X_train"].columns) if hasattr(result["X_train"], "columns") else [],
        "scaled": scale,
        "target_col": target_col,
    }
