"""OmniHealth APIRouter — predictive maintenance for F1 Fleet Overview.

Endpoints:
    GET /api/omni/health/assess/{driver_code}  — full HealthReport for one driver
    GET /api/omni/health/fleet                 — fleet-wide health for all drivers
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from pipeline.anomaly.run_f1_anomaly import (
    load_car_race_data,
    load_bio_race_data,
    merge_telemetry,
)
from omnihealth import assess

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/omni/health", tags=["OmniHealth"])

# Remapped SYSTEM_FEATURES using aggregated column names from load_car_race_data / load_bio_race_data
COMPONENT_MAP = {
    "Power Unit":   ["RPM_mean", "RPM_max", "RPM_std", "nGear_mean", "nGear_std"],
    "Brakes":       ["Brake_pct", "Speed_mean", "Speed_std"],
    "Drivetrain":   ["Throttle_mean", "Throttle_max", "Throttle_std", "DRS_pct"],
    "Suspension":   ["Speed_mean", "Speed_max", "Distance_mean", "Distance_std"],
    "Thermal":      ["HeartRate_bpm_mean", "CockpitTemp_C_mean", "AirTemp_C_mean", "TrackTemp_C_mean"],
    "Electronics":  ["DRS_pct", "RPM_mean", "nGear_mean"],
}

DRIVERS = {"NOR": "Lando Norris", "PIA": "Oscar Piastri"}


def _load_merged(driver_code: str):
    """Load and merge car + bio telemetry for a driver."""
    car_df = load_car_race_data(driver_code)
    if car_df.empty:
        raise HTTPException(404, f"No telemetry data for driver {driver_code}")
    bio_df = load_bio_race_data(driver_code)
    return merge_telemetry(car_df, bio_df)


def _filter_component_map(df):
    """Only include columns that actually exist in the DataFrame."""
    return {
        system: [c for c in cols if c in df.columns]
        for system, cols in COMPONENT_MAP.items()
        if any(c in df.columns for c in cols)
    }


@router.get("/assess/{driver_code}")
def assess_driver(
    driver_code: str,
    horizon: int = Query(10, ge=1, le=50),
    forecast_method: str = Query("auto"),
):
    """Run omnihealth.assess() on a driver's aggregated race telemetry."""
    driver_code = driver_code.upper()
    if driver_code not in DRIVERS:
        raise HTTPException(404, f"Unknown driver: {driver_code}. Valid: {list(DRIVERS)}")

    merged = _load_merged(driver_code)
    cmap = _filter_component_map(merged)

    report = assess(
        merged, cmap,
        horizon=horizon,
        forecast_method=forecast_method,
    )
    return {
        "driver": DRIVERS[driver_code],
        "code": driver_code,
        "races": len(merged),
        **report.to_dict(),
    }


@router.get("/fleet")
def fleet_health(horizon: int = Query(10, ge=1, le=50)):
    """Run health assessment for all drivers. Used by FleetOverview."""
    results = []
    for code, name in DRIVERS.items():
        try:
            merged = _load_merged(code)
            cmap = _filter_component_map(merged)
            report = assess(merged, cmap, horizon=horizon)
            results.append({
                "driver": name,
                "code": code,
                "number": {"NOR": 4, "PIA": 81}.get(code),
                "races": len(merged),
                **report.to_dict(),
            })
        except Exception as e:
            logger.warning(f"OmniHealth assess failed for {code}: {e}")
            results.append({"driver": name, "code": code, "error": str(e)})
    return {"drivers": results}
