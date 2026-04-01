"""
Feature engineering for ML evaluation
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Optional, Sequence
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_all_features(
    df: pd.DataFrame,
    *,
    device_id_col: str = "device_id",
    latency_col: str = "latency_seconds",
    timestamp_col: str = "date_time_utc",
    processing_timestamp_col: str = "ingest_at",
    expected_rate_per_minute: Optional[float] = None,
    flag_columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    computed = df.copy()

    # Latency
    if latency_col in computed.columns:
        latency = pd.to_numeric(computed[latency_col], errors="coerce")
        computed["latency_raw"] = latency
        computed["latency_rolling_mean_5min"] = latency.rolling(5, min_periods=1).mean()
        computed["latency_volatility"] = latency.rolling(5, min_periods=2).std().fillna(0)
    else:
        for c in ["latency_raw", "latency_rolling_mean_5min", "latency_volatility"]:
            computed[c] = np.nan

    # Missing rate
    measure_cols = [c for c in ["mean_wash_tank_temp", "mean_rinse_temp", "mean_conductivity"] if c in computed.columns]
    if measure_cols:
        computed["missing_rate"] = computed[measure_cols].isna().mean(axis=1)
    else:
        computed["missing_rate"] = 0.0

    # Field consistency
    computed["field_consistency_score"] = 1.0 - computed["missing_rate"]

    # Timestamp skew
    if timestamp_col in computed.columns and processing_timestamp_col in computed.columns:
        evt = pd.to_numeric(computed[timestamp_col], errors="coerce")
        proc = computed[processing_timestamp_col]
        if pd.api.types.is_datetime64_any_dtype(proc):
            proc_epoch = proc.astype("int64") // 10**9
        else:
            proc_epoch = pd.to_numeric(proc, errors="coerce")
        computed["timestamp_skew_ms"] = (proc_epoch - evt).abs() * 1000
    else:
        computed["timestamp_skew_ms"] = np.nan

    # Out of order
    if timestamp_col in computed.columns:
        ts = pd.to_numeric(computed[timestamp_col], errors="coerce")
        computed["out_of_order_indicator"] = ts.diff().fillna(0) < 0
    else:
        computed["out_of_order_indicator"] = False

    # Event count ratios / window integrity
    rate = expected_rate_per_minute if expected_rate_per_minute else 60.0
    for window in (1, 5, 15):
        expected = rate * window
        computed[f"event_count_ratio_{window}min"] = 1.0
        computed[f"window_integrity_score_{window}min"] = min(len(computed) / max(expected, 1), 1.0)

    # Flag stability
    flag_cols = list(flag_columns or [])
    existing_flags = [c for c in flag_cols if c in computed.columns]
    if existing_flags:
        flips = sum(computed[c].astype(float).diff().abs().fillna(0) for c in existing_flags)
        computed["flag_flip_rate"] = flips / max(len(existing_flags), 1)

        temp_flags = [c for c in existing_flags if "temp" in c]
        conc_flags = [c for c in existing_flags if "conc" in c]
        computed["freq_temp_flag"] = sum(computed[c].astype(float).fillna(0) for c in temp_flags).clip(0, 1) if temp_flags else 0.0
        computed["freq_conductivity_flag"] = sum(computed[c].astype(float).fillna(0) for c in conc_flags).clip(0, 1) if conc_flags else 0.0
        computed["combined_flag_stability"] = 1.0 - (computed["flag_flip_rate"] / max(len(computed), 1)).clip(0, 1)
    else:
        computed["flag_flip_rate"] = 0.0
        computed["freq_temp_flag"] = 0.0
        computed["freq_conductivity_flag"] = 0.0
        computed["combined_flag_stability"] = 1.0

    computed["features_computed_at"] = datetime.now(timezone.utc)

    return computed