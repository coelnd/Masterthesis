"""
Helper utilities for metric computation
"""
import logging
from typing import Tuple
import numpy as np
import pandas as pd

log = logging.getLogger("metrics")

def event_time_to_s(s: pd.Series) -> pd.Series:
    v = pd.to_numeric(s, errors="coerce")
    return v.where(v <= 1e12, v / 1000.0)

def persisted_at_to_s(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    return dt.astype("int64") / 1e9

def get_events_per_device(
    df: pd.DataFrame,
    device_col: str = "mac_address_string",
    time_col: str = "date_time_utc",
) -> pd.Series:

    cadences = {}
    all_diffs: list[float] = []
    for device, group in df.groupby(device_col):
        ts = group[time_col].sort_values().dropna()
        if len(ts) < 2:
            cadences[device] = np.nan
            continue
        diffs = ts.diff().dropna()
        diffs = diffs[diffs > 0]
        if len(diffs) > 0:
            cadences[device] = float(diffs.median())
            all_diffs.extend(diffs.tolist())
        else:
            cadences[device] = np.nan

    if all_diffs:
        global_median = float(np.median(all_diffs))
        for device in cadences:
            if np.isnan(cadences[device]):
                cadences[device] = global_median

    return pd.Series(cadences, name="cadence_s")


def compute_fal(
    df: pd.DataFrame,
    device_col: str = "mac_address_string",
    time_col: str = "date_time_utc",
    window_seconds: int = 1200,
    allowed_lateness_s: float = 0.0,
) -> dict:
   
    empty = {
        "fal_total_window_s": pd.Series(dtype=float),
        "fal_pipeline_window_s": pd.Series(dtype=float),
        "fal_freshness_window_s": pd.Series(dtype=float),
        "fal_window_s": pd.Series(dtype=float),
        "window_emit_delay_s": pd.Series(dtype=float),
    }

    anchor_col = None
    if "kafka_logappend_ms" in df.columns and df["kafka_logappend_ms"].notna().any():
        anchor_col = "kafka_logappend_ms"
    elif "produced_at_ms" in df.columns and df["produced_at_ms"].notna().any():
        anchor_col = "produced_at_ms"
    if anchor_col is None or time_col not in df.columns:
        return empty
    
    persist_source = None
    persist_is_ms = False

    if "flags_computed_at" in df.columns and df["flags_computed_at"].notna().any():
        if "features_ready_ms" in df.columns and "speed_features_ready_at" in df.columns:
            if df["speed_features_ready_at"].notna().any():
                persist_source = "speed_features_ready_at"
                persist_is_ms = False
            elif df["features_ready_ms"].notna().any():
                persist_source = "features_ready_ms"
                persist_is_ms = True
        elif "features_ready_ms" in df.columns and df["features_ready_ms"].notna().any():
            persist_source = "features_ready_ms"
            persist_is_ms = True

    if persist_source is None and "speed_features_ready_at" in df.columns and df[
        "speed_features_ready_at"
    ].notna().any():
        persist_source = "speed_features_ready_at"
        persist_is_ms = False
    if persist_source is None and "stream_features_ready_at" in df.columns and df[
        "stream_features_ready_at"
    ].notna().any():
        persist_source = "stream_features_ready_at"
        persist_is_ms = False
    if persist_source is None and "features_ready_ms" in df.columns and df["features_ready_ms"].notna().any():
        persist_source = "features_ready_ms"
        persist_is_ms = True
    if persist_source is None and "persisted_at" in df.columns and df["persisted_at"].notna().any():
        persist_source = "persisted_at"
        persist_is_ms = False
    if persist_source is None:
        return empty

    if (
        persist_source == "stream_features_ready_at"
        and "persisted_at" in df.columns
        and df["persisted_at"].notna().any()
        and anchor_col in df.columns
        and df[anchor_col].notna().any()
    ):
        try:
            _anchor_s = pd.to_numeric(df[anchor_col], errors="coerce") / 1000.0
            _persisted_s = persisted_at_to_s(df["persisted_at"])
            _stream_s = persisted_at_to_s(df["stream_features_ready_at"])
            _persisted_med = float((_persisted_s - _anchor_s).dropna().median())
            _stream_med = float((_stream_s - _anchor_s).dropna().median())

            if 0 <= _persisted_med < 3600 and _stream_med - _persisted_med > 60:
                persist_source = "persisted_at"
                persist_is_ms = False
        except Exception:
            pass
    if (
        persist_source == "speed_features_ready_at"
        and "persisted_at" in df.columns
        and df["persisted_at"].notna().any()
        and anchor_col in df.columns
        and df[anchor_col].notna().any()
    ):
        try:
            _anchor_s = pd.to_numeric(df[anchor_col], errors="coerce") / 1000.0
            _persisted_s = persisted_at_to_s(df["persisted_at"])
            _speed_s = persisted_at_to_s(df["speed_features_ready_at"])
            _persisted_med = float((_persisted_s - _anchor_s).dropna().median())
            _speed_med = float((_speed_s - _anchor_s).dropna().median())

            if 0 <= _persisted_med < 3600 and _speed_med - _persisted_med > 60:
                persist_source = "persisted_at"
                persist_is_ms = False
        except Exception:
            pass
    evt_s = pd.to_numeric(df[time_col], errors="coerce").values
    anchor_s = pd.to_numeric(df[anchor_col], errors="coerce").values / 1000.0
    if persist_is_ms:
        persist_s = pd.to_numeric(df[persist_source], errors="coerce").values / 1000.0
    else:
        persist_s = persisted_at_to_s(df[persist_source]).values
    order = np.argsort(evt_s)
    sorted_evt = evt_s[order]
    sorted_anchor = anchor_s[order]

    valid_evt = sorted_evt[~np.isnan(sorted_evt)]
    if len(valid_evt) < 2:
        return empty
    t_min = float(valid_evt[0])
    t_max = float(valid_evt[-1])

    window_starts = np.arange(t_min, t_max, window_seconds)

    fal_total_values: list[float] = []
    fal_pipeline_values: list[float] = []
    fal_freshness_values: list[float] = []

    for ws in window_starts:
        we = ws + window_seconds

        mask_w = (evt_s >= ws) & (evt_s < we) & ~np.isnan(persist_s)
        if mask_w.sum() == 0:
            continue

        flag_visible_s = float(np.nanmax(persist_s[mask_w]))

        window_anchor = anchor_s[mask_w]
        valid_anchor = window_anchor[~np.isnan(window_anchor)]
        if len(valid_anchor) == 0:
            continue

        min_kafka_in_window = float(np.nanmin(valid_anchor))
        max_kafka_in_window = float(np.nanmax(valid_anchor))
        fal_total = flag_visible_s - min_kafka_in_window
        fal_total_values.append(max(0.0, fal_total))
        fal_pipeline = flag_visible_s - max_kafka_in_window
        fal_pipeline_values.append(max(0.0, fal_pipeline))
        closing_threshold = we + allowed_lateness_s
        idx = np.searchsorted(sorted_evt, closing_threshold, side="left")

        while idx < len(sorted_anchor) and np.isnan(sorted_anchor[idx]):
            idx += 1

        if idx >= len(sorted_anchor):
            fal_freshness_values.append(0.0)
            continue

        window_ready_s = float(sorted_anchor[idx])
        if np.isnan(window_ready_s):
            fal_freshness_values.append(0.0)
            continue

        raw_freshness = flag_visible_s - window_ready_s
        if raw_freshness < 0:
            fal_freshness_values.append(0.0)
        else:
            fal_freshness_values.append(raw_freshness)

    result = {
        "fal_total_window_s": pd.Series(fal_total_values, dtype=float),
        "fal_pipeline_window_s": pd.Series(fal_pipeline_values, dtype=float),
        "fal_freshness_window_s": pd.Series(fal_freshness_values, dtype=float),
        "fal_window_s": pd.Series(fal_freshness_values, dtype=float),
        "window_emit_delay_s": pd.Series(fal_pipeline_values, dtype=float),
    }
    return result

def conductivity_violation(
    cond: pd.Series,
    sp: pd.Series,
    *,
    conductivity_threshold: float = 1.0,
    conductivity_relative_band: float = 0.1,
) -> pd.Series:
    valid = (sp.fillna(0) > 0) & cond.notna() & sp.notna()
    tol = np.maximum(sp * float(conductivity_relative_band), float(conductivity_threshold))
    return valid & ((cond - sp).abs() > tol)


def compute_latencies(df: pd.DataFrame) -> dict[str, pd.Series]:
    result: dict[str, pd.Series] = {
        "ingest_s": pd.Series(dtype=float),
        "persist_s": pd.Series(dtype=float),
        "availability_s": pd.Series(dtype=float),
    }
    persisted_s = None
    if "persisted_at" in df.columns and df["persisted_at"].notna().any():
        persisted_s = persisted_at_to_s(df["persisted_at"])

    kafka_s = None
    if "kafka_logappend_ms" in df.columns:
        kafka_s = pd.to_numeric(df["kafka_logappend_ms"], errors="coerce") / 1000.0

    produced_s = None
    if "produced_at_ms" in df.columns:
        produced_s = pd.to_numeric(df["produced_at_ms"], errors="coerce") / 1000.0

    if kafka_s is not None and produced_s is not None:
        ingest = (kafka_s - produced_s).dropna()
        if len(ingest) > 0 and ingest.median() < 3600:
            result["ingest_s"] = ingest.clip(lower=0.0)

    if "event_latency_seconds" in df.columns:
        lat = pd.to_numeric(df["event_latency_seconds"], errors="coerce").dropna()
        if len(lat) > 0 and lat.median() < 3600:
            result["persist_s"] = lat
    if result["persist_s"].empty and persisted_s is not None and kafka_s is not None:
        lat = (persisted_s - kafka_s).dropna()
        if len(lat) > 0 and lat.median() >= 0:
            result["persist_s"] = lat

    if persisted_s is not None and produced_s is not None:
        avail = (persisted_s - produced_s).dropna()
        if len(avail) > 0 and avail.median() < 3600:
            result["availability_s"] = avail

    return result


def derive_alarm_flag(
    df: pd.DataFrame,
    conductivity_threshold: float = 1.0,
    conductivity_relative_band: float = 0.1,
    opstate_threshold: int = 100,
) -> pd.Series:
    flag = pd.Series(np.zeros(len(df)), index=df.index, dtype=int)

    if "y_alarm" in df.columns:
        ya = pd.to_numeric(df["y_alarm"], errors="coerce").fillna(0).astype(int)
        if ya.sum() > 0:
            flag = flag | ya

    rule_cols = (
        "temp_below_setpoint",
        "rinse_temp_below_setpoint",
        "conc_constant_over",
        "conc_constant_under",
        "wash_temp_recovery_failure",
        "rinse_temp_recovery_failure",
        "conc_recovery_failure",
        "wash_temp_repeated_issue",
        "rinse_temp_repeated_issue",
        "conc_repeated_issue",
        "wash_temp_high_variance",
        "rinse_temp_high_variance",
    )
    for c in rule_cols:
        if c not in df.columns:
            continue
        flag = flag | pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int).clip(0, 1)

    if "mean_conductivity" in df.columns and "setpoint_conductivity" in df.columns:
        cond = pd.to_numeric(df["mean_conductivity"], errors="coerce")
        sp = pd.to_numeric(df["setpoint_conductivity"], errors="coerce")
        cond_alarm = conductivity_violation(
            cond,
            sp,
            conductivity_threshold=conductivity_threshold,
            conductivity_relative_band=conductivity_relative_band,
        ).astype(int)
        flag = flag | cond_alarm

    if "op_state" in df.columns:
        op = pd.to_numeric(df["op_state"], errors="coerce").fillna(0)
        flag = flag | (op >= opstate_threshold).astype(int)

    return flag

def compute_flip_rate_per_device(
    df: pd.DataFrame,
    device_col: str = "mac_address_string",
    time_col: str = "date_time_utc",
    conductivity_threshold: float = 1.0,
    conductivity_relative_band: float = 0.1,
) -> pd.Series:
    
    flag = derive_alarm_flag(
        df,
        conductivity_threshold=conductivity_threshold,
        conductivity_relative_band=conductivity_relative_band,
    )
    arrival_col = check_arrival_col(df)
    result = {}
    for dev, idx in df.groupby(device_col).groups.items():
        grp = df.loc[idx].sort_values([arrival_col, "kafka_logappend_ms"]) if "kafka_logappend_ms" in df.columns else df.loc[idx].sort_values(arrival_col) 
        f = flag.loc[grp.index]
        flips = (f.diff().abs() > 0).sum()
        t = pd.to_numeric(grp[time_col], errors="coerce")
        span_hours = (t.max() - t.min()) / 3600.0 if len(t) > 1 else 0.0
        result[dev] = flips / span_hours if span_hours > 0 else 0.0
    return pd.Series(result, name="flip_rate_per_hour")


def check_arrival_col(df: pd.DataFrame) -> str:
    if "persisted_at" in df.columns and df["persisted_at"].notna().any():
        return "persisted_at"
    return "date_time_utc"

def compute_mean_time_between_flips(
    df: pd.DataFrame,
    device_col: str = "mac_address_string",
    time_col: str = "date_time_utc",
    conductivity_threshold: float = 1.0,
    conductivity_relative_band: float = 0.1,
) -> pd.Series:

    flag = derive_alarm_flag(
        df,
        conductivity_threshold=conductivity_threshold,
        conductivity_relative_band=conductivity_relative_band,
    )
    arrival_col = check_arrival_col(df)
    result = {}
    for device, index in df.groupby(device_col).groups.items():
        grp = df.loc[index].sort_values([arrival_col, "kafka_logappend_ms"]) if "kafka_logappend_ms" in df.columns else df.loc[index].sort_values(arrival_col) 
        f = flag.loc[grp.index]
        t = pd.to_numeric(grp[time_col], errors="coerce")
        flip_mask = f.diff().abs() > 0
        flip_times = t[flip_mask].values
        if len(flip_times) >= 2:
            gaps = np.diff(flip_times)
            result[device] = float(np.mean(gaps)) / 60.0
        else:
            result[device] = np.nan
    return pd.Series(result, name="mtbf_minutes")

def compute_missing_rate_per_device(
    df: pd.DataFrame,
    cadence_map: pd.Series,
    device_col: str = "mac_address_string",
    time_col: str = "date_time_utc",
    gap_factor: float = 3.0,
) -> pd.Series:
    
    result = {}
    for dev, grp in df.groupby(device_col):
        ts = pd.to_numeric(grp[time_col], errors="coerce").sort_values().dropna()
        if len(ts) < 2:
            result[dev] = np.nan
            continue
        cad = cadence_map.get(dev, np.nan)
        if np.isnan(cad) or cad <= 0:
            result[dev] = np.nan
            continue
        diffs = ts.diff().dropna()
        diffs = diffs[diffs > 0]
        if len(diffs) == 0:
            result[dev] = np.nan
            continue
        gap_threshold = gap_factor * cad
        n_gaps = int((diffs > gap_threshold).sum())
        result[dev] = float(n_gaps / len(diffs))
    return pd.Series(result, name="gap_rate")

def compute_ooo_per_device(
    df: pd.DataFrame,
    device_col: str = "mac_address_string",
    time_col: str = "date_time_utc",
) -> Tuple[pd.Series, int]:
    
    arrival_col = check_arrival_col(df)
    result = {}
    total = 0
    for dev, grp in df.groupby(device_col):
        g = grp.sort_values([arrival_col, "kafka_logappend_ms"]) if "kafka_logappend_ms" in df.columns else grp.sort_values(arrival_col)
        ts = pd.to_numeric(g[time_col], errors="coerce")
        ooo = int((ts.diff() < 0).sum())
        result[dev] = ooo
        total += ooo
    return pd.Series(result, name="ooo_count"), total

def compute_rolling_features_and_integrity(
    df: pd.DataFrame,
    cadence_map: pd.Series,
    device_col: str = "mac_address_string",
    time_col: str = "date_time_utc",
    window_seconds: int = 300,
) -> Tuple[pd.DataFrame, pd.Series]:
   
    feature_cols = {
        "mean_conductivity": "conductivity_mean_5min",
        "mean_wash_tank_temp": "wash_temp_mean_5min",
        "mean_rinse_temp": "rinse_temp_mean_5min",
    }

    all_integrity: list[float] = []
    parts: list[pd.DataFrame] = []

    for dev, grp in df.groupby(device_col):
        g = grp.sort_values(time_col).copy()
        t = pd.to_numeric(g[time_col], errors="coerce")
        if t.isna().all():
            parts.append(g)
            continue

        g["_evt_dt"] = pd.to_datetime(t, unit="s", utc=True)
        g = g.set_index("_evt_dt", drop=False)

        for src, dst in feature_cols.items():
            if src in g.columns:
                vals = pd.to_numeric(g[src], errors="coerce")
                g[dst] = vals.rolling(f"{window_seconds}s", min_periods=1).mean()
            else:
                g[dst] = np.nan

        cad = cadence_map.get(dev, np.nan)
        if not np.isnan(cad) and cad > 0:
            expected_per_window = window_seconds / cad
            ts_vals = t.values
            if len(ts_vals) > 0:
                t_min, t_max = float(np.nanmin(ts_vals)), float(np.nanmax(ts_vals))
                bins = np.arange(t_min, t_max + window_seconds, window_seconds)
                counts, _ = np.histogram(ts_vals, bins=bins)
                for c in counts:
                    score = min(1.0, c / expected_per_window) if expected_per_window > 0 else 1.0
                    all_integrity.append(score)

        g = g.reset_index(drop=True)
        parts.append(g)

    out_df = pd.concat(parts, ignore_index=True) if parts else df.copy()
    integrity = pd.Series(all_integrity, name="window_integrity_5min", dtype=float)
    return out_df, integrity

def compute_ml_metrics(
    df: pd.DataFrame,
    features_df: pd.DataFrame,
    conductivity_threshold: float = 1.0,
    conductivity_relative_band: float = 0.1,
) -> dict:
  
    result = {
        "precision": None,
        "recall": None,
        "f1": None,
        "support_positive": 0,
        "support_total": 0,
    }

    sp_col = "setpoint_conductivity"
    raw_col = "mean_conductivity"
    feat_col = "conductivity_mean_5min"

    if raw_col not in df.columns or sp_col not in df.columns:
        return result
    if feat_col not in features_df.columns:
        return result

    common_idx = df.index.intersection(features_df.index)
    if len(common_idx) == 0:
        return result

    raw_cond = pd.to_numeric(df.loc[common_idx, raw_col], errors="coerce")
    sp = pd.to_numeric(df.loc[common_idx, sp_col], errors="coerce")
    feat_cond = pd.to_numeric(features_df.loc[common_idx, feat_col], errors="coerce")

    valid = (sp > 0) & raw_cond.notna() & feat_cond.notna()
    if valid.sum() == 0:
        return result

    raw_cond = raw_cond[valid]
    sp = sp[valid]
    feat_cond = feat_cond[valid]

    tol = np.maximum(sp.to_numpy(dtype=float) * float(conductivity_relative_band), float(conductivity_threshold))
    y_true = ((raw_cond - sp).abs() > tol).astype(int)
    y_pred = ((feat_cond - sp).abs() > tol).astype(int)

    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())

    precision = tp / (tp + fp) if (tp + fp) > 0 else None
    recall = tp / (tp + fn) if (tp + fn) > 0 else None
    if precision is not None and recall is not None and (precision + recall) > 0:
        f1 = 2 * precision * recall / (precision + recall)
    else:
        f1 = None

    result["precision"] = round(precision, 6) if precision is not None else None
    result["recall"] = round(recall, 6) if recall is not None else None
    result["f1"] = round(f1, 6) if f1 is not None else None
    result["support_positive"] = int(y_true.sum())
    result["support_total"] = int(len(y_true))

    return result

def get_mean_p95(s: pd.Series | None) -> dict | None:
    if s is None or len(s) == 0:
        return None
    s = s.dropna()
    if len(s) == 0:
        return None
    return {
        "mean": round(float(s.mean()), 6),
        "std": round(float(s.std()), 6) if len(s) > 1 else 0.0,
        "p95": round(float(s.quantile(0.95)), 6),
        "p99": round(float(s.quantile(0.99)), 6),
    }