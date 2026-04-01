"""
Compute Metrics mentioned in thesis chapter 3
"""
import argparse
import json
import logging
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from helpers import (
    get_mean_p95,
    compute_fal,
    compute_flip_rate_per_device,
    compute_latencies,
    compute_mean_time_between_flips,
    compute_missing_rate_per_device,
    compute_ml_metrics,
    compute_ooo_per_device,
    compute_rolling_features_and_integrity,
    derive_alarm_flag,
    get_events_per_device,
    event_time_to_s,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("metrics")
warnings.filterwarnings("ignore", category=FutureWarning)

AGG_KEYS = [
    "ingest_latency_s",
    "persist_latency_s",
    "availability_latency_s",
    "fal_total_window_s",
    "flag_flip_rate_per_hour",
    "mtbf_minutes",
    "missing_rate",
    "out_of_order_count",
    "window_integrity_score_5min",
]

COMPLETE_AGGREGATION_KEYS = [
    "out_of_order_rate",
]

ML_KEYS = ["ml_precision", "ml_recall", "ml_f1", "ml_support_positive", "ml_support_total"]

def compute_run_metrics(
    csv_path: Path,
    warmup_minutes: int = 5,
    measurement_minutes: int = 120,
    conductivity_threshold: float = 1.0,
    conductivity_relative_band: float = 0.1,
) -> dict:
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return clear_metrics()
    if df.empty:
        return clear_metrics()

    time_col = "date_time_utc"
    if time_col not in df.columns and "event_time_utc_ms" in df.columns:
        df[time_col] = event_time_to_s(df["event_time_utc_ms"])

    if time_col not in df.columns:
        return clear_metrics()

    df[time_col] = pd.to_numeric(df[time_col], errors="coerce")
    mask_ms = df[time_col] > 1e12
    if mask_ms.any():
        df.loc[mask_ms, time_col] = df.loc[mask_ms, time_col] / 1000.0

    t_min = df[time_col].min()
    t_max = df[time_col].max()
    warmup_cutoff = t_min + warmup_minutes * 60
    meas_cutoff = warmup_cutoff + measurement_minutes * 60

    if meas_cutoff > t_max and (t_max - t_min) < (warmup_minutes + measurement_minutes) * 60:
        meas_cutoff = t_max + 1

    df_meas = df[(df[time_col] >= warmup_cutoff) & (df[time_col] <= meas_cutoff)].copy()

    if df_meas.empty:
        df_meas = df.copy()
    if df_meas.empty:
        return clear_metrics()

    device_col = "mac_address_string"

    n_events = len(df_meas)
    n_devices = df_meas[device_col].nunique() if device_col in df_meas.columns else 0
    cadence = get_events_per_device(df_meas, device_col, time_col)

    latency_decomp = compute_latencies(df_meas)
    cfal = compute_fal(df_meas, device_col, time_col)

    alarm = derive_alarm_flag(
        df_meas,
        conductivity_threshold=conductivity_threshold,
        conductivity_relative_band=conductivity_relative_band,
    )
    alarm_rate = alarm.mean() if len(alarm) > 0 else 0.0

    flip_rate = compute_flip_rate_per_device(
        df_meas, device_col, time_col,
        conductivity_threshold=conductivity_threshold,
        conductivity_relative_band=conductivity_relative_band,
    )
    mtbf = compute_mean_time_between_flips(
        df_meas, device_col, time_col,
        conductivity_threshold=conductivity_threshold,
        conductivity_relative_band=conductivity_relative_band,
    )
    missing = compute_missing_rate_per_device(df_meas, cadence, device_col, time_col)
    ooo_per_dev, total_ooo = compute_ooo_per_device(df_meas, device_col, time_col)

    features_df, integrity = compute_rolling_features_and_integrity(
        df_meas, cadence, device_col, time_col
    )
    ml = compute_ml_metrics(
        df_meas,
        features_df,
        conductivity_threshold=conductivity_threshold,
        conductivity_relative_band=conductivity_relative_band,
    )

    ooo_rate = total_ooo / n_events if n_events > 0 else 0.0

    return {
        "ingest_latency_s": get_mean_p95(latency_decomp["ingest_s"]),
        "persist_latency_s": get_mean_p95(latency_decomp["persist_s"]),
        "availability_latency_s": get_mean_p95(latency_decomp["availability_s"]),
        "fal_total_window_s": get_mean_p95(cfal["fal_total_window_s"]),
        "flag_flip_rate_per_hour": get_mean_p95(flip_rate),
        "mtbf_minutes": get_mean_p95(mtbf),
        "missing_rate": get_mean_p95(missing),
        "out_of_order_count": total_ooo,
        "out_of_order_rate": round(ooo_rate, 6),
        "window_integrity_score_5min": get_mean_p95(integrity),
        "alarm_rate": round(alarm_rate, 6),
        "ml_precision": ml["precision"],
        "ml_recall": ml["recall"],
        "ml_f1": ml["f1"],
        "ml_support_positive": ml["support_positive"],
        "ml_support_total": ml["support_total"],
        "event_count": n_events,
        "device_count": n_devices,
    }


def aggregate_hardware_metrics(hardware_metrics_list: list[dict]) -> dict:
    agg = {}
    
    hw_metrics_list = [
        hw.get("hardware_metrics", {}) for hw in hardware_metrics_list
        if hw and isinstance(hw, dict) and hw.get("hardware_metrics")
    ]
    if hw_metrics_list:
        hardw_agg = {}
        for key in ["cpu_usage_percent", "memory_usage_gb"]:
            values_list = [hw.get(key) for hw in hw_metrics_list if hw.get(key) and hw.get(key) != {}]
            if values_list:
                means = [v.get("mean") for v in values_list if v.get("mean") is not None]
                p95s = [v.get("p95") for v in values_list if v.get("p95") is not None]
                p99s = [v.get("p99") for v in values_list if v.get("p99") is not None]
                maxs = [v.get("max") for v in values_list if v.get("max") is not None]
                if means:
                    hardw_agg[key] = {
                        "mean": round(float(np.mean(means)), 2) if means else None,
                        "p95": round(float(np.mean(p95s)), 2) if p95s else None,
                        "p99": round(float(np.mean(p99s)), 2) if p99s else None,
                        "max": round(float(np.max(maxs)), 2) if maxs else None,
                    }
        if hardw_agg:
            agg["hardware_metrics"] = hardw_agg
    
    return agg if agg else None


def _safe_metric_value(metric_entry, key: str) -> float | None:
    if isinstance(metric_entry, dict):
        val = metric_entry.get(key)
    else:
        val = metric_entry
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _compute_online_indices(agg: dict) -> dict:
    availability_p95 = _safe_metric_value(agg.get("availability_latency_s"), "p95")
    missing_mean = _safe_metric_value(agg.get("missing_rate"), "mean")
    window_integrity_mean = _safe_metric_value(agg.get("window_integrity_score_5min"), "mean")
    ooo_rate = _safe_metric_value(agg.get("out_of_order_rate"), "mean")
    if ooo_rate is None:
        ooo_rate = _safe_metric_value(agg.get("out_of_order_rate"), "")
    flip_p95 = _safe_metric_value(agg.get("flag_flip_rate_per_hour"), "p95")
    mtbf_mean = _safe_metric_value(agg.get("mtbf_minutes"), "mean")

    if ooo_rate is None:
        ooo_rate = _safe_metric_value(agg.get("out_of_order_rate"), "")
    if ooo_rate is None and isinstance(agg.get("out_of_order_rate"), (int, float)):
        ooo_rate = float(agg["out_of_order_rate"])

    n_al = (1.0 / (1.0 + availability_p95)) if availability_p95 is not None else None
    n_mr = (1.0 - missing_mean) if missing_mean is not None else None
    n_wi = window_integrity_mean
    n_ooo = (1.0 / (1.0 + 100.0 * ooo_rate)) if ooo_rate is not None else None
    n_flip = (1.0 / (1.0 + flip_p95)) if flip_p95 is not None else None
    n_mtbf = (min(1.0, mtbf_mean / 60.0)) if mtbf_mean is not None else None

    opi = None
    if n_al is not None and n_mr is not None:
        opi = n_al * n_mr
    oti = None
    if all(v is not None for v in [n_wi, n_ooo, n_flip, n_mtbf]):
        oti = n_wi * n_ooo * n_flip * n_mtbf
    ors = None
    if opi is not None and oti is not None and opi >= 0 and oti >= 0:
        ors = float(np.sqrt(opi * oti))

    return {
        "online_performance_index": round(opi, 6) if opi is not None else None,
        "online_trust_index": round(oti, 6) if oti is not None else None,
        "online_readiness_score": round(ors, 6) if ors is not None else None,
        "online_index_components": {
            "n_al": round(n_al, 6) if n_al is not None else None,
            "n_mr": round(n_mr, 6) if n_mr is not None else None,
            "n_wi": round(n_wi, 6) if n_wi is not None else None,
            "n_ooo": round(n_ooo, 6) if n_ooo is not None else None,
            "n_flip": round(n_flip, 6) if n_flip is not None else None,
            "n_mtbf": round(n_mtbf, 6) if n_mtbf is not None else None,
        },
    }


def clear_metrics() -> dict:
    result = {k: None for k in AGG_KEYS}
    result["out_of_order_count"] = 0
    result["out_of_order_rate"] = 0.0
    for k in ML_KEYS:
        result[k] = None
    result["alarm_rate"] = None
    result["event_count"] = 0
    result["device_count"] = 0
    return result

def aggregate_runs(run_metrics: list[dict]) -> dict:
    agg: dict = {}

    for key in AGG_KEYS:
        if key == "out_of_order_count":
            vals = [rm.get(key, 0) for rm in run_metrics if rm.get(key) is not None]
            agg[key] = int(sum(vals)) if vals else 0
            continue

        means = []
        stds = []
        p95s = []
        p99s = []
        for rm in run_metrics:
            v = rm.get(key)
            if v and isinstance(v, dict):
                if v.get("mean") is not None:
                    means.append(v["mean"])
                if v.get("std") is not None:
                    stds.append(v["std"])
                if v.get("p95") is not None:
                    p95s.append(v["p95"])
                if v.get("p99") is not None:
                    p99s.append(v["p99"])
        if means:
            entry: dict = {
                "mean": round(float(np.mean(means)), 6),
                "p95": round(float(np.mean(p95s)), 6) if p95s else None,
                "p99": round(float(np.mean(p99s)), 6) if p99s else None,
            }
            if len(means) > 1:
                entry["std_across_runs"] = round(float(np.std(means, ddof=1)), 6)
            else:
                entry["std_across_runs"] = 0.0
            if stds:
                entry["std_within_run_mean"] = round(float(np.mean(stds)), 6)
            agg[key] = entry
        else:
            agg[key] = None

    for key in COMPLETE_AGGREGATION_KEYS:
        vals = [rm.get(key) for rm in run_metrics if rm.get(key) is not None]
        if vals:
            agg[key] = round(float(np.mean(vals)), 6)
        else:
            agg[key] = None

    alarm_rates = [rm.get("alarm_rate", 0) for rm in run_metrics if rm.get("alarm_rate") is not None]
    agg["alarm_rate"] = round(float(np.mean(alarm_rates)), 6) if alarm_rates else None

    for k in ML_KEYS:
        vals = [rm.get(k) for rm in run_metrics if rm.get(k) is not None]
        if vals:
            if isinstance(vals[0], (int, float)):
                agg[k] = round(float(np.mean(vals)), 6) if any(isinstance(v, float) for v in vals) else int(np.mean(vals))
            else:
                agg[k] = None
        else:
            agg[k] = None

    hardware_metrics_list = [rm.get("hardware_metrics") for rm in run_metrics if rm.get("hardware_metrics")]
    if hardware_metrics_list:
        agg["hardware_metrics"] = aggregate_hardware_metrics(hardware_metrics_list)
    else:
        agg["hardware_metrics"] = None

    total_events = [rm.get("event_count", 0) for rm in run_metrics]
    agg["total_events_per_run"] = {
        "mean": round(float(np.mean(total_events)), 1) if total_events else 0,
        "min": int(min(total_events)) if total_events else 0,
        "max": int(max(total_events)) if total_events else 0,
    }
    total_devices = [rm.get("device_count", 0) for rm in run_metrics]
    agg["devices_per_run"] = {
        "mean": round(float(np.mean(total_devices)), 1) if total_devices else 0,
        "min": int(min(total_devices)) if total_devices else 0,
        "max": int(max(total_devices)) if total_devices else 0,
    }
    return agg

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default="../../results")
    ap.add_argument("--warmup-minutes", type=int, default=5)
    ap.add_argument("--measurement-minutes", type=int, default=120)
    ap.add_argument("--conductivity-threshold", type=float, default=1.0)
    ap.add_argument("--conductivity-relative-band", type=float, default=0.1)
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        log.error("Results directory does not exist: %s", results_dir)
        return

    manifest_path = results_dir / "manifest.json"
    manifest: list[dict] = []
    if manifest_path.exists():
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f) or []

    discovered: list[dict] = []
    for csv_path in sorted(results_dir.rglob("raw_data.csv")):
        parts = csv_path.relative_to(results_dir).parts
        if len(parts) >= 4 and parts[1].startswith("workload_") and parts[2].startswith("run_"):
            try:
                rep = int(parts[2].replace("run_", ""))
            except ValueError:
                continue
            discovered.append(
                {
                    "architecture": parts[0],
                    "workload": parts[1].replace("workload_", ""),
                    "repeat": rep,
                    "csv_path": str(csv_path),
                }
            )

    merged: dict[tuple[str, str, int], dict] = {}
    for entry in discovered + manifest:
        try:
            arch = entry["architecture"]
            wl = entry.get("workload", "A")
            rep = int(entry.get("repeat", 0))
        except Exception:
            continue
        key = (arch, wl, rep)
        if key not in merged:
            merged[key] = dict(entry)
            continue
        for k, v in entry.items():
            if merged[key].get(k) is None and v is not None:
                merged[key][k] = v
            elif k == "csv_path" and v:
                merged[key][k] = v

    manifest = sorted(merged.values(), key=lambda e: (e.get("architecture", ""), e.get("workload", ""), int(e.get("repeat", 0))))

    if not manifest:
        log.error("No runs found in %s", results_dir)
        return

    grouped: dict[str, dict[str, list[dict]]] = {}

    for entry in manifest:
        arch = entry["architecture"]
        wl = entry.get("workload", "A")
        csv_path = Path(entry.get("csv_path", ""))
        if not csv_path.is_absolute():
            csv_path = results_dir / csv_path
        if not csv_path.exists():
            rep = entry.get("repeat", 0)
            csv_path = results_dir / arch / f"workload_{wl}" / f"run_{rep}" / "raw_data.csv"

        rm = compute_run_metrics(
            csv_path, args.warmup_minutes, args.measurement_minutes,
            conductivity_threshold=args.conductivity_threshold,
            conductivity_relative_band=args.conductivity_relative_band,
        )
        rep = entry.get("repeat", 0)
        run_meta_path = results_dir / arch / f"workload_{wl}" / f"run_{rep}" / "run_meta.json"
        if run_meta_path.exists():
            try:
                with open(run_meta_path, encoding="utf-8") as f:
                    run_meta = json.load(f)
                    if "hardware_metrics" in run_meta:
                        rm["hardware_metrics"] = run_meta["hardware_metrics"]
            except Exception:
                pass
        
        grouped.setdefault(arch, {}).setdefault(wl, []).append(rm)
    summary: dict = {"architectures": {}}
    for arch, workloads in grouped.items():
        summary["architectures"][arch] = {}
        for wl, runs in workloads.items():
            agg = aggregate_runs(runs)
            agg["repeats"] = len(runs)
            agg["warmup_minutes"] = args.warmup_minutes
            agg["measurement_minutes"] = args.measurement_minutes
            run_entries_wl = [e for e in manifest if e["architecture"] == arch and e.get("workload") == wl]
            producer_reports = []
            for re_ in run_entries_wl:
                rep_n = re_.get("repeat", 0)
                rp = results_dir / arch / f"workload_{wl}" / f"run_{rep_n}" / "producer_report.json"
                if rp.exists():
                    with open(rp) as f:
                        producer_reports.append(json.load(f))

            if producer_reports:
                pr0 = producer_reports[0]
                agg["producer_mode"] = pr0.get("mode")
                agg["target_rate"] = pr0.get("rate_limit")
                agg["replication_factor"] = pr0.get("replication_factor")

                wc_secs = [p.get("wall_clock_seconds") for p in producer_reports if p.get("wall_clock_seconds")]
                wc_rates = [p.get("actual_msg_per_sec") for p in producer_reports if p.get("actual_msg_per_sec")]
                if wc_secs:
                    agg["producer_wall_clock_s"] = {
                        "mean": round(float(np.mean(wc_secs)), 1),
                        "min": round(float(min(wc_secs)), 1),
                        "max": round(float(max(wc_secs)), 1),
                    }
                if wc_rates:
                    agg["producer_actual_msg_per_sec"] = {
                        "mean": round(float(np.mean(wc_rates)), 1),
                        "min": round(float(min(wc_rates)), 1),
                        "max": round(float(max(wc_rates)), 1),
                    }
            produced_list = [e.get("produced_events", 0) for e in run_entries_wl if "produced_events" in e]
            written_list = [e.get("db_written_events", 0) for e in run_entries_wl if "db_written_events" in e]
            if produced_list:
                agg["produced_events_per_run"] = {
                    "mean": round(float(np.mean(produced_list)), 0),
                    "min": int(min(produced_list)),
                    "max": int(max(produced_list)),
                }
            if written_list:
                agg["db_written_events_per_run"] = {
                    "mean": round(float(np.mean(written_list)), 0),
                    "min": int(min(written_list)),
                    "max": int(max(written_list)),
                }
            db_rates = [e.get("db_write_rate_msg_s") for e in run_entries_wl
                        if e.get("db_write_rate_msg_s") is not None]
            if db_rates:
                agg["db_write_rate_msg_s"] = {
                    "mean": round(float(np.mean(db_rates)), 1),
                    "min": round(float(min(db_rates)), 1),
                    "max": round(float(max(db_rates)), 1),
                }
            agg.update(_compute_online_indices(agg))

            summary["architectures"][arch][f"workload_{wl}"] = agg

    summary_path = results_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

if __name__ == "__main__":
    main()