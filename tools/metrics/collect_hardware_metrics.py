"""CPU and RAM from Prometheus"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
import numpy as np
import requests

log = logging.getLogger("hardware_metrics")

def query_range(
    base_url: str,
    query: str,
    start: float,
    end: float,
    step: int,
    timeout: int = 10,
) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/api/v1/query_range"
    r = requests.get(
        url,
        params={"query": query, "start": start, "end": end, "step": step},
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        log.warning("Prometheus: %s", data.get("error", "unknown"))
        return []
    out = []
    for item in data.get("data", {}).get("result", []):
        series = []
        for ts, val in item.get("values", []):
            try:
                v = float(val) if val != "NaN" else None
            except (ValueError, TypeError):
                v = None
            if v is not None:
                series.append({"timestamp": float(ts), "value": v})
        if series:
            out.append({"metric": item.get("metric", {}), "series": series})
    return out


def stats_from_range(results: list[dict[str, Any]]) -> dict[str, float] | None:
    vals = [p["value"] for r in results for p in r.get("series", []) if p.get("value") is not None]
    if not vals:
        return None
    a = np.asarray(vals, dtype=float)
    return {
        "mean": round(float(np.mean(a)), 2),
        "p95": round(float(np.percentile(a, 95)), 2),
        "p99": round(float(np.percentile(a, 99)), 2),
        "max": round(float(np.max(a)), 2),
    }


def collect_cpu_ram(
    prometheus_url: str,
    start_time: float,
    end_time: float,
    step: int = 15,
) -> dict[str, Any]:
    hw: dict[str, Any] = {}

    cpu_query = '100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)'
    cpu = query_range(prometheus_url, cpu_query, start_time, end_time, step)
    if not cpu:
        cpu = query_range(
            prometheus_url,
            'sum(rate(container_cpu_usage_seconds_total{image!=""}[1m])) * 100',
            start_time,
            end_time,
            step,
        )
    stat = stats_from_range(cpu)
    if stat:
        hw["cpu_usage_percent"] = stat

    mem_q = "(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / 1024^3"
    mem = query_range(prometheus_url, mem_q, start_time, end_time, step)
    if not mem:
        mem = query_range(
            prometheus_url,
            'sum(container_memory_usage_bytes{image!=""}) / 1024^3',
            start_time,
            end_time,
            step,
        )
    stat = stats_from_range(mem)
    if stat:
        hw["memory_usage_gb"] = stat

    return hw


def collect_all_metrics(
    prometheus_url: str,
    start_time: float,
    duration_seconds: int,
    step: int = 15,
) -> dict[str, Any]:
    end_time = start_time + duration_seconds
    log.info("Prometheus %s  range %s–%s (%ds)", prometheus_url, start_time, end_time, duration_seconds)
    try:
        hardware = collect_cpu_ram(prometheus_url, start_time, end_time, step)
    except Exception as e:
        log.error("Hardware metrics failed: %s", e)
        hardware = {}
    return {
        "collection_start_time": start_time,
        "collection_end_time": end_time,
        "collection_duration_seconds": duration_seconds,
        "hardware_metrics": hardware,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prometheus-url", default="http://localhost:9090")
    ap.add_argument("--start-time", required=True)
    ap.add_argument("--duration-seconds", type=int, required=True)
    ap.add_argument("--step", type=int, default=15)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    try:
        start = float(args.start_time) if args.start_time.isdigit() else datetime.fromisoformat(
            args.start_time.replace("Z", "+00:00")
        ).timestamp()
    except (ValueError, AttributeError):
        log.error("Invalid --start-time: %s", args.start_time)
        return 1

    try:
        payload = collect_all_metrics(args.prometheus_url, start, args.duration_seconds, args.step)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log.info("Saved %s", args.output)
        return 0
    except Exception as e:
        log.error("Failed: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())