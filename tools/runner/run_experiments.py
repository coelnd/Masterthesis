import csv
import json
import logging
import os
import subprocess
import sys
import time
import psycopg2
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "experiment_config.yaml"
PRODUCER_PY = PROJECT_ROOT / "tools" / "producer" / "producer.py"
METRICS_PY = PROJECT_ROOT / "tools" / "metrics" / "compute_metrics.py"
HARDWARE_METRICS_PY = PROJECT_ROOT / "tools" / "metrics" / "collect_hardware_metrics.py"
PLOT_RESULTS_PY = PROJECT_ROOT / "tools" / "visualization" / "plot_results.py"

EXTRACT_TABLE = "dishwasher_data"
MAC_COL = "mac_address_string"
ARCHITECTURES = (
    "baseline",
    "lambda",
    "kappa",
    "datafabric",
    "datamesh",
)
ARCH_FILE_STRUCT = {
    "baseline": {
        "compose_dir": PROJECT_ROOT / "stacks" / "baseline",
        "table": EXTRACT_TABLE,
        "mac_col": MAC_COL,
    },
    "lambda": {
        "compose_dir": PROJECT_ROOT / "stacks" / "lambda",
        "table": EXTRACT_TABLE,
        "mac_col": MAC_COL,
    },
    "kappa": {
        "compose_dir": PROJECT_ROOT / "stacks" / "kappa",
        "table": EXTRACT_TABLE,
        "mac_col": MAC_COL,
    },
    "datafabric": {
        "compose_dir": PROJECT_ROOT / "stacks" / "datafabric",
        "table": EXTRACT_TABLE,
        "mac_col": MAC_COL,
    },
    "datamesh": {
        "compose_dir": PROJECT_ROOT / "stacks" / "datamesh",
        "table": EXTRACT_TABLE,
        "mac_col": MAC_COL,
    },
}

EXTRACT_COLS = [
    "mac_address_string",
    "beacon_event_id",
    "date_time_utc",
    "event_time_utc_ms",
    "kafka_logappend_ms",
    "produced_at_ms",
    "event_latency_seconds",
    "fal_seconds",
    "features_ready_ms",
    "flags_computed_at",
    "speed_features_ready_at",
    "stream_features_ready_at",
    "op_state",
    "mean_conductivity",
    "setpoint_conductivity",
    "mean_wash_tank_temp",
    "mean_rinse_temp",
    "persisted_at",
]

DRAIN_TIMEOUT_SECONDS = 120
DRAIN_STABLE_SECONDS = 20
CONDUCTIVITY_ANOMALY_THRESHOLD = 1.0
PROMETHEUS_URL = "http://localhost:9090"

def load_experiment_config(config_path: Path) -> dict:
    if not config_path.is_file():
        raise FileNotFoundError(f"Experiment config not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if raw is None:
        raise ValueError(f"Experiment config is empty: {config_path}")
    return raw

def _cfg(cfg: dict, *keys: str) -> Any:
    parent: Any = cfg
    acc: list[str] = []
    for k in keys:
        acc.append(k)
        if not isinstance(parent, dict) or k not in parent:
            raise KeyError(".".join(acc))
        parent = parent[k]
    return parent

def setup_logging(config: dict, run_log_path: Path | None = None) -> logging.Logger:
    level_name = str(_cfg(config, "logging", "level")).upper()
    level = getattr(logging, level_name, logging.INFO)

    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(ch)
    if _cfg(config, "logging", "log_to_file") and run_log_path:
        run_log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(str(run_log_path), encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        root.addHandler(fh)

    return logging.getLogger("runner")


def add_run_file_handler(config: dict, log_path: Path):
    root = logging.getLogger()
    level_name = str(_cfg(config, "logging", "level")).upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    root.handlers = [h for h in root.handlers if not isinstance(h, logging.FileHandler)]

    if _cfg(config, "logging", "log_to_file"):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(str(log_path), encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        root.addHandler(fh)

def get_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def elapsed_time(start: float) -> str:
    s = time.monotonic() - start
    if s < 60:
        return f"{s:.1f}s"
    m, sec = divmod(int(s), 60)
    return f"{m}m {sec}s"


def run(cmd: list[str], cwd: str | Path | None = None, check: bool = True,
         timeout: int = 300, log: logging.Logger | None = None):
    log = log or logging.getLogger("runner")
    log.info("$ %s", " ".join(str(c) for c in cmd))
    result = subprocess.run(cmd, cwd=cwd, check=False, timeout=timeout,
                            capture_output=True, text=True)
    if result.returncode != 0:
        stderr_tail = (result.stderr or "")[-3000:]
        log.warning("Command exited %d. stderr:\n%s", result.returncode, stderr_tail)
        if check:
            raise subprocess.CalledProcessError(result.returncode, cmd)
    return result

def compose_command(compose_dir: Path) -> list[str]:
    return ["docker", "compose", "-f", str(compose_dir / "docker-compose.yml")]

def start_stack(arch: str, log: logging.Logger):
    cfg = ARCH_FILE_STRUCT[arch]
    log.info("[STACK] Starting %s", arch)
    run(compose_command(cfg["compose_dir"]) + ["up", "-d", "--build"],
         cwd=cfg["compose_dir"], check=False, timeout=600, log=log)
    log.info("[STACK] docker compose up finished")


def stop_stack(arch: str, log: logging.Logger):
    cfg = ARCH_FILE_STRUCT[arch]
    log.info("[STACK] Stopping %s", arch)
    run(compose_command(cfg["compose_dir"]) + ["down", "-v", "--remove-orphans"],
         cwd=cfg["compose_dir"], check=False, timeout=120, log=log)
    log.info("[STACK] docker compose down finished")


def start_hardware_metrics_collection(
    prometheus_url: str,
    start_time: float,
    duration_seconds: int,
    output_path: Path,
    log: logging.Logger,
) -> subprocess.Popen | None:
    try:
        cmd = [
            sys.executable,
            str(HARDWARE_METRICS_PY),
            "--prometheus-url", prometheus_url,
            "--start-time", str(int(start_time)),
            "--duration-seconds", str(duration_seconds),
            "--output", str(output_path),
        ]
        log.debug("[METRICS] Starting: %s", " ".join(cmd))
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return process
    except Exception as e:
        log.error("[METRICS] Failed to start collection process: %s", e)
        return None

def run_producer(
    wl_key: str,
    wl_cfg: dict,
    duration_minutes: int,
    data_dir: Path,
    broker: str,
    topic: str,
    report_path: Path,
    log: logging.Logger,
) -> int:
    mode = wl_cfg.get("mode", "real")
    speed_factor = wl_cfg.get("speed_factor", 10)
    replication = wl_cfg.get("replication_factor", 1)
    rate = wl_cfg.get("rate")
    fault = wl_cfg.get("fault_injection", 0.0)
    schema_violation = wl_cfg.get("schema_violation_rate", 0.0)
    sustained = wl_cfg.get("sustained_seconds", 0)

    cmd = [
        sys.executable, str(PRODUCER_PY),
        "--data-dir", str(data_dir),
        "--broker", broker,
        "--topic", topic,
        "--mode", mode,
        "--speed-factor", str(speed_factor),
        "--replication-factor", str(replication),
        "--duration-minutes", str(duration_minutes),
    ]
    if rate is not None:
        cmd += ["--rate", str(rate)]
    if fault and fault > 0:
        cmd += ["--fault-injection", str(fault)]
    if schema_violation and schema_violation > 0:
        cmd += ["--schema-violation-rate", str(schema_violation)]
    if sustained and sustained > 0:
        cmd += ["--sustained-seconds", str(sustained)]

    env = os.environ.copy()
    env["PRODUCER_REPORT"] = str(report_path)

    log.info("[PRODUCER] Starting workload %s (mode=%s speed=%.1f repl=%d rate=%s) …",
             wl_key, mode, speed_factor, replication, rate)
    start = time.monotonic()
    result = subprocess.run(cmd, env=env, timeout=7200, capture_output=True, text=True)

    if result.returncode != 0:
        log.error("[PRODUCER] Failed.")
    else:
        log.info("[PRODUCER] Finished OK in %s", elapsed_time(start))
    return result.returncode

def pg_connect(config: dict):
    
    pg = _cfg(config, "postgres")
    return psycopg2.connect(
        host=pg["host"],
        port=int(pg["port"]),
        dbname=pg["dbname"],
        user=pg["user"],
        password=pg["password"],
    )


def wait_drain(arch: str, config: dict, log: logging.Logger) -> int:
    ameta = ARCH_FILE_STRUCT[arch]
    table = ameta["table"]
    base_timeout = DRAIN_TIMEOUT_SECONDS
    if arch == "datafabric":
        timeout_s = max(base_timeout, 900)
    else:
        timeout_s = base_timeout
    stable_s = DRAIN_STABLE_SECONDS

    prev_count = -1
    stable_since = None
    deadline = time.monotonic() + timeout_s
    start = time.monotonic()

    while time.monotonic() < deadline:
        try:
            conn = pg_connect(config)
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table}")  
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
        except Exception as exc:
            log.debug("[DRAIN] pg query failed: %s", exc)
            time.sleep(5)
            continue

        if count == prev_count and count > 0:
            if stable_since is None:
                stable_since = time.monotonic()
            elif time.monotonic() - stable_since >= stable_s:
                return count
        else:
            stable_since = None
        prev_count = count
        time.sleep(5)

    return prev_count


def extract_data(arch: str, config: dict, out_csv: Path, log: logging.Logger,
                 run_id: str = "") -> int:
    ameta = ARCH_FILE_STRUCT[arch]
    table = ameta["table"]
    mac_col = ameta["mac_col"]

    select_parts = []
    for col in EXTRACT_COLS:
        if col == "mac_address_string" and mac_col != "mac_address_string":
            select_parts.append(f"{mac_col} AS mac_address_string")
        elif col == "date_time_utc":
            select_parts.append(
                "CASE WHEN event_time_utc_ms > 1000000000000 "
                "THEN event_time_utc_ms / 1000 "
                "ELSE event_time_utc_ms END AS date_time_utc"
            )
        else:
            select_parts.append(col)
    order_expr = (
        "CASE WHEN event_time_utc_ms > 1000000000000 "
        "THEN event_time_utc_ms / 1000 "
        "ELSE event_time_utc_ms END"
    )
    query = f"SELECT {', '.join(select_parts)} FROM {table} ORDER BY {order_expr}"  

    start = time.monotonic()
    try:
        conn = pg_connect(config)
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]
        cur.close()
        conn.close()
    except Exception:
        rows, col_names = extract_cols(arch, config, table, mac_col, log)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names + ["run_id", "architecture"])
        for row in rows:
            writer.writerow(list(row) + [run_id, arch])
    log.info("[EXTRACT] %d rows to %s  (took %s, run_id=%s, arch=%s)",
             len(rows), out_csv, elapsed_time(start), run_id, arch)
    return len(rows)


def extract_cols(arch, config, table, mac_col, log):
    try:
        conn = pg_connect(config)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table} LIMIT 0")  
        available = {d[0] for d in cur.description}
        cur.close()

        safe_parts = []
        for col in EXTRACT_COLS:
            real_col = mac_col if col == "mac_address_string" and mac_col != "mac_address_string" else col
            if real_col in available:
                if col == "mac_address_string" and mac_col != "mac_address_string":
                    safe_parts.append(f"{mac_col} AS mac_address_string")
                elif col == "date_time_utc":
                    safe_parts.append(
                        "CASE WHEN event_time_utc_ms > 1000000000000 "
                        "THEN event_time_utc_ms / 1000 "
                        "ELSE event_time_utc_ms END AS date_time_utc"
                    )
                else:
                    safe_parts.append(col)
            else:
                safe_parts.append(f"NULL AS {col}")

        order_expr = (
            "CASE WHEN event_time_utc_ms > 1000000000000 "
            "THEN event_time_utc_ms / 1000 "
            "ELSE COALESCE(event_time_utc_ms, 0) END"
        )
        query = f"SELECT {', '.join(safe_parts)} FROM {table} ORDER BY {order_expr}"  
        cur2 = conn.cursor()
        cur2.execute(query)
        rows = cur2.fetchall()
        col_names = [d[0] for d in cur2.description]
        cur2.close()
        conn.close()
        return rows, col_names
    except Exception as exc2:
        return [], EXTRACT_COLS


def read_producer_report(report_path: Path) -> dict:
    if not report_path.exists():
        return {}
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def main():
    import argparse

    ap = argparse.ArgumentParser(description="Experiment Runner")
    ap.add_argument(
        "--config", "-c",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to experiment_config.yaml (default: project root)",
    )
    cli = ap.parse_args()
    config = load_experiment_config(cli.config)
    results_dir = PROJECT_ROOT / Path(_cfg(config, "paths", "results_dir"))
    data_dir = PROJECT_ROOT / Path(_cfg(config, "paths", "data_dir"))
    log_dir = PROJECT_ROOT / Path(_cfg(config, "logging", "log_dir"))
    results_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_log = log_dir / f"session_{session_ts}.log"
    log = setup_logging(config, run_log_path=session_log)

    log.info("═" * 60)
    log.info(" EXPERIMENT SESSION %s", session_ts)
    log.info("═" * 60)
    log.info("Config loaded from: %s", cli.config)
    log.info("Results directory: %s", results_dir)

    archs = _cfg(config, "architectures")
    all_workloads = _cfg(config, "workloads")
    workloads = {k: v for k, v in all_workloads.items() if v.get("enabled", True)}
    repeats = _cfg(config, "runner", "repeats")
    warmup = _cfg(config, "runner", "warmup_minutes")
    measurement = _cfg(config, "runner", "measurement_minutes")
    duration = int(warmup + measurement)
    broker = _cfg(config, "kafka", "broker")
    topic = _cfg(config, "kafka", "topic")

    total_runs = len(archs) * len(workloads) * repeats
    log.info("Plan: %d arch(s) × %d workload(s) × %d repeat(s) = %d total runs",
             len(archs), len(workloads), repeats, total_runs)
    log.info("Architectures : %s", ", ".join(archs))
    log.info("Workloads: %s", ", ".join(f"{k} ({v.get('description', '')})" for k, v in workloads.items()))
    log.info("Duration: %d min warmup + %d min measurement = %d min per run",
             warmup, measurement, duration)
    log.info("Kafka broker: %s  topic: %s", broker, topic)
    log.info("═" * 60)

    effective_cfg_path = results_dir / "effective_config.yaml"
    with open(effective_cfg_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    session_start = time.monotonic()
    run_manifest: list[dict] = []
    run_counter = 0

    for arch in archs:
        if arch not in ARCH_FILE_STRUCT:
            log.error("[SKIP] Unknown architecture: %s", arch)
            continue

        for wl_key, wl_cfg in workloads.items():
            for rep in range(repeats):
                run_counter += 1
                run_id = f"{arch}_workload{wl_key}_run{rep}"
                run_dir = results_dir / arch / f"workload_{wl_key}" / f"run_{rep}"
                run_dir.mkdir(parents=True, exist_ok=True)
                csv_path = run_dir / "raw_data.csv"
                report_path = run_dir / "producer_report.json"
                meta_path = run_dir / "run_meta.json"
                run_log_path = run_dir / "run.log"

                add_run_file_handler(config, run_log_path)

                log.info("")
                log.info("═" * 60)
                log.info("  RUN %d/%d: %s  (repeat %d/%d)", run_counter, total_runs, run_id, rep + 1, repeats)
                log.info("  Workload %s: %s", wl_key, wl_cfg.get("description", ""))
                log.info("═" * 60)
                run_t0 = time.monotonic()

                phase_times: dict[str, str] = {}
                start = time.monotonic()
                stop_stack(arch, log)
                start_stack(arch, log)
                wait_s = float(_cfg(config, "runner", "settle_seconds"))
                time.sleep(wait_s)
                phase_times["stack_up"] = elapsed_time(start)
                log.info("[PHASE] Stack ready in %s", phase_times["stack_up"])

                metrics_start_time = time.time()
                metrics_collection_process = None
                hardware_metrics_path = run_dir / "hardware_metrics.json"
                try:
                    log.info("[METRICS] Starting hardware metrics collection from %s", PROMETHEUS_URL)
                    metrics_duration = (duration * 60) + DRAIN_TIMEOUT_SECONDS
                    metrics_collection_process = start_hardware_metrics_collection(
                        PROMETHEUS_URL, metrics_start_time, metrics_duration, hardware_metrics_path, log
                    )
                except Exception as e:
                    log.warning("[METRICS] Failed to start hardware metrics collection: %s", e)
                    metrics_collection_process = None
                
                start = time.monotonic()
                rc = run_producer(
                    wl_key, wl_cfg, duration, data_dir,
                    broker, topic, report_path, log,
                )
                phase_times["producer"] = elapsed_time(start)
                log.info("[PHASE] Producer finished in %s (rc=%d)", phase_times["producer"], rc)

                producer_report = read_producer_report(report_path)
                produced_count = producer_report.get("produced", 0)
                start = time.monotonic()
                row_count = wait_drain(arch, config, log)
                phase_times["drain"] = elapsed_time(start)
            
                hardware_metrics = None
                if metrics_collection_process:
                    try:
                        log.info("[METRICS] Stopping hardware metrics collection...")
                        metrics_collection_process.wait(timeout=30)
                        if hardware_metrics_path.exists():
                            with open(hardware_metrics_path, "r", encoding="utf-8") as f:
                                hardware_metrics = json.load(f)
                            log.info("[METRICS] Hardware metrics collected successfully")
                        else:
                            log.warning("[METRICS] Hardware metrics file not found: %s", hardware_metrics_path)
                    except Exception as e:
                        log.warning("[METRICS] Failed to read hardware metrics: %s", e)
                        if metrics_collection_process.poll() is None:
                            metrics_collection_process.terminate()
                            metrics_collection_process.wait(timeout=5)
                start = time.monotonic()
                extracted = extract_data(arch, config, csv_path, log, run_id=run_id)
                phase_times["extract"] = elapsed_time(start)
                log.info("[PHASE] Extract finished in %s (%d rows)", phase_times["extract"], extracted)

                start = time.monotonic()
                stop_stack(arch, log)
                phase_times["stack_down"] = elapsed_time(start)
                log.info("[PHASE] Stack down in %s", phase_times["stack_down"])
                meta = {
                    "run_id": run_id,
                    "architecture": arch,
                    "workload": wl_key,
                    "workload_config": wl_cfg,
                    "repeat": rep,
                    "warmup_minutes": warmup,
                    "measurement_minutes": measurement,
                    "duration_minutes": duration,
                    "producer_rc": rc,
                    "produced_events": produced_count,
                    "db_written_events": row_count,
                    "rows_extracted": extracted,
                    "producer_wall_clock_s": producer_report.get("wall_clock_seconds"),
                    "producer_actual_msg_s": producer_report.get("actual_msg_per_sec"),
                    "hardware_metrics": hardware_metrics,
                    "csv_path": str(csv_path),
                    "log_path": str(run_log_path),
                    "phase_times": phase_times,
                    "total_run_time": elapsed_time(run_t0),
                    "started_at": get_now(),
                }
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2)
                run_manifest.append(meta)

                log.info("[DONE] Run %s completed in %s and %d rows extracted",
                         run_id, elapsed_time(run_t0), extracted)
                log.info("═" * 60)

    add_run_file_handler(config, session_log)
    manifest_path = results_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(run_manifest, f, indent=2)
    log.info("[METRICS] Running metrics computation")
    start = time.monotonic()
    try:
        result = subprocess.run(
            [
                sys.executable, str(METRICS_PY),
                "--results-dir", str(results_dir),
                "--warmup-minutes", str(warmup),
                "--measurement-minutes", str(measurement),
                "--conductivity-threshold", str(CONDUCTIVITY_ANOMALY_THRESHOLD),
            ],
            timeout=3600,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            log.info("[METRICS] Completed in %s", elapsed_time(start))
        else:
            log.error("[METRICS] Failed (rc=%d):\n%s", result.returncode,
                      result.stderr[-2000:] if result.stderr else "")
    except Exception as exc:
        log.error("[METRICS] Exception: %s", exc)

    log.info("[PLOTS] Generating plots from summary.json")
    start = time.monotonic()
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(PLOT_RESULTS_PY),
                "--summary",
                str(results_dir / "summary.json"),
                "--out-dir",
                str(results_dir / "figures"),
            ],
            timeout=3600,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            log.info("[PLOTS] Completed in %s", elapsed_time(start))
        else:
            log.error(
                "[PLOTS] Failed (rc=%d):\n%s",
                result.returncode,
                result.stderr[-2000:] if result.stderr else "",
            )
    except Exception as exc:
        log.error("[PLOTS] Exception: %s", exc)

    log.info("")
    log.info("═" * 60)
    log.info("SESSION COMPLETE! %d runs in %s", run_counter, elapsed_time(session_start))
    log.info("Results: %s", results_dir)
    log.info("Summary: %s", results_dir / "summary.json")
    log.info("Figures: %s", results_dir / "figures")
    log.info("Logs: %s", log_dir)
    log.info("═" * 60)

if __name__ == "__main__":
    main()