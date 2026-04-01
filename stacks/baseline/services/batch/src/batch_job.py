"""
Baseline batch job to compute anomaly flags and persist them to dishwasher_data
"""
from __future__ import annotations
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional, Tuple
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from arch_common.anomaly_detection import detect_all_anomalies
from arch_common.config_loader import get_config_value, load_config
from arch_common.machine_loader import load_machines
from arch_common.postgres_utils import connect_postgres

CONFIG_PATH = os.getenv("CONFIG_PATH", "/config/config.yaml")
CONFIG = load_config(CONFIG_PATH)

LOG_LEVEL = get_config_value(CONFIG, "logging.level").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)

BATCH_INTERVAL_SECONDS = get_config_value(CONFIG, "batch.interval_seconds")
BATCH_SIZE = get_config_value(CONFIG, "batch.batch_size")


def connect_pg():
    pg_config = {
        "host": get_config_value(CONFIG, "postgres.host"),
        "dbname": get_config_value(CONFIG, "postgres.db"),
        "user": get_config_value(CONFIG, "postgres.user"),
        "password": get_config_value(CONFIG, "postgres.password"),
        "port": get_config_value(CONFIG, "postgres.port"),
    }
    return connect_postgres(**pg_config)


def fetch_machine_setpoints(conn) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT mac, wash_temp_setpoint, rinse_temp_setpoint
        FROM machines
        """
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return pd.DataFrame(columns=["mac_address_string", "wash_temp_setpoint", "rinse_temp_setpoint"])
    df = pd.DataFrame(rows, columns=["mac_address_string", "wash_temp_setpoint", "rinse_temp_setpoint"])
    df = df.drop_duplicates(subset=["mac_address_string"])
    return df

def get_pending_row_count(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM dishwasher_data WHERE flags_computed_at IS NULL"
    )
    count = cur.fetchone()[0]
    cur.close()
    return count or 0

def fetch_pending_batch(conn, batch_size: int) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            d.id,
            d.device_id,
            d.mac_address_string,
            d.date_time_utc,
            d.ingest_at,
            d.latency_seconds,
            d.kafka_logappend_ms,
            d.mean_wash_tank_temp,
            d.mean_rinse_temp,
            d.mean_conductivity,
            d.setpoint_conductivity,
            d.en_wash_on,
            d.en_rinse_on
        FROM dishwasher_data d
        WHERE d.flags_computed_at IS NULL
        ORDER BY d.id
        LIMIT %s
        """,
        (batch_size,),
    )
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=[
            "id",
            "device_id",
            "mac_address_string",
            "date_time_utc",
            "ingest_at",
            "latency_seconds",
            "kafka_logappend_ms",
            "mean_wash_tank_temp",
            "mean_rinse_temp",
            "mean_conductivity",
            "setpoint_conductivity",
            "en_wash_on",
            "en_rinse_on",
        ],
    )
    df["latency_seconds"] = df["latency_seconds"].fillna(0.0)
    df["ingest_at"] = pd.to_datetime(df["ingest_at"], errors="coerce")
    df["en_wash_on"] = pd.to_numeric(df["en_wash_on"], errors="coerce").fillna(0).astype(int)
    df["en_rinse_on"] = pd.to_numeric(df["en_rinse_on"], errors="coerce").fillna(0).astype(int)
    return df

def merge_setpoints(df: pd.DataFrame, machine_setpoints: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["wash_temp_setpoint"] = None
        df["rinse_temp_setpoint"] = None
        return df
    if machine_setpoints.empty:
        df["wash_temp_setpoint"] = None
        df["rinse_temp_setpoint"] = None
        return df
    merged = df.merge(machine_setpoints, on="mac_address_string", how="left")
    return merged

def add_rolling_statistics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        for base in ("wash", "rinse", "conductivity"):
            for window in (1, 5, 15):
                df[f"rolling_{base}_temp_avg_{window}min" if base != "conductivity" else f"rolling_{base}_avg_{window}min"] = None
        return df

    enriched = df.copy()
    enriched["__ts_dt"] = pd.to_datetime(enriched["date_time_utc"], unit="s", errors="coerce")

    rolling_specs = [
        ("mean_wash_tank_temp", "rolling_wash_temp_avg"),
        ("mean_rinse_temp", "rolling_rinse_temp_avg"),
        ("mean_conductivity", "rolling_conductivity_avg"),
    ]

    for _, prefix in rolling_specs:
        for window in (1, 5, 15):
            enriched[f"{prefix}_{window}min"] = None

    for mac, group in enriched.groupby("mac_address_string"):
        if group.empty:
            continue
        group_sorted = group.sort_values("__ts_dt")
        ts_indexed = group_sorted.set_index("__ts_dt")
        if ts_indexed.index.isna().all():
            continue
        for source_col, prefix in rolling_specs:
            if source_col not in ts_indexed.columns:
                continue
            series = ts_indexed[source_col]
            for window in (1, 5, 15):
                col_name = f"{prefix}_{window}min"
                rolling = series.rolling(f"{window}min", min_periods=1).mean()
                enriched.loc[group_sorted.index, col_name] = rolling.values

    enriched = enriched.drop(columns=["__ts_dt"])
    return enriched


def set_rules(df: pd.DataFrame, machine_setpoints: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    try:
        rolling_conc_col = None
        if "rolling_conductivity_avg_15min" in df.columns:
            rolling_conc_col = "rolling_conductivity_avg_15min"
        
        detected = detect_all_anomalies(
            df,
            machine_setpoints=machine_setpoints,
            temp_column="mean_wash_tank_temp",
            rinse_temp_column="mean_rinse_temp",
            conc_column="mean_conductivity",
            setpoint_conc_column="setpoint_conductivity",
            timestamp_column="date_time_utc",
            device_id_col="mac_address_string",
            mac_column="mac_address_string",
            en_wash_col="en_wash_on",
            en_rinse_col="en_rinse_on",
            rolling_conc_avg_column=rolling_conc_col,
        )
        return detected
    except Exception as exc:
        logger.error("Failed to detect anomalies: %s", exc)
        raise


def to_boolean(value) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return bool(value)


def update_flags(conn, df: pd.DataFrame, flag_timestamp: datetime) -> int:
    if df.empty:
        return 0

    features_ready_ms = int(flag_timestamp.timestamp() * 1000)
    rows: list[tuple] = []
    for _, row in df.iterrows():
        kafka_ms = row.get("kafka_logappend_ms")
        fal_seconds = (
            (features_ready_ms - int(kafka_ms)) / 1000.0
            if kafka_ms is not None and pd.notna(kafka_ms)
            else None
        )
        rows.append(
            (
                to_boolean(row.get("temp_below_setpoint")),
                to_boolean(row.get("rinse_temp_below_setpoint")),
                to_boolean(row.get("conc_constant_over")),
                to_boolean(row.get("conc_constant_under")),
                to_boolean(row.get("conc_ok", False)),
                to_boolean(row.get("wash_temp_recovery_failure")),
                to_boolean(row.get("rinse_temp_recovery_failure")),
                to_boolean(row.get("conc_recovery_failure")),
                to_boolean(row.get("wash_temp_repeated_issue")),
                to_boolean(row.get("rinse_temp_repeated_issue")),
                to_boolean(row.get("conc_repeated_issue")),
                to_boolean(row.get("wash_temp_high_variance")),
                to_boolean(row.get("rinse_temp_high_variance")),
                flag_timestamp,
                features_ready_ms,
                fal_seconds,
                int(row["id"]),
            )
        )

    cur = conn.cursor()
    execute_batch(
        cur,
        """
        UPDATE dishwasher_data
        SET
            temp_below_setpoint = %s,
            rinse_temp_below_setpoint = %s,
            conc_constant_over = %s,
            conc_constant_under = %s,
            conc_ok = %s,
            wash_temp_recovery_failure = %s,
            rinse_temp_recovery_failure = %s,
            conc_recovery_failure = %s,
            wash_temp_repeated_issue = %s,
            rinse_temp_repeated_issue = %s,
            conc_repeated_issue = %s,
            wash_temp_high_variance = %s,
            rinse_temp_high_variance = %s,
            flags_computed_at = %s,
            features_ready_ms = %s,
            fal_seconds = %s
        WHERE id = %s
        """,
        rows,
    )
    conn.commit()
    cur.close()
    return len(rows)


def process_batches(conn) -> Tuple[int, int, int]:
    rows_scanned = 0
    features_written = 0
    flags_updated = 0

    pending = get_pending_row_count(conn)
    if pending == 0:
        logger.info("No pending rows")
        return rows_scanned, features_written, flags_updated

    machine_setpoints = fetch_machine_setpoints(conn)
    logger.info("Processing %d pending rows", pending)

    while True:
        batch_df = fetch_pending_batch(conn, BATCH_SIZE)
        if batch_df.empty:
            break

        rows_scanned += len(batch_df)
        batch_df = merge_setpoints(batch_df, machine_setpoints)
        batch_df = add_rolling_statistics(batch_df)
        detected_df = set_rules(batch_df, machine_setpoints)

        flag_timestamp = datetime.now(timezone.utc)
        detected_df["flags_computed_at"] = flag_timestamp
        flags_updated += update_flags(conn, detected_df, flag_timestamp)

        if len(batch_df) < BATCH_SIZE:
            break

    return rows_scanned, features_written, flags_updated

def run_batch():
    logger.info("Starting baseline batch job")
    start_time = time.time()
    rows_processed = 0
    features_written = 0
    flags_updated = 0
    conn: Optional[psycopg2.extensions.connection] = None

    try:
        csv_path = get_config_value(CONFIG, "data.machine_csv_path")
        load_machines(csv_path=csv_path)

        conn = connect_pg()
        rows_processed, features_written, flags_updated = process_batches(conn)

        runtime = time.time() - start_time

        logger.info(
            "Baseline batch finished: rows_scanned=%d, features_written=%d, flags_updated=%d, runtime=%.2fs",
            rows_processed,
            features_written,
            flags_updated,
            runtime
        )
    except Exception as exc:
        logger.exception("Baseline batch failed: %s", exc)
    finally:
        if conn:
            try:
                conn.close()
            except Exception as close_error:
                logger.warning("Error closing PostgreSQL connection: %s", close_error)

def main():
    while True:
        try:
            run_batch()
        except Exception:
            time.sleep(10)
        sleep_left = BATCH_INTERVAL_SECONDS
        while sleep_left > 0:
            step = min(30, sleep_left)
            time.sleep(step)
            sleep_left -= step

if __name__ == "__main__":
    try:
        main()
    except Exception as fatal:
        logger.exception("Fatal error in baseline batch job")
        sys.exit(1)