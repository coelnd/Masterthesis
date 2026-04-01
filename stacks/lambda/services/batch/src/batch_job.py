"""
Lambda batch layer for periodic aggregation
"""

import logging
import os
import sys
import time
from typing import Optional
import psycopg2
from arch_common.config_loader import get_config_value, load_config
from arch_common.machine_loader import load_machines
from arch_common.postgres_utils import connect_postgres

try:
    import pandas as pd
    from arch_common.anomaly_detection import detect_all_anomalies
    ANOMALY_DETECTION_AVAILABLE = True
except ImportError:
    ANOMALY_DETECTION_AVAILABLE = False

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

try:
    import pandas as pd
    from arch_common.anomaly_detection import detect_all_anomalies
    ANOMALY_DETECTION_AVAILABLE = True
except ImportError:
    ANOMALY_DETECTION_AVAILABLE = False

try:
    from arch_common.compute_features_job import compute_features_batch
    FEATURE_ENGINEERING_AVAILABLE = True
except ImportError:
    FEATURE_ENGINEERING_AVAILABLE = False
    logger.warning("Feature engineering not available")
    logger.warning("Anomaly detection not available (pandas required)")

BATCH_INTERVAL_SECONDS = get_config_value(CONFIG, "batch.interval_seconds")

def compute_anomaly_flags(conn, batch_size: int = 1000) -> tuple[int, dict]: 
    cur = conn.cursor()
    
    latency_samples = []
    
    try:
        cur.execute("""
            SELECT COUNT(*) FROM dishwasher_data 
        """)
        total_rows = cur.fetchone()[0]
        
        if total_rows == 0:
            logger.info("No rows to process")
            return 0, {}
        
        logger.info("Found %d rows to process (Lambda: Batch Layer overwrites Speed Layer flags), processing in batches of %d", total_rows, batch_size)
        
        rows_updated = 0
        
        cur.execute("SELECT mac, wash_temp_setpoint, rinse_temp_setpoint FROM machines")
        machine_data = cur.fetchall()
        machine_setpoints = pd.DataFrame(
            machine_data,
            columns=["mac_address_string", "wash_temp_setpoint", "rinse_temp_setpoint"]
        )
        
        offset = 0
        while True:
            cur.execute("""
                SELECT 
                    id, mac_address_string, device_id,
                    mean_wash_tank_temp, mean_rinse_temp,
                    mean_conductivity, setpoint_conductivity,
                    date_time_utc, ingest_at, latency_seconds,
                    en_wash_on, en_rinse_on
                FROM dishwasher_data
                ORDER BY id
                LIMIT %s OFFSET %s
            """, (batch_size, offset))
            
            batch_data = cur.fetchall()
            if not batch_data:
                break
            
            df = pd.DataFrame(
                batch_data,
                columns=[
                    "id", "mac_address_string", "device_id",
                    "mean_wash_tank_temp", "mean_rinse_temp",
                    "mean_conductivity", "setpoint_conductivity",
                    "date_time_utc", "ingest_at", "latency_seconds",
                    "en_wash_on", "en_rinse_on"
                ]
            )
            df["en_wash_on"] = pd.to_numeric(df["en_wash_on"], errors="coerce").fillna(0).astype(int)
            df["en_rinse_on"] = pd.to_numeric(df["en_rinse_on"], errors="coerce").fillna(0).astype(int)
            
            if df.empty:
                break
            
            try:
                result_df = detect_all_anomalies(
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
                )
                
                for _, row in result_df.iterrows():
                    row_id = int(row["id"])
                    ingest_at = row["ingest_at"]
                    consumer_latency = float(row["latency_seconds"]) if row["latency_seconds"] is not None else 0.0
                    
                    flags_computed_at = time.time()
                    
                    if ingest_at is not None:
                        if hasattr(ingest_at, 'timestamp'):
                            ingest_at_epoch = ingest_at.timestamp()
                        else:
                            ingest_at_epoch = float(ingest_at)
                        batch_latency = flags_computed_at - ingest_at_epoch + consumer_latency
                        latency_samples.append(batch_latency)

                    features_ready_ms = int(flags_computed_at * 1000)
                    
                    cur.execute("""
                        UPDATE dishwasher_data
                        SET
                            conc_ok = %s,
                            temp_below_setpoint = %s,
                            rinse_temp_below_setpoint = %s,
                            conc_constant_over = %s,
                            conc_constant_under = %s,
                            wash_temp_recovery_failure = %s,
                            rinse_temp_recovery_failure = %s,
                            conc_recovery_failure = %s,
                            wash_temp_repeated_issue = %s,
                            rinse_temp_repeated_issue = %s,
                            conc_repeated_issue = %s,
                            wash_temp_high_variance = %s,
                            rinse_temp_high_variance = %s,
                            flags_computed_at = NOW(),
                            features_ready_ms = %s
                        WHERE id = %s
                    """, (
                        bool(row.get("conc_ok", False)),
                        bool(row.get("temp_below_setpoint", False)),
                        bool(row.get("rinse_temp_below_setpoint", False)),
                        bool(row.get("conc_constant_over", False)),
                        bool(row.get("conc_constant_under", False)),
                        bool(row.get("wash_temp_recovery_failure", False)),
                        bool(row.get("rinse_temp_recovery_failure", False)),
                        bool(row.get("conc_recovery_failure", False)),
                        bool(row.get("wash_temp_repeated_issue", False)),
                        bool(row.get("rinse_temp_repeated_issue", False)),
                        bool(row.get("conc_repeated_issue", False)),
                        bool(row.get("wash_temp_high_variance", False)),
                        bool(row.get("rinse_temp_high_variance", False)),
                        features_ready_ms,
                        row_id
                    ))
                    rows_updated += 1
                
                conn.commit()
                logger.debug("Updated %d rows in batch (offset %d)", len(result_df), offset)
                
            except Exception as e:
                logger.error("Error computing flags for batch: %s", e, exc_info=True)
                conn.rollback()
                offset += batch_size
                continue
            
            offset += batch_size
            if len(batch_data) < batch_size:
                break
        
        latency_stats = {}
        if latency_samples:
            sorted_latencies = sorted(latency_samples)
            latency_stats = {
                "count": len(latency_samples),
                "avg": sum(latency_samples) / len(latency_samples),
                "min": min(latency_samples),
                "max": max(latency_samples),
                "p50": sorted_latencies[len(sorted_latencies) // 2],
                "p95": sorted_latencies[int(len(sorted_latencies) * 0.95)],
                "p99": sorted_latencies[int(len(sorted_latencies) * 0.99)],
            }
        return rows_updated, latency_stats
        
    except Exception as e:
        logger.error("Error in compute_anomaly_flags: %s", e, exc_info=True)
        conn.rollback()
        return 0, {}
    finally:
        cur.close()

def connect_pg():
    """Create PostgreSQL connection."""
    pg_config = {
        "host": get_config_value(CONFIG, "postgres.host"),
        "dbname": get_config_value(CONFIG, "postgres.db"),
        "user": get_config_value(CONFIG, "postgres.user"),
        "password": get_config_value(CONFIG, "postgres.password"),
        "port": get_config_value(CONFIG, "postgres.port"),
    }
    return connect_postgres(**pg_config)

def run_batch() -> None:
    logger.info("Starting Batch Aggregation")
    start_time = time.time()
    rows_processed = 0
    conn: Optional[psycopg2.extensions.connection] = None

    try:
        csv_path = get_config_value(CONFIG, "data.machine_csv_path")
        load_machines(csv_path=csv_path)
        logger.debug("Machines metadata loaded")

        conn = connect_pg()

        flags_updated, latency_stats = compute_anomaly_flags(conn, batch_size=1000)
        if flags_updated > 0:
            logger.info("Updated anomaly flags for %d rows", flags_updated)
        
        if FEATURE_ENGINEERING_AVAILABLE:
            features_updated = compute_features_batch(
                conn,
                batch_size=1000,
                expected_rate_per_minute=None,
            )
            if features_updated > 0:
                logger.info("Updated features for %d rows", features_updated)

        runtime = time.time() - start_time
        logger.info("Batch completed: runtime=%.2fs", runtime)

    except Exception as e:
        logger.exception("Unexpected error in batch job")
    finally:
        if conn:
            conn.close()

def main() -> None:
    while True:
        run_batch()
        sleep_left = BATCH_INTERVAL_SECONDS
        while sleep_left > 0:
            step = min(30, sleep_left)
            logger.debug("Next run in %ds", sleep_left)
            time.sleep(step)
            sleep_left -= step


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Error in batch job")
        sys.exit(1)