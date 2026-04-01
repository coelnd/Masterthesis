"""
Shared batch feature computation job
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Optional
import pandas as pd
from psycopg2.extras import execute_batch
from arch_common.anomaly_detection import detect_all_anomalies
from arch_common.feature_engineering import compute_all_features

logger = logging.getLogger(__name__)


def compute_features_batch(
    conn,
    *,
    architecture: str | None = None,
    batch_size: int = 500,
    expected_rate_per_minute: Optional[float] = None,
) -> int:
    arch = (architecture or "").lower()
    target_table = "dishwasher_data"
    if arch == "datafabric":

        cur_probe = conn.cursor()
        cur_probe.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'fabric_processed_data'
            )
            """
        )
        has_fabric_table = bool(cur_probe.fetchone()[0])
        cur_probe.close()
        if has_fabric_table:
            target_table = "fabric_processed_data"

    cur = conn.cursor()

    count_sql = f"""
        SELECT COUNT(*)
        FROM {target_table}
        WHERE features_computed_at IS NULL
    """

    if target_table == "fabric_processed_data":
        select_sql = f"""
            SELECT
                id,
                device_id,
                mac_address AS mac_address_string,
                (event_time_utc_ms / 1000.0) AS date_time_utc,
                ingest_at,
                latency_seconds,
                mean_wash_tank_temp,
                mean_rinse_temp,
                mean_conductivity,
                setpoint_conductivity,
                en_wash_on,
                en_rinse_on
            FROM {target_table}
            WHERE features_computed_at IS NULL
            ORDER BY mac_address, event_time_utc_ms
            LIMIT %s
        """
    else:
        select_sql = f"""
            SELECT
                id, device_id, mac_address_string, date_time_utc,
                ingest_at, latency_seconds,
                mean_wash_tank_temp, mean_rinse_temp,
                mean_conductivity, setpoint_conductivity,
                en_wash_on, en_rinse_on
            FROM {target_table}
            WHERE features_computed_at IS NULL
            ORDER BY mac_address_string, date_time_utc
            LIMIT %s
        """

    cur.execute(count_sql)
    pending = cur.fetchone()[0]
    cur.close()

    if pending == 0:
        logger.info("No pending rows for feature computation")
        return 0

    logger.info("Feature batch: %d pending rows (batch_size=%d)", pending, batch_size)

    cur = conn.cursor()
    cur.execute("SELECT mac, wash_temp_setpoint, rinse_temp_setpoint FROM machines")
    sp_rows = cur.fetchall()
    cur.close()
    machine_sp = pd.DataFrame(sp_rows, columns=["mac_address_string", "wash_temp_setpoint", "rinse_temp_setpoint"]) if sp_rows else pd.DataFrame()

    total_updated = 0
    while True:
        cur = conn.cursor()
        cur.execute(select_sql, (batch_size,))
        rows = cur.fetchall()
        cur.close()

        if not rows:
            break

        df = pd.DataFrame(
            rows,
            columns=[
                "id", "device_id", "mac_address_string", "date_time_utc",
                "ingest_at", "latency_seconds",
                "mean_wash_tank_temp", "mean_rinse_temp",
                "mean_conductivity", "setpoint_conductivity",
                "en_wash_on", "en_rinse_on",
            ],
        )
        df["en_wash_on"] = pd.to_numeric(df["en_wash_on"], errors="coerce").fillna(0).astype(int)
        df["en_rinse_on"] = pd.to_numeric(df["en_rinse_on"], errors="coerce").fillna(0).astype(int)

        detected = detect_all_anomalies(
            df,
            machine_setpoints=machine_sp,
            mac_column="mac_address_string",
            en_wash_col="en_wash_on",
            en_rinse_col="en_rinse_on",
        )

        features = compute_all_features(
            detected,
            latency_col="latency_seconds",
            timestamp_col="date_time_utc",
            processing_timestamp_col="ingest_at",
            expected_rate_per_minute=expected_rate_per_minute,
            flag_columns=[
                "temp_below_setpoint",
                "rinse_temp_below_setpoint",
                "conc_constant_over", "conc_constant_under", "conc_ok",
                "wash_temp_recovery_failure", "rinse_temp_recovery_failure", "conc_recovery_failure",
                "wash_temp_repeated_issue", "rinse_temp_repeated_issue", "conc_repeated_issue",
                "wash_temp_high_variance", "rinse_temp_high_variance",
            ],
        )

        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)

        update_rows = []
        for _, r in features.iterrows():
            update_rows.append((
                to_boolean(r.get("temp_below_setpoint")),
                to_boolean(r.get("rinse_temp_below_setpoint")),
                to_boolean(r.get("conc_constant_over")),
                to_boolean(r.get("conc_constant_under")),
                to_boolean(r.get("conc_ok")),
                to_boolean(r.get("wash_temp_recovery_failure")),
                to_boolean(r.get("rinse_temp_recovery_failure")),
                to_boolean(r.get("conc_recovery_failure")),
                to_boolean(r.get("wash_temp_repeated_issue")),
                to_boolean(r.get("rinse_temp_repeated_issue")),
                to_boolean(r.get("conc_repeated_issue")),
                to_boolean(r.get("wash_temp_high_variance")),
                to_boolean(r.get("rinse_temp_high_variance")),
                to_float(r.get("field_consistency_score")),
                to_float(r.get("missing_rate")),
                to_float(r.get("window_integrity_score_5min")),
                now,
                now_ms,
                now,
                int(r["id"]),
            ))

        cur = conn.cursor()
        update_sql = f"""
            UPDATE {target_table} SET
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
                field_consistency_score = %s,
                missing_rate = %s,
                window_integrity_5min = %s,
                features_computed_at = %s,
                features_ready_ms = %s,
                flags_computed_at = %s
            WHERE id = %s
        """
        execute_batch(
            cur,
            update_sql,
            update_rows,
            page_size=200,
        )
        conn.commit()
        cur.close()
        total_updated += len(update_rows)

        if len(rows) < batch_size:
            break

    logger.info("Feature batch computed complete: %d rows updated", total_updated)
    return total_updated


def to_boolean(val) -> bool:
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    return bool(val)


def to_float(val):
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None