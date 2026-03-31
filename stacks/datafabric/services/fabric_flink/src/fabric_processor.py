"""
Data Fabric Flink job
"""

from __future__ import annotations
import logging
import os
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment
from arch_common.config_loader import get_config_value, load_config

CONFIG_PATH = os.getenv("CONFIG_PATH", "/config/config.yaml")
CONFIG = load_config(CONFIG_PATH)

LOG_LEVEL = get_config_value(CONFIG, "logging.level").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger("fabric_flink")

env = StreamExecutionEnvironment.get_execution_environment()

jar_paths = [
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar",
    "file:///opt/flink/lib/flink-connector-jdbc-3.3.0-1.20.jar",
    "file:///opt/flink/lib/flink-json-1.20.0.jar",
    "file:///opt/flink/lib/postgresql.jar",
]
for jar in jar_paths:
    try:
        env.add_jars(jar)
    except Exception as exc:
        logger.warning("Failed to add jar %s", exc)

settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)
t_env.get_config().set("pipeline.name", "Data Fabric Processor")
t_env.get_config().set("table.dynamic-table-options.enabled", "true")
t_env.get_config().set("pipeline.object-reuse", "true")

KAFKA_BROKER = get_config_value(CONFIG, "kafka.broker")
CANONICAL_TOPIC = get_config_value(CONFIG, "kafka.canonical_topic")
KAFKA_GROUP = get_config_value(CONFIG, "kafka.consumer_group")

PG_HOST = get_config_value(CONFIG, "postgres.host")
PG_DB = get_config_value(CONFIG, "postgres.db")
PG_USER = get_config_value(CONFIG, "postgres.user")
PG_PASS = get_config_value(CONFIG, "postgres.password")
PG_PORT = int(get_config_value(CONFIG, "postgres.port"))

SCHEMA_OK_VALIDATION = """(
  (e.MeanWashTankTemp IS NULL OR (e.MeanWashTankTemp >= CAST(0 AS DOUBLE) AND e.MeanWashTankTemp <= CAST(255 AS DOUBLE)))
  AND (e.MeanRinseTemp IS NULL OR (e.MeanRinseTemp >= CAST(0 AS DOUBLE) AND e.MeanRinseTemp <= CAST(255 AS DOUBLE)))
  AND (e.MeanConductivity IS NULL OR (e.MeanConductivity >= CAST(0 AS DOUBLE) AND e.MeanConductivity <= CAST(255 AS DOUBLE)))
)"""

machines_lookup_ddl = f"""
CREATE TEMPORARY TABLE machines_lookup (
    mac STRING NOT NULL,
    wash_temp_setpoint INT,
    rinse_temp_setpoint INT,
    PRIMARY KEY (mac) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}',
  'table-name' = 'public.machines',
  'username' = '{PG_USER}',
  'password' = '{PG_PASS}',
  'driver' = 'org.postgresql.Driver',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '1h'
)
"""

source_table = f"""
CREATE TEMPORARY TABLE beacon_events (
    Id STRING,
    DeviceID STRING,
    MACAddressString STRING,
    BeaconEventID INT,
    MeanConductivity DOUBLE,
    SetpointConductivity DOUBLE,
    MeanWashTankTemp DOUBLE,
    MeanRinseTemp DOUBLE,
    EnFillOn INT,
    EnRinseOn INT,
    EnWashOn INT,
    VersionAndMessageType INT,
    DateTimeUTC BIGINT,
    _produced_at_ms BIGINT,
    event_ts AS TO_TIMESTAMP_LTZ(CAST(DateTimeUTC AS BIGINT) * 1000, 3),
    proc_time AS PROCTIME(),
    kafka_ts_ltz TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_ts AS event_ts - INTERVAL '2' YEAR
) WITH (
  'connector' = 'kafka',
  'topic' = '{CANONICAL_TOPIC}',
  'properties.bootstrap.servers' = '{KAFKA_BROKER}',
  'properties.group.id' = '{KAFKA_GROUP}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
)
"""

pg_sink = f"""
CREATE TEMPORARY TABLE dishwasher_pg (
    event_id STRING,
    device_id STRING,
    mac_address_string STRING,
    beacon_event_id INT,
    mean_wash_tank_temp DOUBLE,
    mean_rinse_temp DOUBLE,
    mean_conductivity DOUBLE,
    setpoint_conductivity DOUBLE,
    temp_below_setpoint BOOLEAN,
    conc_constant_over BOOLEAN,
    conc_constant_under BOOLEAN,
    rinse_temp_below_setpoint BOOLEAN,
    conc_ok BOOLEAN,
    wash_temp_recovery_failure BOOLEAN,
    rinse_temp_recovery_failure BOOLEAN,
    conc_recovery_failure BOOLEAN,
    wash_temp_repeated_issue BOOLEAN,
    rinse_temp_repeated_issue BOOLEAN,
    conc_repeated_issue BOOLEAN,
    wash_temp_high_variance BOOLEAN,
    rinse_temp_high_variance BOOLEAN,
    schema_valid BOOLEAN,
    en_wash_on INT,
    en_rinse_on INT,
    op_state INT,
    date_time_utc BIGINT,
    event_time_utc_ms BIGINT,
    kafka_logappend_ms BIGINT,
    produced_at_ms BIGINT,
    ingest_at TIMESTAMP(3),
    persisted_at TIMESTAMP(3),
    stream_features_ready_at TIMESTAMP(3),
    PRIMARY KEY (mac_address_string, beacon_event_id, event_time_utc_ms, kafka_logappend_ms, produced_at_ms) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}',
  'table-name' = 'public.dishwasher_data',
  'username' = '{PG_USER}',
  'password' = '{PG_PASS}',
  'driver' = 'org.postgresql.Driver',
  'sink.buffer-flush.max-rows' = '10000',
  'sink.buffer-flush.interval' = '50ms',
  'sink.max-retries' = '5'
)
"""

logger.info("Creating JDBC dimension (machines) …")
t_env.execute_sql(machines_lookup_ddl)

logger.info("Creating Kafka source table...")
t_env.execute_sql(source_table)

t_env.execute_sql(pg_sink)
statement_set = t_env.create_statement_set()
statement_set.add_insert_sql(
    f"""
    INSERT INTO dishwasher_pg
    SELECT
      e.Id AS event_id,
      e.DeviceID AS device_id,
      e.MACAddressString AS mac_address_string,
      e.BeaconEventID AS beacon_event_id,
      e.MeanWashTankTemp AS mean_wash_tank_temp,
      e.MeanRinseTemp AS mean_rinse_temp,
      e.MeanConductivity AS mean_conductivity,
      e.SetpointConductivity AS setpoint_conductivity,

      CASE
          WHEN e.EnWashOn >= 1 AND e.MeanWashTankTemp IS NOT NULL
               AND m.wash_temp_setpoint IS NOT NULL
          THEN e.MeanWashTankTemp < CAST(m.wash_temp_setpoint AS DOUBLE)
          ELSE CAST(NULL AS BOOLEAN)
      END AS temp_below_setpoint,

      CASE
          WHEN e.MeanConductivity IS NOT NULL AND e.SetpointConductivity IS NOT NULL
          THEN e.MeanConductivity > e.SetpointConductivity * 1.1
          ELSE CAST(NULL AS BOOLEAN)
      END AS conc_constant_over,

      CASE
          WHEN e.MeanConductivity IS NOT NULL AND e.SetpointConductivity IS NOT NULL
          THEN e.MeanConductivity < e.SetpointConductivity * 0.9
          ELSE CAST(NULL AS BOOLEAN)
      END AS conc_constant_under,

      CASE
          WHEN e.EnRinseOn >= 1 AND e.MeanRinseTemp IS NOT NULL
               AND m.rinse_temp_setpoint IS NOT NULL
          THEN e.MeanRinseTemp < CAST(m.rinse_temp_setpoint AS DOUBLE)
          ELSE CAST(NULL AS BOOLEAN)
      END AS rinse_temp_below_setpoint,

      CASE
          WHEN e.MeanConductivity IS NOT NULL AND e.SetpointConductivity IS NOT NULL AND e.SetpointConductivity > 0
               AND w.avg_conductivity IS NOT NULL
          THEN (w.avg_conductivity BETWEEN e.SetpointConductivity * 0.9 AND e.SetpointConductivity * 1.1)
          ELSE CAST(NULL AS BOOLEAN)
      END AS conc_ok,

      CAST(FALSE AS BOOLEAN) AS wash_temp_recovery_failure,
      CAST(FALSE AS BOOLEAN) AS rinse_temp_recovery_failure,
      CAST(FALSE AS BOOLEAN) AS conc_recovery_failure,
      CAST(FALSE AS BOOLEAN) AS wash_temp_repeated_issue,
      CAST(FALSE AS BOOLEAN) AS rinse_temp_repeated_issue,
      CAST(FALSE AS BOOLEAN) AS conc_repeated_issue,
      CAST(FALSE AS BOOLEAN) AS wash_temp_high_variance,
      CAST(FALSE AS BOOLEAN) AS rinse_temp_high_variance,

      {SCHEMA_OK_VALIDATION} AS schema_valid,
      e.EnWashOn AS en_wash_on,
      e.EnRinseOn AS en_rinse_on,
      CAST(NULL AS INT) AS op_state,
      CASE
        WHEN e.DateTimeUTC IS NULL THEN CAST(EXTRACT(EPOCH FROM e.event_ts) AS BIGINT)
        WHEN CAST(e.DateTimeUTC AS BIGINT) > 1000000000000
          THEN CAST(CAST(e.DateTimeUTC AS BIGINT) / 1000 AS BIGINT)
        ELSE CAST(e.DateTimeUTC AS BIGINT)
      END AS date_time_utc,
      CASE
        WHEN e.DateTimeUTC IS NULL THEN CAST(EXTRACT(EPOCH FROM e.event_ts) * 1000 AS BIGINT)
        WHEN CAST(e.DateTimeUTC AS BIGINT) > 1000000000000
          THEN CAST(e.DateTimeUTC AS BIGINT)
        ELSE CAST(e.DateTimeUTC AS BIGINT) * 1000
      END AS event_time_utc_ms,
      CAST(
        GREATEST(
          EXTRACT(EPOCH FROM e.kafka_ts_ltz) * 1000,
          CAST(e._produced_at_ms AS DOUBLE)
        ) AS BIGINT
      ) AS kafka_logappend_ms,
      e._produced_at_ms AS produced_at_ms,
      CAST(e.proc_time AS TIMESTAMP(3)) AS ingest_at,
      CAST(NULL AS TIMESTAMP(3)) AS persisted_at,
      w.window_end AS stream_features_ready_at
    FROM beacon_events AS e
    LEFT JOIN machines_lookup FOR SYSTEM_TIME AS OF e.proc_time AS m
      ON e.MACAddressString = m.mac
    LEFT JOIN (
        SELECT
            MACAddressString,
            TUMBLE_START(event_ts, INTERVAL '20' MINUTE) AS window_start,
            TUMBLE_END(event_ts, INTERVAL '20' MINUTE) AS window_end,
            AVG(MeanConductivity) AS avg_conductivity,
            MIN(MeanWashTankTemp) AS min_wash_temp,
            MAX(MeanWashTankTemp) AS max_wash_temp,
            COUNT(MeanWashTankTemp) AS wash_temp_count,
            MIN(MeanRinseTemp) AS min_rinse_temp,
            MAX(MeanRinseTemp) AS max_rinse_temp,
            COUNT(MeanRinseTemp) AS rinse_temp_count,
            COUNT(*) AS total_events
        FROM beacon_events
        WHERE MeanConductivity IS NOT NULL OR MeanWashTankTemp IS NOT NULL OR MeanRinseTemp IS NOT NULL
        GROUP BY TUMBLE(event_ts, INTERVAL '20' MINUTE), MACAddressString
    ) AS w
    ON e.MACAddressString = w.MACAddressString
       AND e.event_ts >= w.window_start
       AND e.event_ts < w.window_end
    WHERE {SCHEMA_OK_VALIDATION}
    """
)
logger.info("Submitting Data Fabric Flink job...")
statement_set.execute()