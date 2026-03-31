"""
Lambda speed layer flink job
"""

import logging
import os
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment
from arch_common.config_loader import get_config_value, load_config

# Load configuration from yaml
CONFIG_PATH = os.getenv("CONFIG_PATH", "/config/config.yaml")
CONFIG = load_config(CONFIG_PATH)

# Configure logging
LOG_LEVEL = get_config_value(CONFIG, "logging.level").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = get_config_value(CONFIG, "kafka.broker")
TOPIC_NAME = get_config_value(CONFIG, "kafka.topic")
PG_HOST = get_config_value(CONFIG, "postgres.host")
PG_DB = get_config_value(CONFIG, "postgres.db")
PG_USER = get_config_value(CONFIG, "postgres.user")
PG_PASS = get_config_value(CONFIG, "postgres.password")
PG_PORT = int(get_config_value(CONFIG, "postgres.port"))

logger.info("Initializing Flink execution environment")
env = StreamExecutionEnvironment.get_execution_environment()

jar_paths = [
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar",
    "file:///opt/flink/lib/flink-connector-jdbc-3.3.0-1.20.jar",
    "file:///opt/flink/lib/flink-json-1.20.0.jar",
    "file:///opt/flink/lib/postgresql.jar",
]

for jar_path in jar_paths:
    try:
        env.add_jars(jar_path)
        logger.debug("Added JAR: %s", jar_path)
    except Exception as e:
        logger.warning("Failed to add JAR %s: %s", jar_path, e)

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)
t_env.get_config().set("pipeline.name", "Lambda Speed Layer")
t_env.get_config().set("table.dynamic-table-options.enabled", "true")

logger.info("Using in-memory catalog; Hive disabled")
machines_lookup_ddl = f"""
CREATE TEMPORARY TABLE machines_lookup (
    mac STRING,
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
    'lookup.cache.max-rows' = '1000',
    'lookup.cache.ttl' = '1h'
)
"""

try:
    t_env.execute_sql(machines_lookup_ddl)
    logger.info("Created JDBC lookup table for machines")
except Exception as e:
    logger.error("Failed to create machines lookup table: %s", e, exc_info=True)
    raise

source_ddl = f"""
CREATE TEMPORARY TABLE beacon_events (
    Id STRING,
    DeviceID STRING,
    BeaconEventID INT,
    RawData STRING,
    MinConductivity DOUBLE,
    MeanConductivity DOUBLE,
    MaxConductivity DOUBLE,
    SetpointConductivity DOUBLE,
    MeanWashTankTemp DOUBLE,
    MeanRinseTemp DOUBLE,
    EnFillOn INT,
    EnRinseOn INT,
    EnWashOn INT,
    RackCounter INT,
    OpState INT,
    ProjectCode STRING,
    VersionAndMessageType INT,
    InputID INT,
    RSSI INT,
    EventID INT,
    EventCode INT,
    SubEventCode INT,
    MACAddressString STRING,
    InputCounter INT,
    MOD_OPER STRING,
    CommissionedSensorID INT,
    DateTimeLocal BIGINT,
    DateTimeUTC BIGINT,
    MessageType STRING,
    prod_ts DOUBLE,
    _produced_at_ms BIGINT,
    _simdevice_id INT,
    _architecture STRING,
    `ts` AS TO_TIMESTAMP_LTZ(DateTimeUTC, 3),
    proc_time AS PROCTIME(),
    kafka_ts_ltz TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    WATERMARK FOR `ts` AS `ts` - INTERVAL '2' YEAR
) WITH (
  'connector' = 'kafka',
  'topic' = '{TOPIC_NAME}',
  'properties.bootstrap.servers' = '{KAFKA_BROKER}',
  'properties.group.id' = 'flink-lambda-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'value.json.ignore-parse-errors' = 'true'
)
"""

try:
    t_env.execute_sql(source_ddl)
    logger.info("Created Kafka source table")
except Exception as e:
    logger.error("Failed to create source table: %s", e, exc_info=True)
    raise

dishwasher_sink_ddl = f"""
CREATE TEMPORARY TABLE dishwasher_sink (
    event_id STRING PRIMARY KEY NOT ENFORCED,
    device_id STRING,
    mac_address_string STRING,
    beacon_event_id INT,
    mean_conductivity DOUBLE,
    setpoint_conductivity DOUBLE,
    mean_wash_tank_temp DOUBLE,
    mean_rinse_temp DOUBLE,
    en_fill_on INT,
    en_rinse_on INT,
    en_wash_on INT,
    version_and_message_type INT,
    date_time_utc BIGINT,
    event_time_utc_ms BIGINT,
    kafka_logappend_ms BIGINT,
    produced_at_ms BIGINT,
    ingest_at TIMESTAMP(3),
    persisted_at TIMESTAMP(3),
    latency_seconds DOUBLE,
    temp_below_setpoint BOOLEAN,
    rinse_temp_below_setpoint BOOLEAN,
    conc_constant_over BOOLEAN,
    conc_constant_under BOOLEAN,
    conc_ok BOOLEAN,
    wash_temp_recovery_failure BOOLEAN,
    rinse_temp_recovery_failure BOOLEAN,
    conc_recovery_failure BOOLEAN,
    wash_temp_repeated_issue BOOLEAN,
    rinse_temp_repeated_issue BOOLEAN,
    conc_repeated_issue BOOLEAN,
    wash_temp_high_variance BOOLEAN,
    rinse_temp_high_variance BOOLEAN,
    flags_computed_at TIMESTAMP,
    speed_features_ready_at TIMESTAMP(3)
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}',
  'table-name' = 'public.dishwasher_data',
  'username' = '{PG_USER}',
  'password' = '{PG_PASS}',
  'driver' = 'org.postgresql.Driver',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '100ms',
  'sink.max-retries' = '3'
)
"""

try:
    t_env.execute_sql(dishwasher_sink_ddl)
    logger.info("Created PostgreSQL dishwasher_data sink table (Speed Layer)")
except Exception as e:
    logger.error("Failed to create dishwasher sink table: %s", e, exc_info=True)
    raise

tables = t_env.list_tables()
logger.info("Available tables: %s", [t for t in tables])

dishwasher_insert_stmt = """
INSERT INTO dishwasher_sink
SELECT
    e.Id AS event_id,
    e.DeviceID AS device_id,
    e.MACAddressString AS mac_address_string,
    e.BeaconEventID AS beacon_event_id,
    e.MeanConductivity AS mean_conductivity,
    e.SetpointConductivity AS setpoint_conductivity,
    e.MeanWashTankTemp AS mean_wash_tank_temp,
    e.MeanRinseTemp AS mean_rinse_temp,
    e.EnFillOn AS en_fill_on,
    e.EnRinseOn AS en_rinse_on,
    e.EnWashOn AS en_wash_on,
    e.VersionAndMessageType AS version_and_message_type,
    e.DateTimeUTC AS date_time_utc,
    CASE
        WHEN e.DateTimeUTC IS NULL THEN CAST(EXTRACT(EPOCH FROM e.`ts`) * 1000 AS BIGINT)
        WHEN CAST(e.DateTimeUTC AS BIGINT) > 1000000000000 THEN CAST(e.DateTimeUTC AS BIGINT)
        ELSE CAST(e.DateTimeUTC AS BIGINT) * 1000
    END AS event_time_utc_ms,
    CAST(EXTRACT(EPOCH FROM e.kafka_ts_ltz) * 1000 AS BIGINT) AS kafka_logappend_ms,
    e._produced_at_ms AS produced_at_ms,
    LOCALTIMESTAMP AS ingest_at,
    CAST(NULL AS TIMESTAMP(3)) AS persisted_at,
    CAST(NULL AS DOUBLE) AS latency_seconds,

    CASE 
        WHEN e.EnWashOn >= 1 AND e.MeanWashTankTemp IS NOT NULL 
             AND m.wash_temp_setpoint IS NOT NULL
        THEN e.MeanWashTankTemp < CAST(m.wash_temp_setpoint AS DOUBLE)
        ELSE CAST(NULL AS BOOLEAN)
    END AS temp_below_setpoint,
    
    CASE 
        WHEN e.EnRinseOn >= 1 AND e.MeanRinseTemp IS NOT NULL 
             AND m.rinse_temp_setpoint IS NOT NULL
        THEN e.MeanRinseTemp < CAST(m.rinse_temp_setpoint AS DOUBLE)
        ELSE CAST(NULL AS BOOLEAN)
    END AS rinse_temp_below_setpoint,

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
    
    CAST(NULL AS TIMESTAMP) AS flags_computed_at,
    w.window_end AS speed_features_ready_at
FROM beacon_events AS e
LEFT JOIN machines_lookup AS m
ON e.MACAddressString = m.mac
LEFT JOIN (
    SELECT
        MACAddressString,
        TUMBLE_START(`ts`, INTERVAL '6' MINUTE) AS window_start,
        TUMBLE_END(`ts`, INTERVAL '6' MINUTE) AS window_end,
        AVG(MeanConductivity) AS avg_conductivity,
        MIN(MeanRinseTemp) AS min_rinse_temp,
        MAX(MeanRinseTemp) AS max_rinse_temp,
        COUNT(MeanRinseTemp) AS rinse_temp_count,
        COUNT(*) AS total_events
    FROM beacon_events
    WHERE MeanConductivity IS NOT NULL OR MeanWashTankTemp IS NOT NULL OR MeanRinseTemp IS NOT NULL
    GROUP BY TUMBLE(`ts`, INTERVAL '6' MINUTE), MACAddressString
) AS w
ON e.MACAddressString = w.MACAddressString
   AND e.`ts` >= w.window_start
   AND e.`ts` < w.window_end
"""

try:
    logger.info("Executing Flink job: Lambda Speed Layer")
    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(dishwasher_insert_stmt)
    statement_set.execute()
    logger.info("Flink job submitted and executed successfully")
except Exception as e:
    logger.error("Failed to execute Flink job: %s", e, exc_info=True)
    raise