"""
Kafka consumer
"""
from __future__ import annotations
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg2.extras import execute_batch
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
logger = logging.getLogger("consumer")

KAFKA_BROKER = get_config_value(CONFIG, "kafka.broker")
TOPIC = get_config_value(CONFIG, "kafka.topic")
GROUP_ID = get_config_value(CONFIG, "kafka.consumer_group_id")

PG_HOST = get_config_value(CONFIG, "postgres.host")
PG_PORT = int(get_config_value(CONFIG, "postgres.port"))
PG_DB = get_config_value(CONFIG, "postgres.db")
PG_USER = get_config_value(CONFIG, "postgres.user")
PG_PASS = get_config_value(CONFIG, "postgres.password")

BATCH_SIZE = 200
FLUSH_INTERVAL = 0.5

_running = True

FIELDS_MAPPED = {
    "Id": "event_id",
    "DeviceID": "device_id",
    "MACAddressString": "mac_address_string",
    "BeaconEventID": "beacon_event_id",
    "MeanConductivity": "mean_conductivity",
    "SetpointConductivity": "setpoint_conductivity",
    "MeanWashTankTemp": "mean_wash_tank_temp",
    "MeanRinseTemp": "mean_rinse_temp",
    "EnFillOn": "en_fill_on",
    "EnRinseOn": "en_rinse_on",
    "EnWashOn": "en_wash_on",
    "VersionAndMessageType": "version_and_message_type",
    "DateTimeUTC": "date_time_utc",
    "_produced_at_ms": "produced_at_ms",
}

INSERT_COLS = [
    "event_id", "device_id", "mac_address_string",
    "beacon_event_id",
    "mean_conductivity", "setpoint_conductivity",
    "mean_wash_tank_temp", "mean_rinse_temp",
    "en_fill_on", "en_rinse_on", "en_wash_on",
    "version_and_message_type",
    "date_time_utc",
    "event_time_utc_ms", "kafka_logappend_ms",
    "produced_at_ms", "ingest_at",
]

PLACEHOLDER = ", ".join(["%s"] * len(INSERT_COLS))
INSERT_SQL = f"""
INSERT INTO dishwasher_data ({", ".join(INSERT_COLS)})
VALUES ({PLACEHOLDER})
ON CONFLICT (event_id) DO NOTHING
"""

def parse_event(raw_json: bytes, kafka_ts_ms: Optional[int]) -> Optional[tuple]:
    try:
        evt = json.loads(raw_json)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    values = {}
    for json_key, db_col in FIELDS_MAPPED.items():
        values[db_col] = evt.get(json_key)

    date_time_utc = values.get("date_time_utc")
    if date_time_utc is not None:
        try:
            date_time_utc = int(date_time_utc)
            event_time_utc_ms = date_time_utc if date_time_utc > 1e12 else date_time_utc * 1000
        except (ValueError, TypeError):
            event_time_utc_ms = 0
    else:
        event_time_utc_ms = 0

    values["event_time_utc_ms"] = event_time_utc_ms
    values["kafka_logappend_ms"] = kafka_ts_ms
    values["ingest_at"] = datetime.now(timezone.utc)

    return tuple(values.get(col) for col in INSERT_COLS)

def connect_postgres() -> psycopg2.extensions.connection:
    for attempt in range(5):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASS,
            )
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("Postgres connect attempt %d failed: %s", attempt + 1, exc)
            time.sleep(2)
    raise RuntimeError("Could not connect to PostgreSQL after 5 attempts")


def flush_rows(conn, buffer: list[tuple]) -> int:
    if not buffer:
        return 0
    try:
        cur = conn.cursor()
        execute_batch(cur, INSERT_SQL, buffer, page_size=200)
        conn.commit()
        cur.close()
        return len(buffer)
    except Exception as exc:
        conn.rollback()
        logger.error("Insert failed for %d rows: %s", len(buffer), exc)
        return 0

def main():
    logger.info(
        "Consumer starting - broker=%s topic=%s pg=%s:%s/%s",
        KAFKA_BROKER, TOPIC, PG_HOST, PG_PORT, PG_DB,
    )

    pg_conn = connect_postgres()
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe([TOPIC])
    logger.info("Started consumer")

    buffer: list[tuple] = []
    last_flush = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=0.2)

            if msg is None:
                if buffer and (time.monotonic() - last_flush) >= FLUSH_INTERVAL:
                    flush_rows(pg_conn, buffer)
                    buffer.clear()
                    last_flush = time.monotonic()
                continue

            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue

            ts_type, ts_val = msg.timestamp()
            kafka_ts_ms = ts_val if ts_type != 0 and ts_val > 0 else None

            row = parse_event(msg.value(), kafka_ts_ms)
            if row is not None:
                buffer.append(row)

            if len(buffer) >= BATCH_SIZE or (time.monotonic() - last_flush) >= FLUSH_INTERVAL:
                flush_rows(pg_conn, buffer)
                buffer.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    finally:
        if buffer:
            flush_rows(pg_conn, buffer)
        consumer.close()
        pg_conn.close()

if __name__ == "__main__":
    main()