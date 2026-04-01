"""
Data Fabric feature builder service
"""
from __future__ import annotations
import logging
import os
import time
from arch_common.compute_features_job import compute_features_batch
from arch_common.config_loader import get_config_value, load_config
from arch_common.postgres_utils import connect_postgres as pg_connect

CONFIG_PATH = os.getenv("CONFIG_PATH", "/config/config.yaml")
CONFIG = load_config(CONFIG_PATH)

ARCHITECTURE = os.getenv("ARCHITECTURE", "datafabric").lower()
METRIC_PREFIX = "fabric" if ARCHITECTURE == "datafabric" else ARCHITECTURE
LOG_LEVEL = get_config_value(CONFIG, "logging.level").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger(f"{METRIC_PREFIX}_feature_builder")

def _connect_postgres():
    return pg_connect(
        host=get_config_value(CONFIG, "postgres.host"),
        port=get_config_value(CONFIG, "postgres.port"),
        database=get_config_value(CONFIG, "postgres.db"),
        user=get_config_value(CONFIG, "postgres.user"),
        password=get_config_value(CONFIG, "postgres.password"),
    )

def run() -> None:
    interval = max(int(get_config_value(CONFIG, "feature_builder.interval_seconds")), 5)
    batch_size = int(get_config_value(CONFIG, "feature_builder.batch_size"))

    start_time = time.time()
    conn = None
    try:
        conn = _connect_postgres()
        rows = compute_features_batch(
            conn,
            architecture=ARCHITECTURE,
            batch_size=batch_size,
            expected_rate_per_minute=None,
        )
        duration = time.time() - start_time
        logger.info(
            "Feature builder run completed: rows=%d duration=%.2fs",
            rows,
            duration,
        )
    except Exception as exc:
        logger.exception("Feature builder run failed: %s", exc)
    finally:
        if conn:
            try:
                conn.close()
            except Exception as close_exc:
                logger.warning("Failed to close Postgres connection: %s", close_exc)
    time.sleep(interval)

def main() -> None:
    while True:
        run()

if __name__ == "__main__":
    main()