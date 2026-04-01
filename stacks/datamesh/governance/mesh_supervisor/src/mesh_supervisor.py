"""
Mesh supervisor for the Data Mesh stack, simulates decentralized ownership
"""
from __future__ import annotations
import logging
import os
import time
from typing import Dict, Tuple
import psycopg2
from prometheus_client import Gauge, start_http_server

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "dbname": os.getenv("POSTGRES_DB", "archdb"),
    "user": os.getenv("POSTGRES_USER", "archuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "archpass"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("mesh_supervisor")

GATE_METRICS = {
    "missing_rate": Gauge(
        "datamesh_quality_missing_rate_avg",
        "Average missing rate per domain",
        ["domain"],
    ),
    "field_consistency": Gauge(
        "datamesh_quality_field_consistency_avg",
        "Average field consistency score per domain",
        ["domain"],
    ),
    "flag_flip_rate": Gauge(
        "datamesh_quality_flag_flip_rate_avg",
        "Average flag flip rate per domain",
        ["domain"],
    ),
    "gate_status": Gauge(
        "datamesh_quality_gate_status",
        "Quality gate status per domain (1=pass,0=warn/fail)",
        ["domain"],
    ),
}

def connect_pg():
    return psycopg2.connect(**PG_CONFIG)

def compute_gates(conn) -> Dict[str, Tuple[float, float, float, str]]:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO mesh_quality_gates (domain, last_gate_run, missing_rate_avg, field_consistency_avg, flag_flip_rate_avg, status, updated_at)
        SELECT
          domain,
          NOW() AS last_gate_run,
          COALESCE(AVG(missing_rate), 0.0) AS missing_rate_avg,
          COALESCE(AVG(field_consistency_score), 0.5) AS field_consistency_avg,
          COALESCE(AVG(flag_flip_rate), 0.0) AS flag_flip_rate_avg,
          CASE 
            WHEN COALESCE(AVG(missing_rate), 0.0) < 0.05 
              AND COALESCE(AVG(field_consistency_score), 0.5) > 0.7
            THEN 'pass'
            ELSE 'warn'
          END AS status,
          NOW()
        FROM dishwasher_data
        GROUP BY domain
        ON CONFLICT (domain) DO UPDATE SET
          last_gate_run = EXCLUDED.last_gate_run,
          missing_rate_avg = EXCLUDED.missing_rate_avg,
          field_consistency_avg = EXCLUDED.field_consistency_avg,
          flag_flip_rate_avg = EXCLUDED.flag_flip_rate_avg,
          status = EXCLUDED.status,
          updated_at = EXCLUDED.updated_at;
        """
    )
    conn.commit()

    cur.execute(
        """
        SELECT domain, missing_rate_avg, field_consistency_avg, flag_flip_rate_avg, status
        FROM mesh_quality_gates
        ORDER BY domain
        """
    )
    results = {
        domain: (missing, consistency, flip_rate, status)
        for domain, missing, consistency, flip_rate, status in cur.fetchall()
    }
    cur.close()
    return results

def publish_metrics(results: Dict[str, Tuple[float, float, float, str]]) -> None:
    for domain, (missing, consistency, flip_rate, status) in results.items():
        GATE_METRICS["missing_rate"].labels(domain=domain).set(missing or 0.0)
        GATE_METRICS["field_consistency"].labels(domain=domain).set(consistency or 0.0)
        GATE_METRICS["flag_flip_rate"].labels(domain=domain).set(flip_rate or 0.0)
        GATE_METRICS["gate_status"].labels(domain=domain).set(1 if status == "pass" else 0)

def main() -> None:
    metrics_port = int(os.getenv("PROMETHEUS_PORT", "8110"))
    start_http_server(metrics_port)
    interval = int(os.getenv("GATE_INTERVAL_SECONDS", "60"))
    while True:
        try:
            with connect_pg() as conn:
                results = compute_gates(conn)
                publish_metrics(results)
                logger.info("Quality gates updated for domains: %s", ", ".join(results.keys()))
        except Exception as exc:
            logger.exception("Mesh supervisor iteration failed: %s", exc)
        time.sleep(interval)

if __name__ == "__main__":
    main()