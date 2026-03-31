"""
Load machine reference data from CSV into the machines table
"""

from __future__ import annotations

import csv
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

def load_machines(
    csv_path: Optional[str] = None,
    conn=None,
) -> int:

    csv_path = csv_path or os.getenv(
        "MACHINE_CSV_PATH",
        "/data/Machines_simulatedV1.csv",
    )

    if not os.path.isfile(csv_path):
        logger.warning("Machine CSV not found at %s skipping", csv_path)
        return 0

    # Read CSV
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        rows = list(reader)

    if not rows:
        logger.warning("Machine CSV is empty: %s", csv_path)
        return 0

    logger.info("Read %d machines from %s", len(rows), csv_path)

    # Map csv to db columns
    mapped = []
    for r in rows:
        mac = r.get("MACAdress String").strip() or None
        if not mac:
            continue
        mapped.append(
            {
                "mac": mac,
                "device_name": r.get("Device Name"),
                "model": r.get("Maschine Name And Model"),
                "site": r.get("Site"),
                "wash_temp_setpoint": r.get("Wash Temp Threshold") or None,
                "rinse_temp_setpoint": r.get("Rinse Temp Threshold") or None,
                "dosing_unit_type": r.get("Dosing Unit Type"),
                "energy_kw": r.get("Energy KW") or None,
            }
        )

    if not mapped:
        logger.warning("No machines found")
        return 0

    close_conn = False
    if conn is None:
        from arch_common.postgres_utils import connect_postgres

        conn = connect_postgres(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "archdb"),
            user=os.getenv("POSTGRES_USER", "archuser"),
            password=os.getenv("POSTGRES_PASSWORD", "archpass"),
        )
        close_conn = True

    try:
        cur = conn.cursor()
        for m in mapped:
            cur.execute(
                """
                INSERT INTO machines (mac, device_name, model, site,
                                      wash_temp_setpoint, rinse_temp_setpoint,
                                      dosing_unit_type, energy_kw)
                VALUES (%(mac)s, %(device_name)s, %(model)s, %(site)s,
                        %(wash_temp_setpoint)s, %(rinse_temp_setpoint)s,
                        %(dosing_unit_type)s, %(energy_kw)s)
                ON CONFLICT (mac) DO UPDATE SET
                    device_name = EXCLUDED.device_name,
                    model = EXCLUDED.model,
                    site = EXCLUDED.site,
                    wash_temp_setpoint = EXCLUDED.wash_temp_setpoint,
                    rinse_temp_setpoint = EXCLUDED.rinse_temp_setpoint,
                    dosing_unit_type = EXCLUDED.dosing_unit_type,
                    energy_kw = EXCLUDED.energy_kw
                """,
                m,
            )
        conn.commit()
        cur.close()
        logger.info("Upserted %d machines into DB", len(mapped))
        return len(mapped)
    except Exception:
        conn.rollback()
        raise
    finally:
        if close_conn:
            conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_machines()