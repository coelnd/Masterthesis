"""
Using psycopg2 for psql connection
"""

from __future__ import annotations

import logging
from typing import Optional

import psycopg2

logger = logging.getLogger(__name__)


def connect_postgres(
    host: str = "postgres",
    port: int = 5432,
    database: Optional[str] = None,
    dbname: Optional[str] = None,
    user: str = "archuser",
    password: str = "archpass",
) -> psycopg2.extensions.connection:

    db = database or dbname or "archdb"
    conn = psycopg2.connect(
        host=host,
        port=int(port),
        dbname=db,
        user=user,
        password=password,
    )
    conn.autocommit = False
    logger.debug("Connected to PostgreSQL %s:%s/%s", host, port, db)
    return conn