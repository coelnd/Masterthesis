"""YAML configuration loader — requires an existing config file."""

from __future__ import annotations
import os
from pathlib import Path
from typing import Any
import yaml

def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    config: dict = raw

    env_map = {
        "KAFKA_BROKER": "kafka.broker",
        "CONSUMER_GROUP_ID": "kafka.consumer_group_id",
        "POSTGRES_HOST": "postgres.host",
        "POSTGRES_PORT": "postgres.port",
        "POSTGRES_DB": "postgres.db",
        "POSTGRES_USER": "postgres.user",
        "POSTGRES_PASSWORD": "postgres.password",
        "LOG_LEVEL": "logging.level",
    }
    for env_key, dotted in env_map.items():
        val = os.environ.get(env_key)
        if val is not None:
            set_config_value(config, dotted, val)

    return config


def set_config_value(config: dict, key: str, value: Any) -> None:
    keys = key.split(".")
    conf = config
    for k in keys[:-1]:
        conf = conf.setdefault(k, {})
    conf[keys[-1]] = value


def get_config_value(config: dict, key: str) -> Any:
    keys = key.split(".")
    conf: Any = config
    for k in keys:
        if not isinstance(conf, dict) or k not in conf:
            raise KeyError(f"Missing config key: {key!r}")
        conf = conf[k]
    return conf