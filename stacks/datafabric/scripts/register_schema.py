"""
Register the event schema with Confluent Schema Registry
"""
import json
import os
import sys
import time
import urllib.request

def wait_for_registry(registry_url: str) -> None:
    base = registry_url.rstrip("/")
    check_url = f"{base}/subjects"
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(check_url, method="GET")
            with urllib.request.urlopen(req, timeout=5) as _:
                return
        except OSError:
            time.sleep(2.0)
    raise RuntimeError(f"Schema Registry not reachable after 60s")

def register_schema(registry_url: str, subject: str, schema_path: str, schema_type: str) -> None:
    registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8085")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, "r", encoding="utf-8") as handle:
        schema_text = handle.read()

    payload = json.dumps(
        {
            "schemaType": schema_type,
            "schema": schema_text,
        }
    ).encode("utf-8")

    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=10) as resp:
        sys.stdout.write(resp.read().decode("utf-8"))
        sys.stdout.flush()

def main() -> None:
    registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8085")
    subject = os.getenv("SCHEMA_SUBJECT", "ecolab.beacon.canonical.v1-value")
    schema_path = os.getenv("SCHEMA_FILE", "/schemas/beacon_event.schema.json")
    schema_type = os.getenv("SCHEMA_TYPE", "JSON")
    wait_for_registry(registry_url)
    register_schema(registry_url, subject, schema_path, schema_type)

if __name__ == "__main__":
    main()