"""
Event Producer
"""
import argparse
import json
import logging
import os
import random
import re
import sys
import time
import uuid
from pathlib import Path
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("producer")

DEFAULT_BROKER = "localhost:29092"
DEFAULT_TOPIC = "ecolab.beacon.canonical.v1"

def get_first_datetime(filepath: Path) -> int | None:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            chunk = f.read(16384)
        m = re.search(r'"DateTimeUTC"\s*:\s*(\d+)', chunk)
        return int(m.group(1)) if m else None
    except Exception:
        return None


def load_events(data_dir: str, duration_minutes: int | None = None) -> list[dict]:
    data_path = Path(data_dir)
    json_files = sorted(
        p for p in data_path.glob("*.json")
        if p.stem not in ("Machines_simulatedV1", "window")
    )
    if not json_files:
        log.error("No JSON files found in %s", data_dir)
        sys.exit(1)
        
    start_times: list[int] = []
    for jf in json_files:
        t = get_first_datetime(jf)
        if t is not None:
            start_times.append(t)
    if not start_times:
        log.error("Could not determine start time from data files")
        sys.exit(1)
    start_time = min(start_times)
    cutoff = start_time + duration_minutes * 60 if duration_minutes else float("inf")

    events: list[dict] = []
    for jf in json_files:
        with open(jf, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            continue
        for evt in data:
            t = evt.get("DateTimeUTC", 0)
            if start_time <= t <= cutoff:
                events.append(evt)
        del data

    events.sort(key=lambda e: e.get("DateTimeUTC", 0))
    log.info(
        "Loaded %d events from %d devices (event-time %d .. %s)",
        len(events),
        len({e.get("MACAddressString") for e in events}),
        start_time,
        cutoff if cutoff != float("inf") else "inf",
    )
    return events

def inject_faults(events: list[dict], fraction: float) -> tuple[list[dict], dict]:
    stats = {"late_injected": 0, "future_injected": 0, "total": len(events)}
    n = max(1, int(len(events) * fraction))
    indices = random.sample(range(len(events)), min(n, len(events)))
    for idx in indices:
        if random.random() < 0.5:
            events[idx]["DateTimeUTC"] -= random.randint(10, 60)
            events[idx]["_fault"] = "late"
            stats["late_injected"] += 1
        else:
            events[idx]["DateTimeUTC"] += 3600
            events[idx]["_fault"] = "future"
            stats["future_injected"] += 1
    return events, stats


def inject_schema_violations(events: list[dict], fraction: float) -> tuple[list[dict], dict]:
    stats = {
        "wash_temp_violations": 0,
        "rinse_temp_violations": 0,
        "conductivity_violations": 0,
        "total": len(events)
    }
    n = max(1, int(len(events) * fraction))
    indices = random.sample(range(len(events)), min(n, len(events)))
    
    violation_types = [-1, 256, 300]
    
    for idx in indices:
        event = events[idx]
        violation_type = random.choice(violation_types)
        
        sensor_choice = random.random()
        if sensor_choice < 0.33:
            event["MeanWashTankTemp"] = violation_type
            event["_schema_violation"] = "wash_temp"
            stats["wash_temp_violations"] += 1
        elif sensor_choice < 0.67:
            event["MeanRinseTemp"] = violation_type
            event["_schema_violation"] = "rinse_temp"
            stats["rinse_temp_violations"] += 1
        else:
            event["MeanConductivity"] = violation_type
            event["_schema_violation"] = "conductivity"
            stats["conductivity_violations"] += 1
    
    return events, stats

delivery_errors = 0


def _on_delivery(err, _msg):
    global delivery_errors
    if err:
        delivery_errors += 1
        if delivery_errors <= 5:
            log.error("Delivery failed: %s", err)


def produce(
    events: list[dict],
    broker: str,
    topic: str,
    rate: int | None,
    speed_factor: float,
    replication_factor: int = 1,
    sustained_seconds: int = 0,
) -> dict:
    conf = {
        "bootstrap.servers": broker,
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 1048576,
        "batch.num.messages": 10000,
        "linger.ms": 5,
    }
    p = Producer(conf)

    if not events:
        log.error("No events to produce")
        return {"produced": 0, "wall_clock_seconds": 0, "actual_msg_per_sec": 0}

    t0_event = events[0].get("DateTimeUTC", 0)
    t0_wall = time.monotonic()
    t0_wall_clock = time.time() 
    burst = max(1, (rate // 10) if rate else 1000)
    burst_count = 0
    burst_start = time.monotonic()
    produced = 0
    total_to_send = len(events) * replication_factor

    if sustained_seconds > 0:
        deadline = t0_wall_clock + sustained_seconds
        cycle = 0
        log.info("Sustained mode: %d events/cycle × %d replicas, target %d msg/s for %ds wall-clock",
                 len(events), replication_factor, rate or 0, sustained_seconds)

        while time.time() < deadline:
            cycle += 1
            for evt in events:
                if time.time() >= deadline:
                    break
                for r in range(replication_factor):
                    if time.time() >= deadline:
                        break
                    payload = {k: v for k, v in evt.items() if k not in ("_fault", "_schema_violation")}
                    if r > 0:
                        orig_mac = evt.get("MACAddressString", "UNK")
                        payload["MACAddressString"] = f"{orig_mac}_R{r:04d}"
                        payload["Id"] = str(uuid.uuid4())
                        payload["id"] = str(uuid.uuid4())
                    payload["_produced_at_ms"] = int(time.time() * 1000)

                    key = payload.get("MACAddressString", "").encode("utf-8")
                    value = json.dumps(payload).encode("utf-8")
                    p.produce(topic, value=value, key=key, callback=_on_delivery)
                    produced += 1
                    burst_count += 1

                    if rate and burst_count >= burst:
                        expected = burst_count / rate
                        wall = time.monotonic() - burst_start
                        if wall < expected:
                            time.sleep(expected - wall)
                        burst_count = 0
                        burst_start = time.monotonic()

                if produced % 5000 == 0:
                    p.poll(0)
                    elapsed_s = time.time() - t0_wall_clock
                    log.info("Sustained: produced %d  (%.1fs / %ds, cycle %d)",
                             produced, elapsed_s, sustained_seconds, cycle)

        log.info("Sustained mode: completed %d full cycles", cycle - 1)
    else:
        for evt in events:
            evt_time = evt.get("DateTimeUTC", 0)
            if speed_factor > 0:
                target_wall = (evt_time - t0_event) / speed_factor
                elapsed = time.monotonic() - t0_wall
                if target_wall > elapsed:
                    time.sleep(target_wall - elapsed)

            for r in range(replication_factor):
                if r == 0:
                    payload = {k: v for k, v in evt.items() if k not in ("_fault", "_schema_violation")}
                else:
                    payload = {k: v for k, v in evt.items() if k not in ("_fault", "_schema_violation")}
                    orig_mac = evt.get("MACAddressString", "UNK")
                    payload["MACAddressString"] = f"{orig_mac}_R{r:04d}"
                    payload["Id"] = str(uuid.uuid4())
                    payload["id"] = str(uuid.uuid4())

                payload["_produced_at_ms"] = int(time.time() * 1000)

                key = payload.get("MACAddressString", "").encode("utf-8")
                value = json.dumps(payload).encode("utf-8")
                p.produce(topic, value=value, key=key, callback=_on_delivery)
                produced += 1
                burst_count += 1
                if rate and burst_count >= burst:
                    expected = burst_count / rate
                    wall = time.monotonic() - burst_start
                    if wall < expected:
                        time.sleep(expected - wall)
                    burst_count = 0
                    burst_start = time.monotonic()

            if produced % 5000 == 0:
                p.poll(0)
                evt_min = (evt_time - t0_event) / 60.0
                log.info(
                    "Produced %d / %d  (event-time +%.1f min)",
                    produced,
                    total_to_send,
                    evt_min,
                )

    log.info("Flushing remaining messages")
    p.flush(timeout=60)

    wall_s = time.time() - t0_wall_clock
    actual_rate = produced / wall_s if wall_s > 0 else 0
    log.info(
        "Done.  produced=%d  delivery_errors=%d  wall_clock=%.1fs  actual_rate=%.1f msg/s",
        produced,
        delivery_errors,
        wall_s,
        actual_rate,
    )
    return {
        "produced": produced,
        "delivery_errors": delivery_errors,
        "wall_clock_seconds": round(wall_s, 2),
        "actual_msg_per_sec": round(actual_rate, 1),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["real", "replicated"], default="real")
    ap.add_argument("--data-dir", default="../../data", help="Path to per-device JSON files")
    ap.add_argument("--broker", default=DEFAULT_BROKER)
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--rate", type=int, default=None)
    ap.add_argument(
        "--speed-factor",
        type=float,
        default=10.0
    )
    ap.add_argument("--replication-factor", type=int, default=2, help="Device cloning multiplier")
    ap.add_argument(
        "--duration-minutes",
        type=int,
        default=125
    )
    ap.add_argument(
        "--fault-injection",
        type=float,
        default=0.0
    )
    ap.add_argument(
        "--schema-violation-rate",
        type=float,
        default=0.0
    )
    ap.add_argument(
        "--sustained-seconds",
        type=int,
        default=0
    )
    args = ap.parse_args()

    log.info(
        "Producer: mode=%s  broker=%s  topic=%s  rate=%s  speed_factor=%s",
        args.mode,
        args.broker,
        args.topic,
        args.rate,
        args.speed_factor,
    )

    events = load_events(args.data_dir, args.duration_minutes)

    if not events:
        log.error("No events loaded – aborting.")
        sys.exit(1)

    fault_stats = None
    if args.fault_injection > 0:
        events, fault_stats = inject_faults(events, args.fault_injection)
        log.info("Fault injection stats: %s", fault_stats)

    schema_violation_stats = None
    if args.schema_violation_rate > 0:
        events, schema_violation_stats = inject_schema_violations(events, args.schema_violation_rate)
        log.info("Schema violation injection stats: %s", schema_violation_stats)

    rf = args.replication_factor if args.mode == "replicated" else 1
    sf = args.speed_factor

    stats = produce(events, args.broker, args.topic, args.rate, sf, rf,
                    sustained_seconds=args.sustained_seconds)

    report = {
        "mode": args.mode,
        "base_events": len(events),
        "replication_factor": rf,
        "speed_factor": sf,
        "rate_limit": args.rate,
        "duration_minutes": args.duration_minutes,
        "fault_injection": fault_stats,
        "schema_violation_rate": schema_violation_stats,
        **stats,
    }
    report_path = os.environ.get("PRODUCER_REPORT", None)
    if report_path:
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        log.info("Report written to %s", report_path)

    log.info("Producer finished. %s", report)

if __name__ == "__main__":
    main()