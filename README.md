# Masterthesis D.T. - Architecture Comparison

Compares multiple data architectures (Baseline, Lambda, Kappa, Data Fabric,
Data Mesh) in IIoT Context.
---

## Repository Layout

Main Components:

```
data/
stacks/
  baseline/docker-compose.yml
  lambda/docker-compose.yml
  kappa/docker-compose.yml
  datafabric/docker-compose.yml
  datamesh/docker-compose.yml
tools/
  producer/producer.py
  runner/run_experiments.py
results/
  summary.json
```

---

## Setup & Prerequisites
Setup tested on Windows only. 

Required:
- Docker + Docker Compose
- Python 3.10+
- pip packages: confluent-kafka, psycopg2-binary, pandas

Install Python dependencies (one-time) via venv:

```bash
python -m venv .venv

.\.venv\Scripts\activate

pip install confluent-kafka psycopg2-binary pandas matplotlib numpy pyyaml
```

Setup Flink and Spark for Streaming (Windows only)
```bash
.\infra\scripts\build-base-images.ps1
```

Start the Tests. It will start the stack container automatically:

```bash
.\tools\runner\run_experiments.py
```

### Quick Test Setup

For reproduction quick test setup can be found in `experiment_config.yaml`. It will be automatically applied to the experimental runner. Duration: Around 
120mins.

### Thesis Experimental Setup
In the Thesis the configuration is set to 5x5x5 = 125 Runs. The Experiment will take around 22 hours. Rename `experiment_config thesis.yaml` to `experiment_config.yaml`.

### Customize Setup

Customize Runner:
```
repeats: Count of repeats
warmup_minutes: minutes to warm up the setup with events (to remove setup artifacts)
measurement_minutes: event minutes
settle_seconds: additionally wait for services to settle
```

Pipeline drain timing (max wait and stability window) is fixed in `tools/runner/run_experiments.py`.

The workloads can be adjusted by the following parameters:
```
enabled: true | false
mode: real | replicated
speed_factor: time multiplier (0 = no pacing)
replication_factor: device cloning (1 = no cloning)
rate: max events/s (null = unlimited)
sustained_seconds: optional
fault_injection: number of events with faults in %
```

Each stack can be started via

```bash
  cd stacks/ARCHITECTURE_NAME
  
  docker compose up -d
```

## Results

Results will be saved in folder /results:

```bash
summary.json -> Summarizes whole experiment

/figures -> Contains figures to experiment
```