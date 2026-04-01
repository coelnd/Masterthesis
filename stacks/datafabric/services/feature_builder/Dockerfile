FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    librdkafka-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY stacks/datafabric/services/feature_builder/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY stacks/datafabric/services/feature_builder/src/ /app/src/
COPY common/python/ /opt/arch_common/

ENV PYTHONPATH="/opt/arch_common:/app/src:${PYTHONPATH}"

ENV ARCHITECTURE=datafabric

CMD ["python", "src/feature_builder.py"]