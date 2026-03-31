FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY stacks/lambda/services/batch/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY stacks/lambda/services/batch/src/ ./src/
COPY common/python/ /opt/arch_common/

ENV PYTHONPATH="/opt/arch_common:/app/src:${PYTHONPATH}"

ENV POSTGRES_HOST=postgres \
    POSTGRES_DB=archdb \
    POSTGRES_USER=archuser \
    POSTGRES_PASSWORD=archpass \
    KAFKA_BROKER=kafka:9092

CMD ["python", "src/batch_job.py"]