FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY services/consumer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/consumer/src/ ./src/
COPY common/python/ /opt/arch_common/

ENV PYTHONPATH="/opt/arch_common:/app/src:${PYTHONPATH}"

CMD ["python", "src/consumer.py"]