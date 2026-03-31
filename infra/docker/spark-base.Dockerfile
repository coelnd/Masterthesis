FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary \
    prometheus_client \
    pyyaml \
    kafka-python

COPY common/python/ /opt/arch_common/

WORKDIR /opt/spark/work-dir

USER spark