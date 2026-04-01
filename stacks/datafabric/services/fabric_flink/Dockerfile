FROM arch-flink:1.20.0

USER root

RUN pip install --no-cache-dir apache-flink==1.20.0 fastjsonschema

COPY common/python /opt/arch_common
ENV PYTHONPATH=/opt/arch_common

WORKDIR /opt/flink/usrlib

USER flink