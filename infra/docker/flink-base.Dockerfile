FROM flink:1.20.0-java17

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl ntp && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip install --no-cache-dir pyflink psycopg2-binary prometheus_client ruamel.yaml typing_extensions confluent-kafka pyyaml && \
    echo "server 127.127.1.0" >> /etc/ntp.conf && \
    echo "fudge 127.127.1.0 stratum 10" >> /etc/ntp.conf && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.20.0/flink-json-1.20.0.jar /opt/flink/lib/
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/flink/lib/postgresql.jar

RUN chmod 0644 /opt/flink/lib/*.jar && chown flink:flink /opt/flink/lib/*.jar
COPY common/python/ /opt/arch_common/
ENV PYTHONPATH="/opt/arch_common:${PYTHONPATH}"

WORKDIR /opt/flink/usrlib