
FROM apache/superset:latest
USER root
RUN pip install clickhouse-connect==0.5.25
USER superset
