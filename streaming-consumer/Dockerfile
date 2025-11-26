FROM python:3.11-slim
WORKDIR /app
COPY kafka_to_clickhouse_consumer.py ./
RUN pip install confluent-kafka clickhouse-driver
CMD ["python", "kafka_to_clickhouse_consumer.py"]
