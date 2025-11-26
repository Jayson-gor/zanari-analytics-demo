from confluent_kafka import Consumer
from clickhouse_driver import Client
import json

# Kafka config
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'erp-cdc-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['erp.erp.sales'])

# ClickHouse config
client = Client(host='clickhouse', port=9000, user='default', password='', database='default')

def process_message(msg):
    try:
        value = msg.value()
        if not value:
            print("Received empty message")
            return
        event = json.loads(value)
        row = event
        # Helper for unknown dimension
        def unknown(val, default):
            return val if val not in (None, '', 'UNKNOWN') else default

        # Always treat as sales record from faker
        print(f"Processing sale_id: {row.get('sale_id', 0)}")
        client.execute(
            """
            INSERT INTO fact_sales (
                order_id, order_date_key, customer_id, truck_id, product_id, depot_id, bank_id,
                quantity_pms, quantity_ago, total_amount, transport_cost, amount_received, balance
            ) VALUES
            """,
            [[
                row.get('sale_id', 0),
                0,
                0,
                unknown(row.get('truck_id', 0), 0),
                unknown(row.get('product_id', 0), 0),
                unknown(row.get('depot_id', 0), 0),
                unknown(row.get('bank_id', 0), 0),
                int(row.get('pms_qty', 0)),
                int(row.get('ago_qty', 0)),
                float(row.get('total', 0)),
                float(row.get('transport_cost', 0)),
                float(row.get('amount_received', 0)),
                float(row.get('balance', 0))
            ]]
        )
    except Exception as e:
        print(f"Error processing message: {e}")

import time
print("Kafka consumer started. Waiting for messages...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            time.sleep(1)
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        process_message(msg)
except KeyboardInterrupt:
    print("Consumer interrupted. Closing...")
finally:
    consumer.close()