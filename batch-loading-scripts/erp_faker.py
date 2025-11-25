import os
import random
import time
from datetime import datetime
import json
import psycopg2
from confluent_kafka import Producer

POSTGRES_DSN = os.getenv("PG_DSN", "dbname=ducklens_db user=user password=password host=postgres_container port=5432")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "erp.erp.sales")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Kafka delivery failed: {err}")
    else:
        print(f"📤 Kafka produced to {msg.topic()} [offset={msg.offset()}]")

def get_conn():
    return psycopg2.connect(POSTGRES_DSN)

trucks = ["TRUCK-001","TRUCK-002","TRUCK-003"]
depots = ["Central Depot","North Depot","JAFFCOM","MAJESTIC"]
banks = ["Bank A","Bank B","Bank KCB","Bank ABSA","Bank COOP"]

# Generate a fake ERP sales row

def generate_sales_row():
    ts = datetime.utcnow()
    order_date = ts.date()
    truck = random.choice(trucks)
    depot = random.choice(depots)
    bank = random.choice(banks)
    pms_qty = round(random.uniform(100, 500), 2)
    pms_price = round(random.uniform(150, 200), 2)
    ago_qty = round(random.uniform(50, 300), 2)
    ago_price = round(random.uniform(140, 180), 2)
    total = round(pms_qty * pms_price + ago_qty * ago_price, 2)
    transport_cost = round(total * 0.05, 2)
    amount_received = round(total * random.uniform(0.7, 1.0), 2)
    balance = round(total - amount_received, 2)
    sale = {
        "order_date": str(order_date),
        "truck": truck,
        "pms_qty": pms_qty,
        "pms_price": pms_price,
        "ago_qty": ago_qty,
        "ago_price": ago_price,
        "total": total,
        "depot": depot,
        "transport_cost": transport_cost,
        "pay_date": str(order_date),
        "amount_received": amount_received,
        "bank": bank,
        "balance": balance
    }
    return sale

def insert_postgres_sale(cur, sale):
    cur.execute(
        """
        INSERT INTO erp.sales(order_date, truck, pms_qty, pms_price, ago_qty, ago_price, total, depot, transport_cost, pay_date, amount_received, bank, balance)
        VALUES (%(order_date)s,%(truck)s,%(pms_qty)s,%(pms_price)s,%(ago_qty)s,%(ago_price)s,%(total)s,%(depot)s,%(transport_cost)s,%(pay_date)s,%(amount_received)s,%(bank)s,%(balance)s)
        RETURNING sale_id
        """, sale)
    return cur.fetchone()[0]

def main():
    print("🚀 ERP Faker starting. Writing to Postgres and Kafka.")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        while True:
            sale = generate_sales_row()
            sale_id = insert_postgres_sale(cur, sale)
            sale["sale_id"] = sale_id
            producer.produce(KAFKA_TOPIC, json.dumps(sale), callback=delivery_report)
            producer.poll(0)
            print(f"✅ Generated sale sale_id={sale_id} total={sale['total']}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("🛑 Faker stopped.")
    finally:
        producer.flush()
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
