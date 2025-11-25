"""Airflow DAG: Sync dimension tables from Postgres to ClickHouse.
Runs every 30 minutes. Simple approach using python operators.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import psycopg2
import clickhouse_connect

POSTGRES_DSN = "dbname=ducklens_db user=user password=password host=db port=5432"
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "demo"

DIM_TABLES = [
    ("dim_customer", ["customer_id", "customer_name", "region"]),
    ("dim_supplier", ["supplier_id", "supplier_name", "region"]),
    ("dim_truck", ["truck_id", "truck_number"]),
    ("dim_product", ["product_id", "product_name", "unit_price"]),
    ("dim_depot", ["depot_id", "depot_name"]),
    ("dim_bank", ["bank_id", "bank_name"]),
    ("dim_account", ["account_id", "account_code", "account_name", "type"]),
]

def fetch_postgres(table, columns, since_ts):
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()
    where_clause = ""
    if since_ts:
        where_clause = f" WHERE updated_at > %s"
        cur.execute(f"SELECT {', '.join(columns)} FROM {table}{where_clause}", (since_ts,))
    else:
        cur.execute(f"SELECT {', '.join(columns)} FROM {table}")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def load_clickhouse(table, columns, rows):
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
    if rows:
        client.insert(table, rows, column_names=columns)

def sync_table(table, columns):
    # Track last sync timestamp per table
    var_key = f"last_sync_{table}"
    since_ts = Variable.get(var_key, default_var=None)
    rows = fetch_postgres(table, columns, since_ts)
    load_clickhouse(table, columns, rows)
    # Update variable to now if rows were fetched
    if rows:
        Variable.set(var_key, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

def make_task(table, columns):
    return PythonOperator(
        task_id=f"sync_{table}",
        python_callable=lambda: sync_table(table, columns)
    )

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

sync_dimensions_clickhouse = DAG(
    dag_id="sync_dimensions_clickhouse",
    default_args=default_args,
    start_date=datetime(2025, 11, 19),
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=["clickhouse","dimension","sync"],
)
tasks = [make_task(t, c) for t, c in DIM_TABLES]
for task in tasks:
    task.dag = sync_dimensions_clickhouse
