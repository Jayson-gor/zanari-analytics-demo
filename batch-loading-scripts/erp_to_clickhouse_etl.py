
import sys
import subprocess
try:
    from clickhouse_driver import Client
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'clickhouse-driver'])
    from clickhouse_driver import Client
import psycopg2
import datetime

# --- CONFIG ---
POSTGRES_CONFIG = {
    'host': 'db',  # Docker Compose service name for Postgres
    'port': 5432,
    'dbname': 'erp_db',
    'user': 'erp_user',
    'password': 'erp_pass',
}
CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',
    'port': 9000,
    'user': 'default',
    'password': '',
    'database': 'default',
}

# --- ETL FUNCTIONS ---
def fetch_postgres(query):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def insert_clickhouse(table, columns, rows):
    client = Client(**CLICKHOUSE_CONFIG)
    client.execute(f"INSERT INTO {table} ({', '.join(columns)}) VALUES", rows)

# --- DIMENSION LOADERS ---
def load_dim_account():
    rows = fetch_postgres("SELECT account_code, account_name, account_type FROM erp.coa")
    # Generate account_id as row number, cast all except id to str
    data = [(i+1, str(row[0]), str(row[1]), str(row[2])) for i, row in enumerate(rows)]
    insert_clickhouse('dim_account', ['account_id', 'account_code', 'account_name', 'type'], data)

def load_dim_bank():
    rows = fetch_postgres("SELECT DISTINCT bank FROM erp.sales UNION SELECT DISTINCT bank FROM erp.purchases")
    data = [(i+1, row[0]) for i, row in enumerate(rows)]
    insert_clickhouse('dim_bank', ['bank_id', 'bank_name'], data)

def load_dim_customer():
    rows = fetch_postgres("SELECT DISTINCT customer_id, customer_name, region FROM erp.customer")
    data = [(row[0], row[1], row[2]) for row in rows]
    insert_clickhouse('dim_customer', ['customer_id', 'customer_name', 'region'], data)

def load_dim_supplier():
    rows = fetch_postgres("SELECT DISTINCT supplier_id, supplier_name, region FROM erp.suppliers")
    data = [(row[0], row[1], row[2]) for row in rows]
    insert_clickhouse('dim_supplier', ['supplier_id', 'supplier_name', 'region'], data)

def load_dim_truck():
    rows = fetch_postgres("SELECT DISTINCT truck FROM erp.sales")
    data = [(i+1, str(row[0])) for i, row in enumerate(rows)]
    insert_clickhouse('dim_truck', ['truck_id', 'truck_number'], data)

def load_dim_product():
    rows = fetch_postgres("SELECT DISTINCT product_id, product_name, unit_price FROM erp.products")
    data = [(row[0], row[1], row[2]) for row in rows]
    insert_clickhouse('dim_product', ['product_id', 'product_name', 'unit_price'], data)

def load_dim_depot():
    rows = fetch_postgres("SELECT DISTINCT depot FROM erp.sales")
    data = [(i+1, str(row[0])) for i, row in enumerate(rows)]
    insert_clickhouse('dim_depot', ['depot_id', 'depot_name'], data)

def load_dim_date():
    rows = fetch_postgres("SELECT DISTINCT order_date FROM erp.sales UNION SELECT DISTINCT order_date FROM erp.purchases")
    data = []
    for i, row in enumerate(rows):
        date = row[0]
        if isinstance(date, str):
            date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        data.append((i+1, date, date.month, (date.month-1)//3+1, date.year))
    insert_clickhouse('dim_date', ['date_key', 'date', 'month', 'quarter', 'year'], data)

# --- FACT LOADERS ---
def load_fact_sales():
    rows = fetch_postgres("""
        SELECT sale_id, order_date, truck, pms_qty, ago_qty, total, depot, transport_cost, amount_received, bank, balance
        FROM erp.sales
    """)
    # Fetch dimension tables for surrogate key mapping
    client = Client(**CLICKHOUSE_CONFIG)
    truck_map = {row[1]: row[0] for row in client.execute('SELECT truck_id, truck_number FROM dim_truck')}
    depot_map = {row[1]: row[0] for row in client.execute('SELECT depot_id, depot_name FROM dim_depot')}
    bank_map = {row[1]: row[0] for row in client.execute('SELECT bank_id, bank_name FROM dim_bank')}
    date_map = {str(row[1]): row[0] for row in client.execute('SELECT date_key, date FROM dim_date')}
    # For demo, customer_id and product_id are set to 0 (missing tables)
    data = []
    for row in rows:
        sale_id = row[0]
        order_date = str(row[1])
        truck = row[2]
        pms_qty = int(row[3]) if row[3] is not None else 0
        ago_qty = int(row[4]) if row[4] is not None else 0
        total_amount = float(row[5]) if row[5] is not None else 0.0
        depot = row[6]
        transport_cost = float(row[7]) if row[7] is not None else 0.0
        amount_received = float(row[8]) if row[8] is not None else 0.0
        bank = row[9]
        balance = float(row[10]) if row[10] is not None else 0.0
        # Surrogate key lookups
        order_date_key = date_map.get(order_date, 0)
        truck_id = truck_map.get(truck, 0)
        depot_id = depot_map.get(depot, 0)
        bank_id = bank_map.get(bank, 0)
        # Insert row (customer_id, product_id set to 0)
        data.append((sale_id, order_date_key, 0, truck_id, 0, depot_id, bank_id, pms_qty, ago_qty, total_amount, transport_cost, amount_received, balance))
    insert_clickhouse('fact_sales', [
        'order_id', 'order_date_key', 'customer_id', 'truck_id', 'product_id', 'depot_id', 'bank_id',
        'quantity_pms', 'quantity_ago', 'total_amount', 'transport_cost', 'amount_received', 'balance'
    ], data)

def load_fact_purchases():
    rows = fetch_postgres("""
        SELECT purchase_id, order_date, truck, pms_qty, ago_qty, total, amount_paid, bank, balance
        FROM erp.purchases
    """)
    client = Client(**CLICKHOUSE_CONFIG)
    truck_map = {row[1]: row[0] for row in client.execute('SELECT truck_id, truck_number FROM dim_truck')}
    bank_map = {row[1]: row[0] for row in client.execute('SELECT bank_id, bank_name FROM dim_bank')}
    date_map = {str(row[1]): row[0] for row in client.execute('SELECT date_key, date FROM dim_date')}
    # For demo, supplier_id and product_id are set to 0 (missing tables)
    data = []
    for row in rows:
        purchase_id = row[0]
        order_date = str(row[1])
        truck = row[2]
        pms_qty = int(row[3]) if row[3] is not None else 0
        ago_qty = int(row[4]) if row[4] is not None else 0
        total_amount = float(row[5]) if row[5] is not None else 0.0
        amount_paid = float(row[6]) if row[6] is not None else 0.0
        bank = row[7]
        balance = float(row[8]) if row[8] is not None else 0.0
        order_date_key = date_map.get(order_date, 0)
        truck_id = truck_map.get(truck, 0)
        bank_id = bank_map.get(bank, 0)
        # Insert row (supplier_id, product_id set to 0)
        data.append((purchase_id, order_date_key, 0, truck_id, 0, bank_id, pms_qty, ago_qty, total_amount, amount_paid, balance))
    insert_clickhouse('fact_purchases', [
        'purchase_id', 'order_date_key', 'supplier_id', 'truck_id', 'product_id', 'bank_id',
        'quantity_pms', 'quantity_ago', 'total_amount', 'amount_paid', 'balance'
    ], data)

def load_fact_financials():
    rows = fetch_postgres("""
        SELECT txn_id, txn_date, account_name, debit, credit, memo, txn_type
        FROM erp.transactions
    """)
    client = Client(**CLICKHOUSE_CONFIG)
    account_map = {row[2]: row[0] for row in client.execute('SELECT account_id, account_code, account_name FROM dim_account')}
    date_map = {str(row[1]): row[0] for row in client.execute('SELECT date_key, date FROM dim_date')}
    data = []
    for row in rows:
        txn_id = row[0]
        txn_date = str(row[1])
        account_name = row[2]
        debit = float(row[3]) if row[3] is not None else 0.0
        credit = float(row[4]) if row[4] is not None else 0.0
        memo = row[5]
        txn_type = row[6]
        date_key = date_map.get(txn_date, 0)
        account_id = account_map.get(account_name, 0)
        data.append((txn_id, date_key, account_id, debit, credit, memo, txn_type))
    insert_clickhouse('fact_financials', [
        'txn_id', 'date_key', 'account_id', 'debit', 'credit', 'memo', 'txn_type'
    ], data)

# --- MAIN ---
def main():
    load_dim_account()
    load_dim_bank()
    # load_dim_customer()  # Skipped: customer table does not exist
    # load_dim_supplier()  # Skipped: suppliers table does not exist
    load_dim_truck()
    # load_dim_product()  # Skipped: products table does not exist
    load_dim_depot()
    load_dim_date()
    load_fact_sales()
    load_fact_purchases()
    load_fact_financials()
    print("ETL complete: ERP data loaded into ClickHouse star schema.")

if __name__ == "__main__":
    main()
