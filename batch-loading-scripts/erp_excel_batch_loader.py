import pandas as pd
import psycopg2
import openpyxl
from psycopg2.extras import execute_batch

import time
from concurrent.futures import ThreadPoolExecutor

# --- CONFIG ---

EXCEL_FOLDER = '/home/jovyan/demo_excels/'
EXCEL_FILES = {
    'main': EXCEL_FOLDER + 'Chart_of_Accounts_Demo_5000.xlsx',
    'suppliers': EXCEL_FOLDER + 'Suppliers_5000_Rows.xlsx',
    'customers': EXCEL_FOLDER + 'Customers_5000_Rows.xlsx',
}

DB_CONFIG = {
    'host': 'db',  # Use Docker Compose service name
    'port': 5432,
    'dbname': 'erp_db',
    'user': 'erp_user',
    'password': 'erp_pass',
}

# --- LOADERS ---

def load_charts_of_accounts(conn, path):
    df = pd.read_excel(path, sheet_name='Chart_of_Accounts')
    rows = [
        (int(r['Account_Code']), r['Account_Name'], r['Type'], r['Description'])
        for _, r in df.iterrows()
    ]
    try:
        pass
    except Exception as e:
        print("Error in load_charts_of_accounts:", e)
        conn.rollback()


def load_opening_balances(conn, path):
    df = pd.read_excel(path, sheet_name='Opening_Balances')
    rows = [
        (int(r['Account_Code']), float(r['Opening_Balance']))
        for _, r in df.iterrows()
    ]
    try:
        with conn.cursor() as cur:
            execute_batch(cur, '''
                INSERT INTO erp.opening_balances(account_code, opening_balance)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            ''', rows)
        conn.commit()
    except Exception as e:
        print("Error in load_opening_balances:", e)
        conn.rollback()


def load_transactions(conn, path):
    df = pd.read_excel(path, sheet_name='Transactions')
    rows = [
        (
            pd.to_datetime(r['Date']), r['Type'], r['Num'], r['Memo'],
            r['Account'], r['Split'], float(r['Debit']), float(r['Credit'])
        )
        for _, r in df.iterrows()
    ]
    try:
        with conn.cursor() as cur:
            execute_batch(cur, '''
                INSERT INTO erp.transactions(
                    txn_date, txn_type, reference_num, memo, account_name, split_account, debit, credit
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            ''', rows)
        conn.commit()
    except Exception as e:
        print("Error in load_transactions:", e)
        conn.rollback()

def load_suppliers(conn, path):
    df = pd.read_excel(path)
    rows = [
        (
            pd.to_datetime(r['Order_Date']), r['Truck'], float(r['PMS_Qty']), float(r['PMS_Price']),
            float(r['AGO_Qty']), float(r['AGO_Price']), float(r['Total']),
            pd.to_datetime(r['Pay_Date']), float(r['Amount_Paid']), r['Bank'], float(r['Balance'])
        )
        for _, r in df.iterrows()
    ]
    try:
        with conn.cursor() as cur:
            execute_batch(cur, '''
                INSERT INTO erp.purchases(
                    order_date, truck, pms_qty, pms_price, ago_qty, ago_price, total,
                    pay_date, amount_paid, bank, balance
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            ''', rows)
        conn.commit()
    except Exception as e:
        print("Error in load_suppliers:", e)
        conn.rollback()

def load_customers(conn, path):
    df = pd.read_excel(path)
    rows = [
        (
            pd.to_datetime(r['Order_Date']), r['Truck'], float(r['PMS_Qty']), float(r['PMS_Price']),
            float(r['AGO_Qty']), float(r['AGO_Price']), float(r['Total']), r['Depot'],
            float(r['Transport_Cost']), pd.to_datetime(r['Pay_Date']), float(r['Amount_Received']),
            r['Bank'], float(r['Balance'])
        )
        for _, r in df.iterrows()
    ]
    try:
        with conn.cursor() as cur:
            execute_batch(cur, '''
                INSERT INTO erp.sales(
                    order_date, truck, pms_qty, pms_price, ago_qty, ago_price, total,
                    depot, transport_cost, pay_date, amount_received, bank, balance
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            ''', rows)
        conn.commit()
    except Exception as e:
        print("Error in load_customers:", e)
        conn.rollback()

# --- MAIN ---


LOADERS = [
    ('charts_of_accounts', load_charts_of_accounts, EXCEL_FILES['main']),
    ('opening_balances', load_opening_balances, EXCEL_FILES['main']),
    ('transactions', load_transactions, EXCEL_FILES['main']),
    ('suppliers', load_suppliers, EXCEL_FILES['suppliers']),
    ('customers', load_customers, EXCEL_FILES['customers']),
]


def run_loader(name, loader, conn, path):
    print(f"Loading {name} from {path} ...")
    start = time.time()
    loader(conn, path)
    elapsed = time.time() - start
    print(f"Loaded {name} in {elapsed:.2f} seconds.")
    return name, elapsed


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(run_loader, name, loader, conn, path)
            for name, loader, path in LOADERS
        ]
        for f in futures:
            name, elapsed = f.result()
            print(f"{name} loaded in {elapsed:.2f} seconds.")
    conn.close()

if __name__ == "__main__":
    main()
