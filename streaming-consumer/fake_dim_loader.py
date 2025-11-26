from clickhouse_driver import Client
import random

client = Client(host='clickhouse', port=9000, user='default', password='', database='default')

def load_dim_customer():
    data = [(i+1, f'Customer_{i+1}', f'Region_{random.choice(["A","B","C"])})') for i in range(5)]
    client.execute('INSERT INTO dim_customer (customer_id, customer_name, region) VALUES', data)
    print(f"Loaded {len(data)} rows into dim_customer")

def load_dim_supplier():
    data = [(i+1, f'Supplier_{i+1}', f'Region_{random.choice(["A","B","C"])})') for i in range(5)]
    client.execute('INSERT INTO dim_supplier (supplier_id, supplier_name, region) VALUES', data)
    print(f"Loaded {len(data)} rows into dim_supplier")

def load_dim_truck():
    data = [(i+1, f'TRUCK_{100+i}') for i in range(5)]
    client.execute('INSERT INTO dim_truck (truck_id, truck_number) VALUES', data)
    print(f"Loaded {len(data)} rows into dim_truck")

def load_dim_product():
    data = [(i+1, f'Product_{i+1}', round(random.uniform(10,100),2)) for i in range(5)]
    client.execute('INSERT INTO dim_product (product_id, product_name, unit_price) VALUES', data)
    print(f"Loaded {len(data)} rows into dim_product")

def load_dim_depot():
    data = [(i+1, f'Depot_{i+1}') for i in range(5)]
    client.execute('INSERT INTO dim_depot (depot_id, depot_name) VALUES', data)
    print(f"Loaded {len(data)} rows into dim_depot")

def load_dim_bank():
    data = [(i+1, f'Bank_{i+1}') for i in range(5)]
    client.execute('INSERT INTO dim_bank (bank_id, bank_name) VALUES', data)
    print(f"Loaded {len(data)} rows into dim_bank")

def load_dim_date():
    from datetime import date, timedelta
    today = date.today()
    data = []
    for i in range(5):
        d = today - timedelta(days=i)
        data.append((i+1, d, d.month, (d.month-1)//3+1, d.year))
    client.execute('INSERT INTO dim_date (date_key, date, month, quarter, year) VALUES', data)
    print(f"Loaded {len(data)} rows into dim_date")

if __name__ == "__main__":
    load_dim_customer()
    load_dim_supplier()
    load_dim_truck()
    load_dim_product()
    load_dim_depot()
    load_dim_bank()
    load_dim_date()
    print("All fake dimension loads complete.")
