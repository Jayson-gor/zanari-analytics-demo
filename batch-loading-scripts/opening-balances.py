import psycopg2

DB_CONFIG = {
    'host': 'db',
    'port': 5432,
    'dbname': 'erp_db',
    'user': 'erp_user',
    'password': 'erp_pass',
}

rows = [
    (1000, 'Cash', 14585.44),
    (1100, 'Accounts Receivable', 25393.35),
    (1200, 'Inventory', 25730.77),
    (2000, 'Accounts Payable', 20127.05),
    (2100, 'Loans Payable', 9157.16),
    (3000, "Owner's Equity", 31424.13),
    (3100, 'Retained Earnings', 22544.71),
    (4000, 'Sales Revenue', 36989.12),
    (4100, 'Other Income', 34429.31),
    (5000, 'Fuel Purchase', 23488.11),
    (5100, 'Transport Expense', 21379.65),
    (5200, 'Salaries', 8079.34),
    (5300, 'Miscellaneous Expense', 2378.58),
    (5400, 'Bank KCB', 4035.2),
    (5500, 'Bank ABSA', 34945.1),
    (5600, 'Bank COOP', 20674.52)
]

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
for row in rows:
    cur.execute("""
        INSERT INTO erp.opening_balances(account_code, account_name, opening_balance)
        VALUES (%s, %s, %s);
    """, row)
conn.commit()
cur.close()
conn.close()
print("Opening balances inserted successfully.")