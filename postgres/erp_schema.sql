-- ERP Raw Layer Schema (Postgres)
-- Source tables reflecting operational Excel structure.
-- Includes basic CDC columns (created_at, updated_at) and surrogate keys where needed.

CREATE SCHEMA IF NOT EXISTS erp;

-- Chart of Accounts
CREATE TABLE IF NOT EXISTS erp.coa (
    id BIGSERIAL PRIMARY KEY,
    account_code INT UNIQUE NOT NULL,
    account_name VARCHAR(120) NOT NULL,
    account_type VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SCD Type 2 History for Chart of Accounts
CREATE TABLE IF NOT EXISTS erp.coa_history (
    hist_id BIGSERIAL PRIMARY KEY,
    account_code INT NOT NULL,
    account_name VARCHAR(120) NOT NULL,
    account_type VARCHAR(50) NOT NULL,
    description TEXT,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Trigger function for COA SCD history
CREATE OR REPLACE FUNCTION erp.fn_coa_history() RETURNS TRIGGER AS $$
BEGIN
    -- Close existing current row
    UPDATE erp.coa_history
      SET valid_to = CURRENT_TIMESTAMP,
          is_current = FALSE
    WHERE account_code = OLD.account_code AND is_current = TRUE;
    -- Insert new version
    INSERT INTO erp.coa_history(account_code, account_name, account_type, description, valid_from)
    VALUES (NEW.account_code, NEW.account_name, NEW.account_type, NEW.description, CURRENT_TIMESTAMP);
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Trigger function to refresh updated_at
CREATE OR REPLACE FUNCTION erp.fn_touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Attach triggers
DROP TRIGGER IF EXISTS trg_coa_history ON erp.coa;
CREATE TRIGGER trg_coa_history
AFTER UPDATE ON erp.coa
FOR EACH ROW EXECUTE FUNCTION erp.fn_coa_history();

DROP TRIGGER IF EXISTS trg_touch_coa ON erp.coa;
CREATE TRIGGER trg_touch_coa
BEFORE UPDATE ON erp.coa
FOR EACH ROW EXECUTE FUNCTION erp.fn_touch_updated_at();

-- Opening Balances
CREATE TABLE IF NOT EXISTS erp.opening_balances (
    id BIGSERIAL PRIMARY KEY,
    account_code INT REFERENCES erp.coa(account_code),
    account_name VARCHAR(120),
    opening_balance NUMERIC(16,2) NOT NULL,
    fiscal_year INT DEFAULT 2025,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- General Ledger Transactions
CREATE TABLE IF NOT EXISTS erp.transactions (
    txn_id BIGSERIAL PRIMARY KEY,
    txn_date DATE NOT NULL,
    txn_type VARCHAR(50),
    reference_num VARCHAR(50),
    memo TEXT,
    account_name VARCHAR(120),
    split_account VARCHAR(120),
    debit NUMERIC(16,2) DEFAULT 0,
    credit NUMERIC(16,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_date ON erp.transactions(txn_date);

-- Supplier Purchases
CREATE TABLE IF NOT EXISTS erp.purchases (
    purchase_id BIGSERIAL PRIMARY KEY,
    order_date DATE NOT NULL,
    truck VARCHAR(30),
    pms_qty NUMERIC(14,2) DEFAULT 0,
    pms_price NUMERIC(14,4),
    ago_qty NUMERIC(14,2) DEFAULT 0,
    ago_price NUMERIC(14,4),
    total NUMERIC(16,2),
    pay_date DATE,
    amount_paid NUMERIC(16,2) DEFAULT 0,
    bank VARCHAR(60),
    balance NUMERIC(16,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_purchases_order_date ON erp.purchases(order_date);

-- Customer Sales
CREATE TABLE IF NOT EXISTS erp.sales (
    sale_id BIGSERIAL PRIMARY KEY,
    order_date DATE NOT NULL,
    truck VARCHAR(30),
    pms_qty NUMERIC(14,2) DEFAULT 0,
    pms_price NUMERIC(14,4),
    ago_qty NUMERIC(14,2) DEFAULT 0,
    ago_price NUMERIC(14,4),
    total NUMERIC(16,2),
    depot VARCHAR(80),
    transport_cost NUMERIC(16,2) DEFAULT 0,
    pay_date DATE,
    amount_received NUMERIC(16,2) DEFAULT 0,
    bank VARCHAR(60),
    balance NUMERIC(16,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sales_order_date ON erp.sales(order_date);

-- Outbox table for domain events (Debezium monitored)
CREATE TABLE IF NOT EXISTS erp.outbox (
    id BIGSERIAL PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO erp.coa(account_code, account_name, account_type, description) VALUES
 (1000,'Cash','Asset','Cash on hand'),
 (1100,'Accounts Receivable','Asset','Customer balances'),
 (1200,'Inventory','Asset','Fuel stock'),
 (2000,'Accounts Payable','Liability','Supplier balances'),
 (2100,'Loans Payable','Liability','Bank loans'),
(3000,'Owner''s Equity','Equity','Capital account'),
 (3100,'Retained Earnings','Equity','Accumulated profits'),
 (4000,'Sales Revenue','Income','Fuel sales'),
 (4100,'Other Income','Income','Miscellaneous income'),
 (5000,'Fuel Purchase','Expense','Cost of fuel'),
 (5100,'Transport Expense','Expense','Delivery costs'),
 (5200,'Salaries','Expense','Employee wages'),
 (5300,'Miscellaneous Expense','Expense','Other expenses'),
 (5400,'Bank KCB','Asset','KCB bank account'),
 (5500,'Bank ABSA','Asset','ABSA bank account'),
 (5600,'Bank COOP','Asset','COOP bank account')
ON CONFLICT (account_code) DO NOTHING;

INSERT INTO erp.opening_balances(account_code, account_name, opening_balance) VALUES
 (1000,'Cash',14585.44),(1100,'Accounts Receivable',25393.35),(1200,'Inventory',25730.77),(2000,'Accounts Payable',20127.05),(2100,'Loans Payable',9157.16),
(3000,'Owner''s Equity',31424.13),(3100,'Retained Earnings',22544.71),(4000,'Sales Revenue',36989.12),(4100,'Other Income',34429.31),(5000,'Fuel Purchase',23488.11),
 (5100,'Transport Expense',21379.65),(5200,'Salaries',8079.34),(5300,'Miscellaneous Expense',2378.58),(5400,'Bank KCB',4035.2),(5500,'Bank ABSA',34945.1),(5600,'Bank COOP',20674.52)
ON CONFLICT DO NOTHING;