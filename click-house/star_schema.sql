
-- Dimension Tables
CREATE TABLE dim_date (
    date_key UInt32,
    date Date,
    month UInt8,
    quarter UInt8,
    year UInt16
) ENGINE = MergeTree() ORDER BY date_key;

CREATE TABLE dim_customer (
    customer_id UInt32,
    customer_name String,
    region String
) ENGINE = MergeTree() ORDER BY customer_id;

CREATE TABLE dim_supplier (
    supplier_id UInt32,
    supplier_name String,
    region String
) ENGINE = MergeTree() ORDER BY supplier_id;

CREATE TABLE dim_truck (
    truck_id UInt32,
    truck_number String
) ENGINE = MergeTree() ORDER BY truck_id;

CREATE TABLE dim_product (
    product_id UInt32,
    product_name String,
    unit_price Float64
) ENGINE = MergeTree() ORDER BY product_id;

CREATE TABLE dim_depot (
    depot_id UInt32,
    depot_name String
) ENGINE = MergeTree() ORDER BY depot_id;

CREATE TABLE dim_bank (
    bank_id UInt32,
    bank_name String
) ENGINE = MergeTree() ORDER BY bank_id;

CREATE TABLE dim_account (
    account_id UInt32,
    account_code String,
    account_name String,
    type String
) ENGINE = MergeTree() ORDER BY account_id;

-- Fact Tables
CREATE TABLE fact_sales (
    order_id UInt32,
    order_date_key UInt32,
    customer_id UInt32,
    truck_id UInt32,
    product_id UInt32,
    depot_id UInt32,
    bank_id UInt32,
    quantity_pms UInt32,
    quantity_ago UInt32,
    total_amount Float64,
    transport_cost Float64,
    amount_received Float64,
    balance Float64
) ENGINE = MergeTree() ORDER BY order_id;

CREATE TABLE fact_purchases (
    purchase_id UInt32,
    order_date_key UInt32,
    supplier_id UInt32,
    truck_id UInt32,
    product_id UInt32,
    bank_id UInt32,
    quantity_pms UInt32,
    quantity_ago UInt32,
    total_amount Float64,
    amount_paid Float64,
    balance Float64
) ENGINE = MergeTree() ORDER BY purchase_id;

CREATE TABLE fact_financials (
    txn_id UInt32,
    date_key UInt32,
    account_id UInt32,
    debit Float64,
    credit Float64,
    memo String,
    txn_type String
) ENGINE = MergeTree() ORDER BY txn_id;
