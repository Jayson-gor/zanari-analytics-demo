-- Comprehensive dataset for Metabase demo dashboard
-- Source: fact_sales joined with core dimensions
-- Adjust table and column names if they differ in your schema

WITH base AS (
  SELECT
    fs.order_id,
    fs.order_date_key,
    fs.customer_id,
    fs.truck_id,
    fs.product_id,
    fs.depot_id,
    fs.bank_id,
    fs.quantity_pms,
    fs.quantity_ago,
    fs.total_amount,
    fs.transport_cost,
    fs.amount_received,
    fs.balance,
    -- Derived measures
    (fs.quantity_pms + fs.quantity_ago) AS total_quantity,
    (fs.total_amount - fs.transport_cost) AS net_revenue,
    (fs.amount_received - fs.total_amount) AS cash_variance
  FROM fact_sales AS fs
)
SELECT
  -- Date
  dd.date_key,
  dd.date AS order_date,
  dd.month,
  dd.quarter,
  dd.year,

  -- Keys
  b.order_id,
  b.customer_id,
  b.truck_id,
  b.product_id,
  b.depot_id,
  b.bank_id,

  -- Customer
  dc.customer_name,
  dc.region AS customer_region,

  -- Supplier (optional for purchases, retained for symmetry)
  -- ds.supplier_name,
  -- ds.region AS supplier_region,

  -- Truck
  dt.truck_number,

  -- Product
  dp.product_name,
  dp.unit_price,

  -- Depot
  ddp.depot_name,

  -- Bank
  db.bank_name,

  -- Measures
  b.quantity_pms,
  b.quantity_ago,
  b.total_quantity,
  b.total_amount,
  b.transport_cost,
  b.net_revenue,
  b.amount_received,
  b.balance,
  b.cash_variance,

  -- Helpers
  1 AS order_row
FROM base AS b
LEFT JOIN dim_date AS dd
  ON dd.date_key = b.order_date_key
LEFT JOIN dim_customer AS dc
  ON dc.customer_id = b.customer_id
LEFT JOIN dim_truck AS dt
  ON dt.truck_id = b.truck_id
LEFT JOIN dim_product AS dp
  ON dp.product_id = b.product_id
LEFT JOIN dim_depot AS ddp
  ON ddp.depot_id = b.depot_id
LEFT JOIN dim_bank AS db
  ON db.bank_id = b.bank_id;

-- Suggested aggregations for Metabase (define in GUI or create views):
-- KPIs (for demo)
-- 1) Revenue: SUM(net_revenue)
-- 2) Orders: COUNT(DISTINCT order_id)
-- 3) AOV: SUM(net_revenue) / COUNT(DISTINCT order_id)
-- 4) Quantity Mix: SUM(quantity_pms) vs SUM(quantity_ago)
-- 5) Cash Variance: SUM(cash_variance)

-- Charts
-- - Revenue Trend: GROUP BY order_date, SUM(net_revenue)
-- - Orders Trend: GROUP BY order_date, COUNT(DISTINCT order_id) or SUM(order_row)
-- - AOV Trend: GROUP BY order_date, SUM(net_revenue)/COUNT(DISTINCT order_id)
-- - Quantity Mix by Depot: GROUP BY depot_name, SUM(quantity_pms), SUM(quantity_ago)
-- - Top 10 Products: GROUP BY product_name, SUM(net_revenue) ORDER BY DESC LIMIT 10
-- - Revenue by Customer Region: GROUP BY customer_region, SUM(net_revenue)
-- - Bank Collections: GROUP BY bank_name, SUM(amount_received)
-- - Transport Cost by Truck: GROUP BY truck_number, SUM(transport_cost)
-- - Balance by Customer: GROUP BY customer_name, SUM(balance)
-- - Unit Price vs Revenue: GROUP BY product_name, AVG(unit_price) AS avg_price, SUM(net_revenue) AS revenue
