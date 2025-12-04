-- Create a view for the comprehensive sales dataset
-- This avoids repeating the complex JOIN query in Metabase

CREATE OR REPLACE VIEW vw_sales_comprehensive AS
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
    (fs.quantity_pms + fs.quantity_ago) AS total_quantity,
    (fs.total_amount - fs.transport_cost) AS net_revenue,
    (fs.amount_received - fs.total_amount) AS cash_variance
  FROM fact_sales AS fs
)
SELECT
  dd.date_key,
  dd.date AS order_date,
  dd.month,
  dd.quarter,
  dd.year,
  b.order_id,
  b.customer_id,
  b.truck_id,
  b.product_id,
  b.depot_id,
  b.bank_id,
  dc.customer_name,
  dc.region AS customer_region,
  dt.truck_number,
  dp.product_name,
  dp.unit_price,
  ddp.depot_name,
  db.bank_name,
  b.quantity_pms,
  b.quantity_ago,
  b.total_quantity,
  b.total_amount,
  b.transport_cost,
  b.net_revenue,
  b.amount_received,
  b.balance,
  b.cash_variance,
  1 AS order_row
FROM base AS b
LEFT JOIN dim_date AS dd ON dd.date_key = b.order_date_key
LEFT JOIN dim_customer AS dc ON dc.customer_id = b.customer_id
LEFT JOIN dim_truck AS dt ON dt.truck_id = b.truck_id
LEFT JOIN dim_product AS dp ON dp.product_id = b.product_id
LEFT JOIN dim_depot AS ddp ON ddp.depot_id = b.depot_id
LEFT JOIN dim_bank AS db ON db.bank_id = b.bank_id;
