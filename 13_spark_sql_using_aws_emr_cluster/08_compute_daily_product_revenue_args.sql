CREATE DATABASE IF NOT EXISTS retail_ods;
USE retail_ods;

CREATE TEMPORARY VIEW orders
USING JSON
OPTIONS (path='s3://${s3.bucket}/retail_db_json/orders');

CREATE TEMPORARY VIEW order_items
USING JSON
OPTIONS (path='s3://${s3.bucket}/retail_db_json/order_items');

INSERT OVERWRITE DIRECTORY 's3://${s3.bucket}/retail_db_json/daily_product_revenue'
USING JSON
SELECT o.order_date,
    o.order_status,
    oi.order_item_product_id,
    round(sum(oi.order_item_subtotal), 2) AS revenue
FROM orders AS o JOIN order_items AS oi
  ON o.order_id = oi.order_item_order_id
GROUP BY o.order_date,
    o.order_status,
    oi.order_item_product_id
ORDER BY o.order_date,
    revenue DESC;