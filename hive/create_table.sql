CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE ecommerce_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS streaming_orders (
order_id INT,
customer_id INT,
product_id INT,
quantity INT,
price DOUBLE,
order_timestamp TIMESTAMP,
total_amount DOUBLE
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ecommerce_dw.db/streaming_orders';

SELECT COUNT(*) AS total_orders
FROM streaming_orders;

SELECT ROUND(SUM(total_amount), 2) AS gross_revenue
FROM streaming_orders;

SELECT ROUND(AVG(total_amount), 2) AS average_order_value
FROM streaming_orders;

SELECT product_id, ROUND(SUM(total_amount), 2) AS total_sales
FROM streaming_orders
GROUP BY product_id
ORDER BY total_sales DESC;

SELECT customer_id, ROUND(SUM(total_amount), 2) AS total_spent
FROM streaming_orders
GROUP BY customer_id
ORDER BY total_spent DESC;

SELECT product_id, ROUND(SUM(total_amount), 2) AS gross_revenue
FROM streaming_orders
GROUP BY product_id
ORDER BY gross_revenue DESC
LIMIT 5;

