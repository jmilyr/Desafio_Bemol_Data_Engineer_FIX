
CREATE DATABASE IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.mart_customers_by_state AS
SELECT
  customer_state AS state,
  COUNT(*) AS total_customers
FROM gold.dim_customer
GROUP BY customer_state;

CREATE OR REPLACE TABLE gold.mart_customers_by_city AS
SELECT
  customer_state AS state,
  customer_city  AS city,
  COUNT(*) AS total_customers
FROM gold.dim_customer
GROUP BY customer_state, customer_city;
