
CREATE DATABASE IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.dim_customer AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  ingestion_ts
FROM silver.dim_customer;
