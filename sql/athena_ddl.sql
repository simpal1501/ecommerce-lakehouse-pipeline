-- ============================================================================
-- AWS ATHENA / GCP BIGQUERY EXTERNAL DDL SCHEMAS
-- Points external table pointers to Snappy-compressed Parquet files in Cloud Storage
-- ============================================================================

-- 1. Fact Orders Table (Partitioned by Year and Month for Cost Reduction)
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse.fact_orders (
    order_id STRING,
    customer_key INT,
    product_key INT,
    date_key INT,
    order_status STRING,
    price DOUBLE,
    freight_value DOUBLE,
    payment_value DOUBLE,
    actual_delivery_days DOUBLE,
    estimated_delivery_days DOUBLE,
    is_delayed INT
)
PARTITIONED BY (order_year INT, order_month INT)
STORED AS PARQUET
LOCATION 's3://your-company-lakehouse-bucket/gold/fact_orders/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Load new partitions automatically in Athena
MSCK REPAIR TABLE ecommerce_lakehouse.fact_orders;

-- 2. Customer Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse.dim_customers (
    customer_id STRING,
    customer_unique_id STRING,
    customer_city STRING,
    customer_state STRING,
    customer_zip_code_prefix INT,
    customer_key INT
)
STORED AS PARQUET
LOCATION 's3://your-company-lakehouse-bucket/gold/dim_customers/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 3. Product Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse.dim_products (
    product_id STRING,
    product_category_name STRING,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    product_volume_cm3 INT,
    product_key INT
)
STORED AS PARQUET
LOCATION 's3://your-company-lakehouse-bucket/gold/dim_products/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 4. Customer RFM Feature Store Table
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse.features_customer_rfm (
    customer_unique_id STRING,
    recency_days INT,
    frequency_orders INT,
    monetary_spend DOUBLE,
    r_score INT,
    f_score INT,
    m_score INT,
    rfm_combined_score STRING,
    customer_segment STRING
)
STORED AS PARQUET
LOCATION 's3://your-company-lakehouse-bucket/gold/features_customer_rfm/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
