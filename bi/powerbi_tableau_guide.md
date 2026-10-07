# Power BI & Tableau Connection Guide

This guide walks you through connecting **Power BI Desktop** and **Tableau** to the Lakehouse Gold Layer (either directly via the exported Parquet files or via AWS Athena / Google BigQuery).

---

## Option 1: Connecting Power BI Directly to Gold Parquet Files (Local / Direct)

1. Open **Power BI Desktop**.
2. Click **Get Data** > **More...** > search for **Parquet** (or **Folder**).
3. Select the file:
   * `data/lakehouse/gold/fact_orders/fact_orders.parquet`
   * `data/lakehouse/gold/dim_customers/dim_customers.parquet`
   * `data/lakehouse/gold/dim_products/dim_products.parquet`
   * `data/lakehouse/gold/features_customer_rfm/features_customer_rfm.parquet`
4. Click **Load**.

### Power BI Data Model (Star Schema) Relationships:
In the **Model View**, establish the following 1-to-Many relationships:
* `dim_customers[customer_key]` $\to$ `fact_orders[customer_key]` (1 to *)
* `dim_products[product_key]` $\to$ `fact_orders[product_key]` (1 to *)
* `dim_date[date_key]` $\to$ `fact_orders[date_key]` (1 to *)

### Key DAX Measures to Create:
```dax
Total Revenue = SUM(fact_orders[payment_value])

Total Orders = DISTINCTCOUNT(fact_orders[order_id])

Average Order Value (AOV) = DIVIDE([Total Revenue], [Total Orders], 0)

Delayed Orders Count = CALCULATE(COUNT(fact_orders[order_id]), fact_orders[is_delayed] = 1)

On-Time Delivery Rate = 1 - DIVIDE([Delayed Orders Count], [Total Orders], 0)
```

---

## Option 2: Connecting to AWS Athena (Cloud Mode)

1. In Power BI / Tableau, select **Amazon Athena** connector.
2. Enter the ODBC/JDBC Server and Port.
3. Catalog: `AwsDataCatalog`, Database: `ecommerce_lakehouse`.
4. Authentication: Provide your AWS Access Key & Secret Key, and specify your S3 Query Output location.
5. Select tables: `fact_orders`, `dim_customers`, `dim_products`, `features_customer_rfm`.

---

## Option 3: Connecting Tableau to BigQuery (GCP Mode)

1. In Tableau, under **Connect to a Server**, select **Google BigQuery**.
2. Sign in with your Google account.
3. Select your Project ID, Dataset (`ecommerce_lakehouse`), and drag the tables onto the canvas.
4. Establish joins or relationships on `customer_key` and `product_key`.

---

## Dashboard Pages Recommended for Interviews

### Page 1: Executive KPI Summary
* **Cards:** Total Revenue, Total Orders, Average Order Value (AOV), On-Time Delivery Rate (%).
* **Line Chart:** Monthly GMV Trend (X: `order_year-order_month`, Y: `Total Revenue`).
* **Bar Chart:** Top 10 Product Categories by Revenue.
* **Donut Chart:** Order Status Breakdown (`delivered`, `shipped`, `canceled`).
* **Slicers:** Customer State, Year/Month, Product Category.

### Page 2: Customer RFM Behavioral Segmentation & Churn
* **Tree Map / Bar Chart:** Customer counts by `customer_segment` (*Champions*, *Loyal Customers*, *At Risk*, *Hibernating*).
* **Scatter Plot:** Recency Days (X-axis) vs. Total Spend (Y-axis) colored by Segment.
* **Table:** Top Customers at Risk with high historical spend (candidates for re-engagement marketing).
