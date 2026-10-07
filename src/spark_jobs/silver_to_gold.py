"""
Silver to Gold Processing Engine
Implements Dimensional Data Modeling (Star Schema) and ML Feature Engineering.
Produces:
1. Dim_Customers: Customer dimension with geographical hierarchies
2. Dim_Products: Product dimension with volumetric attributes
3. Dim_Date: Complete calendar dimension for time-series slicing
4. Fact_Orders: Grain at order-item level with measures (revenue, freight, delay days)
5. Feature_Store_Customer_RFM: Machine learning feature table with Recency, Frequency,
   Monetary metrics and behavioral segmentation tiers.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

def run_silver_to_gold(silver_dir: str, gold_dir: str):
    print("\n" + "="*60)
    print("[*] EXECUTING SILVER -> GOLD TRANSFORMATION (Star Schema & RFM)")
    print("="*60)
    os.makedirs(gold_dir, exist_ok=True)

    # Load Silver Parquet datasets
    df_cust = pd.read_parquet(os.path.join(silver_dir, "customers", "customers.parquet"))
    df_prod = pd.read_parquet(os.path.join(silver_dir, "products", "products.parquet"))
    df_orders = pd.read_parquet(os.path.join(silver_dir, "orders", "orders.parquet"))
    df_items = pd.read_parquet(os.path.join(silver_dir, "order_items", "order_items.parquet"))
    df_payments = pd.read_parquet(os.path.join(silver_dir, "order_payments", "order_payments.parquet"))

    # 1. Build Dim_Customers
    print("[*] Building Dim_Customers...")
    dim_cust = df_cust[[
        "customer_id", "customer_unique_id", "customer_city", 
        "customer_state", "customer_zip_code_prefix"
    ]].copy()
    dim_cust["customer_key"] = range(1, len(dim_cust) + 1)
    
    dim_cust_dir = os.path.join(gold_dir, "dim_customers")
    os.makedirs(dim_cust_dir, exist_ok=True)
    dim_cust.to_parquet(os.path.join(dim_cust_dir, "dim_customers.parquet"), index=False, compression="snappy")
    print(f"  [+] Dim_Customers built: {len(dim_cust)} records.")

    # 2. Build Dim_Products
    print("[*] Building Dim_Products...")
    dim_prod = df_prod.copy()
    # Compute volumetric dimensions
    dim_prod["product_volume_cm3"] = (
        dim_prod["product_length_cm"] * dim_prod["product_height_cm"] * dim_prod["product_width_cm"]
    )
    dim_prod["product_key"] = range(1, len(dim_prod) + 1)
    
    dim_prod_dir = os.path.join(gold_dir, "dim_products")
    os.makedirs(dim_prod_dir, exist_ok=True)
    dim_prod.to_parquet(os.path.join(dim_prod_dir, "dim_products.parquet"), index=False, compression="snappy")
    print(f"  [+] Dim_Products built: {len(dim_prod)} records.")

    # 3. Build Dim_Date
    print("[*] Building Dim_Date calendar dimension...")
    min_date = df_orders["order_purchase_timestamp"].min()
    max_date = df_orders["order_purchase_timestamp"].max()
    date_range = pd.date_range(start=min_date.floor("D"), end=max_date.ceil("D"), freq="D")
    
    dim_date = pd.DataFrame({"full_date": date_range})
    dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["quarter"] = dim_date["full_date"].dt.quarter
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.strftime("%B")
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["day_of_week"] = dim_date["full_date"].dt.day_name()
    dim_date["is_weekend"] = dim_date["full_date"].dt.dayofweek.isin([5, 6]).astype(int)

    dim_date_dir = os.path.join(gold_dir, "dim_date")
    os.makedirs(dim_date_dir, exist_ok=True)
    dim_date.to_parquet(os.path.join(dim_date_dir, "dim_date.parquet"), index=False, compression="snappy")
    print(f"  [+] Dim_Date built: {len(dim_date)} days generated.")

    # 4. Build Fact_Orders
    print("[*] Building Fact_Orders (Denormalized star schema grain)...")
    # Join orders with items and payment aggregates
    order_pay_agg = df_payments.groupby("order_id")["payment_value"].sum().reset_index()
    
    fact_orders = df_orders.merge(df_items, on="order_id", how="inner")
    fact_orders = fact_orders.merge(order_pay_agg, on="order_id", how="left")
    fact_orders["payment_value"] = fact_orders["payment_value"].fillna(fact_orders["price"] + fact_orders["freight_value"])

    # Surrogate key lookups
    fact_orders = fact_orders.merge(dim_cust[["customer_id", "customer_key"]], on="customer_id", how="left")
    fact_orders = fact_orders.merge(dim_prod[["product_id", "product_key"]], on="product_id", how="left")
    fact_orders["date_key"] = fact_orders["order_purchase_timestamp"].dt.strftime("%Y%m%d").astype(float).fillna(0).astype(int)

    fact_clean = fact_orders[[
        "order_id", "customer_key", "product_key", "date_key",
        "order_status", "price", "freight_value", "payment_value",
        "actual_delivery_days", "estimated_delivery_days", "is_delayed",
        "order_year", "order_month"
    ]].copy()

    fact_dir = os.path.join(gold_dir, "fact_orders")
    os.makedirs(fact_dir, exist_ok=True)
    fact_clean.to_parquet(os.path.join(fact_dir, "fact_orders.parquet"), index=False, compression="snappy")
    print(f"  [+] Fact_Orders built: {len(fact_clean)} line-item facts saved.")

    # 5. Machine Learning Feature Store: RFM Customer Segmentation
    print("[*] Generating ML Feature Store: Customer RFM Segmentation...")
    snapshot_date = df_orders["order_purchase_timestamp"].max() + pd.Timedelta(days=1)
    
    # Calculate R, F, M per unique customer
    cust_orders = df_orders.merge(df_cust[["customer_id", "customer_unique_id"]], on="customer_id", how="inner")
    cust_orders = cust_orders.merge(order_pay_agg, on="order_id", how="left")
    cust_orders["payment_value"] = cust_orders["payment_value"].fillna(0)

    rfm = cust_orders.groupby("customer_unique_id").agg({
        "order_purchase_timestamp": lambda x: (snapshot_date - x.max()).days,
        "order_id": "nunique",
        "payment_value": "sum"
    }).reset_index()

    rfm.columns = ["customer_unique_id", "recency_days", "frequency_orders", "monetary_spend"]

    # Calculate Quartile R, F, M Scores (1 to 4)
    # Recency: lower days is better (rank reversed)
    rfm["r_score"] = pd.qcut(rfm["recency_days"], q=4, labels=[4, 3, 2, 1]).astype(int)
    # Frequency: rank based on rank percentiles
    rfm["f_score"] = pd.qcut(rfm["frequency_orders"].rank(method="first"), q=4, labels=[1, 2, 3, 4]).astype(int)
    # Monetary: higher spend is better
    rfm["m_score"] = pd.qcut(rfm["monetary_spend"].rank(method="first"), q=4, labels=[1, 2, 3, 4]).astype(int)

    rfm["rfm_combined_score"] = rfm["r_score"].astype(str) + rfm["f_score"].astype(str) + rfm["m_score"].astype(str)

    # Customer Persona Labeling
    def assign_segment(row):
        r, f, m = row["r_score"], row["f_score"], row["m_score"]
        if r >= 3 and f >= 3 and m >= 3:
            return "Champions"
        elif r >= 3 and f >= 2:
            return "Loyal Customers"
        elif r >= 3:
            return "Recent Customers"
        elif r <= 2 and f >= 3:
            return "At Risk"
        elif r <= 2 and f <= 2:
            return "Hibernating"
        else:
            return "Promising"

    rfm["customer_segment"] = rfm.apply(assign_segment, axis=1)

    rfm_dir = os.path.join(gold_dir, "features_customer_rfm")
    os.makedirs(rfm_dir, exist_ok=True)
    rfm.to_parquet(os.path.join(rfm_dir, "features_customer_rfm.parquet"), index=False, compression="snappy")
    print(f"  [+] Feature Store: RFM computed for {len(rfm)} distinct customers.")

    print("\n[OK] Gold Layer Complete. Ready for Athena/BigQuery & Power BI/Tableau.")

if __name__ == "__main__":
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    s_dir = os.path.join(base, "data", "lakehouse", "silver")
    g_dir = os.path.join(base, "data", "lakehouse", "gold")
    run_silver_to_gold(s_dir, g_dir)
