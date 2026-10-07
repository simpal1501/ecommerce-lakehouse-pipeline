"""
Analytical SQL Execution Engine
Uses DuckDB as an in-process columnar SQL engine to query Gold Parquet files directly.
Simulates AWS Athena and GCP BigQuery serverless querying with zero database setup required.
Executes window functions, CTEs, cohort retention, and RFM segment rollups.
"""

import os
import duckdb

def run_analytical_queries(gold_dir: str, reports_dir: str = None):
    print("\n" + "="*60)
    print("[*] EXECUTING ANALYTICAL SQL SUITE ON GOLD PARQUET")
    print("="*60)

    con = duckdb.connect()

    # Register Gold Parquet tables as views
    fact_orders_path = os.path.join(gold_dir, "fact_orders", "fact_orders.parquet")
    dim_cust_path = os.path.join(gold_dir, "dim_customers", "dim_customers.parquet")
    dim_prod_path = os.path.join(gold_dir, "dim_products", "dim_products.parquet")
    dim_date_path = os.path.join(gold_dir, "dim_date", "dim_date.parquet")
    rfm_path = os.path.join(gold_dir, "features_customer_rfm", "features_customer_rfm.parquet")

    con.execute(f"CREATE VIEW fact_orders AS SELECT * FROM read_parquet('{fact_orders_path}')")
    con.execute(f"CREATE VIEW dim_customers AS SELECT * FROM read_parquet('{dim_cust_path}')")
    con.execute(f"CREATE VIEW dim_products AS SELECT * FROM read_parquet('{dim_prod_path}')")
    con.execute(f"CREATE VIEW dim_date AS SELECT * FROM read_parquet('{dim_date_path}')")
    con.execute(f"CREATE VIEW features_customer_rfm AS SELECT * FROM read_parquet('{rfm_path}')")

    if reports_dir:
        os.makedirs(reports_dir, exist_ok=True)

    # 1. Month-over-Month Growth (LAG Window Function)
    print("\n[SQL Query 1] Month-over-Month Revenue Growth & Order Velocity (Window LAG):")
    q1 = """
    WITH monthly_metrics AS (
        SELECT
            order_year,
            order_month,
            COUNT(DISTINCT order_id) AS total_orders,
            ROUND(SUM(payment_value), 2) AS total_revenue,
            ROUND(AVG(payment_value), 2) AS avg_order_value
        FROM fact_orders
        WHERE order_status != 'canceled'
        GROUP BY order_year, order_month
    )
    SELECT
        order_year,
        order_month,
        total_orders,
        total_revenue,
        avg_order_value,
        LAG(total_revenue, 1) OVER (ORDER BY order_year, order_month) AS prev_month_revenue,
        ROUND(
            ((total_revenue - LAG(total_revenue, 1) OVER (ORDER BY order_year, order_month)) /
             LAG(total_revenue, 1) OVER (ORDER BY order_year, order_month)) * 100, 2
        ) AS mom_growth_pct
    FROM monthly_metrics
    ORDER BY order_year, order_month;
    """
    df_q1 = con.execute(q1).fetchdf()
    print(df_q1.to_string(index=False))
    if reports_dir:
        df_q1.to_csv(os.path.join(reports_dir, "mom_revenue_growth.csv"), index=False)

    # 2. Customer RFM Segmentation Summary
    print("\n[SQL Query 2] Customer RFM Behavioral Segmentation (Gold Feature Store):")
    q2 = """
    SELECT
        customer_segment,
        COUNT(customer_unique_id) AS customer_count,
        ROUND(AVG(recency_days), 1) AS avg_recency_days,
        ROUND(AVG(frequency_orders), 2) AS avg_orders,
        ROUND(SUM(monetary_spend), 2) AS total_segment_revenue,
        ROUND(AVG(monetary_spend), 2) AS avg_clv
    FROM features_customer_rfm
    GROUP BY customer_segment
    ORDER BY total_segment_revenue DESC;
    """
    df_q2 = con.execute(q2).fetchdf()
    print(df_q2.to_string(index=False))
    if reports_dir:
        df_q2.to_csv(os.path.join(reports_dir, "rfm_segments.csv"), index=False)

    # 3. Delivery SLA Performance by State
    print("\n[SQL Query 3] Delivery SLA Reliability & Logistics Bottleneck by State:")
    q3 = """
    SELECT
        c.customer_state,
        COUNT(f.order_id) AS total_deliveries,
        ROUND(AVG(f.actual_delivery_days), 1) AS avg_delivery_days,
        SUM(f.is_delayed) AS delayed_orders,
        ROUND((CAST(SUM(f.is_delayed) AS DOUBLE) / COUNT(f.order_id)) * 100, 2) AS delay_pct,
        ROUND(AVG(f.freight_value), 2) AS avg_freight
    FROM fact_orders f
    JOIN dim_customers c ON f.customer_key = c.customer_key
    WHERE f.order_status = 'delivered'
    GROUP BY c.customer_state
    HAVING COUNT(f.order_id) >= 20
    ORDER BY delay_pct DESC
    LIMIT 5;
    """
    df_q3 = con.execute(q3).fetchdf()
    print(df_q3.to_string(index=False))
    if reports_dir:
        df_q3.to_csv(os.path.join(reports_dir, "delivery_sla_by_state.csv"), index=False)

    print("\n[OK] Analytical SQL Suite Completed Successfully.")

if __name__ == "__main__":
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    g_dir = os.path.join(base, "data", "lakehouse", "gold")
    r_dir = os.path.join(base, "reports")
    run_analytical_queries(g_dir, r_dir)
