"""
BI Dashboard Reference Generator
Generates publication-quality charts replicating the Power BI / Tableau dashboard pages:
1. Executive Performance Dashboard: Monthly GMV trend, Order Volumes, Delivery SLA.
2. Customer RFM Segmentation & CLV Matrix.
Saves PNG artifacts to the bi/ directory for portfolio and GitHub README integration.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def generate_visuals(gold_dir: str, output_dir: str):
    print("\n" + "="*60)
    print("[*] GENERATING BI DASHBOARD ASSETS (Power BI / Tableau Replication)")
    print("="*60)
    os.makedirs(output_dir, exist_ok=True)

    sns.set_theme(style="whitegrid")
    fact_path = os.path.join(gold_dir, "fact_orders", "fact_orders.parquet")
    rfm_path = os.path.join(gold_dir, "features_customer_rfm", "features_customer_rfm.parquet")

    df_fact = pd.read_parquet(fact_path)
    df_rfm = pd.read_parquet(rfm_path)

    # -------------------------------------------------------------
    # Visual 1: Executive KPI Dashboard (2x2 Grid)
    # -------------------------------------------------------------
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("E-Commerce Executive Performance Dashboard (Gold Layer)", fontsize=16, fontweight="bold")

    # 1.1 Monthly Revenue Trend
    monthly = df_fact[df_fact["order_status"] != "canceled"].groupby(["order_year", "order_month"])["payment_value"].sum().reset_index()
    monthly["period"] = monthly["order_year"].astype(str) + "-" + monthly["order_month"].astype(str).str.zfill(2)
    sns.lineplot(ax=axes[0, 0], data=monthly, x="period", y="payment_value", marker="o", color="#1f77b4", linewidth=2.5)
    axes[0, 0].set_title("Gross Merchandise Value (GMV) by Month", fontsize=12, fontweight="bold")
    axes[0, 0].set_xlabel("Month")
    axes[0, 0].set_ylabel("Revenue ($)")
    axes[0, 0].tick_params(axis='x', rotation=45)

    # 1.2 Order Status Distribution
    status_counts = df_fact["order_status"].value_counts()
    axes[0, 1].pie(status_counts, labels=status_counts.index, autopct="%1.1f%%", colors=sns.color_palette("pastel"))
    axes[0, 1].set_title("Order Fulfillment Status Ratio", fontsize=12, fontweight="bold")

    # 1.3 Delivery Days Distribution
    delivered = df_fact[df_fact["order_status"] == "delivered"]["actual_delivery_days"].dropna()
    sns.histplot(ax=axes[1, 0], data=delivered, kde=True, color="#2ca02c", bins=20)
    axes[1, 0].axvline(delivered.mean(), color="red", linestyle="--", label=f"Mean: {delivered.mean():.1f}d")
    axes[1, 0].set_title("Delivery Duration Distribution (Days)", fontsize=12, fontweight="bold")
    axes[1, 0].set_xlabel("Actual Delivery Days")
    axes[1, 0].legend()

    # 1.4 On-time vs Delayed Orders
    delayed_counts = df_fact[df_fact["order_status"] == "delivered"]["is_delayed"].value_counts().rename({0: "On Time", 1: "Delayed"})
    sns.barplot(ax=axes[1, 1], x=delayed_counts.index, y=delayed_counts.values, palette=["#2ca02c", "#d62728"])
    axes[1, 1].set_title("Logistics SLA: On-Time vs Delayed Deliveries", fontsize=12, fontweight="bold")
    axes[1, 1].set_ylabel("Order Count")

    plt.tight_layout()
    v1_path = os.path.join(output_dir, "dashboard_executive_kpis.png")
    plt.savefig(v1_path, dpi=200)
    plt.close()
    print(f"  [+] Saved Visual 1: {v1_path}")

    # -------------------------------------------------------------
    # Visual 2: Customer RFM Segmentation & Feature Analysis
    # -------------------------------------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Customer RFM Behavioral Segmentation & Churn Matrix", fontsize=16, fontweight="bold")

    # 2.1 Segment Distribution
    seg_counts = df_rfm["customer_segment"].value_counts().reset_index()
    seg_counts.columns = ["customer_segment", "count"]
    sns.barplot(ax=axes[0], data=seg_counts, x="count", y="customer_segment", palette="viridis")
    axes[0].set_title("Customer Count per Persona Segment", fontsize=12, fontweight="bold")
    axes[0].set_xlabel("Number of Customers")
    axes[0].set_ylabel("Segment")

    # 2.2 Recency vs Monetary Scatter by Segment
    sample_rfm = df_rfm[df_rfm["monetary_spend"] < df_rfm["monetary_spend"].quantile(0.98)]
    sns.scatterplot(
        ax=axes[1], data=sample_rfm, x="recency_days", y="monetary_spend", 
        hue="customer_segment", palette="tab10", alpha=0.7, s=40
    )
    axes[1].set_title("Recency vs Lifetime Spend by Segment", fontsize=12, fontweight="bold")
    axes[1].set_xlabel("Recency (Days since last purchase)")
    axes[1].set_ylabel("Total Spend ($)")
    axes[1].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.tight_layout()
    v2_path = os.path.join(output_dir, "dashboard_rfm_segments.png")
    plt.savefig(v2_path, dpi=200)
    plt.close()
    print(f"  [+] Saved Visual 2: {v2_path}")

    print("\n[OK] BI Dashboard visual assets generated successfully.")

if __name__ == "__main__":
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    g_dir = os.path.join(base, "data", "lakehouse", "gold")
    out_dir = os.path.join(base, "bi")
    generate_visuals(g_dir, out_dir)
