"""
Streamlit Web Application
Interactive Data Lakehouse & Executive Analytics Platform
Deployable to Streamlit Community Cloud for a permanent public web link.
"""

import os
import streamlit as st
import pandas as pd
import duckdb
import altair as alt

st.set_page_config(
    page_title="Retail Lakehouse & Analytics",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Base Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GOLD_DIR = os.path.join(BASE_DIR, "data", "lakehouse", "gold")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

@st.cache_data
def load_gold_data():
    fact_orders_path = os.path.join(GOLD_DIR, "fact_orders", "fact_orders.parquet")
    dim_cust_path = os.path.join(GOLD_DIR, "dim_customers", "dim_customers.parquet")
    dim_prod_path = os.path.join(GOLD_DIR, "dim_products", "dim_products.parquet")
    dim_date_path = os.path.join(GOLD_DIR, "dim_date", "dim_date.parquet")
    rfm_path = os.path.join(GOLD_DIR, "features_customer_rfm", "features_customer_rfm.parquet")

    df_fact = pd.read_parquet(fact_orders_path)
    df_cust = pd.read_parquet(dim_cust_path)
    df_prod = pd.read_parquet(dim_prod_path)
    df_date = pd.read_parquet(dim_date_path)
    df_rfm = pd.read_parquet(rfm_path)

    # Enrich Fact for immediate analytics
    df_merged = df_fact.merge(df_cust, on="customer_key", how="left")
    df_merged = df_merged.merge(df_prod, on="product_key", how="left")

    return df_merged, df_rfm, df_fact, df_cust, df_prod, df_date

try:
    df_merged, df_rfm, df_fact, df_cust, df_prod, df_date = load_gold_data()
except Exception as e:
    st.error(f"Error loading Gold Parquet data: {e}. Please run `python run_pipeline.py` first.")
    st.stop()

# -------------------------------------------------------------
# SIDEBAR
# -------------------------------------------------------------
st.sidebar.title("🛍️ Retail Lakehouse")
st.sidebar.markdown("**Engineered for Celebal Technologies**")
st.sidebar.markdown("Architecture: **Medallion (Bronze ➔ Silver ➔ Gold)**")

menu = st.sidebar.radio(
    "Navigation",
    [
        "📊 Executive KPI Dashboard",
        "🎯 Customer RFM & Churn Matrix",
        "⚡ Interactive SQL Sandbox",
        "🛡️ Data Quality & Statistical Profiler",
        "🏗️ Lakehouse Architecture"
    ]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔍 Global Filters")
selected_states = st.sidebar.multiselect(
    "Filter by Customer State",
    options=sorted(df_merged["customer_state"].dropna().unique()),
    default=sorted(df_merged["customer_state"].dropna().unique())[:5]
)

filtered_df = df_merged[df_merged["customer_state"].isin(selected_states)] if selected_states else df_merged

# -------------------------------------------------------------
# PAGE 1: EXECUTIVE KPI DASHBOARD
# -------------------------------------------------------------
if menu == "📊 Executive KPI Dashboard":
    st.title("📊 Executive Performance & Logistics Dashboard")
    st.markdown("Real-time metrics served directly from the **Lakehouse Gold Layer**.")

    # High-level KPIs
    col1, col2, col3, col4 = st.columns(4)
    total_revenue = filtered_df["payment_value"].sum()
    total_orders = filtered_df["order_id"].nunique()
    aov = total_revenue / total_orders if total_orders > 0 else 0
    
    delivered_df = filtered_df[filtered_df["order_status"] == "delivered"]
    ontime_rate = (1 - (delivered_df["is_delayed"].sum() / len(delivered_df))) * 100 if len(delivered_df) > 0 else 100

    col1.metric("Gross Merchandise Value (GMV)", f"${total_revenue:,.2f}")
    col2.metric("Total Completed Orders", f"{total_orders:,}")
    col3.metric("Average Order Value (AOV)", f"${aov:.2f}")
    col4.metric("On-Time Delivery Rate", f"{ontime_rate:.1f}%")

    st.markdown("---")

    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("📈 Monthly Revenue Trend (GMV)")
        monthly = filtered_df.groupby(["order_year", "order_month"])["payment_value"].sum().reset_index()
        monthly["period"] = monthly["order_year"].astype(str) + "-" + monthly["order_month"].astype(str).str.zfill(2)
        
        chart_revenue = alt.Chart(monthly).mark_line(point=True, color="#1f77b4").encode(
            x=alt.X("period:N", title="Month"),
            y=alt.Y("payment_value:Q", title="Revenue ($)"),
            tooltip=["period", alt.Tooltip("payment_value:Q", format="$,.2f")]
        ).properties(height=350)
        st.altair_chart(chart_revenue, use_container_width=True)

    with col_right:
        st.subheader("🚚 Delivery Delay Rate by State")
        state_sla = delivered_df.groupby("customer_state").agg(
            total=("order_id", "count"),
            delayed=("is_delayed", "sum")
        ).reset_index()
        state_sla["delay_rate"] = (state_sla["delayed"] / state_sla["total"]) * 100
        
        chart_sla = alt.Chart(state_sla).mark_bar().encode(
            x=alt.X("customer_state:N", title="State"),
            y=alt.Y("delay_rate:Q", title="Delay Rate (%)"),
            color=alt.Color("delay_rate:Q", scale=alt.Scale(scheme="reds")),
            tooltip=["customer_state", alt.Tooltip("delay_rate:Q", format=".1f%")]
        ).properties(height=350)
        st.altair_chart(chart_sla, use_container_width=True)

    st.subheader("🏆 Top Product Categories by Revenue")
    top_cats = filtered_df.groupby("product_category_name")["payment_value"].sum().reset_index()
    top_cats = top_cats.sort_values(by="payment_value", ascending=False).head(8)
    
    chart_cats = alt.Chart(top_cats).mark_bar(color="#2ca02c").encode(
        x=alt.X("payment_value:Q", title="Revenue ($)"),
        y=alt.Y("product_category_name:N", sort="-x", title="Category"),
        tooltip=["product_category_name", alt.Tooltip("payment_value:Q", format="$,.2f")]
    ).properties(height=300)
    st.altair_chart(chart_cats, use_container_width=True)

# -------------------------------------------------------------
# PAGE 2: CUSTOMER RFM & CHURN MATRIX
# -------------------------------------------------------------
elif menu == "🎯 Customer RFM & Churn Matrix":
    st.title("🎯 Customer RFM Behavioral Segmentation & Feature Store")
    st.markdown("Pre-computed machine learning feature table used for **Customer Lifetime Value (CLV)** & **Churn Prediction**.")

    col1, col2 = st.columns([2, 3])

    with col1:
        st.subheader("Customer Personas Distribution")
        seg_summary = df_rfm["customer_segment"].value_counts().reset_index()
        seg_summary.columns = ["Segment", "Count"]

        chart_seg = alt.Chart(seg_summary).mark_bar().encode(
            x=alt.X("Count:Q", title="Customer Count"),
            y=alt.Y("Segment:N", sort="-x", title="Segment"),
            color=alt.Color("Segment:N", scale=alt.Scale(scheme="category10")),
            tooltip=["Segment", "Count"]
        ).properties(height=320)
        st.altair_chart(chart_seg, use_container_width=True)

    with col2:
        st.subheader("Recency vs. Lifetime Spend by Persona")
        sample_rfm = df_rfm[df_rfm["monetary_spend"] < df_rfm["monetary_spend"].quantile(0.98)]
        chart_scatter = alt.Chart(sample_rfm).mark_circle(size=60, opacity=0.7).encode(
            x=alt.X("recency_days:Q", title="Recency (Days since last order)"),
            y=alt.Y("monetary_spend:Q", title="Lifetime Monetary Spend ($)"),
            color="customer_segment:N",
            tooltip=["customer_unique_id", "customer_segment", "recency_days", "monetary_spend"]
        ).properties(height=320)
        st.altair_chart(chart_scatter, use_container_width=True)

    st.subheader("📋 Customer Feature Store Explorer")
    st.dataframe(df_rfm.head(100), use_container_width=True)

# -------------------------------------------------------------
# PAGE 3: INTERACTIVE SQL SANDBOX
# -------------------------------------------------------------
elif menu == "⚡ Interactive SQL Sandbox":
    st.title("⚡ Interactive Serverless SQL Sandbox")
    st.markdown("Execute live SQL queries directly against **Gold Parquet files** (simulating **AWS Athena** / **Google BigQuery**).")

    con = duckdb.connect()
    con.register("fact_orders", df_fact)
    con.register("dim_customers", df_cust)
    con.register("dim_products", df_prod)
    con.register("dim_date", df_date)
    con.register("features_customer_rfm", df_rfm)

    sample_queries = {
        "Month-over-Month Revenue Growth (LAG Window)": """SELECT
    order_year,
    order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(payment_value), 2) AS total_revenue,
    LAG(ROUND(SUM(payment_value), 2), 1) OVER (ORDER BY order_year, order_month) AS prev_month_revenue,
    ROUND(((SUM(payment_value) - LAG(SUM(payment_value), 1) OVER (ORDER BY order_year, order_month)) /
     LAG(SUM(payment_value), 1) OVER (ORDER BY order_year, order_month)) * 100, 2) AS mom_growth_pct
FROM fact_orders
WHERE order_status != 'canceled'
GROUP BY order_year, order_month
ORDER BY order_year, order_month;""",

        "Top 3 Selling Categories per State (DENSE_RANK)": """WITH ranked AS (
    SELECT
        c.customer_state,
        p.product_category_name,
        ROUND(SUM(f.payment_value), 2) AS revenue,
        DENSE_RANK() OVER (PARTITION BY c.customer_state ORDER BY SUM(f.payment_value) DESC) AS rank
    FROM fact_orders f
    JOIN dim_customers c ON f.customer_key = c.customer_key
    JOIN dim_products p ON f.product_key = p.product_key
    GROUP BY c.customer_state, p.product_category_name
)
SELECT * FROM ranked WHERE rank <= 3 ORDER BY customer_state, rank;""",

        "Customer RFM Segment Revenue Rollup": """SELECT
    customer_segment,
    COUNT(customer_unique_id) AS customer_count,
    ROUND(AVG(recency_days), 1) AS avg_recency,
    ROUND(AVG(frequency_orders), 1) AS avg_orders,
    ROUND(SUM(monetary_spend), 2) AS total_revenue
FROM features_customer_rfm
GROUP BY customer_segment
ORDER BY total_revenue DESC;"""
    }

    choice = st.selectbox("Select a Preset Analytical Query:", list(sample_queries.keys()))
    query_text = st.text_area("SQL Query Editor", value=sample_queries[choice], height=180)

    if st.button("▶ Run SQL Query"):
        try:
            result_df = con.execute(query_text).fetchdf()
            st.success(f"Query returned {len(result_df)} rows successfully.")
            st.dataframe(result_df, use_container_width=True)
        except Exception as e:
            st.error(f"SQL Execution Error: {e}")

# -------------------------------------------------------------
# PAGE 4: DATA QUALITY & STATISTICAL PROFILER
# -------------------------------------------------------------
elif menu == "🛡️ Data Quality & Statistical Profiler":
    st.title("🛡️ Statistical Data Quality Gates & Profiling")
    st.markdown("Automated validation gates running between **Bronze ➔ Silver ➔ Gold**.")

    report_path = os.path.join(REPORTS_DIR, "data_quality_report.json")
    if os.path.exists(report_path):
        import json
        with open(report_path, "r") as f:
            report = json.load(f)

        st.success(f"Overall Lakehouse Health Status: **{report['status']}** (Audited: {report['timestamp']})")

        for test in report["tests"]:
            with st.expander(f"Gate: {test['name']} - Status: {'✅ PASSED' if test.get('passed') else '❌ FAILED'}"):
                st.json(test)
    else:
        st.info("Run `python run_pipeline.py` to produce data quality reports.")

# -------------------------------------------------------------
# PAGE 5: LAKEHOUSE ARCHITECTURE
# -------------------------------------------------------------
elif menu == "🏗️ Lakehouse Architecture":
    st.title("🏗️ Medallion Lakehouse Architecture")
    st.markdown("""
    This project is engineered based on the **Medallion Data Architecture**:
    
    1. **Bronze Layer (Raw Ingestion):**
       - Immutable storage of raw CSV/JSON dumps partitioned by `year=YYYY/month=MM/day=DD`.
       - Emulates AWS S3 or GCP Cloud Storage ingestion.
    2. **Silver Layer (Cleansed & Enriched):**
       - PySpark distributed processing for schema casting, null enforcement, and deduplication.
       - Parquet conversion with Snappy compression for 70%+ storage reduction.
    3. **Gold Layer (Business & Feature Store):**
       - Kimball Star Schema (`Fact_Orders` + `Dim_Customers`, `Dim_Products`, `Dim_Date`).
       - RFM Customer Feature Store computed with PySpark window functions (`ntile(4)`).
    4. **Serving Layer:**
       - Serverless SQL engine (AWS Athena / GCP BigQuery / DuckDB).
       - Live BI Dashboards in Streamlit / Power BI / Tableau.
    """)

st.sidebar.markdown("---")
st.sidebar.info("💡 **Tip:** Push this code to GitHub and connect to [share.streamlit.io](https://share.streamlit.io) for an instant public link.")
