"""
Silver to Gold PySpark Job (Enterprise Databricks / EMR Implementation)
Demonstrates:
- Broadcast joins for small dimension tables (Dim_Products, Dim_Customers)
- Spark SQL Window functions (ntile, row_number) for RFM feature engineering
- Partitioned Snappy Parquet writes for downstream Athena/BigQuery cost optimization
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, broadcast, countDistinct, sum as spark_sum, max as spark_max,
    datediff, current_date, ntile, concat, lit, when, to_date
)
from pyspark.sql.window import Window

def create_spark_session(app_name="SilverToGoldLakehouse"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

def run_pyspark_silver_to_gold(silver_uri: str, gold_uri: str):
    spark = create_spark_session()
    print(f"[*] Spark Application ID: {spark.sparkContext.applicationId}")

    # Read Silver tables
    df_orders = spark.read.parquet(f"{silver_uri}/orders")
    df_items = spark.read.parquet(f"{silver_uri}/order_items")
    df_cust = spark.read.parquet(f"{silver_uri}/customers")
    df_prod = spark.read.parquet(f"{silver_uri}/products")
    df_pay = spark.read.parquet(f"{silver_uri}/order_payments")

    # 1. Fact_Orders with Broadcast Joins
    print("[*] Building Fact_Orders with broadcast join optimization...")
    pay_agg = df_pay.groupBy("order_id").agg(spark_sum("payment_value").alias("total_payment"))

    # Broadcast smaller dimension tables to avoid network shuffle
    fact_df = df_orders \
        .join(df_items, on="order_id", how="inner") \
        .join(pay_agg, on="order_id", how="left") \
        .join(broadcast(df_cust.select("customer_id", "customer_city", "customer_state")), on="customer_id", how="left") \
        .join(broadcast(df_prod.select("product_id", "product_category_name")), on="product_id", how="left")

    # Write Fact_Orders partitioned by year and month
    fact_df.write \
        .mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .parquet(f"{gold_uri}/fact_orders")

    # 2. Customer RFM Feature Table using Spark Window Functions
    print("[*] Computing Customer RFM Feature Store using Spark Window ntile(4)...")
    cust_full = df_orders.join(df_cust, on="customer_id", how="inner").join(pay_agg, on="order_id", how="left")

    # Aggregations per customer
    rfm_base = cust_full.groupBy("customer_unique_id").agg(
        datediff(to_date(lit("2026-01-01")), spark_max("order_purchase_timestamp")).alias("recency_days"),
        countDistinct("order_id").alias("frequency_orders"),
        spark_sum("total_payment").alias("monetary_spend")
    )

    # Window specs for quartile scoring
    window_r = Window.orderBy(col("recency_days").asc())
    window_f = Window.orderBy(col("frequency_orders").desc())
    window_m = Window.orderBy(col("monetary_spend").desc())

    rfm_scored = rfm_base \
        .withColumn("r_score", ntile(4).over(window_r)) \
        .withColumn("f_score", ntile(4).over(window_f)) \
        .withColumn("m_score", ntile(4).over(window_m)) \
        .withColumn("rfm_score", concat(col("r_score"), col("f_score"), col("m_score"))) \
        .withColumn("segment", 
                    when((col("r_score") >= 3) & (col("f_score") >= 3), "Champions")
                    .when((col("r_score") >= 3) & (col("f_score") >= 2), "Loyal Customers")
                    .when(col("r_score") <= 2, "At Risk")
                    .otherwise("Standard"))

    rfm_scored.write.mode("overwrite").parquet(f"{gold_uri}/features_customer_rfm")

    print("[✔] PySpark Silver to Gold Job Finished successfully.")
    spark.stop()

if __name__ == "__main__":
    import sys
    silver = sys.argv[1] if len(sys.argv) > 1 else "data/lakehouse/silver"
    gold = sys.argv[2] if len(sys.argv) > 2 else "data/lakehouse/gold"
    run_pyspark_silver_to_gold(silver, gold)
