"""
Bronze to Silver PySpark Job (Enterprise Reference for AWS EMR / Databricks)
Uses Apache Spark distributed DataFrame transformations, schema casting,
broadcast joins, and partitioned Parquet writes.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, datediff, round as spark_round, 
    when, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

def create_spark_session(app_name="BronzeToSilverLakehouse"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def run_pyspark_bronze_to_silver(bronze_s3_uri: str, silver_s3_uri: str):
    spark = create_spark_session()
    print(f"[*] Spark Application ID: {spark.sparkContext.applicationId}")

    # 1. Orders: Schema Enforcement & Transformations
    order_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
    ])

    print("[*] Ingesting Bronze Orders via PySpark...")
    df_orders_raw = spark.read.option("header", "true").schema(order_schema).csv(f"{bronze_s3_uri}/orders_raw.csv")

    # Deduplication and Casting
    df_orders_clean = df_orders_raw \
        .dropDuplicates(["order_id"]) \
        .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
        .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
        .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date"))) \
        .withColumn("actual_delivery_days", 
                    datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
        .withColumn("is_delayed", 
                    when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1).otherwise(0)) \
        .withColumn("order_year", year(col("order_purchase_timestamp"))) \
        .withColumn("order_month", month(col("order_purchase_timestamp")))

    # Write Silver Orders partitioned by year and month
    print(f"[*] Writing Silver Orders to {silver_s3_uri}/orders in Snappy Parquet...")
    df_orders_clean.write \
        .mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .parquet(f"{silver_s3_uri}/orders")

    # 2. Customers
    df_customers = spark.read.option("header", "true").csv(f"{bronze_s3_uri}/customers_raw.csv") \
        .dropDuplicates(["customer_id"])
    
    df_customers.write.mode("overwrite").parquet(f"{silver_s3_uri}/customers")

    # 3. Payments (Filter corrupted payments)
    df_payments = spark.read.option("header", "true").csv(f"{bronze_s3_uri}/order_payments_raw.csv") \
        .withColumn("payment_value", col("payment_value").cast(DoubleType())) \
        .filter(col("payment_value") >= 0)

    df_payments.write.mode("overwrite").parquet(f"{silver_s3_uri}/order_payments")

    print("[✔] PySpark Silver Job Completed successfully.")
    spark.stop()

if __name__ == "__main__":
    import sys
    bronze = sys.argv[1] if len(sys.argv) > 1 else "data/lakehouse/bronze"
    silver = sys.argv[2] if len(sys.argv) > 2 else "data/lakehouse/silver"
    run_pyspark_bronze_to_silver(bronze, silver)
