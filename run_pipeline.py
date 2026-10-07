"""
Master Pipeline Orchestrator
Executes the End-to-End Retail Data Lakehouse:
1. Synthetic Data Generation (with edge cases)
2. Ingestion into Bronze Layer (Cloud S3/GCS or Local)
3. Bronze -> Silver Transformation (Cleansing, Casting, Deduplication, Parquet)
4. Statistical Data Quality Gates (Null checks, Uniqueness, IQR Outliers, Delivery Distribution)
5. Silver -> Gold Transformation (Star Schema Dimensional Modeling & RFM Feature Store)
6. Analytical SQL Suite (Window Functions, MoM Growth, Cohorts)
7. BI Dashboard Visual Asset Generation (Power BI / Tableau Replication)
"""

import os
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Ensure src modules are resolvable
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from src.generator.generate_data import generate_ecommerce_data
from src.ingestion.ingest_to_lake import ingest_to_bronze
from src.spark_jobs.bronze_to_silver import run_bronze_to_silver
from src.quality.data_quality import run_quality_checks
from src.spark_jobs.silver_to_gold import run_silver_to_gold
from src.analytics.sql_runner import run_analytical_queries
from bi.generate_bi_visuals import generate_visuals

def main():
    start_time = time.time()
    print("\n" + "#"*70)
    print("  RETAIL DATA LAKEHOUSE & ANALYTICS PIPELINE (MEDALLION ARCHITECTURE)")
    print("  Engineered for Celebal Technologies Data Engineer Role")
    print("#"*70 + "\n")

    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    lake_dir = os.path.join(PROJECT_ROOT, "data", "lakehouse")
    bronze_dir = os.path.join(lake_dir, "bronze")
    silver_dir = os.path.join(lake_dir, "silver")
    gold_dir = os.path.join(lake_dir, "gold")
    reports_dir = os.path.join(PROJECT_ROOT, "reports")
    bi_dir = os.path.join(PROJECT_ROOT, "bi")

    # Step 1: Generate Raw Data
    print("[STEP 1/7] Generating Raw E-Commerce Transaction Data...")
    generate_ecommerce_data(raw_dir, num_customers=1500, num_orders=5000)

    # Step 2: Ingest to Bronze
    print("\n[STEP 2/7] Ingesting to Bronze Layer (Audit Storage)...")
    s3_bucket = os.getenv("AWS_S3_LAKEHOUSE_BUCKET")
    gcs_bucket = os.getenv("GCP_GCS_LAKEHOUSE_BUCKET")
    ingest_to_bronze(raw_dir, lake_dir, s3_bucket, gcs_bucket)

    # Step 3: Bronze to Silver
    print("\n[STEP 3/7] Processing Bronze to Silver (Cleaning & Parquet Conversion)...")
    run_bronze_to_silver(bronze_dir, silver_dir)

    # Step 4: Statistical Quality Gates
    print("\n[STEP 4/7] Running Statistical Data Quality Gates & Profiling...")
    quality_report = os.path.join(reports_dir, "data_quality_report.json")
    run_quality_checks(silver_dir, quality_report)

    # Step 5: Silver to Gold
    print("\n[STEP 5/7] Modeling Silver to Gold (Star Schema & RFM Feature Store)...")
    run_silver_to_gold(silver_dir, gold_dir)

    # Step 6: Analytical SQL Queries
    print("\n[STEP 6/7] Executing Analytical SQL Suite on Gold Parquet...")
    run_analytical_queries(gold_dir, reports_dir)

    # Step 7: BI Visuals Generation
    print("\n[STEP 7/7] Generating BI Dashboard Visual Artifacts...")
    generate_visuals(gold_dir, bi_dir)

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "="*70)
    print(f" [OK] PIPELINE EXECUTION FINISHED SUCCESSFULLY IN {elapsed}s!")
    print("="*70)
    print(f"  * Gold Parquet Tables : {gold_dir}")
    print(f"  * Data Quality Audit : {quality_report}")
    print(f"  * SQL Query Reports  : {reports_dir}")
    print(f"  * BI Visuals Assets  : {bi_dir}")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
