"""
Bronze Layer Ingestion Engine
Handles ingestion of raw incoming CSV/JSON dumps into the Data Lake Bronze Zone.
Supports:
1. Local Data Lake filesystem emulation (default)
2. AWS S3 Data Lake (via boto3 when S3_BUCKET is specified)
3. GCP Cloud Storage (via google-cloud-storage when GCS_BUCKET is specified)
Organizes files using temporal partitioning: year=YYYY/month=MM/day=DD
"""

import os
import sys
import shutil
from datetime import datetime

def ingest_to_bronze(source_dir: str, target_base_dir: str, s3_bucket: str = None, gcs_bucket: str = None):
    now = datetime.now()
    partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    
    print(f"[*] Starting Bronze Layer Ingestion for partition: {partition_path}")
    
    if not os.path.exists(source_dir):
        raise FileNotFoundError(f"Source directory '{source_dir}' does not exist. Run generator first.")

    raw_files = [f for f in os.listdir(source_dir) if f.endswith(".csv") or f.endswith(".json")]
    if not raw_files:
        print("[!] No files found to ingest in source directory.")
        return

    # 1. Local Lakehouse Storage (Always created as primary local target)
    bronze_dir = os.path.join(target_base_dir, "bronze", partition_path)
    os.makedirs(bronze_dir, exist_ok=True)

    for filename in raw_files:
        src_path = os.path.join(source_dir, filename)
        dest_path = os.path.join(bronze_dir, filename)
        shutil.copy2(src_path, dest_path)
        print(f"  [+] Ingested to Local Bronze Lake: {dest_path}")

    # 2. AWS S3 Target Hook (if configured)
    if s3_bucket:
        try:
            import boto3
            s3_client = boto3.client("s3")
            for filename in raw_files:
                src_path = os.path.join(source_dir, filename)
                s3_key = f"bronze/{partition_path}/{filename}"
                s3_client.upload_file(src_path, s3_bucket, s3_key)
                print(f"  [+] Uploaded to AWS S3: s3://{s3_bucket}/{s3_key}")
        except Exception as e:
            print(f"  [-] S3 Upload skipped or failed: {e}")

    # 3. GCP GCS Target Hook (if configured)
    if gcs_bucket:
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket(gcs_bucket)
            for filename in raw_files:
                src_path = os.path.join(source_dir, filename)
                blob_name = f"bronze/{partition_path}/{filename}"
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(src_path)
                print(f"  [+] Uploaded to GCP GCS: gs://{gcs_bucket}/{blob_name}")
        except Exception as e:
            print(f"  [-] GCS Upload skipped or failed: {e}")

    print("[OK] Bronze Ingestion Complete. Raw audit trail persisted.")

if __name__ == "__main__":
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    src = os.path.join(base, "data", "raw")
    dest = os.path.join(base, "data", "lakehouse")
    s3_b = os.getenv("AWS_S3_LAKEHOUSE_BUCKET")
    gcs_b = os.getenv("GCP_GCS_LAKEHOUSE_BUCKET")
    ingest_to_bronze(src, dest, s3_b, gcs_b)
