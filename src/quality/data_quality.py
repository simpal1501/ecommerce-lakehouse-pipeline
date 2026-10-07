"""
Statistical Data Quality & Validation Gate
Automates statistical profiling, data drift inspection, and outlier detection:
1. Primary Key Uniqueness & Null Rate Verification
2. Statistical Outlier Detection using the Interquartile Range (IQR) technique
3. SLA Delivery Distribution Profiling (Mean, Standard Deviation, Percentiles)
4. Generates a persistent Data Quality Audit Report
"""

import os
import json
import pandas as pd
import numpy as np

def run_quality_checks(silver_dir: str, report_output_path: str = None) -> dict:
    print("\n" + "="*60)
    print("[*] EXECUTING STATISTICAL DATA QUALITY GATE")
    print("="*60)

    results = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "status": "PASSED",
        "tests": []
    }

    # Load Silver tables
    orders_path = os.path.join(silver_dir, "orders", "orders.parquet")
    payments_path = os.path.join(silver_dir, "order_payments", "order_payments.parquet")

    df_orders = pd.read_parquet(orders_path)
    df_payments = pd.read_parquet(payments_path)

    # Test 1: Null Rate Verification
    print("[*] Test 1: Null Rate Verification on Primary Keys...")
    null_orders = int(df_orders["order_id"].isnull().sum())
    null_rate = null_orders / len(df_orders)
    test_1 = {
        "name": "Primary Key Null Rate (order_id)",
        "null_count": null_orders,
        "null_rate": f"{null_rate:.4%}",
        "passed": null_orders == 0
    }
    results["tests"].append(test_1)
    status_icon = "PASS" if test_1["passed"] else "FAIL"
    print(f"  [{status_icon}] Order ID Null Count: {null_orders} (Expected: 0)")

    # Test 2: Primary Key Uniqueness
    print("[*] Test 2: Primary Key Uniqueness...")
    unique_orders = df_orders["order_id"].nunique()
    total_orders = len(df_orders)
    is_unique = (unique_orders == total_orders)
    test_2 = {
        "name": "Order ID Uniqueness",
        "total_records": total_orders,
        "unique_records": unique_orders,
        "passed": is_unique
    }
    results["tests"].append(test_2)
    status_icon = "PASS" if is_unique else "FAIL"
    print(f"  [{status_icon}] Uniqueness: {unique_orders} unique / {total_orders} total")

    # Test 3: Statistical Outlier Profiling on Payments (IQR Method)
    print("[*] Test 3: Statistical Outlier Profiling (IQR Method)...")
    payments = df_payments["payment_value"].dropna()
    q1 = float(np.percentile(payments, 25))
    q3 = float(np.percentile(payments, 75))
    iqr = q3 - q1
    lower_bound = max(0.0, q1 - 1.5 * iqr)
    upper_bound = q3 + 1.5 * iqr
    outliers = payments[(payments < lower_bound) | (payments > upper_bound)]
    outlier_rate = len(outliers) / len(payments)

    test_3 = {
        "name": "Payment Value Statistical Outlier Profile (IQR)",
        "q1_25th_percentile": round(q1, 2),
        "q3_75th_percentile": round(q3, 2),
        "iqr": round(iqr, 2),
        "statistical_upper_bound": round(upper_bound, 2),
        "outlier_count": len(outliers),
        "outlier_percentage": f"{outlier_rate:.2%}",
        "passed": True  # Profiling metric
    }
    results["tests"].append(test_3)
    print(f"  [i] Payment Q1: ${q1:.2f}, Q3: ${q3:.2f}, IQR: ${iqr:.2f}")
    print(f"  [i] Statistical Cutoff Threshold: ${upper_bound:.2f}")
    print(f"  [i] Detected {len(outliers)} statistical outlier transactions ({outlier_rate:.2%})")

    # Test 4: Delivery Duration SLA Distribution (Mean & Std Dev)
    print("[*] Test 4: Delivery SLA Distribution Profiling...")
    delivered = df_orders[df_orders["actual_delivery_days"].notnull()]["actual_delivery_days"]
    mean_days = float(delivered.mean())
    std_days = float(delivered.std())
    p95_days = float(np.percentile(delivered, 95))
    delayed_count = int(df_orders["is_delayed"].sum())
    delayed_pct = delayed_count / len(df_orders)

    test_4 = {
        "name": "Delivery SLA Statistical Distribution",
        "mean_delivery_days": round(mean_days, 2),
        "std_dev_delivery_days": round(std_days, 2),
        "p95_delivery_days": round(p95_days, 2),
        "delayed_orders_count": delayed_count,
        "delayed_orders_rate": f"{delayed_pct:.2%}",
        "passed": mean_days < 25.0
    }
    results["tests"].append(test_4)
    print(f"  [i] Mean Delivery Time: {mean_days:.1f} days (Std Dev: {std_days:.1f})")
    print(f"  [i] 95th Percentile Delivery Time: {p95_days:.1f} days")
    print(f"  [i] Delayed Deliveries Rate: {delayed_pct:.2%}")

    # Determine Overall Gate Status
    all_passed = all(t["passed"] for t in results["tests"])
    results["status"] = "PASSED" if all_passed else "FAILED"

    if report_output_path:
        os.makedirs(os.path.dirname(report_output_path), exist_ok=True)
        with open(report_output_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n[OK] Data Quality Report written to: {report_output_path}")

    return results

if __name__ == "__main__":
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    s_dir = os.path.join(base, "data", "lakehouse", "silver")
    rep = os.path.join(base, "reports", "data_quality_report.json")
    run_quality_checks(s_dir, rep)
