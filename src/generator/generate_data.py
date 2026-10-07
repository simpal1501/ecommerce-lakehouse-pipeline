"""
Synthetic E-Commerce Dataset Generator
Generates realistic multi-table e-commerce data with intentional real-world anomalies:
- Duplicate records (to test deduplication in Silver layer)
- Null and missing attributes (to test schema enforcement)
- Outlier payments (to test IQR statistical quality filters)
"""

import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def generate_ecommerce_data(output_dir: str, num_customers: int = 1500, num_orders: int = 5000):
    os.makedirs(output_dir, exist_ok=True)
    random.seed(42)
    np.random.seed(42)

    print(f"[*] Generating synthetic retail dataset in: {output_dir}")

    # 1. Customers
    cities_states = [
        ("São Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
        ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Salvador", "BA"),
        ("Brasília", "DF"), ("Fortaleza", "CE"), ("Recife", "PE"), ("Campinas", "SP")
    ]
    
    customer_ids = [str(uuid.uuid4())[:8] for _ in range(num_customers)]
    customer_unique_ids = [str(uuid.uuid4())[:12] for _ in range(int(num_customers * 0.85))]
    # Assign some customers to have multiple orders (for repeat purchase & RFM)
    assigned_uniques = [random.choice(customer_unique_ids) for _ in range(num_customers)]

    customers = []
    for c_id, u_id in zip(customer_ids, assigned_uniques):
        city, state = random.choice(cities_states)
        zip_code = random.randint(10000, 99999)
        customers.append({
            "customer_id": c_id,
            "customer_unique_id": u_id,
            "customer_zip_code_prefix": zip_code,
            "customer_city": city,
            "customer_state": state
        })
    df_customers = pd.DataFrame(customers)

    # 2. Products
    categories = [
        "health_beauty", "computers_accessories", "auto", "bed_bath_table",
        "furniture_decor", "sports_leisure", "watches_gifts", "telephony",
        "housewares", "garden_tools", "electronics", "fashion_bags"
    ]
    product_ids = [str(uuid.uuid4())[:8] for _ in range(300)]
    products = []
    for p_id in product_ids:
        products.append({
            "product_id": p_id,
            "product_category_name": random.choice(categories),
            "product_weight_g": random.randint(100, 10000),
            "product_length_cm": random.randint(10, 80),
            "product_height_cm": random.randint(5, 50),
            "product_width_cm": random.randint(10, 60)
        })
    df_products = pd.DataFrame(products)

    # 3. Orders & Items & Payments
    start_date = datetime(2025, 1, 1)
    statuses = ["delivered", "delivered", "delivered", "delivered", "shipped", "canceled", "invoiced"]
    payment_types = ["credit_card", "credit_card", "boleto", "voucher", "debit_card"]

    orders = []
    order_items = []
    payments = []

    for i in range(num_orders):
        order_id = str(uuid.uuid4())[:10]
        cust_id = random.choice(customer_ids)
        status = random.choice(statuses)
        
        days_offset = random.randint(0, 365)
        purchase_time = start_date + timedelta(days=days_offset, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        approved_time = purchase_time + timedelta(minutes=random.randint(5, 180)) if status != "canceled" else None
        carrier_time = purchase_time + timedelta(days=random.randint(1, 3)) if status in ["delivered", "shipped"] else None
        
        # Delivery delay calculation
        estimated_delivery = purchase_time + timedelta(days=random.randint(10, 20))
        delivered_time = None
        if status == "delivered":
            # 85% on time, 15% delayed
            delay_days = random.randint(-5, 8)
            delivered_time = estimated_delivery + timedelta(days=delay_days)

        orders.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "order_status": status,
            "order_purchase_timestamp": purchase_time.strftime("%Y-%m-%d %H:%M:%S"),
            "order_approved_at": approved_time.strftime("%Y-%m-%d %H:%M:%S") if approved_time else None,
            "order_delivered_carrier_date": carrier_time.strftime("%Y-%m-%d %H:%M:%S") if carrier_time else None,
            "order_delivered_customer_date": delivered_time.strftime("%Y-%m-%d %H:%M:%S") if delivered_time else None,
            "order_estimated_delivery_date": estimated_delivery.strftime("%Y-%m-%d %H:%M:%S")
        })

        # Items for this order (1 to 4 items)
        num_items = random.choices([1, 2, 3, 4], weights=[0.75, 0.15, 0.07, 0.03])[0]
        order_total_value = 0.0
        for item_idx in range(1, num_items + 1):
            prod_id = random.choice(product_ids)
            price = round(random.uniform(15.0, 450.0), 2)
            freight = round(random.uniform(8.0, 45.0), 2)
            order_total_value += (price + freight)

            order_items.append({
                "order_id": order_id,
                "order_item_id": item_idx,
                "product_id": prod_id,
                "seller_id": str(uuid.uuid4())[:8],
                "shipping_limit_date": (purchase_time + timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "freight_value": freight
            })

        # Payments
        if status != "canceled":
            pay_type = random.choice(payment_types)
            installments = random.randint(1, 10) if pay_type == "credit_card" else 1
            
            # Introduce intentional outlier payment for statistical validation testing
            if i == 42:
                payment_val = 25000.0  # Intentional massive outlier
            elif i == 99:
                payment_val = -50.0    # Intentional negative value bug
            else:
                payment_val = round(order_total_value, 2)

            payments.append({
                "order_id": order_id,
                "payment_sequential": 1,
                "payment_type": pay_type,
                "payment_installments": installments,
                "payment_value": payment_val
            })

    df_orders = pd.DataFrame(orders)
    df_order_items = pd.DataFrame(order_items)
    df_payments = pd.DataFrame(payments)

    # Inject intentional duplicates into orders and customers to test Silver deduplication
    df_orders = pd.concat([df_orders, df_orders.iloc[:25]], ignore_index=True)
    df_customers = pd.concat([df_customers, df_customers.iloc[:15]], ignore_index=True)

    # Save to raw directory
    df_customers.to_csv(os.path.join(output_dir, "customers_raw.csv"), index=False)
    df_products.to_csv(os.path.join(output_dir, "products_raw.csv"), index=False)
    df_orders.to_csv(os.path.join(output_dir, "orders_raw.csv"), index=False)
    df_order_items.to_csv(os.path.join(output_dir, "order_items_raw.csv"), index=False)
    df_payments.to_csv(os.path.join(output_dir, "order_payments_raw.csv"), index=False)

    print(f"[+] Successfully generated:")
    print(f"    - Customers: {len(df_customers)} records (with duplicates)")
    print(f"    - Products: {len(df_products)} records")
    print(f"    - Orders: {len(df_orders)} records (with duplicates)")
    print(f"    - Order Items: {len(df_order_items)} records")
    print(f"    - Payments: {len(df_payments)} records (with outliers)")

if __name__ == "__main__":
    target = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")
    generate_ecommerce_data(target)
