-- ============================================================================
-- ADVANCED ANALYTICAL SQL QUERIES (ATHENA / BIGQUERY / DUCKDB)
-- Demonstrating Window Functions, CTEs, Cohorts, and Aggregations
-- ============================================================================

-- Query 1: Month-over-Month Revenue Growth & Order Velocity
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
    ) AS mom_growth_percentage
FROM monthly_metrics
ORDER BY order_year, order_month;


-- Query 2: Top 3 Highest Grossing Product Categories per State (Window Partitioning)
WITH state_category_revenue AS (
    SELECT
        c.customer_state,
        p.product_category_name,
        ROUND(SUM(f.payment_value), 2) AS category_revenue,
        COUNT(f.order_id) AS items_sold,
        DENSE_RANK() OVER (
            PARTITION BY c.customer_state 
            ORDER BY SUM(f.payment_value) DESC
        ) AS rank_in_state
    FROM fact_orders f
    JOIN dim_customers c ON f.customer_key = c.customer_key
    JOIN dim_products p ON f.product_key = p.product_key
    GROUP BY c.customer_state, p.product_category_name
)
SELECT
    customer_state,
    rank_in_state,
    product_category_name,
    category_revenue,
    items_sold
FROM state_category_revenue
WHERE rank_in_state <= 3
ORDER BY customer_state, rank_in_state;


-- Query 3: Customer RFM Segment Analysis (Executive Feature Metrics)
SELECT
    customer_segment,
    COUNT(customer_unique_id) AS customer_count,
    ROUND(AVG(recency_days), 1) AS avg_recency_days,
    ROUND(AVG(frequency_orders), 2) AS avg_orders_per_customer,
    ROUND(SUM(monetary_spend), 2) AS total_segment_revenue,
    ROUND(AVG(monetary_spend), 2) AS avg_customer_lifetime_value
FROM features_customer_rfm
GROUP BY customer_segment
ORDER BY total_segment_revenue DESC;


-- Query 4: Delivery SLA Reliability & Logistics Bottleneck by State
SELECT
    c.customer_state,
    COUNT(f.order_id) AS total_deliveries,
    ROUND(AVG(f.actual_delivery_days), 1) AS avg_delivery_days,
    SUM(f.is_delayed) AS delayed_deliveries,
    ROUND((CAST(SUM(f.is_delayed) AS DOUBLE) / COUNT(f.order_id)) * 100, 2) AS delay_percentage,
    ROUND(AVG(f.freight_value), 2) AS avg_freight_cost
FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state
HAVING COUNT(f.order_id) >= 20
ORDER BY delay_percentage DESC;
