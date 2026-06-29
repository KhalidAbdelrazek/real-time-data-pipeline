-- =============================================================================
-- DuckDB Analytics Queries for RetailDW
-- Run from CLI: duckdb warehouse/RetailDW.duckdb < warehouse/analytics_queries.sql
-- Or interactively: duckdb warehouse/RetailDW.duckdb
-- =============================================================================

-- ── 1. Row counts (health check) ─────────────────────────────────────────────
SELECT 'dim_customer'      AS table_name, count(*) AS row_count FROM dim_customer
UNION ALL
SELECT 'dim_product',       count(*) FROM dim_product
UNION ALL
SELECT 'dim_store',         count(*) FROM dim_store
UNION ALL
SELECT 'dim_date',          count(*) FROM dim_date
UNION ALL
SELECT 'dim_channel',       count(*) FROM dim_channel
UNION ALL
SELECT 'fact_sales',        count(*) FROM fact_sales
UNION ALL
SELECT 'dead_letter_events', count(*) FROM dead_letter_events
ORDER BY table_name;

-- ── 2. Total revenue (purchases net of returns) ───────────────────────────────
SELECT round(sum(revenue), 2) AS total_revenue
FROM fact_sales
WHERE event_type IN ('purchase', 'return');

-- ── 3. Revenue by product category ───────────────────────────────────────────
SELECT p.product_category,
       round(sum(f.revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.event_type IN ('purchase', 'return')
GROUP BY p.product_category
ORDER BY revenue DESC;

-- ── 4. Revenue by store country ───────────────────────────────────────────────
SELECT s.store_country,
       round(sum(f.revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
WHERE f.event_type IN ('purchase', 'return')
GROUP BY s.store_country
ORDER BY revenue DESC;

-- ── 5. Monthly revenue ────────────────────────────────────────────────────────
SELECT d.year,
       d.month,
       d.month_name,
       round(sum(f.revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.event_type IN ('purchase', 'return')
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- ── 6. Yearly revenue ─────────────────────────────────────────────────────────
SELECT d.year,
       round(sum(f.revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.event_type IN ('purchase', 'return')
GROUP BY d.year
ORDER BY d.year;

-- ── 7. Top 10 customers by net revenue ───────────────────────────────────────
SELECT c.customer_id,
       c.customer_name,
       c.customer_country,
       count(CASE WHEN f.event_type = 'purchase' THEN 1 END) AS purchase_events,
       round(sum(f.revenue), 2) AS net_revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE f.event_type IN ('purchase', 'return')
GROUP BY c.customer_id, c.customer_name, c.customer_country
ORDER BY net_revenue DESC
LIMIT 10;

-- ── 8. Top 10 products by units sold ─────────────────────────────────────────
SELECT p.product_id,
       p.product_name,
       p.product_category,
       sum(CASE WHEN f.event_type = 'purchase' THEN f.quantity ELSE 0 END) AS units_sold,
       round(sum(f.revenue), 2) AS net_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.event_type IN ('purchase', 'return')
GROUP BY p.product_id, p.product_name, p.product_category
ORDER BY net_revenue DESC
LIMIT 10;

-- ── 9. Return rate ────────────────────────────────────────────────────────────
SELECT
    count(CASE WHEN event_type = 'return'   THEN 1 END) AS returns,
    count(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
    round(
        100.0 * count(CASE WHEN event_type = 'return' THEN 1 END)
        / nullif(count(CASE WHEN event_type = 'purchase' THEN 1 END), 0),
        2
    ) AS return_rate_pct
FROM fact_sales;

-- ── 10. Conversion funnel ─────────────────────────────────────────────────────
SELECT
    count(CASE WHEN event_type = 'product_view' THEN 1 END) AS views,
    count(CASE WHEN event_type = 'checkout'     THEN 1 END) AS checkouts,
    count(CASE WHEN event_type = 'purchase'     THEN 1 END) AS purchases,
    round(
        100.0 * count(CASE WHEN event_type = 'purchase' THEN 1 END)
        / nullif(count(CASE WHEN event_type = 'product_view' THEN 1 END), 0),
        2
    ) AS view_to_purchase_pct,
    round(
        100.0 * count(CASE WHEN event_type = 'purchase' THEN 1 END)
        / nullif(count(CASE WHEN event_type = 'checkout' THEN 1 END), 0),
        2
    ) AS checkout_to_purchase_pct
FROM fact_sales;

-- ── 11. Customer lifetime value (top 20) ─────────────────────────────────────
SELECT c.customer_id,
       c.customer_name,
       round(sum(f.revenue), 2) AS customer_lifetime_value
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE f.event_type IN ('purchase', 'return')
GROUP BY c.customer_id, c.customer_name
ORDER BY customer_lifetime_value DESC
LIMIT 20;

-- ── 12. Average product rating ────────────────────────────────────────────────
SELECT p.product_id,
       p.product_name,
       p.product_category,
       round(avg(f.rating::DOUBLE), 2) AS average_rating,
       count(f.rating)                 AS review_count
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.event_type = 'review'
  AND f.rating IS NOT NULL
GROUP BY p.product_id, p.product_name, p.product_category
ORDER BY average_rating DESC, review_count DESC;

-- ── 13. Top marketing channels ────────────────────────────────────────────────
SELECT ch.marketing_channel,
       count(CASE WHEN f.event_type = 'purchase' THEN 1 END) AS purchases,
       round(sum(f.revenue), 2) AS net_revenue
FROM fact_sales f
JOIN dim_channel ch ON f.channel_key = ch.channel_key
WHERE f.event_type IN ('purchase', 'return')
GROUP BY ch.marketing_channel
ORDER BY net_revenue DESC;

-- ── 14. Dead-letter rejection summary ────────────────────────────────────────
SELECT reject_reason,
       count(*) AS rejected_records
FROM dead_letter_events
GROUP BY reject_reason
ORDER BY rejected_records DESC;

-- ── 15. Revenue by payment method ────────────────────────────────────────────
SELECT payment_method,
       count(*)                        AS transactions,
       round(sum(revenue), 2)          AS total_revenue,
       round(avg(final_price), 2)      AS avg_order_value
FROM fact_sales
WHERE event_type = 'purchase'
GROUP BY payment_method
ORDER BY total_revenue DESC;
