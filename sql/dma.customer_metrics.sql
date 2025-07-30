--Лояльность клиентов
-- Измеряет retention rate (процент вернувшихся клиентов)
CREATE MATERIALIZED VIEW dma.customer_metrics AS
WITH customer_stats AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(date)) AS first_month,
        DATE_TRUNC('month', MAX(date)) AS last_month,
        COUNT(DISTINCT DATE_TRUNC('month', date)) AS active_months,
        COUNT(DISTINCT transaction_id) AS total_orders
    FROM dds.sales
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    first_month AS cohort,
    DATE_TRUNC('month', CURRENT_DATE) - first_month AS months_since_first_purchase,
    COUNT(customer_id) AS customers,
    ROUND(100.0 * COUNT(CASE WHEN last_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') THEN 1 END) /
          NULLIF(COUNT(customer_id), 0), 1) AS retention_rate
FROM customer_stats
GROUP BY first_month
WITH DATA;