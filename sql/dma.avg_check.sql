--Средний чек и ценность клиента
-- Показывает, сколько в среднем тратит клиент за покупку (avg_check) и за период (avg_customer_value)
CREATE MATERIALIZED VIEW dma.avg_check AS
SELECT
    DATE_TRUNC('month', s.date) AS month,
    p.category,
    SUM(s.price * s.quantity) / NULLIF(COUNT(DISTINCT s.transaction_id), 0) AS avg_check,
    SUM(s.price * s.quantity) / NULLIF(COUNT(DISTINCT s.customer_id), 0) AS avg_customer_value,
    COUNT(DISTINCT s.customer_id) AS unique_customers
FROM dds.sales s
JOIN dds.products p ON s.product_id = p.product_id
GROUP BY month, p.category
WITH DATA;