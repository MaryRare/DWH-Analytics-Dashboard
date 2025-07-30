-- Детализация по клиентам
-- Сегментирует клиентов по активности и категориям покупок, помогает выявить VIP-клиентов и "спящих"
CREATE MATERIALIZED VIEW dma.customer_activity AS
SELECT
    s.customer_id,
    p.category,
    DATE_TRUNC('month', s.date) AS month,
    SUM(s.price * s.quantity) AS total_spent,
    COUNT(DISTINCT s.transaction_id) AS transactions_count
FROM dds.sales s
JOIN dds.products p ON s.product_id = p.product_id
WHERE s.customer_id IS NOT NULL
GROUP BY s.customer_id, p.category, month
WITH DATA;