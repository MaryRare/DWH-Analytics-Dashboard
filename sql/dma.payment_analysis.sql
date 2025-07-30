-- Анализ способов оплаты
-- Выявляет популярные платежные методы
CREATE MATERIALIZED VIEW dma.payment_analysis AS
SELECT
    payment_method,
    DATE_TRUNC('month', date) AS month,
    COUNT(*) AS transactions,
    SUM(price * quantity) AS revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM dds.sales
WHERE payment_method IS NOT NULL
GROUP BY payment_method, month
WITH DATA;