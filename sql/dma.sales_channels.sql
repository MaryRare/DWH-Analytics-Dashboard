-- Сравнение каналов продаж
-- Cравнивает эффективность онлайн- и оффлайн-продаж (payment_method как прокси для канала)
CREATE MATERIALIZED VIEW dma.sales_channels AS
SELECT
    CASE
        WHEN s.payment_method IN ('online', 'card', 'digital') THEN 'online'
        ELSE 'in-store'
    END AS sales_channel,
    p.category,
    DATE_TRUNC('month', s.date) AS month,
    COUNT(DISTINCT s.transaction_id) AS transactions,
    SUM(s.price * s.quantity) AS revenue,
    COUNT(DISTINCT s.customer_id) AS unique_customers
FROM dds.sales s
JOIN dds.products p ON s.product_id = p.product_id
GROUP BY sales_channel, p.category, month
WITH DATA;