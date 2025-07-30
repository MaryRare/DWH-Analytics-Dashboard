--Анализ омниканальных клиентов
-- Показывает % клиентов, покупающих и онлайн, и оффлайн
CREATE MATERIALIZED VIEW dma.channel_cross_analysis AS
WITH channel_flags AS (
    SELECT
        customer_id,
        BOOL_OR(payment_method IN ('online', 'card', 'digital')) AS is_online,
        BOOL_OR(payment_method NOT IN ('online', 'card', 'digital')) AS is_offline
    FROM dds.sales
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    CASE
        WHEN is_online AND is_offline THEN 'omnichannel'
        WHEN is_online THEN 'online-only'
        WHEN is_offline THEN 'offline-only'
        ELSE 'unknown'
    END AS customer_type,
    COUNT(*) AS customers_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM channel_flags), 2) AS percentage
FROM channel_flags
GROUP BY customer_type
WITH DATA;