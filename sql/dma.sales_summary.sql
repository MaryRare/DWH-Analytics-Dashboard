CREATE MATERIALIZED VIEW dma.sales_summary
TABLESPACE pg_default
AS SELECT p.category,
   date_trunc('month'::text, s.date::timestamp with time zone) AS month,
   sum(s.price * s.quantity::numeric) AS revenue,
   count(*) AS transactions
  FROM dds.sales s
    JOIN dds.products p ON s.product_id = p.product_id
 GROUP BY p.category, (date_trunc('month'::text, s.date::timestamp with time zone))
WITH DATA;

/*
CREATE MATERIALIZED VIEW dma.sales_summary AS
SELECT 
    p.category,
    DATE_TRUNC('month', s.date) AS month,
    SUM(s.price * s.quantity) AS revenue,
    COUNT(*) AS transactions
FROM dds.sales s
JOIN dds.products p ON s.product_id = p.product_id
GROUP BY p.category, month;
*/

