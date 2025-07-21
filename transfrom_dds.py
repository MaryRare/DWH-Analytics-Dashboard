# etl/transform_dds.py
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()


def transform_to_dds():
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    with engine.connect() as conn:
        # 1. Загрузка продуктов
        conn.execute(text("""
            INSERT INTO dds.products (product_id, product_name, category)
            SELECT 
                DISTINCT product_id, 
                TRIM(product_name), 
                TRIM(category)
            FROM ods.raw_sales
            WHERE NOT is_processed
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category
        """))

        # 2. Загрузка продаж
        conn.execute(text("""
            INSERT INTO dds.sales (
                transaction_id, date, product_id, 
                price, quantity, customer_id, payment_method
            )
            SELECT 
                r.transaction_id,
                r.date::DATE,
                r.product_id,
                CAST(REGEXP_REPLACE(r.price, '[^0-9.]', '', 'g') AS NUMERIC),
                CAST(REGEXP_REPLACE(r.quantity, '[^0-9]', '', 'g') AS INT),
                NULLIF(r.customer_id, ''),
                NULLIF(r.payment_method, '')
            FROM ods.raw_sales r
            WHERE NOT r.is_processed
            ON CONFLICT (transaction_id) DO NOTHING
        """))

        # 3. Помечаем как обработанные
        conn.execute(text("""
            UPDATE ods.raw_sales 
            SET is_processed = TRUE 
            WHERE NOT is_processed
        """))

        # 4. Обновляем витрины
        conn.execute(text("REFRESH MATERIALIZED VIEW dma.sales_summary"))

    print("Трансформация в DDS завершена")


if __name__ == "__main__":
    transform_to_dds()