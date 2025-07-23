# etl/transform_dds.py
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

def transform_to_dds():

    # # Вычисление абсолютного пути к файлу .env (подстраховка)
    # env_path = Path(__file__).resolve().parent.parent / '.env'
    # load_dotenv(env_path)

    # Подключение к БД
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    with engine.connect() as conn:
        # Шаг 1: Загрузка продуктов (таблица dds.products)
        result_products = conn.execute(text("""
        TRUNCATE dds.products CASCADE;
        INSERT INTO dds.products (product_id, category)
        WITH prepared_data AS (
            SELECT
                product_id,
                TRIM(category) AS category
            FROM ods.raw_sales
            WHERE
                NOT is_processed
                AND product_id IS NOT NULL
                AND category IS NOT NULL
            GROUP BY product_id, TRIM(category)  -- Избегаем дубликатов
        )
        SELECT
            product_id,
            category
        FROM prepared_data
        ON CONFLICT (product_id) DO UPDATE SET
            category = EXCLUDED.category,
            -- Обновления происходят только при реальных изменениях
            valid_from = CASE
                            WHEN dds.products.category IS DISTINCT FROM EXCLUDED.category
                            THEN CURRENT_DATE
                            ELSE dds.products.valid_from
                         END;
        """))
        print(f"Вставлено/обновлено в dds.products: {result_products.rowcount}")

        # Шаг 2: Загрузка продаж (таблица dds.sales)
        result_sales = conn.execute(text("""
            TRUNCATE dds.sales RESTART IDENTITY;
            ALTER SEQUENCE dds.sales_sale_id_seq RESTART WITH 1;
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
        print(f"Вставлено/обновлено в dds.sales: {result_sales.rowcount}")


        # Шаг 3: Помечаем как обработанные
        conn.execute(text("""
            UPDATE ods.raw_sales
            SET is_processed = TRUE
            WHERE NOT is_processed
        """))

        # Шаг 4: Обновляем витрины
        conn.execute(text("REFRESH MATERIALIZED VIEW dma.sales_summary"))
        conn.commit()
    print("Трансформация в DDS завершена")


if __name__ == "__main__":
    transform_to_dds()