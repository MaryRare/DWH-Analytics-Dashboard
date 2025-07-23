# etl/load_ods.py
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from mappings import COLUMN_MAPPING
from pathlib import Path

load_dotenv()

# РАСШИРЕННАЯ ВАЛИДАЦИЯ ЦЕНЫ (Price Per Unit)
def validate_prices(df):
    """Возвращает кортеж из:
    - df - датафрейм с исправленными ценами
    - stats - статистика по исправлениям
    """

    os.makedirs('../data/logs', exist_ok=True)

    # Конвертируем price в числовой формат
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    stats = {
        'price_recalculated': 0,
        'price_filled_from_other': 0
    }

    # Способ 1: Восстановление через total_spent/quantity
    if 'total_spent' in df.columns and 'quantity' in df.columns:
        recalc_mask = (
                df['price'].isna() &
                df['total_spent'].notna() &
                df['quantity'].notna() &
                (df['quantity'] > 0)
        )

        if recalc_mask.any():
            # Логирование ДО исправления
            df[recalc_mask].to_csv('../data/logs/to_be_repaired_via_ratio.csv', index=False)

            # Вычисление и применение новых цен
            new_prices = df.loc[recalc_mask, 'total_spent'] / df.loc[recalc_mask, 'quantity']
            df.loc[recalc_mask, 'price'] = new_prices
            stats['price_recalculated'] = recalc_mask.sum()

            # Логирование ПОСЛЕ исправления
            repaired = df[recalc_mask].copy()
            repaired.to_csv('../data/logs/repaired_via_ratio.csv', index=False)

    # Способ 2: Заполнение из других транзакций (группировка по product_id для записей, где невозможно посчитать первым способом)
    valid_prices = df[df['price'].notna() & df['product_id'].notna()]
    if not valid_prices.empty:
        price_map = valid_prices.groupby('product_id')['price'].median()
        fill_mask = df['price'].isna() & df['product_id'].isin(price_map.index)

        if fill_mask.any():
            # Логирование для метода заполнения из других транзакций
            df[fill_mask].to_csv('../data/logs/to_be_repaired_via_grouping.csv', index=False)

            df.loc[fill_mask, 'price'] = df.loc[fill_mask, 'product_id'].map(price_map)
            stats['price_filled_from_other'] = fill_mask.sum()

            repaired_group = df[fill_mask].copy()
            repaired_group.to_csv('../data/logs/repaired_via_grouping.csv', index=False)

    # Логирование оставшихся проблем
    remaining_null = df[df['price'].isna()]
    if not remaining_null.empty:
        remaining_null.to_csv('../data/logs/remaining_unsolved_null_prices.csv', index=False)
        print(f"Осталось неисправленных записей: {len(remaining_null)}")

    return df, stats

# ЗАГРУЗКА ДАННЫХ В БД
def load_to_ods():

    # # Вычисление абсолютного пути к файлу .env (подстраховка)
    # env_path = Path(__file__).resolve().parent.parent / '.env'
    # load_dotenv(env_path)

    # Подключение к БД
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    # Чтение CSV
    df = pd.read_csv('../data/dirty_retail.csv',
                     na_values=['NULL', 'N/A', '', 'NaN', 'nan', 'None']).rename(columns=COLUMN_MAPPING)
    print("Датасет считан")

    # Проверка, что все нужные колонки присутствуют, соблюдение маппинга для корректного нейминга
    required_columns = set(COLUMN_MAPPING.values())
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Отсутствуют колонки: {missing_columns}")

    # Валидация цен
    df, price_stats = validate_prices(df)

    # Загрузка в ODS (все записи, включая NULL product_id)
    with engine.begin() as conn:
        # Очистка старых записей ODS и сброс SEQUENCE для корректного load_id (надо для отладки)
        conn.execute(text("""
                                TRUNCATE ods.raw_sales RESTART IDENTITY;
                                ALTER SEQUENCE ods.raw_sales_load_id_seq RESTART WITH 1;
                            """))
        # Получаем следующий load_id (можно доработать инкрементальную загрузку)
        next_id = conn.execute(text("SELECT nextval('ods.raw_sales_load_id_seq')")).scalar()
        # Генерируем load_id
        df['load_id'] = range(next_id, next_id + len(df))

        df.to_sql(
            'raw_sales',
            conn,
            schema='ods',
            if_exists='append',
            index=False,
        )

    # Формирование полного отчёта по перезаписанным записям (для которых заполняли price)
    validation_report = {
        "total_records": len(df),
        "price_recalculated_from_total": price_stats['price_recalculated'],
        "price_filled_from_other_transactions": price_stats['price_filled_from_other'],
    }
    print("\n=== VALIDATION REPORT ===")
    for key, value in validation_report.items():
        print(f"{key.replace('_', ' ').title()}: {value}")

    print("\nДанные успешно загружены в ODS")

if __name__ == "__main__":
    load_to_ods()