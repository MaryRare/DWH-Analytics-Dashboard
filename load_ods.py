# etl/load_ods.py
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from mappings import COLUMN_MAPPING

load_dotenv()

# РАСШИРЕННАЯ ВАЛИДАЦИЯ ЦЕНЫ (Price Per Unit)
def validate_prices(df):
    """Возвращает кортеж из:
    - good_data     - валидные данные
    - new_bad_data  - невалидные данные (не получилось посчитать price)
    - stats         - словарь со статистикой по обработке (для скольких записей удалось посчитать price)
    """

    # Конвертируем price в числовой формат, если это строка
    #if pd.api.types.is_string_dtype(df['price']):
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    stats = {
        'price_recalculated': 0,
        'price_filled_from_other': 0
    }

    # Шаг 1: Пробуем расчитать через total_spent/quantity (если поля есть)
    # Восстановление через total_spent/quantity
    if 'total_spent' in df.columns and 'quantity' in df.columns:
        recalc_mask = df['price'].isna() & df['quantity'].notna() & (df['quantity'] > 0)
        # сохранение кол-ва записей, для которых price рассчитали как total_spent/quantity
        stats['recalculated'] = recalc_mask.sum()
        df.loc[recalc_mask, 'price'] = df.loc[recalc_mask, 'total_spent'] / df.loc[recalc_mask, 'quantity']

    # Шаг 2: Заполняем из других транзакций на основе медианного значения (группировка по product_id)
    # т.е. узнаем цену товара из других транцзакций
    valid_prices = df[df['price'].notna() & df['product_id'].notna()]
    if not valid_prices.empty:
        price_map = valid_prices.groupby('product_id')['price'].median()
        fill_mask = df['price'].isna() & df['product_id'].isin(price_map.index)
        # сохранение кол-ва записей, для которых price узнали из других транзакций
        stats['filled_from_other'] = fill_mask.sum()
        df.loc[fill_mask, 'price'] = df.loc[fill_mask, 'product_id'].map(price_map)

    # Шаг 3: Разделяем на валидные/невалидные, передаем статистику
    # valid_mask = df['price'].notna()
    # return df[valid_mask], df[~valid_mask], stats

    return df, stats

# ЗАГРУЗКА ДАННЫХ В БД
def load_to_ods():

    # Подключение к БД
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    # Чтение CSV
    df = pd.read_csv('data/dirty_retail.csv',
                     na_values=['NULL', 'N/A', '', 'NaN', 'nan', 'None']).rename(columns=COLUMN_MAPPING)
    print("Датасет считан")

    # Проверка, что все нужные колонки присутствуют, соблюдение маппинга для корректного нейминга
    required_columns = set(COLUMN_MAPPING.values())
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Отсутствуют колонки: {missing_columns}")

#  первоначальный подход с отсеиванием записей с product_id=null
#     # Разделение на валидные/невалидные записи
#     # Базовый фильтр данных по product_id!=null
#     error_mask = df['product_id'].notna() #& df['price'].notna()
#     good_data = df[error_mask].copy()
#     bad_data = df[~error_mask].copy()
#
#     # Сохранение ошибок (записи с product_id=null) в файл data/load_errors.csv
#     if not bad_data.empty:
#         bad_data.to_csv('data/load_errors.csv', mode='a', header=not os.path.exists('data/load_errors.csv'),
#                         index=False)
#         print("Плохие записи см. data/load_errors.csv")
#
#     # Валидация price через вызов validate_prices()
#     if not good_data.empty:
#         """
#         good_data  = validate_prices(good_data)[0]
#         new_bad_data= validate_prices(good_data)[1]
#         bad_data = pd.concat([bad_data, new_bad_data])
#         # Получаем данные о кол-ве перезаписанных записей
#         price_stats = validate_prices(good_data)[2]
#         """
#         good_data, new_bad_data, price_stats = validate_prices(good_data)
#         bad_data = pd.concat([bad_data, new_bad_data])
#
#     первоначальный подход с отсеиванием записей с product_id=null
#         # Вставка только валидных данных с load_id в таблицу ods.raw_sales
#         if not good_data.empty:
#             with engine.begin() as conn:
#                 # Сброс sequence в БД при каждой загрузке для обновления load_id (только для отладки, закомментить при ОПЭ или дорабать механизм инкрементальной загрузки)
#                 conn.execute(text("""
#                             TRUNCATE ods.raw_sales RESTART IDENTITY;
#                             ALTER SEQUENCE ods.raw_sales_load_id_seq RESTART WITH 1;
#                         """))
#                 # Получаем следующий load_id
#                 next_id = conn.execute(text("SELECT nextval('ods.raw_sales_load_id_seq')")).scalar()
#
#                 # Генерируем load_id
#                 good_data['load_id'] = range(next_id, next_id + len(good_data))
#
#         # Загрузка в ODS
#         good_data.to_sql(
#             'raw_sales',
#             engine,
#             schema='ods',
#             if_exists='append',
#             index=False,
#             #method='multi'  # Ускорение массовой вставки
#         )
#
    # Валидация цен
    df, price_stats = validate_prices(df)

    # Загрузка в ODS (все записи, включая NULL product_id)
    with engine.begin() as conn:
        # Очистка ODS (надо для отладки)
        conn.execute(text("""
                                TRUNCATE ods.raw_sales RESTART IDENTITY;
                                ALTER SEQUENCE ods.raw_sales_load_id_seq RESTART WITH 1;
                            """))
        # Получаем следующий load_id (для отладки, чтобы нумерация при начиналась с 1)
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

    # Формирование полного отчёта по перезаписанным/плохим записям
    validation_report = {
        "total_records": len(df),
        #    "records_with_null_product_id": len(bad_data),
        "records_with_null_price_in_raw": df['price'].isna().sum(),
        #   "records_with_null_price_in_valid": good_data['price'].isna().sum(),
        "records_with_null_price": (df['price'].isna() & df['product_id'].notna()).sum(),
        "price_recalculated_from_total": price_stats['price_recalculated'],
        "price_filled_from_other_transactions": price_stats['price_filled_from_other'],
        #   "final_valid_records": len(good_data),
        #  "final_invalid_records": len(bad_data)
    }

    # Вывод отчёта
    print("\n=== VALIDATION REPORT ===")
    for key, value in validation_report.items():
        print(f"{key.replace('_', ' ').title()}: {value}")


    print("Данные успешно загружены в ODS")

if __name__ == "__main__":
    load_to_ods()