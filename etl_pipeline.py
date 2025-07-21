# etl/etl_pipeline.py
import subprocess


def run_pipeline():
    print("Запуск ODS-загрузки...")
    subprocess.run(["python", "etl/load_ods.py"], check=True)

    print("Запуск DDS-трансформации...")
    subprocess.run(["python", "etl/transform_dds.py"], check=True)

    print("ETL-пайплайн успешно завершен!")


if __name__ == "__main__":
    run_pipeline()