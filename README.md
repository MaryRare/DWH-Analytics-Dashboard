# **Retail Analytics Dashboard with DWH-pipeline**  
## **End-to-end решение для анализа продаж: от сырых данных до дашборда с полным циклом документации.**
**Stack: PostgreSQL, Python (pandas, sqlalchemy), Power BI**

***
📌 **Дисклеймер**

Данный проект является учебным и создан в демонстрационных целях для развития hard skills в областях:  
🔹 Проектирование DWH  
🔹 ETL-разработка (упрощенная, на Python)  
🔹 Бизнес-аналитика данных  
🔹 Визуализация в Power BI

Ключевые особенности:  
🗂️ Синтетические данные, [датасет по продажам взят с Kaggle](https://www.kaggle.com/datasets/ahmedmohamed2003/retail-store-sales-dirty-for-data-cleaning)   
🤓 Упрощенная архитектура  
📝 Документация имитирует реальные бизнес-требования

***
**Цель проекта:** Разработать дашборд для анализа ключевых метрик по продажам.

**Решаемые задачи:**   
📝 Оформить бизнес-требования (BRD, FSD и пр.)  
🔄 Разработать ETL-процесс на Python для трансформации сырых данных о продажах из csv-файла в бизнес-витрины  
🛢️ Реализовать трехслойную DWH-архитектуру в PostgreSQL базе данных (ODS, DDS, DMA)  
📊 Визуализировать ключевые показатели по продажам в дашборде в Power BI  
📖 Оформить документацию по проекту (диаграммы, инструкции)

**Data Flow: raw sales data (csv) → PostgreSQL DWH (ODS/DDS/DMA) → Power BI dashboard** 

***
## **Структура и содержание репозитория**
```python
├── data/  
│   ├──  dirty_retail.csv               # Исходные данные  
│   └── load.errors.csv                 # Ошибочные записи, которые не получилось записать в ODS   
├── sql/  
│   ├── ods.raw_sales.sql               # Сырые данные с минимальной обработкой  
│   ├── dds.products.sql                # Таблица продуктов (ядро)  
│   ├── dds.sales.sql                   # Таблица продаж (ядро)  
│   ├── dma.sales_summary.sql           # Основные метрики продаж по категориям и месяцам   
│   ├── dma.avg_check.sql               # Средний чек и ценность клиента  
│   ├── dma.customer_metrics.sql        # Лояльность клиентов  
│   ├── dma.customer_activity.sql       # Детализация покупок по клиентам для сегментации  
│   ├── dma.channel_cross_analysis.sql  # Сравнение онлайн/оффлайн-каналов   
│   └── dma.payment_analysis.sql        # Способы оплаты и их связь со скидками  
├── etl/  
│   ├── load_ods.py                     # Загрузка данных в ODS из csv-файла  
│   ├── transform_dds.py                # Перекладка данных из ODS в DDS и DMA, создание витрин   
│   ├── etl_pipeline.py                 # Автоматизация загрузки через bash, т.к. нет Airflow  
│   └── mappings.py                     # Маппинги колонок для csv-ODS   
├── bi/  
│   └── retail_dashboard.pbix           # Дашборд  
├── docs/   
│   ├── diagrams/                       # Диаграммы и схемы   
│   ├── guides/                         # Инструкции
│   │  └── user_guide.md                 
│   ├── requirements/                                 
│   │  ├── brd.md                       # Business Requirements Document (Бизнес-требования)
│   │  └── use_cases.md  
│   ├── technical/                      
│   │  └── fsd.md                       # Functional Specifications Document (Функциональные требования)
└── README.md  
```


