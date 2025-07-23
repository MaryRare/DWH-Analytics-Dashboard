CREATE TABLE dds.products (
	product_id text NOT NULL,
	category text NOT NULL,
	valid_from timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT products_pkey PRIMARY KEY (product_id)
);

/*
-- Таблица продуктов
CREATE TABLE dds.products (
    product_id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
*/

