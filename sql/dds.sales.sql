CREATE TABLE dds.sales (
	sale_id bigserial NOT NULL,
	transaction_id text NOT NULL,
	date date NOT NULL,
	product_id text NULL,
	price numeric(10, 2) NULL,
	quantity int4 NULL,
	customer_id text NULL,
	payment_method text NULL,
	CONSTRAINT sales_pkey PRIMARY KEY (sale_id),
	CONSTRAINT sales_price_check CHECK ((price > (0)::numeric)),
	CONSTRAINT sales_quantity_check CHECK ((quantity > 0)),
	CONSTRAINT sales_transaction_id_key UNIQUE (transaction_id)
);

/*
-- Таблица продаж
CREATE TABLE dds.sales (
    sale_id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT UNIQUE NOT NULL,
    date DATE NOT NULL,
    product_id TEXT REFERENCES dds.products(product_id),
    price DECIMAL(10,2) CHECK (price > 0),
    quantity INT CHECK (quantity > 0),
    customer_id TEXT,
    payment_method TEXT
);
*/


