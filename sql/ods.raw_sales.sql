CREATE TABLE ods.raw_sales (
	load_id serial4 NOT NULL,
	transaction_id text NULL,
	date text NULL,
	product_id text NULL,
	category text NULL,
	price text NULL,
	quantity text NULL,
	customer_id text NULL,
	payment_method text NULL,
	load_time timestamp DEFAULT now() NULL,
	is_processed bool DEFAULT false NULL,
	location text NULL,
	total_spent text NULL,
	discount text NULL,
	CONSTRAINT raw_sales_pkey PRIMARY KEY (load_id)
);

/*
CREATE TABLE ods.raw_sales (
    load_id SERIAL PRIMARY KEY,
    transaction_id TEXT,
    date TEXT,
    product_id TEXT,
    category TEXT,
    product_name TEXT,
    price TEXT,
    quantity TEXT,
    customer_id TEXT,
    payment_method TEXT,
    load_time TIMESTAMP DEFAULT NOW(),
    is_processed BOOLEAN DEFAULT FALSE  -- Флаг для инкрементальной загрузки
);
*/