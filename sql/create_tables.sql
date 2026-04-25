-- DIM_PRODUCT (correct)
CREATE TABLE dim_product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    UNIQUE KEY uk_product_name (product_name)
);

-- DIM_CUSTOMER (correct - customer_id est la clé naturelle)
CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,  -- Pas d'auto-incrément, c'est l'ID réel
    country VARCHAR(100)
);

-- DIM_DATE (améliorée)
CREATE TABLE dim_date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    month INT,
    quarter INT,
    day_of_week INT,
    day_name VARCHAR(10)
);

-- DIM_STORE (corrigée - basée sur StockCode)
CREATE TABLE dim_store (
    store_id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) UNIQUE,
    description VARCHAR(255)
);

-- FACT_SALES (corrigée - avec invoice_no)
CREATE TABLE fact_sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    date_id INT NOT NULL,
    store_id INT NOT NULL,
    invoice_no VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10,2),  -- DECIMAL vs FLOAT pour précision
    total_amount DECIMAL(10,2),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    INDEX idx_invoice (invoice_no)
);