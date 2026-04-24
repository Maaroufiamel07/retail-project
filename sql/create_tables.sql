-- ==========================================
-- RETAIL DATA WAREHOUSE SCHEMA
-- ==========================================

-- Drop existing tables (start fresh)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_store;

-- ==========================================
-- DIMENSION TABLES
-- ==========================================

-- Date dimension
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    year INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    quarter INT,
    INDEX idx_year_month (year, month),
    INDEX idx_full_date (full_date)
);

-- Product dimension
CREATE TABLE dim_product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(50) UNIQUE,
    description TEXT,
    category VARCHAR(50),
    INDEX idx_stock_code (stock_code),
    INDEX idx_category (category)
);

-- Customer dimension
CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    country VARCHAR(100),
    segment VARCHAR(50),
    INDEX idx_country (country),
    INDEX idx_segment (segment)
);

-- Store dimension (for future expansion)
CREATE TABLE dim_store (
    store_id INT AUTO_INCREMENT PRIMARY KEY,
    store_name VARCHAR(100),
    location VARCHAR(100),
    INDEX idx_store_name (store_name)
);

-- ==========================================
-- FACT TABLE
-- ==========================================

-- Sales fact table
CREATE TABLE fact_sales (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    date_id INT NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price FLOAT NOT NULL,
    total_amount FLOAT NOT NULL,
    -- Foreign keys
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    -- Indexes for performance
    INDEX idx_date (date_id),
    INDEX idx_product (product_id),
    INDEX idx_customer (customer_id),
    INDEX idx_total_amount (total_amount),
    INDEX idx_quantity (quantity)
);

-- ==========================================
-- AGGREGATION TABLES (Optional)
-- ==========================================

-- Daily sales summary
CREATE TABLE IF NOT EXISTS daily_sales_summary AS
SELECT 
    d.date_id,
    d.full_date,
    d.year,
    d.month,
    d.day,
    COUNT(DISTINCT f.customer_id) as unique_customers,
    COUNT(*) as transaction_count,
    SUM(f.quantity) as total_units_sold,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_transaction_value
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.date_id, d.full_date, d.year, d.month, d.day;

-- Monthly product performance
CREATE TABLE IF NOT EXISTS monthly_product_performance AS
SELECT 
    d.year,
    d.month,
    p.product_id,
    p.stock_code,
    p.description,
    p.category,
    COUNT(*) as times_ordered,
    SUM(f.quantity) as total_quantity,
    SUM(f.total_amount) as total_revenue,
    AVG(f.unit_price) as avg_price
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY d.year, d.month, p.product_id;

-- Customer lifetime value
CREATE TABLE IF NOT EXISTS customer_lifetime_value AS
SELECT 
    c.customer_id,
    c.country,
    c.segment,
    COUNT(*) as total_transactions,
    SUM(f.quantity) as total_items_purchased,
    SUM(f.total_amount) as total_spent,
    AVG(f.total_amount) as avg_order_value,
    MIN(d.full_date) as first_purchase_date,
    MAX(d.full_date) as last_purchase_date,
    DATEDIFF(MAX(d.full_date), MIN(d.full_date)) as customer_lifetime_days
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY c.customer_id;

-- ==========================================
-- STORED PROCEDURES
-- ==========================================

DELIMITER //

-- Get sales by date range
CREATE PROCEDURE GetSalesByDateRange(
    IN start_date DATE,
    IN end_date DATE
)
BEGIN
    SELECT 
        d.full_date,
        COUNT(*) as transaction_count,
        SUM(f.total_amount) as daily_revenue,
        COUNT(DISTINCT f.customer_id) as unique_customers
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.full_date BETWEEN start_date AND end_date
    GROUP BY d.full_date
    ORDER BY d.full_date;
END //

-- Get top products by revenue
CREATE PROCEDURE GetTopProducts(
    IN top_n INT
)
BEGIN
    SELECT 
        p.stock_code,
        p.description,
        p.category,
        COUNT(*) as times_ordered,
        SUM(f.quantity) as total_quantity,
        SUM(f.total_amount) as total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_id
    ORDER BY total_revenue DESC
    LIMIT top_n;
END //

-- Get customer segmentation analysis
CREATE PROCEDURE GetCustomerSegmentation()
BEGIN
    SELECT 
        segment,
        COUNT(*) as customer_count,
        AVG(total_spent) as avg_lifetime_value,
        SUM(total_spent) as total_revenue,
        AVG(avg_order_value) as avg_order_value
    FROM customer_lifetime_value
    GROUP BY segment
    ORDER BY total_revenue DESC;
END //

DELIMITER ;

-- ==========================================
-- VIEWS FOR REPORTING
-- ==========================================

-- Monthly KPI dashboard view
CREATE OR REPLACE VIEW v_monthly_kpi AS
SELECT 
    d.year,
    d.month,
    COUNT(DISTINCT f.customer_id) as active_customers,
    COUNT(*) as total_transactions,
    SUM(f.quantity) as units_sold,
    SUM(f.total_amount) as revenue,
    AVG(f.total_amount) as avg_transaction_value,
    SUM(f.total_amount) / COUNT(DISTINCT f.customer_id) as revenue_per_customer
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year DESC, d.month DESC;

-- Country performance view
CREATE OR REPLACE VIEW v_country_performance AS
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_id) as customers,
    COUNT(*) as transactions,
    SUM(f.quantity) as units_sold,
    SUM(f.total_amount) as revenue,
    AVG(f.total_amount) as avg_transaction_value,
    SUM(f.total_amount) / COUNT(DISTINCT c.customer_id) as revenue_per_customer
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY revenue DESC;

-- Product category performance view
CREATE OR REPLACE VIEW v_category_performance AS
SELECT 
    p.category,
    COUNT(DISTINCT p.product_id) as products,
    COUNT(*) as transactions,
    SUM(f.quantity) as units_sold,
    SUM(f.total_amount) as revenue,
    AVG(f.total_amount) as avg_transaction_value
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;
CREATE TABLE etl_metadata (
    last_load_date DATETIME
);