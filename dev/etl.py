import mysql.connector
import pandas as pd
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================
# CONFIG DB
# =============================
DB_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'admin',
    'password': 'admin',
    'database': 'retail_dwh_dev'
}

# =============================
# CONNECTION MANAGEMENT
# =============================
def get_connection():
    """Create and return a database connection"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

# =============================
# VERIFICATION FUNCTIONS
# =============================
def verify_load():
    """Verify that all tables were loaded correctly"""
    conn = get_connection()
    cursor = conn.cursor()
    
    logger.info("\n" + "="*50)
    logger.info("📊 VERIFICATION DES CHARGEMENTS:")
    logger.info("="*50)
    
    tables = ['dim_date', 'dim_product', 'dim_customer', 'fact_sales']
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"  ✅ {table}: {count:,} rows")
        except Exception as e:
            logger.error(f"  ❌ {table}: error - {e}")
    
    # Show sample data
    logger.info("\n📊 SAMPLE DATA:")
    
    cursor.execute("SELECT * FROM dim_date LIMIT 3")
    logger.info(f"  dim_date sample: {cursor.fetchall()}")
    
    cursor.execute("SELECT product_id, stock_code, description FROM dim_product LIMIT 3")
    logger.info(f"  dim_product sample: {cursor.fetchall()}")
    
    cursor.execute("SELECT customer_id, country FROM dim_customer LIMIT 3")
    logger.info(f"  dim_customer sample: {cursor.fetchall()}")
    
    cursor.execute("SELECT COUNT(*), MIN(total_amount), MAX(total_amount), AVG(total_amount) FROM fact_sales")
    result = cursor.fetchone()
    if result and result[0] > 0:
        count, min_amt, max_amt, avg_amt = result
        logger.info(f"  fact_sales stats: {count:,} rows, Min: ${min_amt:.2f}, Max: ${max_amt:.2f}, Avg: ${avg_amt:.2f}")
    
    cursor.close()
    conn.close()

# =============================
# DIMENSION TABLES
# =============================
def load_dim_date(df):
    """Load the date dimension table"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Clear existing data
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        cursor.execute("TRUNCATE TABLE dim_date")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        
        # Process dates
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        df = df[df['InvoiceDate'].notna()]

        df['date_id'] = df['InvoiceDate'].dt.strftime('%Y%m%d').astype(int)

        dates = df[['date_id', 'InvoiceDate']].drop_duplicates()
        dates = dates.sort_values('date_id')

        # Use only the columns that exist in your table
        query = """
        INSERT INTO dim_date (date_id, full_date, day, month, year)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        full_date = VALUES(full_date),
        day = VALUES(day),
        month = VALUES(month),
        year = VALUES(year)
        """

        data = []
        for _, row in dates.iterrows():
            data.append((
                int(row['date_id']),
                row['InvoiceDate'].date(),
                row['InvoiceDate'].day,
                row['InvoiceDate'].month,
                row['InvoiceDate'].year
            ))

        cursor.executemany(query, data)
        conn.commit()
        logger.info(f"✅ dim_date chargée: {len(data):,} rows inserted/updated")
        
    except Exception as e:
        logger.error(f"❌ Error loading dim_date: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def load_dim_product(df):
    """Load the product dimension table"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Clear existing data
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        cursor.execute("TRUNCATE TABLE dim_product")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        
        # Clean data
        products = df[['StockCode', 'Description']].drop_duplicates()
        products = products[products['StockCode'].notna()]
        
        # Convert to string and clean description
        products['StockCode'] = products['StockCode'].astype(str).str.strip()
        products['Description'] = products['Description'].fillna('Unknown').astype(str).str.strip()
        
        # Remove empty stock codes
        products = products[products['StockCode'] != '']
        products = products[products['StockCode'] != 'nan']
        products = products[products['StockCode'] != 'None']

        query = """
        INSERT INTO dim_product (stock_code, description)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
        description = VALUES(description)
        """

        data = [
            (row['StockCode'], row['Description'][:255])
            for _, row in products.iterrows()
        ]

        cursor.executemany(query, data)
        conn.commit()
        logger.info(f"✅ dim_product chargée: {len(data):,} rows inserted/updated")
        
    except Exception as e:
        logger.error(f"❌ Error loading dim_product: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def load_dim_customer(df):
    """Load the customer dimension table"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Clear existing data
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        cursor.execute("TRUNCATE TABLE dim_customer")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        
        customers = df[['CustomerID', 'Country']].drop_duplicates()
        customers = customers[customers['CustomerID'].notna()]
        
        # Clean data
        customers['CustomerID'] = customers['CustomerID'].astype(int)
        customers['Country'] = customers['Country'].fillna('Unknown').astype(str).str.strip()

        query = """
        INSERT INTO dim_customer (customer_id, country)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
        country = VALUES(country)
        """

        data = [
            (int(row['CustomerID']), str(row['Country']))
            for _, row in customers.iterrows()
        ]

        # Insert in batches
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(query, batch)
            conn.commit()
        
        logger.info(f"✅ dim_customer chargée: {len(data):,} rows inserted/updated")
        
    except Exception as e:
        logger.error(f"❌ Error loading dim_customer: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# =============================
# FACT TABLE
# =============================
def load_fact_sales(df):
    """Load the fact sales table"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Clear existing data
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        cursor.execute("TRUNCATE TABLE fact_sales")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        
        # Clean and prepare data
        df = df.dropna(subset=['CustomerID', 'StockCode'])
        df = df[df['Quantity'] > 0]
        df = df[df['UnitPrice'] > 0]
        df = df[df['CustomerID'] > 0]

        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        df = df[df['InvoiceDate'].notna()]
        df['date_id'] = df['InvoiceDate'].dt.strftime('%Y%m%d').astype(int)

        df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
        df['StockCode'] = df['StockCode'].astype(str).str.strip()
        
        # Get mapping tables for faster lookup
        cursor.execute("SELECT product_id, stock_code FROM dim_product")
        product_map = {str(row[1]): row[0] for row in cursor.fetchall()}
        logger.info(f"  Found {len(product_map):,} products in dim_product")
        
        cursor.execute("SELECT customer_id FROM dim_customer")
        customer_ids = {row[0] for row in cursor.fetchall()}
        logger.info(f"  Found {len(customer_ids):,} customers in dim_customer")
        
        cursor.execute("SELECT date_id FROM dim_date")
        date_ids = {row[0] for row in cursor.fetchall()}
        logger.info(f"  Found {len(date_ids):,} dates in dim_date")

        # Insert fact records
        query = """
        INSERT INTO fact_sales (date_id, product_id, customer_id, quantity, total_amount)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        data = []
        skipped = 0
        missing_product = 0
        missing_customer = 0
        missing_date = 0
        
        total_rows = len(df)
        logger.info(f"  Processing {total_rows:,} rows...")
        
        for idx, (_, row) in enumerate(df.iterrows()):
            stock_code = str(row['StockCode'])
            product_id = product_map.get(stock_code)
            customer_id = int(row['CustomerID'])
            date_id = int(row['date_id'])
            
            # Only insert if all foreign keys exist
            if product_id is None:
                missing_product += 1
                skipped += 1
            elif customer_id not in customer_ids:
                missing_customer += 1
                skipped += 1
            elif date_id not in date_ids:
                missing_date += 1
                skipped += 1
            else:
                data.append((
                    date_id,
                    product_id,
                    customer_id,
                    int(row['Quantity']),
                    float(row['TotalAmount'])
                ))
            
            # Insert in batches
            if len(data) >= 5000:
                cursor.executemany(query, data)
                conn.commit()
                logger.info(f"  Inserted {len(data):,} records so far... ({idx+1:,}/{total_rows:,})")
                data = []
        
        # Insert remaining records
        if data:
            cursor.executemany(query, data)
            conn.commit()
            logger.info(f"  Inserted final batch of {len(data):,} records")
        
        logger.info(f"✅ fact_sales chargée: {total_rows - skipped:,} rows inserted")
        logger.info(f"  📊 Statistics:")
        logger.info(f"     - Total rows processed: {total_rows:,}")
        logger.info(f"     - Successfully inserted: {total_rows - skipped:,}")
        logger.info(f"     - Skipped (missing product): {missing_product:,}")
        logger.info(f"     - Skipped (missing customer): {missing_customer:,}")
        logger.info(f"     - Skipped (missing date): {missing_date:,}")
        logger.info(f"     - Total skipped: {skipped:,}")
        
    except Exception as e:
        logger.error(f"❌ Error loading fact_sales: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# =============================
# CREATE TABLES
# =============================
def create_tables():
    """Create all necessary tables if they don't exist"""
    conn = get_connection()
    cursor = conn.cursor()
    
    logger.info("🔨 Creating tables if not exists...")
    
    # Drop foreign keys if they exist
    try:
        cursor.execute("ALTER TABLE fact_sales DROP FOREIGN KEY fact_sales_ibfk_1")
        cursor.execute("ALTER TABLE fact_sales DROP FOREIGN KEY fact_sales_ibfk_2")
        cursor.execute("ALTER TABLE fact_sales DROP FOREIGN KEY fact_sales_ibfk_3")
    except:
        pass
    
    create_queries = [
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INT PRIMARY KEY,
            full_date DATE,
            day INT,
            month INT,
            year INT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(50) UNIQUE,
            description TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id INT PRIMARY KEY,
            country VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_store (
            store_id INT AUTO_INCREMENT PRIMARY KEY,
            store_name VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_sales (
            fact_id INT AUTO_INCREMENT PRIMARY KEY,
            date_id INT,
            product_id INT,
            customer_id INT,
            quantity INT,
            total_amount FLOAT,
            INDEX idx_date (date_id),
            INDEX idx_product (product_id),
            INDEX idx_customer (customer_id)
        )
        """
    ]
    
    for query in create_queries:
        try:
            cursor.execute(query)
            logger.info(f"  Created table: {query.split()[5] if 'CREATE TABLE' in query else 'table'}")
        except Exception as e:
            logger.warning(f"Could not execute query: {e}")
    
    # Add foreign keys
    try:
        cursor.execute("""
            ALTER TABLE fact_sales 
            ADD CONSTRAINT fk_fact_date 
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        """)
        cursor.execute("""
            ALTER TABLE fact_sales 
            ADD CONSTRAINT fk_fact_product 
            FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
        """)
        cursor.execute("""
            ALTER TABLE fact_sales 
            ADD CONSTRAINT fk_fact_customer 
            FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
        """)
        logger.info("  Foreign keys added successfully")
    except Exception as e:
        logger.warning(f"Foreign keys may already exist: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("✅ Tables created/verified")

# =============================
# VALIDATION AND QUALITY CHECKS
# =============================
def run_quality_checks(df):
    """Run data quality checks on the source data"""
    logger.info("\n📊 DATA QUALITY CHECKS:")
    logger.info("="*50)
    
    total_rows = len(df)
    logger.info(f"Total rows: {total_rows:,}")
    
    # Check for nulls
    null_counts = df.isnull().sum()
    for col, null_count in null_counts.items():
        if null_count > 0:
            logger.info(f"  ⚠️ {col}: {null_count:,} null values ({null_count/total_rows*100:.2f}%)")
    
    # Check for negative values
    negative_quantity = len(df[df['Quantity'] < 0])
    if negative_quantity > 0:
        logger.info(f"  ⚠️ Negative quantity: {negative_quantity:,} rows ({negative_quantity/total_rows*100:.2f}%)")
    
    negative_price = len(df[df['UnitPrice'] < 0])
    if negative_price > 0:
        logger.info(f"  ⚠️ Negative unit price: {negative_price:,} rows ({negative_price/total_rows*100:.2f}%)")
    
    # Check date range
    if 'InvoiceDate' in df.columns:
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        min_date = df['InvoiceDate'].min()
        max_date = df['InvoiceDate'].max()
        if pd.notna(min_date) and pd.notna(max_date):
            logger.info(f"  ✅ Date range: {min_date.date()} to {max_date.date()}")
    
    # Check unique customers
    unique_customers = df['CustomerID'].nunique()
    logger.info(f"  ✅ Unique customers: {unique_customers:,}")
    
    # Check unique products
    unique_products = df['StockCode'].nunique()
    logger.info(f"  ✅ Unique products: {unique_products:,}")
    
    logger.info("="*50)

# =============================
# MAIN ETL PIPELINE
# =============================
def main():
    """Main ETL pipeline"""
    # File path handling
    file_path = "../data/online_retail.csv"
    if not os.path.exists(file_path):
        file_path = "data/online_retail.csv"
    if not os.path.exists(file_path):
        file_path = "online_retail.csv"
    
    if not os.path.exists(file_path):
        logger.error(f"❌ CSV not found: {file_path}")
        logger.info("Please check the file path and make sure online_retail.csv exists")
        return

    logger.info(f"📂 Reading CSV from: {file_path}")
    
    try:
        # Read CSV with proper encoding
        df = pd.read_csv(file_path, encoding='latin-1')
        logger.info(f"📊 Loaded {len(df):,} rows from CSV")
    except Exception as e:
        logger.error(f"❌ Error reading CSV: {e}")
        return

    # Data cleaning
    initial_rows = len(df)
    df = df[df['Quantity'] > 0]
    df = df[df['UnitPrice'] > 0]
    df = df.dropna(subset=['InvoiceDate', 'CustomerID'])
    
    logger.info(f"🧹 Cleaned data: {initial_rows:,} -> {len(df):,} rows ({len(df)/initial_rows*100:.1f}% retained)")

    # Run quality checks
    run_quality_checks(df)
    
    # Create tables if needed
    create_tables()
    
    logger.info("\n🚀 Début ETL")
    logger.info("="*50)

    try:
        # Load dimensions
        load_dim_date(df)
        load_dim_product(df)
        load_dim_customer(df)
        
        # Load facts
        load_fact_sales(df)
        
        # Verify results
        verify_load()
        
        logger.info("="*50)
        logger.info("🎉 ETL terminé avec succès!")
        
    except Exception as e:
        logger.error(f"❌ ETL failed: {e}")
        raise

# =============================
# SCRIPT ENTRY POINT
# =============================
if __name__ == "__main__":
    start_time = datetime.now()
    logger.info(f"ETL started at: {start_time}")
    
    main()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"ETL completed in: {duration.total_seconds():.2f} seconds")