import mysql.connector
import pandas as pd
from datetime import datetime

conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    user="admin",
    password="admin",
    database="retail_dwh_dev"
)
cursor = conn.cursor()

# =============================
# LOAD & CLEAN DATA
# =============================
df = pd.read_csv("../data/online_retail.csv", encoding="latin-1")

df = df[df['Quantity'] > 0]
df = df[df['UnitPrice'] > 0]
df = df.dropna(subset=['CustomerID', 'InvoiceDate'])
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
df = df[df['InvoiceDate'].notna()]
df['TotalAmount'] = df['Quantity'] * df['UnitPrice']

# =============================
# DIM PRODUCT (FIXED)
# =============================
product_map = {}
products = df[['Description']].drop_duplicates()

for _, row in products.iterrows():
    name = str(row['Description'])
    
    # Vérifier si existe déjà
    cursor.execute("SELECT product_id FROM dim_product WHERE product_name = %s", (name,))
    result = cursor.fetchone()
    
    if result:
        product_map[name] = result[0]
    else:
        cursor.execute("""
            INSERT INTO dim_product (product_name, category)
            VALUES (%s, %s)
        """, (name, "General"))
        product_map[name] = cursor.lastrowid

# =============================
# DIM CUSTOMER (FIXED)
# =============================
customer_map = {}
customers = df[['CustomerID', 'Country']].drop_duplicates()

for _, row in customers.iterrows():
    cid = int(row['CustomerID'])
    
    cursor.execute("SELECT customer_id FROM dim_customer WHERE customer_id = %s", (cid,))
    result = cursor.fetchone()
    
    if result:
        customer_map[cid] = cid
        # Mettre à jour le pays si nécessaire
        cursor.execute("UPDATE dim_customer SET country = %s WHERE customer_id = %s", 
                      (row['Country'], cid))
    else:
        cursor.execute("""
            INSERT INTO dim_customer (customer_id, country)
            VALUES (%s, %s)
        """, (cid, row['Country']))
        customer_map[cid] = cid

# =============================
# DIM DATE (FIXED)
# =============================
date_map = {}
dates = df['InvoiceDate'].dt.date.unique()

for d in dates:
    cursor.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (d,))
    result = cursor.fetchone()
    
    if result:
        date_map[d] = result[0]
    else:
        cursor.execute("""
            INSERT INTO dim_date (full_date, month, year, quarter, day_of_week)
            VALUES (%s, %s, %s, %s, %s)
        """, (d, d.month, d.year, (d.month-1)//3 + 1, d.weekday()))
        date_map[d] = cursor.lastrowid

# =============================
# DIM STORE (CORRECTED - utilise StockCode au lieu d'InvoiceNo)
# =============================
store_map = {}
stores = df[['StockCode']].drop_duplicates()

for _, row in stores.iterrows():
    stockcode = str(row['StockCode'])
    
    cursor.execute("SELECT store_id FROM dim_store WHERE stock_code = %s", (stockcode,))
    result = cursor.fetchone()
    
    if result:
        store_map[stockcode] = result[0]
    else:
        cursor.execute("""
            INSERT INTO dim_store (stock_code, description)
            VALUES (%s, %s)
        """, (stockcode, "Unknown"))
        store_map[stockcode] = cursor.lastrowid

# =============================
# FACT SALES (BATCH INSERT FOR PERFORMANCE)
# =============================
fact_data = []
skipped = 0

for _, row in df.iterrows():
    try:
        product_id = product_map.get(str(row['Description']))
        customer_id = int(row['CustomerID'])
        date_id = date_map[row['InvoiceDate'].date()]
        store_id = store_map.get(str(row['StockCode']))
        
        # Vérifier que tous les IDs existent
        if None in [product_id, customer_id, date_id, store_id]:
            skipped += 1
            continue
        
        fact_data.append((
            product_id, customer_id, date_id, store_id,
            int(row['Quantity']), float(row['UnitPrice']), float(row['TotalAmount']),
            str(row['InvoiceNo'])  # Ajout du numéro de facture
        ))
        
    except Exception as e:
        skipped += 1
        continue

# Insertion en batch
if fact_data:
    cursor.executemany("""
        INSERT INTO fact_sales 
        (product_id, customer_id, date_id, store_id, quantity, unit_price, total_amount, invoice_no)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, fact_data)
    inserted = len(fact_data)
else:
    inserted = 0

conn.commit()
cursor.close()
conn.close()

print(f"✅ ETL TERMINÉ - {inserted} lignes insérées, {skipped} ignorées")