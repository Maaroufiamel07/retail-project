# test_etl.py - Pour tester avec seulement 1000 lignes
import pandas as pd
import mysql.connector

# Lire seulement les 1000 premières lignes
df_test = pd.read_csv('../data/online_retail.csv', nrows=1000, encoding='latin-1')
print(f"Test avec {len(df_test)} lignes")

# Connexion
conn = mysql.connector.connect(
    host='localhost',
    port=3307,
    user='admin',
    password='admin',
    database='retail_dwh_dev'
)

cursor = conn.cursor()

# Insérer
for _, row in df_test.iterrows():
    try:
        cursor.execute("""
            INSERT INTO fact_sales (InvoiceNo, StockCode, Quantity, UnitPrice, CustomerID, Country)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            str(row.get('InvoiceNo', ''))[:20],
            str(row.get('StockCode', ''))[:20],
            int(row.get('Quantity', 0)),
            float(row.get('UnitPrice', 0)),
            int(row.get('CustomerID', 0)) if pd.notna(row.get('CustomerID')) else 0,
            str(row.get('Country', ''))[:50]
        ))
    except Exception as e:
        print(f"Erreur: {e}")
        continue

conn.commit()
cursor.execute("SELECT COUNT(*) FROM fact_sales")
print(f"Total dans la table: {cursor.fetchone()[0]}")

conn.close()