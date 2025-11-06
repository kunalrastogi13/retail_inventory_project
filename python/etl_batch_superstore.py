import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import psycopg2
import os

# ============================================
#  Database Connection (Postgres in Docker)
# ============================================
engine = create_engine("postgresql://admin:admin123@postgres:5432/retail_db")
conn = engine.raw_connection()
cursor = conn.cursor()

print("🚀 Starting batch ETL...")

# ============================================
# 1️⃣ Load CSV and Normalize Columns
# ============================================
csv_path = "/app/data/global_superstore.csv"
print(f"📂 Loading CSV from: {csv_path}")

df = pd.read_csv(csv_path, encoding="latin1")

# Normalize column names
df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
print("✅ Normalized columns:", list(df.columns))

# ============================================
# 2️⃣ Load into Staging Schema
# ============================================
df.to_sql(
    "superstore_orders_raw",
    engine,
    schema="staging",
    if_exists="replace",
    index=False
)
print("📥 Loaded → staging.superstore_orders_raw")

# ============================================
# 3️⃣ Load Customer Dimension
# ============================================
cursor.execute("""
INSERT INTO retail.dim_customer
(customer_key, customer_name, segment, country, region, city, state, postal_code)
SELECT DISTINCT
    s.customer_id,
    s.customer_name,
    s.segment,
    s.country,
    s.region,
    s.city,
    s.state,
    s.postal_code
FROM staging.superstore_orders_raw AS s
ON CONFLICT (customer_key) DO NOTHING;
""")
print("👤 Customer dimension loaded → retail.dim_customer")

# ============================================
# 4️⃣ Load Product Dimension (updated)
# ============================================
cursor.execute("""
INSERT INTO retail.dim_product (product_key, product_id, product_name, category, sub_category)
SELECT DISTINCT
    s.product_id,      -- surrogate key (for internal use)
    s.product_id,      -- natural key from CSV
    s.product_name,
    s.category,
    s.sub_category
FROM staging.superstore_orders_raw AS s
ON CONFLICT (product_key) DO NOTHING;
""")
print("📦 Product dimension loaded → retail.dim_product")

# ============================================
# 5️⃣ Load Fact Table — Sales (updated joins)
# ============================================
cursor.execute("""
INSERT INTO retail.fact_sales (
    order_id, product_id, customer_id, order_date, ship_date,
    sales, quantity, discount, profit
)
SELECT 
    s.order_id,
    p.product_key,         -- FK from dim_product
    c.customer_key,        -- FK from dim_customer
    s.order_date::date,
    s.ship_date::date,
    s.sales::numeric,
    s.quantity::int,
    s.discount::numeric,
    s.profit::numeric
FROM staging.superstore_orders_raw s
JOIN retail.dim_product p ON p.product_id = s.product_id
JOIN retail.dim_customer c ON c.customer_key = s.customer_id
ON CONFLICT DO NOTHING;
""")
print("💰 Sales fact table loaded → retail.fact_sales")

# ============================================
# 6️⃣ Log ETL Run
# ============================================
cursor.execute("""
INSERT INTO retail.etl_run_log (phase, records_processed, status)
VALUES ('batch_load', %s, 'SUCCESS')
""", (len(df),))

conn.commit()
cursor.close()

print("✅ ETL completed & logged successfully!")