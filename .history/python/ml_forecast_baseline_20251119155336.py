import os
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import create_engine

# ============================================
# 1. Connect to Postgres (same style as ETL)
# ============================================
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DB_URL)

print("🚀 Starting baseline ML forecast (7-day moving average)...")

# ============================================
# 2. Load historical sales (last 90 days)
# ============================================
sql = """
SELECT
    product_id,
    order_date::date AS order_date,
    SUM(quantity) AS daily_qty      -- 👈 this matches what Python expects
FROM retail.fact_sales
GROUP BY product_id, order_date
ORDER BY product_id, order_date;
"""

df = pd.read_sql(sql, engine)

if df.empty:
    print("⚠️ No sales data found in fact_sales. Exiting.")
    raise SystemExit

print(f"✅ Loaded {len(df)} sales rows for forecasting")

# ============================================
# 3. Prepare data per product & compute 7-day MA
# ============================================
today = date.today()
horizon_days = 7
model_version = "v1_mavg7"
source = "ml_model"

forecast_rows = []

# group by product
for product_id, g in df.groupby("product_id"):
    # ensure continuous daily index with zero-fill for missing days
    g = g.copy()
    g["order_date"] = pd.to_datetime(g["order_date"])
    g = g.set_index("order_date").asfreq("D", fill_value=0)

    # if too little data, skip
    if len(g) < 7:
        continue

    # 7-day moving average of quantity
    g["ma7"] = g["daily_qty"].rolling(window=7).mean()

    # drop initial NaNs
    g = g.dropna(subset=["ma7"])
    if g.empty:
        continue

    last_ma = float(g["ma7"].iloc[-1])
    # safety check
    if last_ma < 0:
        last_ma = 0.0

    # create 7-day-ahead forecasts
    for d in range(1, horizon_days + 1):
        forecast_date = today + timedelta(days=d)
        forecast_rows.append(
            (
                product_id,
                forecast_date,
                horizon_days,
                round(last_ma, 2),
                model_version,
                source,
            )
        )

print(f"📈 Generated {len(forecast_rows)} forecast rows")

if not forecast_rows:
    print("⚠️ No forecast rows generated (maybe not enough history). Exiting.")
    raise SystemExit

# ============================================
# 4. Write into retail.fact_forecast
#    (Clear existing future forecasts for this model)
# ============================================
import psycopg2

conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    port=POSTGRES_PORT,
)
cur = conn.cursor()

print("🧽 Deleting existing future forecasts for this model...")
cur.execute(
    """
    DELETE FROM retail.fact_forecast
    WHERE forecast_date >= CURRENT_DATE
      AND model_version = %s;
    """,
    (model_version,),
)

print("💾 Inserting new forecasts into retail.fact_forecast...")
cur.executemany(
    """
    INSERT INTO retail.fact_forecast
        (product_id, forecast_date, horizon_days, forecast_qty, model_version, source)
    VALUES (%s, %s, %s, %s, %s, %s);
    """,
    forecast_rows,
)

conn.commit()
cur.close()
conn.close()

print("✅ Forecasts written successfully to retail.fact_forecast!")