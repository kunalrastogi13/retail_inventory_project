import os
from datetime import date, timedelta
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# Connect to Postgres
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DB_URL)

print("🚀 Starting baseline ML forecast (historical + future)...")

# Load daily sales
sql = """
SELECT
    product_id,
    order_date::date AS order_date,
    SUM(quantity) AS daily_qty    
FROM retail.fact_sales
GROUP BY product_id, order_date
ORDER BY product_id, order_date;
"""
df = pd.read_sql(sql, engine)

if df.empty:
    print("⚠️ No sales data found.")
    exit()

print(f"📦 Loaded {len(df)} sales rows")

today = date.today()
HORIZON = 7

future_model = "v1_mavg7"
hist_model = "v2_mavg7_hist"

future_rows = []
hist_rows = []

# Compute rolling average for each product
# ============================================
for product_id, g in df.groupby("product_id"):

    g = g.copy()
    g["order_date"] = pd.to_datetime(g["order_date"])

    # Fill missing days
    g = g.set_index("order_date").asfreq("D", fill_value=0)

    if len(g) < 7:
        continue

    # Rolling 7-day MA
    g["ma7"] = g["daily_qty"].rolling(window=7).mean()

    # --- A) Historical predictions ---
    hist = g.dropna(subset=["ma7"])

    for dt, row in hist.iterrows():
        hist_rows.append(
            (
                product_id,
                dt.date(),
                0,                         # horizon_days=0 for backtest
                round(float(row["ma7"]), 2),
                hist_model,
                "ml_model"
            )
        )

    # --- B) Future 7-day prediction ---
    last_ma = float(hist["ma7"].iloc[-1])

    for d in range(1, HORIZON + 1):
        future_date = today + timedelta(days=d)
        future_rows.append(
            (
                product_id,
                future_date,
                HORIZON,
                round(last_ma, 2),
                future_model,
                "ml_model"
            )
        )

print(f"📈 Generated {len(hist_rows)} historical rows")
print(f"📈 Generated {len(future_rows)} future rows")

# ============================================
# Insert forecasts
# ============================================
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    port=POSTGRES_PORT,
)
cur = conn.cursor()

# Clear old historical + future rows
cur.execute("DELETE FROM retail.fact_forecast WHERE model_version IN (%s, %s)",
            (hist_model, future_model))

# Insert historical
cur.executemany(
    """
    INSERT INTO retail.fact_forecast
        (product_id, forecast_date, horizon_days, forecast_qty, model_version, source)
    VALUES (%s, %s, %s, %s, %s, %s);
    """,
    hist_rows,
)

# Insert future
cur.executemany(
    """
    INSERT INTO retail.fact_forecast
        (product_id, forecast_date, horizon_days, forecast_qty, model_version, source)
    VALUES (%s, %s, %s, %s, %s, %s);
    """,
    future_rows,
)

conn.commit()
cur.close()
conn.close()

print(" Baseline model forecasts inserted successfully!")