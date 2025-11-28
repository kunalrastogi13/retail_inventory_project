import pandas as pd
import psycopg2
from prophet import Prophet
import os

# DB connection (same as yours)
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    database=os.getenv("POSTGRES_DB", "retail_db"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "admin123"),
    port=os.getenv("POSTGRES_PORT", "5432")
)
cur = conn.cursor()

print("🚀 Starting Prophet demand forecasting (history + future)...")

# 1. Load daily sales per product
query = """
SELECT 
    product_id,
    order_date,
    SUM(quantity) AS daily_qty
FROM retail.fact_sales
GROUP BY product_id, order_date
ORDER BY product_id, order_date;
"""
df = pd.read_sql(query, conn)

if df.empty:
    print("No sales data found.")
    exit()

print(f"📦 Loaded {len(df)} rows of daily sales")

MIN_HISTORY = 5   # same as before

hist_rows = []    # for historical predictions (accuracy)
future_rows = []  # for 7-day ahead future forecasts

for product_id, group in df.groupby("product_id"):

    if len(group) < MIN_HISTORY:
        continue

    print(f"📲 Training Prophet for product: {product_id}")

    ts = group.rename(columns={"order_date": "ds", "daily_qty": "y"})

    m = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )
    m.fit(ts)

    # ---------- A) Historical predictions (for accuracy) ----------
    hist_fc = m.predict(ts[["ds"]])   # predict on existing dates only

    for _, row in hist_fc.iterrows():
        hist_rows.append(
            (
                product_id,
                row["ds"].date(),          # same date as actuals
                0,                         # horizon_days = 0 for backtest
                round(float(row["yhat"]), 2),
                "v2_prophet_hist"          # new model_version for accuracy
            )
        )

    # ---------- B) Future 7-day forecasts (for “next week” view) ----------
    future = m.make_future_dataframe(periods=7)
    future_fc = m.predict(future).tail(7)[["ds", "yhat"]]

    for _, row in future_fc.iterrows():
        future_rows.append(
            (
                product_id,
                row["ds"].date(),
                7,                         # 7-day ahead
                round(float(row["yhat"]), 2),
                "v1_prophet"               # keep existing version for future
            )
        )

# 2. Insert both sets into fact_forecast
insert_sql = """
INSERT INTO retail.fact_forecast
(product_id, forecast_date, horizon_days, forecast_qty, model_version)
VALUES (%s, %s, %s, %s, %s);
"""

if hist_rows:
    cur.executemany(insert_sql, hist_rows)
    print(f"✅ Inserted {len(hist_rows)} historical Prophet rows (v2_prophet_hist)")

if future_rows:
    cur.executemany(insert_sql, future_rows)
    print(f"✅ Inserted {len(future_rows)} future Prophet rows (v1_prophet)")

conn.commit()
cur.close()
conn.close()