import pandas as pd
import psycopg2
from prophet import Prophet
from datetime import timedelta
import os

# DB connection
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    database=os.getenv("POSTGRES_DB", "retail_db"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "admin123"),
    port=os.getenv("POSTGRES_PORT", "5432")
)
cur = conn.cursor()

print("🚀 Starting Prophet demand forecasting...")

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
    print("⚠️ No sales data found.")
    exit()

print(f"📦 Loaded {len(df)} rows of daily sales")

# Prepare output list
forecast_rows = []

# 2. Loop product-wise
MIN_HISTORY = 5   # or even 3 if needed
for product_id, group in df.groupby("product_id"):

    if len(group) < MIN_HISTORY:
        continue  # Not enough data

    print(f"📲 Training Prophet for product: {product_id}")

    ts = group.rename(columns={"order_date": "ds", "daily_qty": "y"})

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )
    model.fit(ts)

    # Predict next 7 days
    future = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)

    # Filter only the newly forecasted future values
    fc = forecast.tail(7)[["ds", "yhat"]]

    for _, row in fc.iterrows():
        forecast_rows.append(
            (
                product_id,
                row["ds"].date(),
                7,                 # horizon
                round(float(row["yhat"]), 2),
                "v1_prophet"
            )
        )

# 3. Insert into fact_forecast
insert_sql = """
INSERT INTO retail.fact_forecast
(product_id, forecast_date, horizon_days, forecast_qty, model_version)
VALUES (%s, %s, %s, %s, %s);
"""

cur.executemany(insert_sql, forecast_rows)
conn.commit()

print(f"✅ Inserted {len(forecast_rows)} forecast rows into retail.fact_forecast")

cur.close()
conn.close()