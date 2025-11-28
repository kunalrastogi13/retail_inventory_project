# ml_predict_stockout.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
import joblib

# -----------------------------
# 1. DB connection
# -----------------------------
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DB_URL)

# -----------------------------
# 2. Load latest features per product
#    (scoring dataset)
# -----------------------------
feature_cols = [
    "actual_qty",
    "current_stock",
    "lag_1", "lag_2", "lag_3",
    "roll_7_sum", "roll_7_mean", "roll_14_sum",
]

sql_scoring = """
WITH latest AS (
    SELECT
        product_id,
        MAX(date) AS date
    FROM retail.ml_features_daily_enriched
    GROUP BY product_id
)
SELECT
    f.product_id,
    f.date,
    f.actual_qty,
    f.current_stock,
    f.lag_1,
    f.lag_2,
    f.lag_3,
    f.roll_7_sum,
    f.roll_7_mean,
    f.roll_14_sum
FROM retail.ml_features_daily_enriched f
JOIN latest l
  ON f.product_id = l.product_id
 AND f.date       = l.date
ORDER BY f.product_id;
"""

print("📥 Loading scoring dataset (latest feature row per product)...")
df_scoring = pd.read_sql(sql_scoring, engine)
print(f"✅ Scoring rows: {len(df_scoring)}")

if df_scoring.empty:
    raise SystemExit("⚠️ No scoring data found, aborting.")

X_scoring = df_scoring[feature_cols]

# -----------------------------
# 3. Load trained model
# -----------------------------
print("📦 Loading model models/stockout_rf.pkl ...")
clf = joblib.load("models/stockout_rf.pkl")

# -----------------------------
# 4. Predict probabilities
# -----------------------------
print("🔮 Predicting stockout risk...")
probs = clf.predict_proba(X_scoring)[:, 1]  # probability of class 1 (stockout)

df_scoring["prob_stockout"] = probs
df_scoring["risk_flag"] = (df_scoring["prob_stockout"] >= 0.6).astype(int)
# 1 = HIGH RISK, 0 = LOW RISK

print(df_scoring.head())

# -----------------------------
# 5. Write to fact_stockout_risk
# -----------------------------
model_version = "v1_stockout_rf"
as_of_date = df_scoring["date"].max()  # snapshot date

with engine.begin() as conn:
    # create table if not exists (simple version)
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS retail.fact_stockout_risk (
            product_id      VARCHAR(50),
            as_of_date      DATE,
            prob_stockout   DOUBLE PRECISION,
            risk_flag       INTEGER,
            model_version   TEXT,
            PRIMARY KEY (product_id, as_of_date, model_version)
        );
    """))

    # ⚡ Instead of slow DELETE ... WHERE, just keep one snapshot
    #    and truncate the table before inserting new predictions.
    print("🧽 Truncating retail.fact_stockout_risk before inserting new predictions...")
    conn.execute(text("TRUNCATE TABLE retail.fact_stockout_risk;"))
    print("✅ Old predictions cleared.")

# prepare rows
out_df = df_scoring[["product_id", "date", "prob_stockout", "risk_flag"]].copy()
out_df.rename(columns={"date": "as_of_date"}, inplace=True)
out_df["model_version"] = model_version

# write (append)
out_df.to_sql(
    "fact_stockout_risk",
    engine,
    schema="retail",
    if_exists="append",
    index=False
)

print(f"✅ Inserted {len(out_df)} stockout risk predictions into retail.fact_stockout_risk")
print(f"   Snapshot as_of_date = {as_of_date}, model_version = {model_version}")