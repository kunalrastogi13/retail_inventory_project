# python/ml_build_features.py

import os
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DB_URL)

print("📥 Loading base ml_features_daily ...")
df = pd.read_sql("SELECT * FROM retail.ml_features_daily", engine)

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["product_id", "date"])

def add_features(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("date")

    # simple lags
    g["lag_1"] = g["actual_qty"].shift(1)
    g["lag_2"] = g["actual_qty"].shift(2)
    g["lag_3"] = g["actual_qty"].shift(3)

    # rolling windows
    g["roll_7_sum"] = g["actual_qty"].rolling(7).sum()
    g["roll_7_mean"] = g["actual_qty"].rolling(7).mean()
    g["roll_14_sum"] = g["actual_qty"].rolling(14).sum()

    return g

df_feat = (
    df.groupby("product_id", group_keys=False)
      .apply(add_features)
)

# drop early rows with NaNs from lags/rolls
df_feat = df_feat.dropna(subset=["lag_1", "roll_7_mean"])

print(" Feature rows:", len(df_feat))

df_feat.to_sql(
    "ml_features_daily_enriched",
    engine,
    schema="retail",
    if_exists="replace",
    index=False
)

print("💾 Saved to retail.ml_features_daily_enriched")