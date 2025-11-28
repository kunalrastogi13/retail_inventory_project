import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
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
# 1a. Load data in CHUNKS
# -----------------------------
feature_cols = [
    "actual_qty",
    "current_stock",
    "lag_1", "lag_2", "lag_3",
    "roll_7_sum", "roll_7_mean", "roll_14_sum",
]

all_cols_sql = ", ".join(feature_cols + ["stockout_flag"])

base_query = f"""
SELECT {all_cols_sql}
FROM retail.ml_stockout_training
"""

chunksize = 100_000           # <- as you requested
max_rows = 500_000            # <- safety cap for final training set (tune this)
sample_frac_per_chunk = 0.2   # <- keep 20% from each chunk (also tunable)

print("📥 Loading training data from retail.ml_stockout_training in chunks ...")

samples = []
total_kept = 0

for chunk in pd.read_sql(base_query, engine, chunksize=chunksize):
    # Drop rows with any NaNs in important columns
    chunk = chunk.dropna(subset=feature_cols + ["stockout_flag"])

    if chunk.empty:
        continue

    # sample a fraction from this chunk
    # (you can also use head(N) if you prefer deterministic behaviour)
    n_keep = max(1, int(len(chunk) * sample_frac_per_chunk))
    chunk_sample = chunk.sample(n=n_keep, random_state=42)

    samples.append(chunk_sample)
    total_kept += len(chunk_sample)

    print(f"  ➕ Chunk processed, kept {len(chunk_sample)} rows "
          f"(total so far: {total_kept})")

    if total_kept >= max_rows:
        print(f"🔚 Reached max_rows={max_rows}, stopping chunk loading.")
        break

if not samples:
    raise SystemExit("⚠️ No data loaded after chunking. Check your table / filters.")

df = pd.concat(samples, ignore_index=True)
print(f"✅ Final training set size: {len(df)} rows")

# -----------------------------
# 2. Prepare X, y
# -----------------------------
X = df[feature_cols]
y = df["stockout_flag"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# -----------------------------
# 3. Train model
# -----------------------------
print("🌳 Training RandomForest stockout model...")
clf = RandomForestClassifier(
    n_estimators=100,
    max_depth=8,
    random_state=42,
    class_weight="balanced"
)
clf.fit(X_train, y_train)

# -----------------------------
# 4. Evaluate
# -----------------------------
y_pred = clf.predict(X_test)
print("📊 Classification report:")
print(classification_report(y_test, y_pred))

# -----------------------------
# 5. Save model
# -----------------------------
os.makedirs("models", exist_ok=True)
joblib.dump(clf, "models/stockout_rf.pkl")
print("✅ Saved model to models/stockout_rf.pkl")