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

print("📥 Loading training data from retail.ml_stockout_training ...")
df = pd.read_sql("SELECT * FROM retail.ml_stockout_training", engine)

# -----------------------------
# 2. Prepare X, y
# -----------------------------
feature_cols = [
    "actual_qty",
    "current_stock",
    "lag_1", "lag_2", "lag_3",
    "roll_7_sum", "roll_7_mean", "roll_14_sum",
]

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