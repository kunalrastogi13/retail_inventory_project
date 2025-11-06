from kafka import KafkaConsumer
import json, psycopg2, os

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'superstore_orders_stream',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    database=os.getenv("POSTGRES_DB", "retail_db"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "admin123"),
    port=os.getenv("POSTGRES_PORT", "5432")
)
cur = conn.cursor()

print(" Listening for new messages...")

for message in consumer:
    record = message.value
    order_id = record.get("order_id")
    order_date = record.get("order_date")

    # Detect mode based on order_id pattern
    mode = "LIVE" if order_id.startswith("O-") else "CSV"

    print(f" Received ({mode}): {order_id}")

    # Insert into database
    cur.execute("""
    INSERT INTO retail.fact_sales
    (order_id, product_id, customer_id, category, sub_category, sales, quantity, discount, profit, order_date, ingested_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (order_id) DO NOTHING;
""", (
    record.get("order_id"),
    record.get("product_id"),
    record.get("customer_id"),
    record.get("category"),
    record.get("sub_category"),
    record.get("sales"),
    record.get("quantity"),
    record.get("discount"),
    record.get("profit"),
    record.get("order_date")
))

    conn.commit()