from kafka import KafkaConsumer
import json, psycopg2, os

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "inventory_updates")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    database=os.getenv("POSTGRES_DB", "retail_db"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "admin123"),
    port=os.getenv("POSTGRES_PORT", "5432")
)
cur = conn.cursor()

print(f"📡 Listening for INVENTORY messages on topic: {KAFKA_TOPIC} ...")

for message in consumer:
    record = message.value

    product_id  = record.get("product_id")
    change_qty  = record.get("change_qty")
    event_time  = record.get("event_time")
    source      = record.get("source", "LIVE_GENERATOR")

    print(f" Inventory event: product={product_id}, Δqty={change_qty}")

    cur.execute("""
        INSERT INTO retail.fact_inventory
            (product_id, change_qty, event_time, source)
        VALUES (%s, %s, %s, %s)
    """, (product_id, change_qty, event_time, source))

    conn.commit()