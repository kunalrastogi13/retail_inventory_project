from kafka import KafkaProducer
import json, time, pandas as pd, random, os
from datetime import datetime

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "inventory_updates")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# 1. Initialize producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Load product IDs from the same CSV (so they match dim_product)
df = pd.read_csv("/app/data/global_superstore.csv", encoding="latin1")
product_ids = df["Product ID"].unique().tolist()

print(f"🧾 Loaded {len(product_ids)} unique products for inventory events")

def generate_inventory_event():
    """Create a random inventory movement for a product."""
    product_id = random.choice(product_ids)

    # restock or consumption
    change_qty = random.choice([
        random.randint(1, 10),    # restock +1..+10
        -random.randint(1, 5)     # consumption -1..-5
    ])

    event = {
        "product_id": product_id,
        "change_qty": change_qty,
        "event_time": datetime.utcnow().isoformat(timespec="seconds"),
        "source": "LIVE_GENERATOR"
    }
    return event

if __name__ == "__main__":
    print(f"🚀 Starting Inventory Producer on topic: {KAFKA_TOPIC}")
    while True:
        event = generate_inventory_event()
        producer.send(KAFKA_TOPIC, value=event)
        print(f"📤 Sent inventory event: {event}")
        time.sleep(2)