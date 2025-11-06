from kafka import KafkaProducer
import json, time, pandas as pd, random, argparse
from datetime import datetime

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Category data for live mode
categories = {
    "Furniture": ["Chairs", "Tables", "Bookcases"],
    "Office Supplies": ["Binders", "Pens", "Paper"],
    "Technology": ["Phones", "Accessories", "Copiers"]
}

def stream_from_csv():
    """Send messages from cleaned CSV file"""
    df = pd.read_csv("/app/data/global_superstore.csv", encoding="latin1")
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]

    for _, row in df.sample(10).iterrows():
        message = {
            "order_id": row["order_id"],
            "product_id": row["product_id"],
            "customer_id": row["customer_id"],
            "category": row["category"],
            "sub_category": row["sub_category"],
            "sales": row["sales"],
            "quantity": row["quantity"],
            "discount": row["discount"],
            "profit": row["profit"],
            "order_date": row["order_date"]
        }
        producer.send("superstore_orders_stream", value=message)
        print(f" Sent CSV order: {message['order_id']}")
        time.sleep(2)

def stream_live_data():
    """Generate and send random live data"""
    while True:
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        order_id = f"O-{random.randint(1000,9999)}"
        product_id = f"P-{random.randint(100,999)}"
        customer_id = f"C-{random.randint(1,500)}"
        sales = round(random.uniform(10,1000), 2)
        quantity = random.randint(1,10)
        discount = round(random.choice([0, 0.1, 0.2]), 2)
        profit = round(sales * (1 - discount) * random.uniform(0.1, 0.3), 2)
        order_date = datetime.now().strftime("%Y-%m-%d")

        message = {
            "order_id": order_id,
            "product_id": product_id,
            "customer_id": customer_id,
            "category": category,
            "sub_category": sub_category,
            "sales": sales,
            "quantity": quantity,
            "discount": discount,
            "profit": profit,
            "order_date": order_date
        }

        producer.send("superstore_orders_stream", value=message)
        print(f"⚡ Sent LIVE order: {order_id} ({category} → {sub_category})")
        time.sleep(2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["csv", "live"], default="csv", help="Choose data source mode")
    args = parser.parse_args()

    print(f" Starting Kafka Producer in {args.mode.upper()} mode...")

    if args.mode == "csv":
        stream_from_csv()
    else:
        stream_live_data()