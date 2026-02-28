import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# -------------------------------
# Kafka Producer Configuration
# -------------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# -------------------------------
# Event Generator
# -------------------------------
def generate_inventory_event():
    stock = random.randint(0, 500)

    return {
        "warehouse_id": random.randint(1, 4),
        "product_id": random.randint(100, 500),
        "stock_level": stock,
        "reorder_flag": stock < 50,
        "event_time": datetime.now().isoformat()
    }

# -------------------------------
# Streaming Loop
# -------------------------------
print("Inventory Producer Started...")

while True:
    event = generate_inventory_event()
    producer.send("inventory_events", event)
    print("Sent Inventory:", event)
    time.sleep(2)