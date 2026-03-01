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
    api_version=(0, 10),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# -------------------------------
# Event Generator
# -------------------------------
def generate_warehouse_event():
    return {
        "warehouse_id": random.randint(1, 4),
        "package_id": random.randint(10000, 99999),
        "processing_time_seconds": random.randint(10, 120),
        "status": random.choice(["Processed", "Delayed"]),
        "event_time": datetime.now().isoformat()
    }

# -------------------------------
# Streaming Loop
# -------------------------------
print("Warehouse Producer Started...")

while True:
    event = generate_warehouse_event()
    producer.send("warehouse_events", event)
    print("Sent Warehouse Event:", event)
    time.sleep(3)