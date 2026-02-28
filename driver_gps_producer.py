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
def generate_gps_event():
    return {
        "driver_id": random.randint(101, 104),
        "latitude": round(random.uniform(30.0, 40.0), 6),
        "longitude": round(random.uniform(-90.0, -80.0), 6),
        "speed_kmph": random.randint(20, 100),
        "event_time": datetime.now().isoformat()
    }

# -------------------------------
# Streaming Loop
# -------------------------------
print("Driver GPS Producer Started...")

while True:
    event = generate_gps_event()
    producer.send("driver_gps_events", event)
    print("Sent GPS Event:", event)
    time.sleep(1)