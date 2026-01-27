import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

drivers = ["VER", "HAM", "LEC"]

while True:
    event = {
        "driver": random.choice(drivers),
        "lap": random.randint(1, 70),
        "speed": round(random.uniform(180, 340), 2),
        "throttle": round(random.uniform(0, 100), 1),
        "brake": round(random.uniform(0, 100), 1),
        "timestamp": time.time()
    }

    producer.send("f1-telemetry", event)
    print("Sent:", event)
    time.sleep(1)
