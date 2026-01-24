import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

CAR_ID = "car_44"
lap = 1
speed = 280.0
fuel = 100.0

while True:
    telemetry = {
        "car_id": CAR_ID,
        "timestamp": int(time.time() * 1000),
        "speed": round(speed + random.uniform(-5, 5), 2),
        "rpm": random.randint(9000, 12000),
        "gear": random.randint(1, 8),
        "throttle": round(random.uniform(0.6, 1.0), 2),
        "brake": round(random.uniform(0.0, 0.4), 2),
        "tyre_temp": {
            "FL": round(random.uniform(85, 105), 2),
            "FR": round(random.uniform(85, 105), 2),
            "RL": round(random.uniform(90, 110), 2),
            "RR": round(random.uniform(90, 110), 2),
        },
        "fuel": round(fuel, 2),
        "lap": lap
    }

    producer.send("telemetry.raw", telemetry)
    print("Sent:", telemetry)

    fuel -= random.uniform(0.02, 0.05)
    time.sleep(0.2)
