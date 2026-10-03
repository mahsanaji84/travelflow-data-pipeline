import random
import time
import uuid
import json
from datetime import datetime
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

destinations = [
    "Paris",
    "Rome",
    "Montreal",
    "Cancun",
    "New York"
]

event_types = [
    "view_destination",
    "view_package",
    "start_booking",
    "booking_completed",
    "booking_abandoned"
]

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"U{random.randint(1000, 9999)}",
        "event_type": random.choice(event_types),
        "destination": random.choice(destinations),
        "package_id": f"PKG{random.randint(100, 120)}",
        "price": random.randint(500, 3000),
        "timestamp": datetime.now().isoformat()
    }

    producer.send("travelflow-events", event)

    print("Event envoyé :", event)

    time.sleep(2)