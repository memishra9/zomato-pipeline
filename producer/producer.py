from faker import Faker
import uuid
import random
from datetime import datetime, timezone
from config.settings import KAFKA_CONFIG
from confluent_kafka import Producer
import json


fake =faker("en_IN")

CITIES = ["Bangalore", "Mumbai", "Hyderabad", "Chennai", "Delhi"]
CUISINES = ["Butter Chicken", "Naan", "Dosa", "Pizza", "Biryani", "Burger", "Shushi"]
PAYMENT_METHOD = ["UPI", "Card", "Cash", "Wallet"]

class EventGenerator:
    fake =Faker("en_IN")

CITIES = ["Bangalore", "Mumbai", "Hyderabad", "Chennai", "Delhi"]
CUISINES = ["Butter Chicken", "Naan", "Dosa", "Pizza", "Biryani", "Burger", "Shushi"]
PAYMENT_METHOD = ["UPI", "Card", "Cash", "Wallet"]

####Base Event ######

    def _base_event():
        return {
        "event_id" : str(uuid.uuid4()),
        "timestamp" : datetime.timestamp,
        "user_id" : ("U"+str(uuid.uuid4()))[:8]
        }


def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed for {msg.topic()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

order = {
    "event_id": "e002",
    "event_type": "order_placed",
    "user_id": "u123",
    "restaurant_id": "r456",
    "amount": 500,
    "timestamp": "2026-04-24T15:00:00Z"
}

def place_order(producer, event):
    producer.produce(
        'orders',
        key=event["event_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=delivery_callback
    )

if __name__ == '__main__':
    producer = Producer(KAFKA_CONFIG)
    place_order(producer, order)
    producer.flush()