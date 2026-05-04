from faker import Faker
import uuid
import random
from datetime import datetime, timezone
from config.settings import KAFKA_CONFIG
from confluent_kafka import Producer
import json

fake = Faker("en_IN")

CITIES = ["Bangalore", "Mumbai", "Hyderabad", "Chennai", "Delhi"]
CUISINES = ["Butter Chicken", "Naan", "Dosa", "Pizza", "Biryani", "Burger", "Shushi"]
PAYMENT_METHOD = ["UPI", "Card", "Cash", "Wallet"]


class EventGenerator:
    @staticmethod
    def _base_event(event_type: str, user_id=None) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id or "u_" + str(uuid.uuid4())[:8],
        }

    def order_placed(self) -> dict:
        order = {
            "order_id": "o_" + str(uuid.uuid4())[:8],
            "restaurant_id": "r_" + str(uuid.uuid4())[:8],
            "city": random.choice(CITIES),
            "items": random.choices(CUISINES, k=3),
            "price": round(random.uniform(100.0, 1500.0), 2),
        }
        base = self._base_event("order_placed")
        return {**base, **order}

    def payment_done(self, order_id: str, amount: float, user_id: str) -> dict:
        payment = {
            "payment_id": "p_" + str(uuid.uuid4())[:8],
            "order_id": order_id,
            "amount": amount,
            "payment_method": random.choice(PAYMENT_METHOD),
        }
        base = self._base_event("payment_done", user_id=user_id)
        return {**base, **payment}

    def driver_assigned(self, order_id: str, user_id: str) -> dict:
        driver = {
            "driver_id": "d_" + str(uuid.uuid4())[:8],
            "order_id": order_id,
            "user_id": user_id,
        }
        base = self._base_event("driver_assigned", user_id=user_id)
        return {**base, **driver}

    def order_delivered(self, order_id: str, driver_id: str, user_id: str) -> dict:
        delivered = {
            "order_id": order_id,
            "driver_id": driver_id,
            "user_id": user_id,
            "delivery_time": datetime.now(timezone.utc).isoformat(),
        }
        base = self._base_event("order_delivered", user_id=user_id)
        return {**base, **delivered}

    def rating_given(self, order_id: str, user_id: str) -> dict:
        rating = {
            "order_id": order_id,
            "user_id": user_id,
            "rating": random.randint(1, 5),
        }
        base = self._base_event("rating_given", user_id=user_id)
        return {**base, **rating}

    def run_order_flow(self):
        order_event = self.order_placed()
        curr_user = order_event["user_id"]
        curr_order_id = order_event["order_id"]
        curr_amount = order_event["price"]

        payment_event = self.payment_done(curr_order_id, curr_amount, curr_user)
        driver_event = self.driver_assigned(curr_order_id, curr_user)
        deliver_event = self.order_delivered(
            curr_order_id, driver_event["driver_id"], curr_user
        )
        rating_event = self.rating_given(curr_order_id, curr_user)
        return [order_event, payment_event, driver_event, deliver_event, rating_event]
