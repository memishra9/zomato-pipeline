from confluent_kafka import Producer
from config.settings import KAFKA_CONFIG
from producer.event_generator import EventGenerator
import json
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


TOPIC_MAP = {
    "order_placed": "orders",
    "driver_assigned": "deliveries",
    "order_delivered": "deliveries",
    "rating_given": "ratings",
    "payment_done": "payments",
}


class ZomatoProducer:
    def __init__(self):
        self.producer = Producer(KAFKA_CONFIG)
        self.event_gen = EventGenerator()
        # print("Its working")

    def _on_delivery(self, err, msg):
        if err:
            logger.error(f"message failed to deliver to {msg.topic}: {err}")

        else:
            logger.info(
                f"message delivered to {msg.topic()} and partition{msg.partition()} and offset{msg.offset()}"
            )

    def _publish(self, event: dict) -> None:
        topic = TOPIC_MAP[event["event_type"]]
        key = event["event_id"].encode("utf-8")
        value = json.dumps(event).encode("utf-8")

        self.producer.produce(
            topic, key=key, value=value, on_delivery=self._on_delivery
        )

    def run_order_lifecycle(self):
        print("lifecycle started")
        events = self.event_gen.run_order_flow()

        for event in events:
            self._publish(event)
            # logger.info(f"Published event {event}")

    def run(self, interval_seconds: float = 1.0) -> None:
        logger.info("Producer started")
        try:
            while True:
                self.run_order_lifecycle()
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Shutting Down")
        finally:
            self.producer.flush()
