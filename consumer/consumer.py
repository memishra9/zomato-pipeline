from confluent_kafka import Consumer, KafkaException
from config.settings import KAFKA_CONFIG
import json

KAFKA_CONFIG.update(
    {
        "group.id": "zomato-dev-group-v2",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)


def assignment_callback(c, partitions):
    for p in partitions:
        print(f"Assigned to {p.topic}")


consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(["orders"], on_assign=assignment_callback)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        elif msg.error():
            print(f"The error encountered {msg.error()}")
        else:
            val = msg.value().decode("utf-8")
            partition = msg.partition()
            data = json.loads(val)
            print(f"Received {data} from partition {partition}")
            consumer.commit()
finally:
    consumer.close()
