import logging
from producer.zomato_producer import ZomatoProducer

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    ZomatoProducer().run(interval_seconds=1.0)