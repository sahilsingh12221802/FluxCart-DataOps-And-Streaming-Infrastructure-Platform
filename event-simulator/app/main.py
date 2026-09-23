import logging
import os
import signal
import time

from event_generator import generate_journey
from kafka_producer import FluxCartKafkaProducer


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("fluxcart-event-simulator")


running = True


def shutdown_handler(signum, frame):
    global running

    logger.info("Shutdown signal received")
    running = False


def main():
    global running

    kafka_broker = os.getenv(
        "KAFKA_BROKER",
        "localhost:9092",
    )

    kafka_topic = os.getenv(
        "KAFKA_TOPIC",
        "ecommerce-events",
    )

    event_rate = float(
        os.getenv(
            "EVENT_RATE",
            "5",
        )
    )

    anomaly_rate = float(
        os.getenv(
            "ANOMALY_RATE",
            "0.01",
        )
    )

    if event_rate <= 0:
        raise ValueError(
            "EVENT_RATE must be greater than 0"
        )

    if not 0 <= anomaly_rate <= 1:
        raise ValueError(
            "ANOMALY_RATE must be between 0 and 1"
        )

    producer = FluxCartKafkaProducer(
        bootstrap_servers=kafka_broker,
        topic=kafka_topic,
    )

    interval = 1 / event_rate

    logger.info("==========================================")
    logger.info("FluxCart Event Simulator v2")
    logger.info("==========================================")
    logger.info(
        "Kafka Broker : %s",
        kafka_broker,
    )
    logger.info(
        "Kafka Topic  : %s",
        kafka_topic,
    )
    logger.info(
        "Event Rate   : %.2f events/sec",
        event_rate,
    )
    logger.info(
        "Anomaly Rate : %.2f%%",
        anomaly_rate * 100,
    )
    logger.info("==========================================")

    generated = 0
    published = 0
    failures = 0

    try:
        while running:

            journey = generate_journey(
                anomaly_rate=anomaly_rate
            )

            for event in journey:

                if not running:
                    break

                generated += 1

                try:
                    metadata = producer.publish(event)

                    published += 1

                    product_id = None

                    if event.get("product"):
                        product_id = event["product"]["product_id"]

                    amount = event["transaction"]["amount"]

                    logger.info(
                        "Event published | "
                        "type=%s | "
                        "user=%s | "
                        "product=%s | "
                        "amount=%.2f | "
                        "partition=%s | "
                        "offset=%s",
                        event["event_type"],
                        event["user"]["user_id"],
                        product_id,
                        amount,
                        metadata.partition,
                        metadata.offset,
                    )

                except Exception:
                    failures += 1

                    logger.exception(
                        "Failed to publish event | "
                        "event_id=%s",
                        event["event_id"],
                    )

                time.sleep(interval)

    finally:

        producer.close()

        logger.info("==========================================")
        logger.info("Simulator stopped")
        logger.info(
            "Generated : %s",
            generated,
        )
        logger.info(
            "Published : %s",
            published,
        )
        logger.info(
            "Failures  : %s",
            failures,
        )
        logger.info("==========================================")


if __name__ == "__main__":

    signal.signal(
        signal.SIGINT,
        shutdown_handler,
    )

    signal.signal(
        signal.SIGTERM,
        shutdown_handler,
    )

    main()