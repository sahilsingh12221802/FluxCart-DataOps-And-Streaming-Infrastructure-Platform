import json
import logging

from kafka import KafkaProducer


logger = logging.getLogger(__name__)


class FluxCartKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5,
        )

        logger.info(
            "Kafka producer initialized | broker=%s | topic=%s",
            bootstrap_servers,
            topic,
        )

    def publish(self, event: dict):
        future = self.producer.send(self.topic, value=event)

        metadata = future.get(timeout=10)

        logger.debug(
            "Event published | event_id=%s | partition=%s | offset=%s",
            event["event_id"],
            metadata.partition,
            metadata.offset,
        )

        return metadata

    def close(self):
        self.producer.flush()
        self.producer.close()

        logger.info("Kafka producer closed")