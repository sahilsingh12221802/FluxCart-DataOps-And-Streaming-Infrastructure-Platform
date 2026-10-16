import json
import logging

from kafka import KafkaProducer
from kafka.serializer import Serializer


class StringSerializer(Serializer):
    def serialize(self, topic, value):
        if value is None:
            return None

        return value.encode("utf-8")


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        if value is None:
            return None

        return json.dumps(value).encode("utf-8")


class FluxCartKafkaProducer:

    def __init__(self, bootstrap_servers, topic):
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=StringSerializer(),
            value_serializer=JsonSerializer(),
            acks="all",
            retries=5,
        )

        logging.info(
            "Kafka producer initialized | broker=%s | topic=%s",
            bootstrap_servers,
            topic,
        )

    def publish(self, event):
        user_id = event["user"]["user_id"]

        future = self.producer.send(
            self.topic,
            key=user_id,
            value=event,
        )

        metadata = future.get(timeout=10)

        return metadata

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()