#!/usr/bin/env python3

import json
import logging
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from typing import Optional, Dict, Any
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class KafkaMessageConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "test-consumer-group",
        schema_registry_url: Optional[str] = None,
        auto_offset_reset: str = "earliest",
    ):
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
        }

        self.consumer = Consumer(self.config)
        self.running = True
        self.schema_registry = None
        self.avro_deserializers = {}

        if schema_registry_url:
            self.schema_registry = SchemaRegistryClient({"url": schema_registry_url})

        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("Received shutdown signal. Closing consumer...")
        self.running = False

    def _get_deserializer(self, topic: str) -> Optional[AvroDeserializer]:
        if topic in self.avro_deserializers:
            return self.avro_deserializers[topic]

        if self.schema_registry:
            try:
                # Try to get the latest schema for the topic's value
                schema = self.schema_registry.get_latest_version(f"{topic}-value")
                deserializer = AvroDeserializer(
                    self.schema_registry,
                    schema.schema.schema_str,
                )
                self.avro_deserializers[topic] = deserializer
                return deserializer
            except Exception as e:
                logger.debug(f"Could not load schema for topic {topic}: {e}")

        return None

    def subscribe(self, topics):
        """Subscribe to the specified topics"""
        logger.info(f"Subscribing to topics: {topics}")
        self.consumer.subscribe(topics)

    def start_consuming(self):
        """Start consuming messages from subscribed topics"""
        try:
            while self.running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            f"Reached end of partition: {msg.topic()}/{msg.partition()}"
                        )
                    else:
                        logger.error(f"Error while consuming message: {msg.error()}")
                    continue

                # Try to deserialize with Avro if available
                deserializer = self._get_deserializer(msg.topic())
                try:
                    if deserializer:
                        value = deserializer(
                            msg.value(),
                            SerializationContext(msg.topic(), MessageField.VALUE),
                        )
                    else:
                        # Try to decode as JSON
                        try:
                            value = json.loads(msg.value().decode("utf-8"))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            value = msg.value()

                    # Print message details
                    print("\n=== New Message ===")
                    print(f"Topic: {msg.topic()}")
                    print(f"Partition: {msg.partition()}")
                    print(f"Offset: {msg.offset()}")
                    print(f"Timestamp: {msg.timestamp()[1]}")
                    if msg.key():
                        print(f"Key: {msg.key().decode('utf-8')}")
                    print(
                        "Value:",
                        (
                            json.dumps(value, indent=2)
                            if isinstance(value, (dict, list))
                            else value
                        ),
                    )
                    print("==================\n")

                    # Commit the message
                    self.consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Closing consumer...")
        finally:
            self.close()

    def close(self):
        """Close the consumer and clean up"""
        logger.info("Closing consumer...")
        self.consumer.close()


def main():
    # Create and configure the consumer
    consumer = KafkaMessageConsumer(
        bootstrap_servers="localhost:9092",
        group_id="test-consumer-group",
        schema_registry_url="http://localhost:8081",
        auto_offset_reset="earliest",
    )

    # Get topics to subscribe to
    print("Enter topics to subscribe to (comma-separated):")
    topics = [t.strip() for t in input().split(",")]

    # Subscribe to topics
    consumer.subscribe(topics)

    # Start consuming messages
    print(f"\nStarting consumer for topics: {topics}")
    print("Press Ctrl+C to exit\n")
    consumer.start_consuming()


if __name__ == "__main__":
    main()
