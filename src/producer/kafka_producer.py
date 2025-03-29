#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Producer Module

This module provides a KafkaProducer class for sending messages to Kafka topics
with proper error handling, delivery confirmations, and clean shutdown.
"""

import atexit
import logging
import socket
import time
from typing import Any, Callable, Dict, Optional, Union
import threading
import json
import os
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.error import KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    A Kafka producer client for sending messages to Kafka topics.

    This class provides a wrapper around the confluent_kafka.Producer with
    additional error handling, logging, and delivery confirmations.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: Optional[str] = None,
        schema_registry_url: Optional[str] = None,
        schema_folder: Optional[str] = None,  # New parameter for local schema files
        acks: str = "all",
        retries: int = 3,
        max_in_flight: int = 5,
        linger_ms: int = 5,
        compression_type: str = "snappy",
        batch_size: int = 16384,
        additional_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a new Kafka producer with the specified configuration.

        Args:
            bootstrap_servers: Comma-separated list of broker addresses
            client_id: Identifier for this producer (defaults to hostname if None)
            schema_registry_url: URL of the Schema Registry
            schema_folder: Path to folder containing local schema files (optional)
            acks: Number of acknowledgments the producer requires ('all' means all replicas)
            retries: Number of retries if the producer encounters a recoverable error
            max_in_flight: Maximum number of unacknowledged requests to allow
            linger_ms: Delay in milliseconds to wait for more messages before sending a batch
            compression_type: Type of compression to use ('snappy', 'gzip', 'lz4', None)
            batch_size: Maximum size of a request in bytes
            additional_config: Additional configuration parameters for the producer
        """
        if client_id is None:
            client_id = f"producer-{socket.gethostname()}"

        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": acks,
            "retries": retries,
            "max.in.flight": max_in_flight,
            "linger.ms": linger_ms,
            "compression.type": compression_type,
            "batch.size": batch_size,
            "error_cb": self._error_callback,
            "logger": logger,
            "statistics.interval.ms": 60000,  # 60 seconds
        }

        # Add any additional configuration
        if additional_config:
            self.config.update(additional_config)

        logger.info(f"Initializing Kafka producer with config: {self.config}")
        self.producer = Producer(self.config)

        # Register shutdown handler to ensure proper cleanup
        atexit.register(self.close)

        # Periodic poll for stats and pending messages
        self._running = True
        self._poll_thread = threading.Thread(target=self._poll_stats, daemon=True)
        self._poll_thread.start()

        # Initialize Schema Registry client
        self.schema_registry = None
        self.schema_folder = schema_folder
        self.avro_serializers = {}  # Dictionary to store serializers by topic

        if schema_registry_url:
            self.schema_registry = SchemaRegistryClient({"url": schema_registry_url})

    def _error_callback(self, err: KafkaError) -> None:
        """
        Handle Kafka producer errors.

        Args:
            err: Kafka error object
        """
        logger.error(f"Kafka producer error: {err}")

    def _delivery_callback(
        self, err: Optional[KafkaError], msg: Any, callback: Optional[Callable] = None
    ) -> None:
        """
        Handle message delivery reports.

        Args:
            err: Error object or None on success
            msg: Message object that was delivered
            callback: Optional user-provided callback function
        """
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered successfully to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

        # Call user-provided callback if present
        if callback:
            callback(err, msg)

    def _poll_stats(self) -> None:
        """
        Poll for statistics and pending deliveries.
        This runs in a background thread.
        """
        while self._running:
            self.producer.poll(0)
            # Sleep for 100ms between polls
            time.sleep(0.1)

    def _load_schema_for_topic(self, topic: str) -> Optional[AvroSerializer]:
        """
        Load and cache the schema for a specific topic.
        First tries to get from Schema Registry, then from local files if configured.
        """
        if topic in self.avro_serializers:
            return self.avro_serializers[topic]

        schema_str = None

        # Try to get schema from Schema Registry
        if self.schema_registry:
            try:
                # Get latest schema for the topic's value
                subject_name = f"{topic}-value"
                schema_id = self.schema_registry.get_latest_version(
                    subject_name
                ).schema_id
                schema_str = self.schema_registry.get_schema(schema_id).schema_str
            except Exception as e:
                logger.debug(f"Could not fetch schema from registry: {e}")

        # Try to load from local file if schema registry failed or isn't configured
        if not schema_str and self.schema_folder:
            schema_file = Path(self.schema_folder) / f"{topic}.avsc"
            try:
                if schema_file.exists():
                    schema_str = schema_file.read_text()
            except Exception as e:
                logger.debug(f"Could not load schema from file: {e}")

        if schema_str:
            serializer = AvroSerializer(self.schema_registry, schema_str)
            self.avro_serializers[topic] = serializer
            return serializer

        return None

    def send(
        self,
        topic: str,
        value: Union[str, bytes, Dict],
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: int = 0,
        timestamp: int = int(
            time.time() * 1000
        ),  # Default to current time in milliseconds
        on_delivery: Optional[Callable] = None,
        timeout: float = 10.0,
    ) -> None:
        """
        Asynchronously send a message to a Kafka topic.

        Args:
            topic: Name of the Kafka topic
            value: Message content (string, bytes, or dict that will be JSON serialized)
            key: Optional message key
            headers: Optional message headers as a dictionary
            partition: Partition number (defaults to 0)
            timestamp: Timestamp in milliseconds since epoch (defaults to current time)
            on_delivery: Optional callback function for delivery confirmation
            timeout: Time to wait for message to be acknowledged in seconds

        Returns:
            None. Delivery is confirmed through the callback.
        """
        try:
            # Try to get schema for this topic
            avro_serializer = self._load_schema_for_topic(topic)

            # If we have a schema and the value is a dict, validate and serialize with Avro
            if avro_serializer and isinstance(value, dict):
                try:
                    value = avro_serializer(
                        value, SerializationContext(topic, MessageField.VALUE)
                    )
                except Exception as e:
                    raise ValueError(f"Schema validation failed: {str(e)}")
            else:
                # Fallback to JSON if no schema or not a dict
                if isinstance(value, dict):
                    value = json.dumps(value).encode("utf-8")
                elif isinstance(value, str):
                    value = value.encode("utf-8")

            # Convert key to bytes if it's a string
            if isinstance(key, str):
                key = key.encode("utf-8")

            # Convert headers to format expected by confluent_kafka
            kafka_headers = None
            if headers:
                kafka_headers = [
                    (k, v.encode("utf-8") if isinstance(v, str) else v)
                    for k, v in headers.items()
                ]

            logger.debug(f"Sending message to topic {topic}")

            # Produce message to Kafka
            self.producer.produce(
                topic=topic,
                value=value,
                key=key,
                headers=kafka_headers,
                partition=partition,
                timestamp=timestamp,
                on_delivery=lambda err, msg: self._delivery_callback(
                    err, msg, on_delivery
                ),
            )

            # Poll to handle delivery reports
            self.producer.poll(0)

            # Flush with timeout
            if timeout > 0:
                remaining = self.producer.flush(timeout)
                if remaining > 0:
                    logger.warning(f"{remaining} messages still pending after timeout")

        except Exception as e:
            logger.error(f"Error sending message to Kafka: {str(e)}")
            raise

    def flush(self, timeout: float = 10.0) -> int:
        """
        Wait for all messages in the producer queue to be delivered.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in queue after timeout
        """
        logger.info(f"Flushing producer queue (timeout: {timeout}s)")
        return self.producer.flush(timeout)

    def close(self) -> None:
        """
        Close the producer and release all resources.
        """
        logger.info("Closing Kafka producer")
        self._running = False
        if hasattr(self, "_poll_thread"):
            self._poll_thread.join(timeout=1.0)  # Wait for poll thread to finish
        self.flush()  # Ensure all messages are delivered before closing

        # Explicitly call poll one last time
        self.producer.poll(0)

        logger.info("Kafka producer closed")
