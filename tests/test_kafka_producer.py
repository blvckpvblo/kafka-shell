import json
import pytest
from unittest.mock import MagicMock, patch, call
from contextlib import contextmanager

from src.producer.kafka_producer import KafkaProducer


class MockProducer:
    def __init__(self, config=None):
        self.config = config or {}
        self.messages = []
        self.flush_called = False
        self.poll_called = False

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.messages.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )
        if on_delivery:
            # Simulate successful delivery
            on_delivery(None, MockMessage(topic, 0, 0, value, key, headers))

    def flush(self, timeout=None):
        self.flush_called = True
        return 0

    def poll(self, timeout=None):
        self.poll_called = True
        return 0


class MockMessage:
    def __init__(self, topic, partition, offset, value, key=None, headers=None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value
        self.key = key
        self.headers = headers
        self.error = None


class MockError:
    def __init__(self, code, name, str):
        self.code = code
        self.name = name
        self._str = str

    def str(self):
        return self._str


@pytest.fixture
def mock_producer():
    with patch("confluent_kafka.Producer", MockProducer) as mock:
        yield mock


@pytest.fixture
def kafka_producer():
    """Create a KafkaProducer instance with mocked confluent_kafka.Producer"""
    with patch("confluent_kafka.Producer", MockProducer):
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092", client_id="test-client", acks="all"
        )
        yield producer
        producer.close()


def test_producer_initialization(mock_producer):
    """Test that the producer is initialized with the correct configuration"""
    producer = KafkaProducer(
        bootstrap_servers="test-server:9092", client_id="test-client", acks="all"
    )

    # Check that Producer was initialized with the correct config
    assert mock_producer.call_count == 1
    config = mock_producer.call_args[0][0]
    assert config["bootstrap.servers"] == "test-server:9092"
    assert config["client.id"] == "test-client"


def test_send_message_success(kafka_producer):
    """Test that messages are sent correctly"""
    # Get the underlying mock producer
    mock_producer = kafka_producer._producer

    # Create a test message
    topic = "test-topic"
    message = {"key": "value"}
    message_json = json.dumps(message).encode("utf-8")

    # Send the message
    result = kafka_producer.send(topic, message)

    # Check that the message was sent with the correct parameters
    assert len(mock_producer.messages) == 1
    sent_message = mock_producer.messages[0]
    assert sent_message["topic"] == topic
    assert sent_message["value"] == message_json


def test_send_message_with_key(kafka_producer):
    """Test that messages with keys are sent correctly"""
    # Get the underlying mock producer
    mock_producer = kafka_producer._producer

    # Create a test message with key
    topic = "test-topic"
    key = "test-key"
    message = {"data": "test-value"}

    # Send the message with a key
    result = kafka_producer.send(topic, message, key=key)

    # Check that the message was sent with the correct key
    assert len(mock_producer.messages) == 1
    sent_message = mock_producer.messages[0]
    assert sent_message["key"] == key.encode("utf-8") if isinstance(key, str) else key


def test_send_message_with_headers(kafka_producer):
    """Test that messages with headers are sent correctly"""
    # Get the underlying mock producer
    mock_producer = kafka_producer._producer

    # Create a test message with headers
    topic = "test-topic"
    message = {"data": "test-value"}
    headers = {"header1": "value1", "header2": "value2"}

    # Send the message with headers
    result = kafka_producer.send(topic, message, headers=headers)

    # Check that the message was sent with the correct headers
    assert len(mock_producer.messages) == 1
    sent_message = mock_producer.messages[0]

    # Convert headers to the format used by confluent_kafka
    expected_headers = [
        (k, v.encode("utf-8") if isinstance(v, str) else v) for k, v in headers.items()
    ]

    # Check headers in the sent message
    assert sent_message["headers"] == expected_headers


@patch("confluent_kafka.Producer")
def test_error_handling(mock_producer_class):
    """Test that errors during message production are properly handled"""
    # Setup mock producer to simulate an error
    mock_producer_instance = mock_producer_class.return_value

    # Make produce() raise an exception
    mock_producer_instance.produce.side_effect = Exception("Connection error")

    # Create producer and try to send a message
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    # Test error handling during send
    with pytest.raises(Exception) as excinfo:
        producer.send("test-topic", {"key": "value"})

    assert "Connection error" in str(excinfo.value)


@patch("confluent_kafka.Producer")
def test_delivery_callback(mock_producer_class):
    """Test that delivery callbacks are correctly handled"""
    # Setup mock producer
    mock_producer_instance = mock_producer_class.return_value
    delivery_callback_called = False
    error_message = None

    # Define a delivery callback function to track delivery status
    def on_delivery(err, msg):
        nonlocal delivery_callback_called, error_message
        delivery_callback_called = True
        if err:
            error_message = err

    # Configure mock to call delivery callback
    def mock_produce(topic, value, key=None, on_delivery=None, **kwargs):
        if on_delivery:
            # Simulate successful delivery
            on_delivery(None, MagicMock(topic=topic, value=value, key=key))

    mock_producer_instance.produce.side_effect = mock_produce

    # Create producer and send a message with delivery callback
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    producer.send("test-topic", {"key": "value"}, on_delivery=on_delivery)

    # Verify delivery callback was called
    assert delivery_callback_called
    assert error_message is None


@patch("confluent_kafka.Producer")
def test_connection_verification(mock_producer_class):
    """Test that the producer can verify its connection to Kafka"""
    mock_producer_instance = mock_producer_class.return_value

    # Create producer and verify connection
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    # Test the connection verification method
    producer.verify_connection()

    # Check that the appropriate methods were called
    assert mock_producer_instance.poll.called

    # For API designs that use flush, check that too
    if hasattr(producer, "flush_on_verify") and producer.flush_on_verify:
        assert mock_producer_instance.flush.called


def test_close_properly_flushes(kafka_producer):
    """Test that closing the producer properly flushes any pending messages"""
    # Get the underlying mock producer
    mock_producer = kafka_producer._producer

    # Send a test message
    kafka_producer.send("test-topic", {"key": "value"})

    # Close the producer
    kafka_producer.close()

    # Check that flush was called during close
    assert mock_producer.flush_called


@patch("confluent_kafka.Producer")
def test_producer_context_manager(mock_producer_class):
    """Test that the producer can be used as a context manager"""
    mock_producer_instance = mock_producer_class.return_value

    # Use the producer as a context manager
    with KafkaProducer(bootstrap_servers="localhost:9092") as producer:
        producer.send("test-topic", {"key": "value"})

    # Check that flush was called when exiting the context
    assert mock_producer_instance.flush.called
