import json
from typing import Optional, Union, Dict, Any


def send_json(producer, topic: str, json_str: str, key: Optional[str] = None) -> None:
    """Send a JSON message to Kafka

    Args:
        producer: KafkaProducer instance
        topic: Name of the topic to send to
        json_str: JSON string to send
        key: Optional message key
    """
    try:
        # Parse JSON string to dict
        message = json.loads(json_str)

        # Send the message
        producer.send(
            topic=topic,
            value=message,
            key=key,
            on_delivery=lambda err, msg: print(
                f"Delivered: {msg.topic()}" if not err else f"Error: {err}"
            ),
        )
        print("Message sent successfully!")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {str(e)}")
    except ValueError as e:
        print(f"Schema validation error: {str(e)}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")
