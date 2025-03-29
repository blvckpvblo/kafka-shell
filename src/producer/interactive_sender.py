import time
from typing import Dict, Any


def parse_value(value: str):
    """Helper function to parse input values into appropriate types"""
    # Handle null values
    if not value or value.lower() == "none":
        return None

    # Try to convert to int
    try:
        if value.isdigit():
            return int(value)
    except ValueError:
        pass

    # Try to convert to float
    try:
        return float(value)
    except ValueError:
        pass

    # Try to convert to boolean
    if value.lower() in ("true", "false"):
        return value.lower() == "true"

    # Try to parse as JSON for complex types (arrays, objects)
    try:
        import json

        return json.loads(value)
    except json.JSONDecodeError:
        pass

    # Default to string if no other type matches
    return value


def send_interactive(producer) -> None:
    """Send messages to Kafka using interactive field-by-field input"""
    print("\n=== Interactive Message Producer ===")
    print("Enter 'q' at any prompt to quit")
    print("Type hints:")
    print("- Numbers will be converted to int/float automatically")
    print("- 'true' or 'false' will be converted to boolean")
    print('- Use JSON syntax for arrays/objects ([1,2,3] or {"key":"value"})')
    print("- Empty value or 'None' will be treated as null")

    # Get topic
    topic = input("Enter topic name: ").strip()
    if topic.lower() == "q":
        return

    # Get key
    key = input("Enter message key: ").strip()
    if key.lower() == "q":
        return

    # Get message content
    print("\nEnter message fields (empty field name to finish):")
    message = {}
    while True:
        field_name = input("Field name (or press Enter to finish): ").strip()
        if not field_name:
            break
        if field_name.lower() == "q":
            return

        field_value = input(f"Value for {field_name}: ").strip()
        if field_value.lower() == "q":
            return

        # Parse the value to appropriate type
        parsed_value = parse_value(field_value)
        message[field_name] = parsed_value
        print(f"Field '{field_name}' added as type: {type(parsed_value).__name__}")

    # Add timestamp if not provided
    if "timestamp" not in message:
        message["timestamp"] = int(time.time() * 1000)

    # Send the message
    try:
        producer.send(
            topic=topic,
            value=message,
            key=key,
            on_delivery=lambda err, msg: print(
                f"Delivered: {msg.topic()}" if not err else f"Error: {err}"
            ),
        )
        print("Message sent successfully!")
    except ValueError as e:
        print(f"Schema validation error: {str(e)}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")
