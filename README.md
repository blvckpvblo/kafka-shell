# Kafka Shell

A Python CLI tool that simplifies Kafka interaction from your terminal. Easily produce and consume Kafka messages with intuitive commands, eliminating boilerplate code and complex configuration.

## Features

- Interactive message producer with field-by-field input
- JSON message producer for sending complete JSON messages
- Message consumer with support for multiple topics
- Avro schema support with Schema Registry integration
- Automatic type conversion and validation
- Easy-to-use command-line interface

## Installation

1. Clone the repository:

```bash
git clone https://github.com/blvckpvblo/kafka-shell.git
cd kafka-shell
```

2. Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the main script:

```bash
python src/producer/main.py
```

The tool provides three main options:

1. Producer mode
   - Interactive field-by-field input
   - JSON message input
2. Consumer mode
   - Subscribe to multiple topics
   - View messages in real-time
3. Exit

### Producer Mode

- Choose between interactive or JSON input
- Specify topic name and message key
- Enter message content
- Automatic schema validation (if configured)

### Consumer Mode

- Subscribe to multiple topics (comma-separated)
- View messages with metadata (topic, partition, offset, timestamp)
- Automatic Avro deserialization (if schema exists)
- JSON fallback for non-Avro messages

## Configuration

- Kafka broker: localhost:9092
- Schema Registry: http://localhost:8081
- Schema folder: schemas/ (optional)

## License

MIT License - see LICENSE file for details
