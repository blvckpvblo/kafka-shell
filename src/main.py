#!/usr/bin/env python3

from producer.kafka_producer import KafkaProducer
from producer.interactive_sender import send_interactive
from producer.json_sender import send_json
from consumer.kafka_consumer import KafkaMessageConsumer
import logging


def main():
    print("\n=== Kafka Message Tool ===")
    print("1. Producer mode")
    print("2. Consumer mode")
    print("3. Exit")

    choice = input("\nSelect mode (1-3): ").strip()

    if choice == "1":
        # Producer mode
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            client_id="interactive-producer",
            schema_registry_url="http://localhost:8081",
            schema_folder="schemas",  # Optional local schema folder
            linger_ms=10,
        )

        try:
            while True:
                print("\n=== Producer Options ===")
                print("1. Interactive mode (field by field)")
                print("2. JSON mode (send complete JSON)")
                print("3. Back to main menu")

                producer_choice = input("\nSelect mode (1-3): ").strip()

                if producer_choice == "1":
                    send_interactive(producer)
                elif producer_choice == "2":
                    topic = input("Enter topic name: ").strip()
                    key = (
                        input("Enter message key (or press Enter to skip): ").strip()
                        or None
                    )
                    print("Enter JSON message (single line):")
                    json_str = input().strip()
                    send_json(producer, topic, json_str, key)
                elif producer_choice == "3":
                    break
                else:
                    print("Invalid choice. Please select 1, 2, or 3.")

                if input("\nContinue in producer mode? (y/n): ").lower() != "y":
                    break

        finally:
            producer.close()
            print("\nProducer closed.")

    elif choice == "2":
        # Consumer mode
        consumer = KafkaMessageConsumer(
            bootstrap_servers="localhost:9092",
            group_id="test-consumer-group",
            schema_registry_url="http://localhost:8081",
            auto_offset_reset="earliest",
        )

        print("\n=== Consumer Setup ===")
        print("Enter topics to subscribe to (comma-separated):")
        topics = [t.strip() for t in input().split(",")]

        # Subscribe to topics
        consumer.subscribe(topics)

        print(f"\nStarting consumer for topics: {topics}")
        print("Press Ctrl+C to exit\n")

        try:
            consumer.start_consuming()
        except KeyboardInterrupt:
            print("\nStopping consumer...")
        finally:
            consumer.close()
            print("Consumer closed.")

    elif choice == "3":
        print("\nGoodbye!")
    else:
        print("Invalid choice. Please run the program again.")


if __name__ == "__main__":
    main()
