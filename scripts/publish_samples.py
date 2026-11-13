# Used to publish sample data to Kafka for testing purposes
# Replace once actual producer is implemented
import argparse
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer

DEFAULT_INPUT = Path(__file__).resolve().parent.parent / "data" / "examples" / "topic1.jsonl"


def main():
    parser = argparse.ArgumentParser(description="Publish sample topic1 messages to Kafka")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--input-path", type=Path, default=DEFAULT_INPUT)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    with open(args.input_path, encoding="utf-8") as f:
        for line in f:
            message = json.loads(line)
            producer.send(args.topic, value=message).get(timeout=10)
            print(f"Published: {message['text']}")
            time.sleep(2)

    producer.close()


if __name__ == "__main__":
    main()
