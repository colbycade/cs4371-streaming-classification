# Used to publish sample data to Kafka for testing purposes
# Replace once actual producer is implemented
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open("data/examples/topic1.jsonl", encoding="utf-8") as f:
    for line in f:
        message = json.loads(line)
        producer.send("topic1", value=message).get(timeout=10)
        print(f"Published: {message['text']}")
        time.sleep(2)

producer.close()