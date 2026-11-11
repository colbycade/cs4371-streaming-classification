
import argparse
import os


def main():
    parser = argparse.ArgumentParser(description="Streaming entity counts")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--output-topic", default=os.getenv("OUTPUT_TOPIC", "topic2"))
    parser.add_argument("--checkpoint-path", default=os.getenv("ENTITY_CHECKPOINT_PATH", "/workspace/data/checkpoints/entities"))
    args = parser.parse_args()

    # TODO: Create SparkSession
    # TODO: Read streaming messages from topic1
    # TODO: Extract and normalize named entities using spaCy
    # TODO: Calculate cumulative entity counts
    # TODO: Publish counts matching topic2.schema.json
    # TODO: Configure checkpointing and start streaming query


if __name__ == "__main__":
    main()
