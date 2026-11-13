
import argparse
import os


# Batch classification of the collected messages. Real-time classification runs as the
# second streaming query of the Part 1 application (src/entities/main.py).
def main():
    parser = argparse.ArgumentParser(description="Classify collected Hacker News text in batch")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--model-path", default=os.getenv("MODEL_PATH", "/workspace/data/models/ag_news"))
    args = parser.parse_args()

    # TODO: Create SparkSession
    # TODO: Load saved PipelineModel
    # TODO: Read collected messages as batch
    # TODO: Predict classes and probabilities
    # TODO: Report counts and examples for each class
    # TODO: Support manual evaluation of 50 messages


if __name__ == "__main__":
    main()
