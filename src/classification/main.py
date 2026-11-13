
import argparse
import os


# Batch classification of the collected messages. Real-time classification runs as the
# second streaming query of the Part 1 application (src/entities/main.py).
def main():
    parser = argparse.ArgumentParser(description="Classify collected Hacker News text in batch")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--model-path", default=os.getenv("MODEL_PATH", "/workspace/data/models/ag_news"))
    parser.add_argument("--results-path", default=os.getenv("RESULTS_PATH", "/workspace/results"), help="directory for predictions.csv and manual_labels.csv")
    args = parser.parse_args()

    # TODO: Create SparkSession
    # TODO: Load saved PipelineModel
    # TODO: Read collected messages as batch
    # TODO: Predict classes and probabilities; write predictions.csv to --results-path
    # TODO: Report counts and examples for each class
    # TODO: Support manual evaluation of 50 messages; write manual_labels.csv to --results-path


if __name__ == "__main__":
    main()
