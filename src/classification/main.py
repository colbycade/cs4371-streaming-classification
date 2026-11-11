
import argparse
import os


def main():
    parser = argparse.ArgumentParser(description="Classify Hacker News text")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--model-path", default=os.getenv("MODEL_PATH", "/workspace/data/models/ag_news"))
    parser.add_argument("--checkpoint-path", default=os.getenv("CLASSIFICATION_CHECKPOINT_PATH", "/workspace/data/checkpoints/classification"))
    parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
    args = parser.parse_args()

    # TODO: Create SparkSession
    # TODO: Load saved PipelineModel

    # maybe separate into different scripts for batch and stream modes, or use if/else here
    if args.mode == "batch":
        # TODO: Read collected messages as batch
        # TODO: Predict classes and probabilities
        # TODO: Report counts and examples for each class
        # TODO: Support manual evaluation of 50 messages
        pass

    elif args.mode == "stream":
        # TODO: Read streaming messages from topic1
        # TODO: Apply saved classifier
        # TODO: Calculate running counts per class
        # TODO: Configure checkpointing and start streaming query
        pass


if __name__ == "__main__":
    main()
