
import argparse
import os


def main():
    parser = argparse.ArgumentParser(description="Hacker News producer")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--raw-data-path", default=os.getenv("RAW_DATA_PATH", "/workspace/data/raw"))
    parser.add_argument("--poll-interval", type=int, default=30)
    args = parser.parse_args()

    # TODO: Poll Hacker News API for new stories
    # TODO: Deduplicate stories by ID
    # TODO: Publish messages matching topic1.schema.json
    # TODO: Save raw messages to disk
    # TODO: Collect at least 1,000 unique messages for Part 2

if __name__ == "__main__":
    main()
