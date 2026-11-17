
import argparse
import json
import os
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

API_URL = "https://hacker-news.firebaseio.com/v0"


def fetch(session, path):
    response = session.get(f"{API_URL}/{path}.json", timeout=10)
    response.raise_for_status()
    return response.json()


# Comment and Ask HN bodies are HTML; get_text also decodes entities such as &#x27;
def html_to_text(html):
    return " ".join(BeautifulSoup(html, "html.parser").get_text(" ").split())


# Returns the topic1 message for a Hacker News item, or None if the item has no usable text
def to_message(item):
    if not item or item.get("deleted") or item.get("dead") or item.get("type") not in ("story", "comment"):
        return None
    text = " ".join(part for part in (item.get("title", ""), html_to_text(item.get("text", ""))) if part)
    # "[delayed]" is the placeholder for a comment its author delayed; the real text is not available yet
    if not text or text == "[delayed]":
        return None
    return {"id": str(item["id"]), "type": item["type"], "text": text, "timestamp": item["time"]}


def main():
    parser = argparse.ArgumentParser(description="Hacker News producer")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--raw-data-path", default=os.getenv("RAW_DATA_PATH", "/workspace/data/raw"))
    parser.add_argument("--poll-interval", type=int, default=30)
    args = parser.parse_args()

    output_path = Path(args.raw_data_path) / "hackernews.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    saved = sum(1 for _ in open(output_path, encoding="utf-8")) if output_path.exists() else 0

    session = requests.Session()
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers)

    # Item IDs only increase, so walking them in order from the current maximum never
    # publishes the same ID twice, even across restarts
    next_id = fetch(session, "maxitem") + 1
    try:
        with open(output_path, "a", encoding="utf-8") as out:
            while True:
                try:
                    max_id = fetch(session, "maxitem")
                    while next_id <= max_id:
                        message = to_message(fetch(session, f"item/{next_id}"))
                        if message:
                            line = json.dumps(message, ensure_ascii=False)
                            producer.send(args.topic, value=line.encode("utf-8")).get(timeout=10)
                            out.write(line + "\n")
                            out.flush()
                            saved += 1
                        next_id += 1
                except requests.RequestException as error:
                    # Keep next_id so the failed item is retried on the next poll
                    print(f"Hacker News request failed, retrying next poll: {error}")
                print(f"{saved} messages saved to {output_path}, next item {next_id}", flush=True)
                time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()


if __name__ == "__main__":
    main()
