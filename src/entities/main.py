
import argparse
import json
import os
import spacy
from pyspark.sql import SparkSession

SAMPLE_PATH = "data/examples/topic1.jsonl"
KEEP = {"PERSON", "ORG", "GPE", "NORP", "EVENT", "PRODUCT"}

def main():
    parser = argparse.ArgumentParser(description="Streaming entity counts")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--output-topic", default=os.getenv("OUTPUT_TOPIC", "topic2"))
    parser.add_argument("--checkpoint-path", default=os.getenv("ENTITY_CHECKPOINT_PATH", "/workspace/data/checkpoints/entities"))
    args = parser.parse_args()

    nlp = spacy.load("en_core_web_sm")
    with open(SAMPLE_PATH, encoding="utf-8") as samples:
        for line in samples:
            message = json.loads(line)
            doc = nlp(message["text"])
            print(f"\nText: {message['text']}")
            if not doc.ents:
                print("  No entities found.")
            for entity in doc.ents:
                if entity.label_ in KEEP:
                    print(f"  {entity.text!r}: {entity.label_}")
    
    # TODO: Read streaming messages from topic1
    spark = (
        SparkSession.builder
        .appName("EntityCounter")
        .getOrCreate()
    )
    df = spark.read.json(SAMPLE_PATH)
    df.printSchema()
    df.show(truncate=False)

    # TODO: Extract and normalize named entities using spaCy
    # TODO: Calculate cumulative entity counts
    # TODO: Publish counts matching topic2.schema.json
    # TODO: Configure checkpointing and start streaming query


if __name__ == "__main__":
    main()
