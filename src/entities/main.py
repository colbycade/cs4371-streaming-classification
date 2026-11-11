
import argparse
import json
import os
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType

SAMPLE_PATH = "data/examples/topic1.jsonl"
KEEP = {"PERSON", "ORG", "GPE", "NORP", "EVENT", "PRODUCT"}

_nlp = None # Global spaCy model instance to avoid reloading for each UDF call

# Returns a list of cleaned, named entities in the input text
def entities(text):
    if not text:
        return []

    # Load spaCy model once per worker
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm")

    # Clean text and extract named entities
    result = []
    for entity in _nlp(text).ents:
        if entity.label_ in KEEP:
            # Drop possesive 's and ’s then strip whitespace
            stripped = entity.text.strip()
            cleaned = stripped.removesuffix("'s").removesuffix("’s").strip()
            # Drop 1-character or digit-only entities
            if len(cleaned) > 1 and not cleaned.isdigit():
                result.append(cleaned)
    return result

ner = udf(entities, ArrayType(StringType()))

def main():
    parser = argparse.ArgumentParser(description="Streaming entity counts")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--output-topic", default=os.getenv("OUTPUT_TOPIC", "topic2"))
    parser.add_argument("--checkpoint-path", default=os.getenv("ENTITY_CHECKPOINT_PATH", "/workspace/data/checkpoints/entities"))
    args = parser.parse_args()

    nlp = spacy.load("en_core_web_sm")
    
    # TODO: Read streaming messages from topic1
    spark = (
        SparkSession.builder
        .appName("EntityCounter")
        .getOrCreate()
    )
    df = spark.read.json(SAMPLE_PATH)

    # Extract and normalize named entities using spaCy
    entity_counts = (
        df.select(explode(ner("text")).alias("entity"))
        .groupBy("entity")
        .count()
    )
    entity_counts.show(truncate=False)

    # TODO: Calculate cumulative entity counts
    # TODO: Publish counts matching topic2.schema.json
    # TODO: Configure checkpointing and start streaming query


if __name__ == "__main__":
    main()
