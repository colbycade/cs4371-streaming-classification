
import argparse
import os
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, from_json
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

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

    # Extract entities from the original text, then clean the allowed names
    result = []
    for entity in _nlp(text).ents:
        if entity.label_ in KEEP:
            # Trim whitespace and remove a trailing possessive 's or ’s
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

    spark = (
        SparkSession.builder
        .appName("EntityCounter")
        .getOrCreate()
    )
    schema = StructType([
        StructField("text", StringType()) # we only care about text field from topic1
    ])

    # Read streaming messages from topic1
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")  # read from earliest available if no checkpoint
        .load()
    )

    # Decode message value bytes then parse the JSON to get text
    msgs = (
        raw.select(
            from_json(col("value").cast("string"), schema).alias("message")
        )
        .select("message.text")
        .filter(col("text").isNotNull())
    )

    # Extract named entities from each message into rows, then count occurrences of each across all messages
    entity_counts = (
        msgs.select(explode(ner("text")).alias("entity"))
        .groupBy("entity")
        .count()
    )

    # TODO: Publish counts matching topic2.schema.json
    # For now, write running totals to console each trigger
    query = (
        entity_counts.writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate", "false")
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination() # wait until stream is stopped (Ctrl+C)

if __name__ == "__main__":
    main()
