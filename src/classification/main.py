import argparse
import csv
import os
import random
from collections import Counter
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, element_at, from_json, length, lit, trim
from pyspark.sql.types import StringType, StructField, StructType

CATEGORIES = {
    0: "World",
    1: "Sports",
    2: "Business",
    3: "Sci/Tech",
}
HAND_LABELS = set(CATEGORIES.values())
NONE_LABEL = "None of these"


def read_messages(spark, source, input_path, bootstrap_servers, input_topic):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
    ])

    if source == "file":
        return spark.read.schema(schema).json(str(input_path)).select("id", "text")

    kafka_messages = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    return (
        kafka_messages
        .select(from_json(col("value").cast("string"), schema).alias("message"))
        .select("message.id", "message.text")
    )


def read_existing_labels(path):
    if not path.exists():
        return {}

    labels = {}
    with path.open("r", encoding="utf-8-sig", newline="") as input_file:
        for row in csv.DictReader(input_file):
            message_id = (row.get("id") or "").strip()
            hand_label = (row.get("hand_label") or "").strip()
            if message_id and hand_label:
                labels[message_id] = hand_label
    return labels


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(header)
        writer.writerows(rows)


def main():
    raw_data_path = os.getenv("RAW_DATA_PATH", "/workspace/data/raw")
    parser = argparse.ArgumentParser(description="Classify collected Hacker News messages")
    parser.add_argument("--source", choices=("file", "kafka"), default="file")
    parser.add_argument(
        "--input-path",
        default=os.path.join(raw_data_path, "hackernews.jsonl"),
        help="JSONL file created by the producer",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument(
        "--model-path",
        default=os.getenv("MODEL_PATH", "/workspace/data/models/ag_news"),
    )
    parser.add_argument(
        "--results-path",
        default=os.getenv("RESULTS_PATH", "/workspace/results"),
        help="Directory for predictions.csv and manual_labels.csv",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=4371,
        help="Fixed random seed for the 50-message hand-check sample",
    )
    args = parser.parse_args()

    results_path = Path(args.results_path)
    predictions_path = results_path / "predictions.csv"
    manual_labels_path = results_path / "manual_labels.csv"
    existing_labels = read_existing_labels(manual_labels_path)

    spark = (
        SparkSession.builder
        .appName("HackerNewsBatchClassification")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        messages = read_messages(
            spark,
            args.source,
            args.input_path,
            args.bootstrap_servers,
            args.input_topic,
        )
        total_messages = messages.count()
        clean_messages = (
            messages
            .filter(col("text").isNotNull() & (length(trim(col("text"))) > 0))
            .dropDuplicates(["text"])
        )
        clean_count = clean_messages.count()
        print(f"Messages read: {total_messages}")
        print(f"Non-empty, unique messages: {clean_count}")
        if clean_count < 1000:
            print("WARNING: The assignment requires at least 1,000 usable messages.")
        if clean_count == 0:
            raise RuntimeError("No usable messages found. Check the input file or Kafka topic.")

        model = PipelineModel.load(args.model_path)
        scored_messages = (
            model.transform(clean_messages)
            .withColumn("predicted_label", col("prediction").cast("int"))
            .withColumn(
                "predicted_class",
                element_at(
                    array(*[lit(CATEGORIES[index]) for index in range(4)]),
                    col("predicted_label") + lit(1),
                ),
            )
            .withColumn("probability_values", vector_to_array(col("probability")))
            .withColumn(
                "predicted_probability",
                element_at(col("probability_values"), col("predicted_label") + lit(1)),
            )
        )
        rows = scored_messages.select(
            "id", "text", "predicted_label", "predicted_class", "predicted_probability"
        ).orderBy(col("id").asc_nulls_last(), col("text")).collect()

        predictions = []
        category_counts = Counter()
        for row in rows:
            predicted_class = row["predicted_class"]
            probability = float(row["predicted_probability"])
            category_counts[predicted_class] += 1
            predictions.append([
                row["id"] or "",
                row["text"],
                predicted_class,
                f"{probability:.8f}",
            ])
        write_csv(
            predictions_path,
            ["id", "text", "predicted_class", "predicted_probability"],
            predictions,
        )

        print("Predicted messages per category:")
        for category in CATEGORIES.values():
            print(f"  {category}: {category_counts[category]}")

        print("Five examples per category (with predicted-class probability):")
        for category in CATEGORIES.values():
            print(f"\n{category}:")
            examples = [row for row in rows if row["predicted_class"] == category][:5]
            if not examples:
                print("  No messages predicted in this category.")
            for row in examples:
                text = " ".join((row["text"] or "").split())
                print(f"  p={float(row['predicted_probability']):.4f} | {text[:350]}")

        sample_size = min(50, len(rows))
        sample = random.Random(args.seed).sample(rows, sample_size)
        manual_rows = []
        valid_hand_labels = []
        mistakes = []
        none_count = 0
        invalid_labels = []

        for row in sample:
            message_id = row["id"] or ""
            hand_label = existing_labels.get(message_id, "")
            predicted_class = row["predicted_class"]
            probability = f"{float(row['predicted_probability']):.8f}"
            manual_rows.append([
                message_id,
                row["text"],
                predicted_class,
                probability,
                hand_label,
            ])
            if hand_label in HAND_LABELS:
                valid_hand_labels.append((hand_label, predicted_class))
                if hand_label != predicted_class:
                    mistakes.append((hand_label, predicted_class, row["text"]))
            elif hand_label == NONE_LABEL:
                none_count += 1
            elif hand_label:
                invalid_labels.append(hand_label)

        write_csv(
            manual_labels_path,
            ["id", "text", "predicted_class", "predicted_probability", "hand_label"],
            manual_rows,
        )
        print(f"\nWrote predictions to {predictions_path}")
        print(f"Wrote {sample_size} hand-check messages to {manual_labels_path}")
        print("Fill hand_label with World, Sports, Business, Sci/Tech, or None of these.")

        if valid_hand_labels or none_count:
            correct = sum(
                hand_label == predicted_class
                for hand_label, predicted_class in valid_hand_labels
            )
            if valid_hand_labels:
                accuracy = correct / len(valid_hand_labels)
                print(
                    "Hand-check accuracy (four AG News categories): "
                    f"{accuracy:.4f} ({correct}/{len(valid_hand_labels)})"
                )
            else:
                print("Hand-check accuracy: not available (no four-class labels yet).")
            print(f"Hand-labeled as None of these: {none_count}")
            if mistakes:
                print("Hand-check examples where the prediction differs:")
                for hand_label, predicted_class, text in mistakes[:5]:
                    print(f"  Hand label: {hand_label} | Prediction: {predicted_class} | {text[:300]}")
        else:
            print("Hand-check results will appear after you fill in hand_label and rerun this script.")
        if invalid_labels:
            print("Some hand_label values were not recognized; check the allowed label names.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
