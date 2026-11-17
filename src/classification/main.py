import argparse
import csv
import os
import random
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, element_at, from_json, length, lit, trim, udf
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CATEGORIES = ["World", "Sports", "Business", "Sci/Tech"]
NONE_LABEL = "None of these"


# Select the probability for the category predicted by the model.
def predicted_probability(probabilities, label):
    if probabilities is None or label is None:
        return None
    return float(probabilities[int(label)])


probability_for_prediction = udf(predicted_probability, DoubleType())


def read_messages(spark, source, input_path, bootstrap_servers, input_topic):
    schema = StructType([
        StructField("id", StringType()),
        StructField("text", StringType()),
    ])

    if source == "file":
        return spark.read.schema(schema).json(input_path).select("id", "text")

    messages = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    return (
        messages
        .select(from_json(col("value").cast("string"), schema).alias("message"))
        .select("message.id", "message.text")
    )


def read_hand_labels(path):
    if not path.exists():
        return {}

    labels = {}
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            message_id = (row.get("id") or "").strip()
            hand_label = (row.get("hand_label") or "").strip()
            if message_id and hand_label:
                labels[message_id] = hand_label
    return labels


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
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
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--model-path", default=os.getenv("MODEL_PATH", "/workspace/data/models/ag_news"))
    parser.add_argument("--results-path", default=os.getenv("RESULTS_PATH", "/workspace/results"))
    parser.add_argument("--seed", type=int, default=4371, help="Fixed seed for the 50-message sample")
    args = parser.parse_args()

    results_path = Path(args.results_path)
    predictions_path = results_path / "predictions.csv"
    manual_labels_path = results_path / "manual_labels.csv"
    hand_labels = read_hand_labels(manual_labels_path)

    spark = SparkSession.builder.appName("BatchClassification").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        messages = read_messages(
            spark, args.source, args.input_path, args.bootstrap_servers, args.input_topic
        )
        total = messages.count()
        messages = (
            messages
            .filter(col("text").isNotNull() & (length(trim(col("text"))) > 0))
            .dropDuplicates(["text"])
        )
        remaining = messages.count()
        print(f"Messages read: {total}")
        print(f"Non-empty, unique messages: {remaining}")
        if remaining < 1000:
            print("WARNING: The assignment requires at least 1,000 usable messages.")
        if remaining == 0:
            raise RuntimeError("No usable messages found. Check the input file or Kafka topic.")

        model = PipelineModel.load(args.model_path)
        category = element_at(
            array(*[lit(name) for name in CATEGORIES]),
            col("prediction").cast("int") + lit(1),
        )
        scored = (
            model.transform(messages)
            .withColumn("predicted_class", category)
            .withColumn(
                "predicted_probability",
                probability_for_prediction(col("probability"), col("prediction")),
            )
        )
        rows = scored.select(
            "id", "text", "predicted_class", "predicted_probability"
        ).orderBy(col("id").asc_nulls_last(), col("text")).collect()

        counts = {name: 0 for name in CATEGORIES}
        predictions = []
        for row in rows:
            counts[row["predicted_class"]] += 1
            predictions.append([
                row["id"] or "",
                row["text"],
                row["predicted_class"],
                f"{float(row['predicted_probability']):.8f}",
            ])
        write_csv(
            predictions_path,
            ["id", "text", "predicted_class", "predicted_probability"],
            predictions,
        )

        print("Predicted messages per category:")
        for name in CATEGORIES:
            print(f"  {name}: {counts[name]}")

        print("Five examples per category:")
        for name in CATEGORIES:
            print(f"\n{name}:")
            examples = [row for row in rows if row["predicted_class"] == name][:5]
            for row in examples:
                text = " ".join(row["text"].split())
                print(f"  p={float(row['predicted_probability']):.4f} | {text[:350]}")
            if not examples:
                print("  No messages predicted in this category.")

        sample = random.Random(args.seed).sample(rows, min(50, len(rows)))
        manual_rows = []
        checked = []
        none_count = 0
        for row in sample:
            message_id = row["id"] or ""
            hand_label = hand_labels.get(message_id, "")
            prediction = row["predicted_class"]
            probability = f"{float(row['predicted_probability']):.8f}"
            manual_rows.append([
                message_id, row["text"], prediction, probability, hand_label
            ])
            if hand_label in CATEGORIES:
                checked.append((hand_label, prediction, row["text"]))
            elif hand_label == NONE_LABEL:
                none_count += 1

        write_csv(
            manual_labels_path,
            ["id", "text", "predicted_class", "predicted_probability", "hand_label"],
            manual_rows,
        )
        print(f"\nWrote predictions to {predictions_path}")
        print(f"Wrote {len(sample)} hand-check rows to {manual_labels_path}")
        print("Fill hand_label with World, Sports, Business, Sci/Tech, or None of these.")

        if checked:
            correct = sum(hand_label == prediction for hand_label, prediction, _ in checked)
            accuracy = correct / len(checked)
            print(f"Hand-check accuracy: {accuracy:.4f} ({correct}/{len(checked)})")
        else:
            print("Hand-check accuracy will appear after you add four-class labels and rerun.")
        print(f"Hand-labeled as None of these: {none_count}")
        mistakes = [row for row in checked if row[0] != row[1]]
        if mistakes:
            print("Examples where the hand label and prediction differ:")
            for hand_label, prediction, text in mistakes[:5]:
                print(f"  Hand label: {hand_label} | Prediction: {prediction} | {text[:300]}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
