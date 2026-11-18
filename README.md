# CS4371 Team 32 – Group Project 1: Spark Streaming with Kafka and Text Classification

Hacker News → Python producer → Kafka `topic1` → Spark Structured Streaming (spaCy entity counts → `topic2` → Logstash → Elasticsearch → Kibana top 10 bar chart; AG News MLlib classifier → running per-class counts in the console).

| Path | Purpose |
|---|---|
| `src/producer/main.py` | Polls the Hacker News API, strips comment HTML, publishes to `topic1`, saves `hackernews.jsonl` |
| `src/entities/main.py` | Streaming app: entity counts → `topic2` (Part 1) and real-time classification (Part 2.3c) |
| `src/training/main.py` | Downloads AG News, fits `RegexTokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression`, saves the `PipelineModel` |
| `src/classification/main.py` | Batch classification and the 50-message hand check (Part 2.3a–b) |
| `logstash/`, `kibana/` | Logstash pipeline and the exported Kibana dashboard |
| `results/` | `predictions.csv`, `manual_labels.csv`, and console output from training and classification |

## Setup

Requires Docker Desktop, VS Code, and the Dev Containers extension. No API keys are needed.

1. Open the repository in VS Code and run **Dev Containers: Reopen in Container**. This starts Kafka, Elasticsearch, Logstash, and Kibana and installs Python dependencies and the spaCy model.
2. Check the environment: `python scripts/smoke_test.py`
3. In Kibana (http://localhost:5601), go to **Stack Management → Saved Objects → Import** and import `kibana/kibana-dashboard.ndjson`. This creates the **Top 10 Named Entities** dashboard.

## Running

Run these commands from `/workspace` inside the container. Paths are not hardcoded: every path, topic, and Kafka address is a command-line argument whose default comes from an environment variable set in `compose.yaml` (`KAFKA_BOOTSTRAP_SERVERS`, `RAW_DATA_PATH`, `TRAIN_DATA_PATH`, `MODEL_PATH`, `ENTITY_CHECKPOINT_PATH`, `CLASSIFICATION_CHECKPOINT_PATH`, `RESULTS_PATH`). Pass `--help` to any script to see its arguments.

```bash
# 1. Train the model (downloads AG News to --train-data-path, saves to --model-path)
python src/training/main.py

# 2. Collect data (keep running; about 8 messages/min, so 1,000 messages takes about 2 h)
python src/producer/main.py

# 3. Streaming app, in a second terminal (add --skip-classification to run entities only)
python src/entities/main.py

# 4. Batch classification + hand check (--input-path <jsonl> or --source kafka)
python src/classification/main.py
```

`src/classification/main.py` writes `predictions.csv` and a 50-message sample, picked with fixed `--seed`, to `manual_labels.csv` under `--results-path`. Fill in the `hand_label` column with World, Sports, Business, Sci/Tech, or None of these, then rerun the script to print the accuracy and the None of these count. The submitted results come from the 1,091 collected messages in `data/examples/demo.jsonl` (`--input-path data/examples/demo.jsonl`).

To start a clean run, run this on the host. Kafka offsets, Spark checkpoints, and Elasticsearch data must be reset together. Afterwards, import the Kibana dashboard again.

```bash
docker compose down -v
rm -rf data/checkpoints/entities data/checkpoints/classification data/raw/hackernews.jsonl
docker compose up -d
```

Message schemas are in `contracts/`. `topic1` messages contain `id`, `type`, `text`, and `timestamp`. `topic2` messages contain each entity and its cumulative count. Model classes: `0 = World`, `1 = Sports`, `2 = Business`, `3 = Sci/Tech`.
