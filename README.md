# CS4371 Team 32 Group Project 1 - Streaming & Text Classification

## Assignment overview
We need to build a system that collects text from the internet in real time, identifies and counts named entities (people, organizations, places, etc.), and classifies the text into topics.

### Part 1: Spark Structured Streaming + Kafka
  1. **Text Source**: Hacker News API
  2. **Python collector**: fetches new posts and forwards texts
  3. **Kafka topic1**: Raw text
  4. **Spark Structured Streaming + spaCy**: Extract + count entities
  5. **Kafka topic2**: Entity + running count
  6. **Logstash**: Read topic2
  7. **Elasticsearch**: Store the counts
  8. **Kibana**: Top 10 bar plot
  
### Part 2: Spark MLlib on the streamed text
  1. **AG News dataset:** Load 120,000 labeled news articles (World, Sports, Business, Sci/Tech) to train model on
  2. **Fit the Pipeline:** Use Spark MLlib pipeline (Tokenize → Remove stop words → TF-IDF → Logistic Regression)
  3. **Save model:** Save the trained PipelineModel for reuse
  4. **Batch classification:** Read messages from Kafka topic1, predict a category for each, count messages per category, and show five examples per category with probabilities
  5. **Check by hand:** Manually label 50 random messages (including "None of these" when applicable), compare against predictions, and report accuracy and examples of misclassifications
  6. **Real time classification:** Second streaming query in the Part 1 application (`src/entities/main.py`) classifies each topic1 message and prints running counts per category to the console

## Team Responsibilities

- There are four main components in `src/` that we can divide between us to work on:

| Component | Directory | Responsibility | Assigned To |
|---|---|---|---|
| Data collection | `producer/` | Collect Hacker News stories/comments, publish text to Kafka `topic1`, and save collected messages locally | Julian |
| Entity counting | `entities/` | Use Spark and spaCy to extract and count named entities, publish cumulative counts to topic2, and configure Logstash, Elasticsearch, and the Kibana dashboard | Colby Wright |
| Model training | `training/` | Load AG News, fit the Spark MLlib classification pipeline, and save the trained model | |
| Classification | `classification/` | Classify collected messages in batch, report predictions and category counts, and evaluate 50 manually labeled messages (the real-time query lives in `entities/main.py`) | |

- We'll work on separate Git branches and merge changes through pull requests. Each person should primarily work within their assigned directory.
- The `producer/` output is shared by both parts of the project, and the classification code depends on the saved model from `training/`.
- Each component has a main.py starter with command-line arguments and TODO comments. Teammates can change the internal structure as needed, but should preserve the agreed Kafka message formats and shared model interface.
- We'll also need to write the report and record an 8 minute demo video
- If you use AI, keep track of your chats and export transcripts as PDFs

## Quick Start

**Prerequisites:** Docker Desktop, VS Code, and the VS Code Dev Containers extension.

1. Clone the repository and open it in VS Code.
2. Make sure Docker Desktop is running.
3. Run **Dev Containers: Reopen in Container** from the Command Palette.
4. The initial build installs Python dependencies and the spaCy English model. Spark's Kafka connector is downloaded when first needed.
5. Inside the Dev Container terminal, run:
   ```bash
   python scripts/smoke_test.py
   ```
6. If the test passes, the development environment is configured correctly.

The following interfaces are available when their services are running:
- **Kibana:** http://localhost:5601
- **Elasticsearch:** http://localhost:9200
- **Spark UI:** http://localhost:4040 (while a Spark application is running)

## Running the Components

Run from `/workspace` inside the dev container. Every path, topic, and Kafka address is a command-line argument whose default comes from an environment variable set in `compose.yaml` (`KAFKA_BOOTSTRAP_SERVERS`, `MODEL_PATH`, `ENTITY_CHECKPOINT_PATH`, …), so no local paths are hardcoded. Pass the argument or set the variable to override; `--help` lists each script's arguments.

- **Producer (Part 1):** `python src/producer/main.py` checks Hacker News `maxitem` every `--poll-interval` seconds (default 30), fetches every new story and comment, strips comment HTML, publishes each to `topic1`, and appends it to `hackernews.jsonl` under `RAW_DATA_PATH`. It starts from the newest item at launch, so restarting never re-sends an ID. Hacker News produces roughly 8 items per minute, so 1,000 messages take about 2 hours.
- **Sample messages (offline testing only):** `python scripts/publish_samples.py` publishes the 15 messages in `data/examples/topic1.jsonl`. Don't run it against the `topic1` you collect real data in; the samples would be counted with it.
- **Streaming application (Part 1 + Part 2 real time):** `python src/entities/main.py` runs two queries on `topic1`: cumulative entity counts to `topic2`, and running counts per predicted category printed to the console. Pass `--skip-classification` to run only the entity query before the model in `MODEL_PATH` has been trained.

## Data Contracts

Kafka messages use JSON. Their schemas are defined in `contracts/`:
- `topic1.schema.json`: Hacker News messages containing `id`, `type`, `text`, and `timestamp`.
- `topic2.schema.json`: Named entities and their cumulative occurrence counts.

The collector must avoid publishing duplicate Hacker News IDs. 
Entity counts in `topic2` are running totals, not changes since the previous batch.

### Saved File Interfaces

We need to agree on consistent file paths so teammates can run each other's components and reproduce the required outputs locally:
- `data/raw/hackernews.jsonl` — Collected Hacker News messages, one JSON object per line, matching `topic1.schema.json`
- `data/models/ag_news/` — Saved Spark MLlib PipelineModel
- `results/predictions.csv` — Batch classification predictions and probabilities
- `results/manual_labels.csv` — 50 manually labeled messages and model predictions

## Classification Model Interface

The training component saves a Spark MLlib `PipelineModel` for the classification component to load.
- Input: Spark DataFrame containing a `text` column
- Output: `prediction` and `probability` columns
- Categories: `0 = World`, `1 = Sports`, `2 = Business`, `3 = Sci/Tech`
- The same trained model is used for batch and streaming classification

## Kibana Dashboard

The dashboard configuration is included in `kibana/kibana-dashboard.ndjson`. It displays the top 10 named entities by cumulative count.

**Import the existing dashboard:**

1. Start the Docker Compose services and open http://localhost:5601.
2. Navigate to **Management → Stack Management → Saved Objects**.
3. Click **Import** and select `kibana/kibana-dashboard.ndjson`.
4. Click **Import**, then navigate to **Analytics → Dashboards**.
5. Open **Top 10 Named Entities**.

The dashboard may initially be empty until Spark and Logstash populate the Elasticsearch `entity-counts` index.

Follow the instructions in `kibana/README.md` to recreate the dashboard from scratch if needed.