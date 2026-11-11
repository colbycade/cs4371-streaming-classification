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
  6. **Real time classification:** Run second streaming query on topic1 to continuously classify incoming messages and maintain running counts per category

## Team Responsibilities

- There are four main components in `src/` that we can divide between us to work on:

| Component | Directory | Responsibility | Assigned To |
|---|---|---|---|
| Data collection | `producer/` | Collect Hacker News stories/comments, publish text to Kafka `topic1`, and save collected messages locally | |
| Entity counting | `entities/` | Use Spark and spaCy to extract and count named entities, publish cumulative counts to topic2, and configure Logstash, Elasticsearch, and the Kibana dashboard | Colby Wright |
| Model training | `training/` | Load AG News, fit the Spark MLlib classification pipeline, and save the trained model | |
| Classification | `classification/` | Classify collected messages in batch and real time, report predictions and category counts, and evaluate 50 manually labeled messages | |

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
3. Click **Import** and select `results/kibana-dashboard.ndjson`.
4. Click **Import**, then navigate to **Analytics → Dashboards**.
5. Open **Top 10 Named Entities**.

The dashboard may initially be empty until Spark and Logstash populate the Elasticsearch `entity-counts` index.

Follow the instructions in `kibana/README.md` to recreate the dashboard from scratch if needed.