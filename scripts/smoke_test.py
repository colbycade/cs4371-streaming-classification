"""Basic connectivity checks. Run from inside the VS Code dev container."""
import os
import requests
from kafka import KafkaAdminClient
from pyspark.sql import SparkSession

bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
topics = admin.list_topics()
assert {"topic1", "topic2"}.issubset(topics), topics
admin.close()
print("Kafka topics:", sorted(topics))

url = os.environ.get("ELASTICSEARCH_URL", "http://elasticsearch:9200")
response = requests.get(url, timeout=10)
response.raise_for_status()
print("Elasticsearch:", response.json()["version"]["number"])

spark = SparkSession.builder.master("local[2]").appName("gp1-smoke-test").getOrCreate()
try:
    print("Spark:", spark.version)
    print("Spark rows:", spark.range(3).count())
    # Also verify the Kafka connector is resolvable and can read topic metadata.
    df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", bootstrap)
          .option("subscribe", "topic1")
          .option("startingOffsets", "earliest")
          .option("endingOffsets", "latest")
          .load())
    print("Kafka rows in topic1:", df.count())
finally:
    spark.stop()
print("Smoke test passed")
