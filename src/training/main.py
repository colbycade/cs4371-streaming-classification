import argparse
import glob
import os
import urllib.request

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (HashingTF, IDF, RegexTokenizer, SQLTransformer,
                                StopWordsRemover)
from pyspark.sql import SparkSession, functions as F

HF_BASE = "https://huggingface.co/datasets/fancyzhx/ag_news/resolve/main/data"
HF_FILES = {
    "train": "train-00000-of-00001.parquet",
    "test": "test-00000-of-00001.parquet",
}
LABELS = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}


def parse_args():
    parser = argparse.ArgumentParser(description="Train AG News classifier")
    parser.add_argument("--train-data-path",
                        default=os.getenv("TRAIN_DATA_PATH", "/workspace/data/raw/ag_news"))
    parser.add_argument("--test-data-path",
                        default=os.getenv("TEST_DATA_PATH", "/workspace/data/raw/ag_news_test"))
    parser.add_argument("--model-path",
                        default=os.getenv("MODEL_PATH", "/workspace/data/models/ag_news"))
    parser.add_argument("--skip-eval", action="store_true",
                        help="Do not evaluate on the AG News test split")
    parser.add_argument("--num-features", type=int, default=1 << 18,
                        help="HashingTF feature dimension (default 2^18)")
    parser.add_argument("--min-doc-freq", type=int, default=2,
                        help="IDF: ignore terms in fewer than this many documents")
    parser.add_argument("--max-iter", type=int, default=20,
                        help="LogisticRegression iterations")
    parser.add_argument("--reg-param", type=float, default=0.01,
                        help="LogisticRegression regularization strength")
    return parser.parse_args()


def ensure_dataset(path, split):
    """Make sure `path` holds the AG News parquet for `split`; download it if not.

    `path` may be a parquet file or a folder of parquet files.
    """
    if os.path.isfile(path):
        return path
    if glob.glob(os.path.join(path, "*.parquet")):
        return path
    os.makedirs(path, exist_ok=True)
    url = f"{HF_BASE}/{HF_FILES[split]}"
    dest = os.path.join(path, HF_FILES[split])
    print(f"No parquet found in {path}; downloading AG News {split} split from {url}")
    urllib.request.urlretrieve(url, dest)
    return path


def load_ag_news(spark, path):
    return (spark.read.parquet(path)
            .select("text", F.col("label").cast("double").alias("label")))


def label_name(col):
    expr = F.when(col == 0, LABELS[0])
    for k in (1, 2, 3):
        expr = expr.when(col == k, LABELS[k])
    return expr


def build_pipeline(args):
    clean = SQLTransformer(statement=r"""
        SELECT *, regexp_replace(
                    regexp_replace(coalesce(text, ''), '<[^>]+>', ' '),
                    '&[a-zA-Z]+;|&?#[0-9]+;', ' ') AS clean_text
        FROM __THIS__
    """)
    # Lowercase, split on anything that is not a letter or digit (drops punctuation).
    tokenizer = RegexTokenizer(inputCol="clean_text", outputCol="words",
                               pattern=r"[^a-z0-9]+", toLowercase=True,
                               minTokenLength=2)
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashing_tf = HashingTF(inputCol="filtered", outputCol="tf",
                           numFeatures=args.num_features)
    idf = IDF(inputCol="tf", outputCol="features", minDocFreq=args.min_doc_freq)
    lr = LogisticRegression(featuresCol="features", labelCol="label",
                            maxIter=args.max_iter, regParam=args.reg_param)
    return Pipeline(stages=[clean, tokenizer, remover, hashing_tf, idf, lr])


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("AGNewsTraining").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Load training data and report class counts
    train = load_ag_news(spark, ensure_dataset(args.train_data_path, "train")).cache()
    print(f"\nAG News training articles: {train.count()}")
    print("Articles per class:")
    (train.groupBy("label").count()
          .withColumn("class", label_name(F.col("label")))
          .orderBy("label").select("label", "class", "count").show())

    # Fit and save
    print("Fitting pipeline...")
    model = build_pipeline(args).fit(train)
    model.write().overwrite().save(args.model_path)
    print(f"Saved PipelineModel to {args.model_path}")

    # Evaluate on the held-out test split
    if not args.skip_eval:
        test = load_ag_news(spark, ensure_dataset(args.test_data_path, "test"))
        preds = model.transform(test).cache()
        print(f"\nAG News test articles: {preds.count()}")
        for metric in ("accuracy", "f1"):
            evaluator = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName=metric)
            print(f"Test {metric}: {evaluator.evaluate(preds):.4f}")
        print("Confusion matrix (rows = true label, columns = predicted):")
        (preds.groupBy("label").pivot("prediction", [0.0, 1.0, 2.0, 3.0]).count()
              .na.fill(0).orderBy("label").show())

    spark.stop()


if __name__ == "__main__":
    main()