
import argparse
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, StopWordsRemover, Tokenizer


def main():
    parser = argparse.ArgumentParser(description="Train AG News classifier")
    parser.add_argument("--train-data-path", default=os.getenv("TRAIN_DATA_PATH", "/workspace/data/raw/ag_news"))
    parser.add_argument("--model-path", default=os.getenv("MODEL_PATH", "/workspace/data/models/ag_news"))
    args = parser.parse_args()

    # TODO: Create SparkSession
    # TODO: Download/load the AG News training dataset
    # TODO: Build MLlib pipeline:
    # RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression
    # TODO: Fit pipeline on training data
    # TODO: Save trained PipelineModel to model path


if __name__ == "__main__":
    main()
